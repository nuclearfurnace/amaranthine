// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use backend::{
    message_queue::{MessageState, QueuedMessage},
    processor::{ProcessorError, RequestProcessor, TcpStreamFuture},
};
use bytes::BytesMut;
use common::Message;
use futures::{future::ok, prelude::*};
use itoa;
use protocol::{
    self,
    errors::ProtocolError,
    redis::{self, RedisMessage, RedisMessageStream},
};
use std::{borrow::Borrow, error::Error};
use tokio::{io::ReadHalf, net::TcpStream};

const REDIS_DEL: &[u8] = b"del";
const REDIS_SET: &[u8] = b"set";

#[derive(Clone)]
pub struct RedisRequestProcessor;

impl RedisRequestProcessor {
    pub fn new() -> RedisRequestProcessor { RedisRequestProcessor {} }
}

impl RequestProcessor for RedisRequestProcessor {
    type ClientReader = RedisMessageStream<ReadHalf<TcpStream>>;
    type Future = Box<Future<Item = TcpStream, Error = ProtocolError> + Send>;
    type Message = RedisMessage;

    fn fragment_messages(
        &self, msgs: Vec<Self::Message>,
    ) -> Result<Vec<(MessageState, Self::Message)>, ProcessorError> {
        redis_fragment_messages(msgs)
    }

    fn defragment_messages(&self, msgs: Vec<(MessageState, Self::Message)>) -> Result<Self::Message, ProcessorError> {
        redis_defragment_messages(msgs)
    }

    fn get_error_message(&self, e: Box<Error>) -> Self::Message { RedisMessage::from_error(e) }

    fn get_error_message_str(&self, e: &str) -> Self::Message { RedisMessage::from_error_str(e) }

    fn get_read_stream(&self, client_rx: ReadHalf<TcpStream>) -> Self::ClientReader {
        protocol::redis::read_messages_stream(client_rx)
    }

    fn process(&self, req: Vec<QueuedMessage<Self::Message>>, stream: TcpStreamFuture) -> Self::Future {
        let inner = stream
            .map_err(|e| e.into())
            .and_then(move |server| redis::write_messages(server, req))
            .and_then(move |(server, msgs, _n)| redis::read_messages(server, msgs))
            .and_then(move |(server, _n)| ok(server));
        Box::new(inner)
    }
}

fn redis_fragment_messages(msgs: Vec<RedisMessage>) -> Result<Vec<(MessageState, RedisMessage)>, ProcessorError> {
    let mut fragments = Vec::new();

    for msg in msgs {
        if !redis_is_multi_message(&msg) {
            // This message isn't fragmentable, so it passes through untouched.
            let state = if msg.is_inline() {
                MessageState::Inline
            } else {
                MessageState::Standalone
            };
            fragments.push((state, msg));
        } else {
            match msg {
                RedisMessage::Bulk(_, mut args) => {
                    // Split off the actual command string and figure out what the new command string
                    // will be for our fragments.
                    let cmd = args.remove(0);
                    let cmd_buf = redis_get_data_buffer(&cmd);
                    let new_cmd_buf = match cmd_buf {
                        Some(buf) => {
                            match buf {
                                b"mget" => b"get",
                                b"del" => b"del",
                                b"mset" => b"set",
                                x => {
                                    return Err(ProcessorError::FragmentError(format!(
                                        "tried to fragment command '{:?}' but command is not fragmentable!",
                                        x
                                    )))
                                },
                            }
                        },
                        None => {
                            return Err(ProcessorError::FragmentError(
                                "tried to fragment bulk message with non-data argument in position 0!".to_owned(),
                            ))
                        },
                    };

                    // Now we'll do the actual splitting.  We take the new command string (get for
                    // mget, set for mset, and del for del) and build a buffer for it.  We extract
                    // N arguments at a time from our original message, where N is either 1 or 2
                    // depending on if this is a set operation.  With each N arguments, we build a
                    // new message using the new command string and the arguments we extract.
                    let cmd_arg = redis_new_data_buffer(&new_cmd_buf[..]);
                    let mut cmd_type = BytesMut::with_capacity(new_cmd_buf.len());
                    cmd_type.extend_from_slice(&new_cmd_buf[..]);

                    let arg_take_cnt = if new_cmd_buf == b"set" { 2 } else { 1 };
                    let total_fragments = args.len();

                    // Make sure we won't be left with extra arguments.
                    if total_fragments % arg_take_cnt != 0 {
                        return Err(ProcessorError::FragmentError(format!(
                            "incorrect multiple of argument count! (multiple: {}, arg count: {}, cmd type: {:?})",
                            arg_take_cnt,
                            args.len(),
                            &cmd_type
                        )));
                    }

                    // For get requests, we can stream back the fragments so long as they're in
                    // order.  We also need to make sure we provide the proper header (aka the data
                    // that tells the client the response is going to be multiple items) to the
                    // first fragment so that we generate valid output.
                    let is_streaming = new_cmd_buf == b"get";
                    let mut streaming_hdr = if is_streaming {
                        Some(redis_new_bulk_buffer(total_fragments))
                    } else {
                        None
                    };

                    let mut fragment_count = 0;
                    while !args.is_empty() {
                        // This is contorted but we split off the first N arguments, which leaves `args`
                        // with those N and `new_args` with the rest.  We feed those to a function which
                        // builds us our new message, and then finally we replace `args` with `new_args`
                        // so that we can continue on.
                        let new_args = args.split_off(arg_take_cnt);
                        args.insert(0, cmd_arg.clone());
                        let new_bulk = redis_new_bulk_from_args(args);

                        let state = if is_streaming {
                            MessageState::StreamingFragmented(streaming_hdr.take())
                        } else {
                            // Normal fragments need to know the command they're being used for so
                            // we can properly form a command-specific response when we ultimate
                            // defragment these messages later on.
                            MessageState::Fragmented(cmd_type.clone(), fragment_count, total_fragments)
                        };

                        fragments.push((state, new_bulk));
                        fragment_count += 1;
                        args = new_args;
                    }
                },
                _ => unreachable!(),
            }
        }
    }

    Ok(fragments)
}

fn redis_defragment_messages(fragments: Vec<(MessageState, RedisMessage)>) -> Result<RedisMessage, ProcessorError> {
    // This shouldn't happen but it's a simple invariant that lets me write slightly cleaner code.
    if fragments.is_empty() {
        return Ok(RedisMessage::Null);
    }

    // Peek at the metadata buffer on the first message.  If it's not a fragmented message, then
    // something isn't rightand we need to bomb out.
    let first = fragments.first().unwrap();
    let cmd_type = match first {
        (MessageState::Fragmented(buf, _, _), _) => buf.clone(),
        _ => {
            return Err(ProcessorError::DefragmentError(
                "tried to defragment messages, but got non-fragmented message in list".to_owned(),
            ))
        },
    };

    // We have the command type, so let's actually defragment now.
    match cmd_type.borrow() {
        // DEL returns the number of keys it deleted, so we have to tally up the integer responses.
        REDIS_DEL => {
            let mut keys_deleted = 0;
            for (_state, fragment) in fragments {
                match fragment {
                    RedisMessage::Integer(_, value) => keys_deleted += value,
                    RedisMessage::Error(_, _) => return Ok(fragment),
                    _ => {
                        return Err(ProcessorError::DefragmentError(
                            "non-integer response for DEL!".to_owned(),
                        ))
                    },
                }
            }

            Ok(RedisMessage::from_integer(keys_deleted))
        },
        REDIS_SET => {
            // MSET is funny because it says it can't fail, but really, the command has no failure
            // mode _except_ for, like, you know, the server running out of memory.  However, MSET
            // also promises to be atomic.
            //
            // So, we have to lie a bit here.  If we get back an error, other things could have
            // completed, but we'll send back the first error we iterate over so we can at least
            // inform the caller that _something_ bad happened.  If we see no errors, we assume
            // everything went well, and send back the "normal" OK message.
            for (_state, fragment) in fragments {
                if let RedisMessage::Error(_, _) = fragment {
                    return Ok(fragment);
                }
            }

            Ok(RedisMessage::OK)
        },
        x => {
            Err(ProcessorError::DefragmentError(format!(
                "unknown command type '{:?}'",
                x
            )))
        },
    }
}

fn redis_get_data_buffer(msg: &RedisMessage) -> Option<&[u8]> {
    match msg {
        RedisMessage::Data(buf, offset) => Some(redis_clean_data(buf, *offset)),
        _ => None,
    }
}

fn redis_is_multi_message(msg: &RedisMessage) -> bool {
    match msg {
        RedisMessage::Bulk(_, args) => {
            match args.len() {
                0 => false,
                _ => {
                    let arg = &args[0];
                    match redis_get_data_buffer(arg) {
                        Some(buf) => {
                            match buf {
                                b"mget" | b"mset" | b"del" => true,
                                _ => false,
                            }
                        },
                        None => false,
                    }
                },
            }
        },
        _ => false,
    }
}

fn redis_clean_data(buf: &BytesMut, offset: usize) -> &[u8] {
    assert!(buf.len() > 2);
    let val_len = buf.len() - 2;
    &buf[offset..val_len]
}

fn redis_new_data_buffer(buf: &[u8]) -> RedisMessage {
    let mut new_buf = BytesMut::new();
    new_buf.extend_from_slice(b"$");

    let mut cnt_buf = [b'\0'; 20];
    let n = itoa::write(&mut cnt_buf[..], buf.len()).unwrap();
    new_buf.extend_from_slice(&cnt_buf[..n]);
    new_buf.extend_from_slice(b"\r\n");
    new_buf.extend_from_slice(buf);
    new_buf.extend_from_slice(b"\r\n");

    RedisMessage::Data(new_buf, 1 + n + 2)
}

fn redis_new_bulk_buffer(arg_count: usize) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"*");

    let mut cnt_buf = [b'\0'; 20];
    let n = itoa::write(&mut cnt_buf[..], arg_count).unwrap();
    buf.extend_from_slice(&cnt_buf[..n]);
    buf.extend_from_slice(b"\r\n");
    buf
}

fn redis_new_bulk_from_args(args: Vec<RedisMessage>) -> RedisMessage {
    let mut buf = redis_new_bulk_buffer(args.len());
    let mut new_args = Vec::new();
    for arg in args {
        let arg_buf = arg.get_buf();
        buf.unsplit(arg_buf);

        new_args.push(arg);
    }

    RedisMessage::Bulk(buf, new_args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    const STATUS_BUF: &str = "StAtUs_BuF";
    const DATA_BUF: &[u8; 8] = b"DaTa_BuF";
    const DATA_BUF_2: &[u8; 10] = b"DaTa_BuF_2";
    const DATA_BUF_3: &[u8; 4] = b"mget";

    lazy_static! {
        static ref NULL_MSG: RedisMessage = RedisMessage::Null;
        static ref OK_MSG: RedisMessage = RedisMessage::OK;
        static ref STATUS_MSG: RedisMessage = RedisMessage::from_status(&STATUS_BUF[..]);
        static ref ERR_MSG: RedisMessage =
            RedisMessage::from_error(Box::new(Error::new(ErrorKind::Other, "fake error message")));
        static ref INT_MSG: RedisMessage = RedisMessage::from_integer(-42);
        static ref DATA_MSG: RedisMessage = redis_new_data_buffer(&DATA_BUF[..]);
        static ref DATA_MSG_2: RedisMessage = redis_new_data_buffer(&DATA_BUF_2[..]);
        static ref DATA_MSG_3: RedisMessage = redis_new_data_buffer(&DATA_BUF_3[..]);
        static ref BULK_MSG: RedisMessage =
            redis_new_bulk_from_args(vec![DATA_MSG.clone(), DATA_MSG_2.clone(), DATA_MSG_3.clone()]);
        static ref BULK_MULTI_MSG: RedisMessage =
            redis_new_bulk_from_args(vec![DATA_MSG_3.clone(), DATA_MSG_2.clone(), DATA_MSG.clone()]);
    }

    #[test]
    fn test_is_multi_message() {
        assert!(!redis_is_multi_message(&NULL_MSG));
        assert!(!redis_is_multi_message(&OK_MSG));
        assert!(!redis_is_multi_message(&STATUS_MSG));
        assert!(!redis_is_multi_message(&ERR_MSG));
        assert!(!redis_is_multi_message(&INT_MSG));
        assert!(!redis_is_multi_message(&DATA_MSG));
        assert!(!redis_is_multi_message(&BULK_MSG));
        assert!(redis_is_multi_message(&BULK_MULTI_MSG));
    }

    #[test]
    fn test_get_data_buffer() {
        let nm_buf = redis_get_data_buffer(&NULL_MSG);
        let om_buf = redis_get_data_buffer(&OK_MSG);
        let sm_buf = redis_get_data_buffer(&STATUS_MSG);
        let em_buf = redis_get_data_buffer(&ERR_MSG);
        let im_buf = redis_get_data_buffer(&INT_MSG);
        let dm_buf = redis_get_data_buffer(&DATA_MSG);
        let bm_buf = redis_get_data_buffer(&BULK_MSG);

        assert!(nm_buf.is_none());
        assert!(om_buf.is_none());
        assert!(sm_buf.is_none());
        assert!(em_buf.is_none());
        assert!(im_buf.is_none());
        assert!(dm_buf.is_some());
        assert_eq!(dm_buf, Some(&DATA_BUF[..]));
        assert!(bm_buf.is_none());
    }
}
