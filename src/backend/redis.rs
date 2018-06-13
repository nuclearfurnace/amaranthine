use backend::distributor::Distributor;
use backend::hasher::KeyHasher;
use backend::pool::BackendPool;
use backend::sync::{RequestTransformer, TcpStreamFuture};
use bytes::BytesMut;
use futures::future::{ok, result};
use futures::prelude::*;
use protocol::redis;
use protocol::redis::RedisMessage;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use tokio::net::TcpStream;

type RedisOrderedMessages = Vec<(u64, RedisMessage)>;

#[derive(Clone)]
pub struct RedisRequestTransformer;

impl RedisRequestTransformer {
    pub fn new() -> RedisRequestTransformer {
        RedisRequestTransformer {}
    }
}

impl RequestTransformer for RedisRequestTransformer {
    type Request = RedisOrderedMessages;
    type Response = RedisOrderedMessages;
    type Executor = Box<Future<Item = (TcpStream, Self::Response), Error = Error> + Send>;

    fn transform(&self, req: Self::Request, stream: TcpStreamFuture) -> Self::Executor
    {
        // Break apart IDs and messages.
        let msg_len = req.len();
        let mut msg_ids = Vec::with_capacity(msg_len);
        let mut msgs = Vec::with_capacity(msg_len);
        for (msg_id, msg) in req {
            msg_ids.push(msg_id);
            msgs.push(msg);
        }

        let inner = stream
            .and_then(move |server| {
                debug!("[redis backend] about to write batched messages to backend");
                redis::write_messages(server, msgs)
            })
            .and_then(move |(server, _n)| {
                debug!("[redis backend] now reading the responses from the backend");
                redis::read_messages(server, msg_len)
            })
            .and_then(move |(server, _n, resps)| {
                debug!("[redis backend] assembling backend responses to send to client");
                let result = msg_ids
                    .into_iter()
                    .zip(resps)
                    .collect::<RedisOrderedMessages>();

                ok((server, result))
            });
        Box::new(inner)
    }
}

pub fn generate_batched_redis_writes(
    pool: &BackendPool<RedisRequestTransformer>,
    mut messages: Vec<RedisMessage>,
) -> Vec<impl Future<Item = RedisOrderedMessages, Error = Error>>
{
    // Group all of our messages by their target backend index.
    let mut assigned_msgs = HashMap::new();

    let mut i = 0;
    while messages.len() > 0 {
        let msg = messages.remove(0);
        let msg_key = get_message_key(&msg);
        let backend_idx = pool.get_backend_index(&msg_key[..]);

        let batched_msgs = assigned_msgs.entry(backend_idx).or_insert(Vec::new());
        batched_msgs.push((i, msg));

        i += 1;
    }

    // Now that we have things mapped, create our queues for each backend.
    let mut responses: Vec<_> = Vec::with_capacity(assigned_msgs.len());

    for (backend_idx, msgs) in assigned_msgs {
        let mut msg_indexes = Vec::with_capacity(msgs.len());
        for (id, _) in &msgs {
            msg_indexes.push(*id);
        }

        let mut backend = pool.get_backend_by_index(backend_idx);
        let msg_indexes2 = msg_indexes.clone();
        let response = backend.submit(msgs)
            .and_then(|r| {
                result(r).or_else(move |err| ok(to_vectored_error_response(err, msg_indexes2)))
            })
            .map_err(|_| Error::new(ErrorKind::Other, "internal synchrotron failure (RRCE)"))
            .or_else(move |err| ok(to_vectored_error_response(err, msg_indexes)));

        responses.push(response);
    }

    responses
}

pub fn to_vectored_error_response(err: Error, indexes: Vec<u64>) -> RedisOrderedMessages {
    let mut msgs = Vec::new();
    let msg = RedisMessage::from_error(err);
    for i in indexes {
        msgs.push((i, msg.clone()));
    }
    msgs
}

fn get_message_key(msg: &RedisMessage) -> BytesMut {
    match msg {
        RedisMessage::Bulk(_, ref args) => {
            let arg_pos = if args.len() < 2 { 0 } else { 1 };

            match args.get(arg_pos) {
                Some(RedisMessage::Data(buf, offset)) => {
                    let mut buf2 = buf.clone();
                    let _ = buf2.split_to(*offset);
                    let key_len = buf2.len() - 2;
                    let _ = buf2.split_off(key_len);
                    buf2
                }
                _ => panic!("command message does not have expected structure"),
            }
        }
        _ => panic!("command message should be multi-bulk!"),
    }
}
