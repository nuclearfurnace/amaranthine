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
use backend::processor::{Processor, ProcessorError};
use bytes::BytesMut;
use common::Message;
use futures::prelude::*;
use slab::Slab;
use std::collections::VecDeque;

/// Message state of queued messages.
#[derive(Debug, PartialEq)]
pub enum MessageState {
    /// An unfragmented, standalone message.
    ///
    /// A filled variant of this state can be immediately sent off to the client.
    Standalone,

    /// An unfragmented, standalone message that is _also_ immediately available.
    ///
    /// While normal messages have to be processed before a response, these messages are available
    /// to send as soon as they're enqueued.
    Inline,

    /// A fragmented message.
    ///
    /// This represents a discrete fragment of a parent message.  The buffer represents arbitrary
    /// data that is used to identify the parent message.  Given that fragments may not have the
    /// information any longer, we keep track of it in the message state.
    ///
    /// The integers provide the index of the given fragment and the overall count of fragments
    /// within the parent message.
    Fragmented(BytesMut, usize, usize),

    /// A streaming fragmented message.
    ///
    /// This represents a discrete fragment of a parent message.  The key difference is that the
    /// parent message is "streamable."  This is usually the case for get operations, where, as
    /// long as the fragments are in order, they can be streamed back to the client as they're
    /// available.  This is in contrast to some other fragmented messages, where the response must
    /// be generated by the sum of all the parts.
    ///
    /// The optional buffer represents a header that can be sent before the actual fragment.  This
    /// allows sending any response data that is needed to coalesce the fragments into a meaningful
    /// response to the client.
    StreamingFragmented(Option<BytesMut>),
}

/// Message response types for a queued message.
#[derive(Debug)]
pub enum MessageResponse<M> {
    /// The message ultimately "failed".  This happens if a queued message is dropped before having
    /// a response sent for it, which may happen if an error occurs during the backend read, etc.
    Failed,

    /// The message was processored correctly and a response was submitted to the message queue.
    Complete(M),
}

pub type AssignedBatch<T> = Vec<(usize, T)>;
pub type FulfilledBatch<T> = Vec<(usize, MessageResponse<T>)>;

pub struct MessageQueue<P>
where
    P: Processor,
{
    input_closed: bool,
    output_closed: bool,

    // Processor that provides fragmentation capabilities.
    processor: P,

    // Holds all message slots, and stores the slot IDs in order of the messages tied to them.
    slot_order: VecDeque<(usize, MessageState)>,
    slots: Slab<Option<P::Message>>,
}

impl<P> MessageQueue<P>
where
    P: Processor,
    P::Message: Message + Clone,
{
    pub fn new(processor: P) -> MessageQueue<P> {
        MessageQueue {
            input_closed: false,
            output_closed: false,

            processor,

            slot_order: VecDeque::new(),
            slots: Slab::new(),
        }
    }

    fn is_slot_ready(&self, slot: usize) -> bool {
        match self.slot_order.get(slot) {
            None => false,
            Some((slot_id, _)) => {
                match self.slots.get(*slot_id) {
                    Some(Some(_)) => true,
                    _ => false,
                }
            },
        }
    }

    fn get_next_response(&mut self) -> Poll<Option<BytesMut>, ProcessorError> {
        // See if the next slot is even ready yet.  If it's not, then we can't do anything.
        if !self.is_slot_ready(0) {
            return Ok(Async::NotReady);
        }

        // If we have an immediately available response aka a standalone message or streaming
        // fragment, just return it.
        let has_immediate = match self.slot_order.front() {
            None => false,
            Some((slot_id, state)) => {
                match self.slots.get(*slot_id) {
                    Some(_) => {
                        match state {
                            MessageState::Standalone => true,
                            MessageState::Inline => true,
                            MessageState::StreamingFragmented(_) => true,
                            MessageState::Fragmented(_, _, _) => false,
                        }
                    },
                    None => false,
                }
            },
        };

        if has_immediate {
            let (slot_id, state) = self.slot_order.pop_front().unwrap();
            let slot = self.slots.remove(slot_id).unwrap();

            let buf = match state {
                MessageState::Standalone | MessageState::Inline => slot.into_buf(),
                MessageState::StreamingFragmented(header) => {
                    match header {
                        Some(mut header_buf) => {
                            header_buf.unsplit(slot.into_buf());
                            header_buf
                        },
                        None => slot.into_buf(),
                    }
                },
                _ => unreachable!(),
            };

            return Ok(Async::Ready(Some(buf)));
        }

        // Now we know that the next slot has been fulfilled, and that it's a fragmented message.
        // Let's peek at the slot to grab the fragment count, and then we can loop through to see
        // if all the fragments have completed and are ready to be coalesced.
        let fragment_count = match self.slot_order.front() {
            None => unreachable!(),
            Some((_, state)) => {
                match state {
                    MessageState::Fragmented(_, _, count) => *count,
                    _ => unreachable!(),
                }
            },
        };

        for index in 0..fragment_count {
            if !self.is_slot_ready(index) {
                return Ok(Async::NotReady);
            }
        }

        // We have all the slots filled and ready to coalesce.  Pull out the fragments!
        let mut fragments = Vec::new();
        for _ in 0..fragment_count {
            let (slot_id, state) = self.slot_order.pop_front().unwrap();
            let msg = self.slots.remove(slot_id);
            fragments.push((state, msg.unwrap()));
        }

        let msg = self.processor.defragment_messages(fragments)?;
        Ok(Async::Ready(Some(msg.into_buf())))
    }

    pub fn enqueue(&mut self, msgs: Vec<P::Message>) -> Result<AssignedBatch<P::Message>, ProcessorError> {
        let fmsgs = self.processor.fragment_messages(msgs)?;

        let mut qmsgs = Vec::new();
        for (msg_state, msg) in fmsgs {
            if msg_state == MessageState::Inline {
                let slot_id = self.slots.insert(Some(msg));
                self.slot_order.push_back((slot_id, msg_state));
            } else {
                let slot_id = self.slots.insert(None);
                self.slot_order.push_back((slot_id, msg_state));
                qmsgs.push((slot_id, msg));
            }
        }

        Ok(qmsgs)
    }

    pub fn fulfill(&mut self, batch: FulfilledBatch<P::Message>) -> Result<(), ()> {
        for (slot, response) in batch {
            let slot = self.slots.get_mut(slot).ok_or_else(|| ())?;
            match response {
                MessageResponse::Complete(msg) => {
                    slot.replace(msg);
                },
                MessageResponse::Failed => {
                    let err = self.processor.get_error_message_str("failed to receive response");
                    slot.replace(err);
                },
            }
        }
        Ok(())
    }

    pub fn get_sendable_bufs(&mut self) -> Option<Vec<BytesMut>> {
        if !self.is_slot_ready(0) {
            return None
        }

        let mut bufs = Vec::new();
        loop {
            let resp = self.get_next_response();
            match resp {
                Ok(Async::Ready(Some(buf))) => {
                    bufs.push(buf);
                },
                _ => break,
            }
        }

        Some(bufs)
    }
}
