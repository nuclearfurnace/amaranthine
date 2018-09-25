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
use backend::{message_queue::QueuedMessage, pool::BackendPool, processor::RequestProcessor};
use common::Message;
use futures::prelude::*;
use protocol::errors::ProtocolError;
use routing::{Router, RouterError};
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct FixedRouter<T>
where
    T: RequestProcessor + Clone + Send + 'static,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    processor: T,
    pool: Arc<BackendPool<T>>,
}

impl<T> FixedRouter<T>
where
    T: RequestProcessor + Clone + Send + 'static,
    T::Message: Message + Send,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    pub fn new(processor: T, pool: Arc<BackendPool<T>>) -> FixedRouter<T> { FixedRouter { processor, pool } }
}

impl<T> Router<T> for FixedRouter<T>
where
    T: RequestProcessor + Clone + Send + 'static,
    T::Message: Message + Send,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    fn route(&self, req: Vec<QueuedMessage<T::Message>>) -> Result<(), RouterError> {
        let mut batches: HashMap<usize, Vec<QueuedMessage<T::Message>>> = HashMap::default();

        // Split all the messages out into backend/associated-keys groupings.
        for msg in req {
            let backend_idx = {
                let msg_key = msg.key();
                self.pool.get_backend_index(msg_key)
            };

            let batch = batches.entry(backend_idx).or_insert_with(Vec::new);
            batch.push(msg);
        }

        // At this point, we've batched up all messages according to which backend they should go.
        // Now we need to actually submit these batches to their respective backends, and we're
        // done! The backends will respond to the client's message queue as results come back.
        for (backend_idx, batch) in batches {
            let backend = self.pool.get_backend_by_index(backend_idx);
            backend.submit(batch);
        }

        Ok(())
    }
}
