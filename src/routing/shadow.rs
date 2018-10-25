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
use routing::{Router, RouterError};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct ShadowRouter<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    processor: P,
    default_pool: Arc<BackendPool<P>>,
    shadow_pool: Arc<BackendPool<P>>,
}

impl<P> ShadowRouter<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    pub fn new(processor: P, default_pool: Arc<BackendPool<P>>, shadow_pool: Arc<BackendPool<P>>) -> ShadowRouter<P> {
        ShadowRouter {
            processor,
            default_pool,
            shadow_pool,
        }
    }
}

impl<P> Router<P> for ShadowRouter<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    fn route(&self, req: Vec<QueuedMessage<P::Message>>) -> Result<(), RouterError> {
        let mut default_batches: HashMap<usize, Vec<QueuedMessage<P::Message>>> = HashMap::default();
        let mut shadow_batches: HashMap<usize, Vec<QueuedMessage<P::Message>>> = HashMap::default();

        // Split all the messages out into backend/associated-keys groupings.
        for msg in req {
            let shadow_msg = msg.as_read();

            let default_bidx = {
                let msg_key = msg.key();
                self.default_pool.get_backend_index(msg_key)
            };

            let shadow_bidx = {
                let msg_key = msg.key();
                self.shadow_pool.get_backend_index(msg_key)
            };

            let default_batch = default_batches.entry(default_bidx).or_insert_with(Vec::new);
            default_batch.push(msg);

            let shadow_batch = shadow_batches.entry(shadow_bidx).or_insert_with(Vec::new);
            shadow_batch.push(shadow_msg);
        }

        // At this point, we've batched up all messages according to which backend they should go.
        // Now we need to actually submit these batches to their respective backends, and we're
        // done! The backends will respond to the client's message queue as results come back.
        for (backend_idx, batch) in default_batches {
            let backend = self.default_pool.get_backend_by_index(backend_idx);
            backend.submit(batch);
        }

        for (backend_idx, batch) in shadow_batches {
            let backend = self.shadow_pool.get_backend_by_index(backend_idx);
            backend.submit(batch);
        }

        Ok(())
    }
}
