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
use backend::{pool::BackendPool, processor::Processor};
use common::{AssignedRequests, AssignedResponses, EnqueuedRequest, Message, PendingResponse};
use futures::{
    future::{join_all, JoinAll},
    prelude::*,
};
use routing::RouterError;
use service::DirectService;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct ShadowRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    processor: P,
    default_pool: Arc<BackendPool<P>>,
    shadow_pool: Arc<BackendPool<P>>,
}

impl<P> ShadowRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    pub fn new(processor: P, default: Arc<BackendPool<P>>, shadow: Arc<BackendPool<P>>) -> ShadowRouter<P> {
        ShadowRouter {
            processor,
            default_pool: default,
            shadow_pool: shadow,
        }
    }
}

impl<P> DirectService<AssignedRequests<P::Message>> for ShadowRouter<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
{
    type Error = RouterError;
    type Future = ShadowResponseFuture<P>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> { Ok(Async::Ready(())) }

    fn poll_service(&mut self) -> Poll<(), Self::Error> { Ok(Async::Ready(())) }

    fn poll_close(&mut self) -> Poll<(), Self::Error> { Ok(Async::Ready(())) }

    fn call(&mut self, req: AssignedRequests<P::Message>) -> Self::Future {
        let mut futs = Vec::new();
        let mut default_batches = HashMap::new();
        let mut shadow_batches = HashMap::new();

        // Split all the messages out into backend/associated-keys groupings.
        for (id, msg) in req {
            let shadow_msg = msg.clone();

            let default_backend_idx = {
                let msg_key = msg.key();
                self.default_pool.get_backend_index(msg_key)
            };

            let shadow_backend_idx = {
                let msg_key = msg.key();
                self.default_pool.get_backend_index(msg_key)
            };

            let (rx, wrapped) = EnqueuedRequest::new(id, msg);
            let default_batch = default_batches.entry(default_backend_idx).or_insert_with(Vec::new);
            default_batch.push(wrapped);
            futs.push(rx);

            let wrapped = EnqueuedRequest::with_noop(shadow_msg);
            let shadow_batch = shadow_batches.entry(shadow_backend_idx).or_insert_with(Vec::new);
            shadow_batch.push(wrapped);
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

        ShadowResponseFuture::new(futs)
    }
}

pub struct ShadowResponseFuture<P>
where
    P: Processor,
{
    responses: JoinAll<Vec<PendingResponse<P::Message>>>,
}

impl<P> ShadowResponseFuture<P>
where
    P: Processor,
{
    fn new(responses: Vec<PendingResponse<P::Message>>) -> ShadowResponseFuture<P> {
        ShadowResponseFuture {
            responses: join_all(responses),
        }
    }
}

impl<P> Future for ShadowResponseFuture<P>
where
    P: Processor,
{
    type Error = RouterError;
    type Item = AssignedResponses<P::Message>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.responses
            .poll()
            .map_err(|_| RouterError::BadResponse("response receiver dropped!".to_owned()))
    }
}
