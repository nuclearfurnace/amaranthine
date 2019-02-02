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
use backend::processor::Processor;
use common::{AssignedRequests, EnqueuedRequest, EnqueuedRequests, Message};
use futures::prelude::*;
use tower_service::Service;

#[derive(Clone)]
pub struct FixedRouter<P, S>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send,
    S: Service<EnqueuedRequests<P::Message>> + Clone,
{
    processor: P,
    inner: S,
}

impl<P, S> FixedRouter<P, S>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send,
    S: Service<EnqueuedRequests<P::Message>> + Clone,
{
    pub fn new(processor: P, inner: S) -> FixedRouter<P, S> { FixedRouter { processor, inner } }
}

impl<P, S> Service<AssignedRequests<P::Message>> for FixedRouter<P, S>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send,
    S: Service<EnqueuedRequests<P::Message>> + Clone,
{
    type Error = S::Error;
    type Future = S::Future;
    type Response = S::Response;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> { self.inner.poll_ready() }

    fn call(&mut self, req: AssignedRequests<P::Message>) -> Self::Future {
        let transformed = req.into_iter().map(|(id, msg)| EnqueuedRequest::new(id, msg)).collect();
        self.inner.call(transformed)
    }
}
