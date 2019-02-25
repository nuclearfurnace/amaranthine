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
use futures::{prelude::*, stream::futures_unordered::FuturesUnordered};
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tower_service::Service;

#[derive(Derivative)]
#[derivative(Clone)]
pub struct ShadowRouter<P, S>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
    S: Service<EnqueuedRequests<P::Message>> + Clone,
    S::Future: Future + Send + 'static,
{
    processor: P,
    default_inner: S,
    shadow_inner: S,
    noops: mpsc::UnboundedSender<S::Future>,
}

struct ShadowWorker<S, Request>
where
    S: Service<Request>,
{
    rx: mpsc::UnboundedReceiver<S::Future>,
    should_close: bool,
    inner: FuturesUnordered<S::Future>,
    _service: PhantomData<S>,
}

impl<S, Request> ShadowWorker<S, Request>
where
    S: Service<Request>,
{
    pub fn new(rx: mpsc::UnboundedReceiver<S::Future>) -> ShadowWorker<S, Request> {
        ShadowWorker {
            rx,
            should_close: false,
            inner: FuturesUnordered::new(),
            _service: PhantomData,
        }
    }
}

impl<S, Request> Future for ShadowWorker<S, Request>
where
    S: Service<Request>,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if !self.should_close {
            loop {
                match self.rx.poll() {
                    Ok(Async::Ready(Some(fut))) => self.inner.push(fut),
                    Ok(Async::Ready(None)) => {
                        self.should_close = true;
                        break;
                    },
                    Ok(Async::NotReady) => break,
                    Err(_) => {
                        error!("shadow worker closed unexpectedly");
                        return Ok(Async::Ready(()));
                    },
                }
            }
        }

        // Just drive our inner futures; we don't care about their return value.
        loop {
            match self.inner.poll() {
                // These are successful results, so we just drop the value and keep on moving on.
                Ok(Async::Ready(Some(_))) => {},
                // If we have no more futures to drive, and we've been instructed to close, it's
                // time to go.
                Ok(Async::Ready(None)) => {
                    if self.should_close {
                        return Ok(Async::Ready(()))
                    } else {
                        break
                    }
                },
                Ok(Async::NotReady) => break,
                // We don't really care about errors per se, since it's the shadow pool.
                Err(_) => {},
            }
        }

        Ok(Async::NotReady)
    }
}

impl<P, S> ShadowRouter<P, S>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
    S: Service<EnqueuedRequests<P::Message>> + Clone + Send + 'static,
    S::Future: Future + Send + 'static,
{
    pub fn new(processor: P, default_inner: S, shadow_inner: S) -> ShadowRouter<P, S> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Spin off a task that drives all of the shadow responses.
        let shadow: ShadowWorker<S, EnqueuedRequests<P::Message>> = ShadowWorker::new(rx);
        tokio::spawn(shadow);

        ShadowRouter {
            processor,
            default_inner,
            shadow_inner,
            noops: tx,
        }
    }
}

impl<P, S> Service<AssignedRequests<P::Message>> for ShadowRouter<P, S>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send,
    S: Service<EnqueuedRequests<P::Message>> + Clone,
    S::Future: Future + Send + 'static,
{
    type Error = S::Error;
    type Future = S::Future;
    type Response = S::Response;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> { self.default_inner.poll_ready() }

    fn call(&mut self, req: AssignedRequests<P::Message>) -> Self::Future {
        let shadow_reqs = req
            .clone()
            .into_iter()
            .map(|(_, msg)| EnqueuedRequest::without_response(msg))
            .collect();

        let default_reqs = req.into_iter().map(|(id, msg)| EnqueuedRequest::new(id, msg)).collect();

        let noop = self.shadow_inner.call(shadow_reqs);
        let _ = self.noops.try_send(noop);

        self.default_inner.call(default_reqs)
    }
}
