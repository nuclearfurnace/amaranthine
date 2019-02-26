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
use super::{
    distributor::{configure_distributor, Distributor},
    hasher::{configure_hasher, KeyHasher},
};
use backend::{processor::Processor, Backend, BackendError, PoolError, ResponseFuture};
use common::{AssignedResponses, EnqueuedRequests, Message};
use conf::PoolConfiguration;
use errors::CreationError;
use futures::{
    future::{join_all, JoinAll},
    prelude::*,
};
use hotmic::Sink as MetricSink;
use std::{collections::HashMap, marker::PhantomData};
use tower_direct_service::DirectService;
use util::IntegerMappedVec;

type DistributorFutureSafe = Box<Distributor + Send + 'static>;
type KeyHasherFutureSafe = Box<KeyHasher + Send + 'static>;

pub struct BackendPool<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    distributor: DistributorFutureSafe,
    key_hasher: KeyHasherFutureSafe,
    backends: Vec<Backend<P>>,
    noreply: bool,
    epoch: u64,
    sink: MetricSink<&'static str>,
}

impl<P> BackendPool<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    pub fn new(
        backends: Vec<Backend<P>>, distributor: DistributorFutureSafe, key_hasher: KeyHasherFutureSafe, noreply: bool,
        sink: MetricSink<&'static str>,
    ) -> BackendPool<P> {
        let mut pool = BackendPool {
            distributor,
            key_hasher,
            backends,
            noreply,
            epoch: 0,
            sink,
        };
        pool.regenerate_distribution();
        pool
    }

    pub fn regenerate_distribution(&mut self) {
        let descriptors = self
            .backends
            .iter_mut()
            .enumerate()
            .map(|(idx, backend)| {
                let mut descriptor = backend.get_descriptor();
                descriptor.idx = idx;
                descriptor
            })
            .filter(|backend| backend.healthy)
            .collect();
        self.distributor.update(descriptors);
    }
}

impl<P> DirectService<EnqueuedRequests<P::Message>> for BackendPool<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    type Error = PoolError;
    type Future = PoolResponse<P>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        // Not every backend will be ready all the time, especially if they're knocked out of the
        // pool temporarily, but as long as one is ready, then we're ready.  If any of them are in
        // a bad enough state to throw an error, though, then something is very wrong and we need
        // to bubble that up.
        let mut any_ready = false;
        let mut epoch = 0;
        for backend in &mut self.backends {
            match backend.poll_ready() {
                Ok(Async::Ready(_)) => any_ready = true,
                Ok(Async::NotReady) => {},
                Err(e) => return Err(PoolError::Backend(e)),
            }

            epoch += backend.health().epoch();
        }

        if !any_ready {
            return Ok(Async::NotReady);
        }

        if self.epoch != epoch {
            debug!("regenerating distribution");
            self.regenerate_distribution();
            self.epoch = epoch;
        }

        Ok(Async::Ready(()))
    }

    fn poll_service(&mut self) -> Poll<(), Self::Error> {
        for backend in &mut self.backends {
            // not clear if it actually makes sense to pre-emptively return notready without
            // driving all services.. poll_ready should cover the "am i knocked out of the pool
            // temporarily?" case but would this ever actually return notready when driving the
            // underlying service? unclear
            try_ready!(backend.poll_service());
        }

        Ok(Async::Ready(()))
    }

    fn poll_close(&mut self) -> Poll<(), Self::Error> {
        for backend in &mut self.backends {
            try_ready!(backend.poll_close());
        }

        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: EnqueuedRequests<P::Message>) -> Self::Future {
        let mut futs = Vec::new();
        let mut batches = IntegerMappedVec::new();

        for msg in req {
            let msg_key = msg.key();
            let msg_hashed = self.key_hasher.hash(msg_key);
            let backend_idx = self.distributor.choose(msg_hashed);

            batches.push(backend_idx, msg);
        }

        // make the batch calls to each relevant backend, and collect them
        for (backend_idx, batch) in batches {
            let fut = self.backends[backend_idx].call(batch);
            futs.push(fut);
        }

        PoolResponse::new(futs)
    }
}

pub struct BackendPoolBuilder<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    processor: P,
    config: PoolConfiguration,
    noreply: bool,
    sink: MetricSink<&'static str>,
}

impl<P> BackendPoolBuilder<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Send + 'static,
{
    pub fn new(
        name: String, processor: P, config: PoolConfiguration, sink: MetricSink<&'static str>,
    ) -> BackendPoolBuilder<P> {
        let sink = sink.scoped(&["pools", &name]);

        BackendPoolBuilder {
            processor,
            config,
            noreply: false,
            sink,
        }
    }

    pub fn set_noreply(mut self, noreply: bool) -> Self {
        self.noreply = noreply;
        self
    }

    pub fn build(self) -> Result<BackendPool<P>, CreationError>
    where
        P: Processor + Clone + Send + 'static,
        P::Message: Message + Send + 'static,
    {
        let mut options = self.config.options.unwrap_or_else(HashMap::new);
        let dist_type = options
            .entry("distribution".to_owned())
            .or_insert_with(|| "modulo".to_owned())
            .to_lowercase();
        let distributor = configure_distributor(&dist_type)?;
        debug!("[listener] using distributor '{}'", dist_type);

        let hash_type = options
            .entry("hash".to_owned())
            .or_insert_with(|| "fnv1a_64".to_owned())
            .to_lowercase();
        let hasher = configure_hasher(&hash_type)?;
        debug!("[listener] using hasher '{}'", hash_type);

        // Build all of our backends for this pool.
        let mut backends = Vec::new();
        for address in &self.config.addresses {
            let backend = Backend::new(
                address.address,
                address.identifier.clone(),
                self.processor.clone(),
                options.clone(),
                self.noreply,
                self.sink.clone(),
            )?;
            backends.push(backend);
        }

        Ok(BackendPool::new(backends, distributor, hasher, self.noreply, self.sink))
    }
}

pub struct PoolResponse<P>
where
    P: Processor + Send + 'static,
    P::Message: Message + Send + 'static,
{
    responses: JoinAll<Vec<ResponseFuture<P, BackendError>>>,
    _processor: PhantomData<P>,
}

impl<P> PoolResponse<P>
where
    P: Processor + Send + 'static,
    P::Message: Message + Send + 'static,
{
    pub fn new(responses: Vec<ResponseFuture<P, BackendError>>) -> PoolResponse<P> {
        PoolResponse {
            responses: join_all(responses),
            _processor: PhantomData,
        }
    }
}

impl<P> Future for PoolResponse<P>
where
    P: Processor + Send + 'static,
    P::Message: Message + Send + 'static,
{
    type Error = PoolError;
    type Item = AssignedResponses<P::Message>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = try_ready!(self.responses.poll());
        let flattened = result.into_iter().flatten().collect::<Vec<_>>();
        Ok(Async::Ready(flattened))
    }
}
