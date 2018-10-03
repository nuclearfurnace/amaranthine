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
use super::{distributor, hasher};
use backend::{backend::Backend, processor::RequestProcessor};
use errors::CreationError;
use futures::{future::Shared, prelude::*, sync::mpsc};
use futures_turnstyle::Waiter;
use parking_lot::RwLock;
use protocol::errors::ProtocolError;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::net::TcpStream;

pub struct BackendPool<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    distributor: RwLock<Box<distributor::Distributor + Send + Sync>>,
    hasher: Box<hasher::KeyHasher + Send + Sync>,
    backends: Vec<Backend<P>>,
}

pub struct BackendPoolSupervisor<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    pool: Arc<BackendPool<P>>,
    updates_rx: mpsc::UnboundedReceiver<()>,
    close: Shared<Waiter>,
}

impl<P> BackendPool<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    pub fn new(
        addresses: &[SocketAddr], processor: P, mut options: HashMap<String, String>, close: Shared<Waiter>,
    ) -> Result<Arc<BackendPool<P>>, CreationError> {
        let dist_type = options
            .entry("distribution".to_owned())
            .or_insert_with(|| "modulo".to_owned())
            .to_lowercase();
        let mut distributor = distributor::configure_distributor(&dist_type)?;
        debug!("[listener] using distributor '{}'", dist_type);

        let hash_type = options
            .entry("hash".to_owned())
            .or_insert_with(|| "fnv1a_64".to_owned())
            .to_lowercase();
        let hasher = hasher::configure_hasher(&hash_type)?;
        debug!("[listener] using hasher '{}'", hash_type);

        // Assemble the list of backends and backend descriptors.
        let mut backends = vec![];
        let mut descriptors = vec![];
        let (updates_tx, updates_rx) = mpsc::unbounded();

        for address in addresses {
            // Create the backend and the backend supervisor.  We spawn the supervisor which deals
            // with spawning new backend connections when we're under our limit or a connection has
            // an error and closes.  The backend itself is simply a facade to the work queue by
            // which we submit request batches to be processed.
            let (backend, runner) = Backend::new(
                *address,
                processor.clone(),
                options.clone(),
                updates_tx.clone(),
                close.clone(),
            )?;
            let descriptor = backend.get_descriptor();
            backends.push(backend);
            tokio::spawn(runner);

            // The backend descriptor is a facade we pass to the distributor to provide the
            // necessary information needed to configure the distributor without having to hold an
            // actual reference to the backends.
            descriptors.push(descriptor);
        }

        // Seed/update the distributor.
        distributor.seed(descriptors);

        let pool = Arc::new(BackendPool {
            backends,
            distributor: RwLock::new(distributor),
            hasher,
        });

        let supervisor = BackendPoolSupervisor {
            pool: pool.clone(),
            updates_rx,
            close,
        };
        tokio::spawn(supervisor);

        Ok(pool)
    }

    pub fn update_distributor(&self) {
        trace!("forced distributor update");

        // Tally up the healthy backends.
        let mut healthy_backends = vec![];
        for backend in &self.backends {
            if backend.is_healthy() {
                healthy_backends.push(backend.get_descriptor());
            }
        }

        // Create a new distributor via seeding and set it.
        let mut distributor = self.distributor.write();
        distributor.seed(healthy_backends);
    }

    pub fn get_backend_index(&self, key: &[u8]) -> usize {
        let key_id = self.hasher.hash(key);
        let distributor = self.distributor.read();
        distributor.choose(key_id)
    }

    pub fn get_backend_by_index(&self, idx: usize) -> &Backend<P> {
        match self.backends.get(idx) {
            Some(backend) => backend,
            None => unreachable!("incorrect backend idx"),
        }
    }
}

impl<P> Drop for BackendPool<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    fn drop(&mut self) {
        trace!("[backend pool] dropping");
    }
}

impl<P> Future for BackendPoolSupervisor<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // If we're supposed to close, do it now.
        if let Ok(Async::Ready(_)) = self.close.poll() {
            return Ok(Async::Ready(()));
        }

        // Go through any update messages and batch them, setting our update marker to true if we
        // get an actual message.
        let mut should_update = false;
        loop {
            match self.updates_rx.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(Some(_))) => should_update = true,
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Err(_) => return Err(()),
            }
        }

        if should_update {
            self.pool.update_distributor();
        }

        Ok(Async::NotReady)
    }
}

impl<P> Drop for BackendPoolSupervisor<P>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    fn drop(&mut self) {
        trace!("[backend pool supervisor] dropping");
    }
}
