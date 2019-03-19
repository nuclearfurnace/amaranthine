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
pub mod distributor;
mod errors;
pub mod hasher;
mod health;
pub mod message_queue;
pub mod pool;
pub mod processor;
pub mod redis;

pub use self::errors::{BackendError, PoolError};

use crate::backend::{distributor::BackendDescriptor, health::BackendHealth, processor::Processor};
use crate::common::{AssignedResponses, EnqueuedRequests, Message, PendingResponses};
use crate::errors::CreationError;
use futures::{
    future::{join_all, ok, Either, JoinAll},
    prelude::*,
    Poll,
};
use hotmic::Sink as MetricSink;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    net::SocketAddr,
    str::FromStr,
    time::Duration,
};
use tokio::{
    net::tcp::TcpStream,
    sync::oneshot,
    timer::{timeout::Error as TimeoutError, Timeout},
};
use tower_direct_service::DirectService;
use crate::util::ProcessFuture;

type MaybeTimeout<F> = Either<NotTimeout<F>, Timeout<F>>;

pub struct NotTimeout<F>
where
    F: Future,
{
    inner: F,
}

impl<F> Future for NotTimeout<F>
where
    F: Future,
{
    type Error = TimeoutError<F::Error>;
    type Item = F::Item;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> { self.inner.poll().map_err(TimeoutError::inner) }
}

/// A backend connection.
///
/// This represents a one-to-one mapping with a TCP connection to the given backend server.  This
/// connection will independently poll the work queue for the backend and run requests when
/// available.
///
/// If a backend connection encounters an error, it will terminate and notify its backend
/// supervisor, so that it can be replaced.
pub struct BackendConnection<P>
where
    P: Processor + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
{
    processor: P,
    address: SocketAddr,
    timeout_ms: u64,
    noreply: bool,

    stream: Option<TcpStream>,
    current: Option<MaybeTimeout<ProcessFuture>>,
    pending: VecDeque<EnqueuedRequests<P::Message>>,
    pending_len: usize,

    sink: MetricSink<&'static str>,
}

impl<P> BackendConnection<P>
where
    P: Processor + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
{
    pub fn new(
        address: SocketAddr, processor: P, timeout_ms: u64, noreply: bool, sink: MetricSink<&'static str>,
    ) -> BackendConnection<P> {
        BackendConnection {
            processor,
            address,
            timeout_ms,
            noreply,
            stream: None,
            current: None,
            pending: VecDeque::new(),
            pending_len: 0,
            sink,
        }
    }

    pub fn enqueue(&mut self, batch: EnqueuedRequests<P::Message>) {
        self.pending_len += batch.len();
        self.pending.push_back(batch);
    }
}

impl<P> DirectService<EnqueuedRequests<P::Message>> for BackendConnection<P>
where
    P: Processor + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
{
    type Error = BackendError;
    type Future = ResponseFuture<P, Self::Error>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> { Ok(Async::Ready(())) }

    fn poll_service(&mut self) -> Poll<(), Self::Error> {
        loop {
            // First, check if we have an operation running.  If we do, poll it to drive it towards
            // completion.  If it's done, we'll reclaim the socket and then fallthrough to trying to
            // find another piece of work to run.
            if let Some(task) = self.current.as_mut() {
                match task.poll() {
                    Ok(Async::Ready(stream)) => {
                        // The operation finished, and gave us the connection back.
                        self.stream = Some(stream);
                        self.current = None;
                    },
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => {
                        // If we caught any sort of error, it means this batch has implicitly
                        // failed.  Some callers may have gotten the data they requested, depending
                        // on what step in the process failed.  All requests are protected by a
                        // drop guard that fulfills the response channel if it hasn't been
                        // fulfilled yet, so that we can at least hand back an error saying that
                        // something broke internally.
                        self.current = None;

                        // If this is specifically an inner error, and not a timeout, then the
                        // connection to the backend is also likely compromised, so we'll drop that
                        // as well, giving us a new connection when we go to process our next
                        // batch.
                        if e.is_inner() {
                            self.stream = None;
                            return Err(e.into_inner().unwrap().into());
                        }
                    },
                }
            }

            // If we're here, we have no current operation to drive, so see if anything is in our work
            // queue that we can grab.
            let mut batch: Option<EnqueuedRequests<P::Message>> = None;
            loop {
                if let Some(batch2) = batch.as_ref() {
                    if batch2.len() > 256 {
                        break;
                    }
                }

                match self.pending.pop_front() {
                    Some(batch2) => {
                        if let Some(batch3) = batch.as_mut() {
                            batch3.extend(batch2);
                        } else {
                            batch = Some(batch2);
                        }
                    },
                    None => break,
                }
            }

            match batch {
                Some(batch) => {
                    self.pending_len -= batch.len();

                    // Get our stream, which we either already have or we'll just get a future for.
                    let stream = match self.stream.take() {
                        Some(stream) => Either::A(ok(stream)),
                        None => {
                            self.sink.increment("connects");
                            Either::B(self.processor.preconnect(&self.address, self.noreply))
                        },
                    };

                    // Get the response future from the processor.
                    let inner = self.processor.process(batch, stream);

                    // Wrap it up to handle any configured timeouts.
                    let work = if self.timeout_ms == 0 {
                        Either::A(NotTimeout { inner })
                    } else {
                        Either::B(Timeout::new(inner, Duration::from_millis(self.timeout_ms)))
                    };

                    self.current = Some(work);
                },
                None => return Ok(Async::Ready(())),
            }
        }
    }

    fn poll_close(&mut self) -> Poll<(), Self::Error> {
        if self.current.is_some() || !self.pending.is_empty() {
            return Ok(Async::NotReady);
        }

        Ok(Async::Ready(()))
    }

    fn call(&mut self, mut req: EnqueuedRequests<P::Message>) -> Self::Future {
        // This is weird, but imagine: our requests are actually (request, response channel).
        //
        // We send this around via `EnqueuedRequest` because we need to be able to send back a
        // future from the leaf service in this stack -- us, `BackendConnection` -- when we get
        // called.
        //
        // Since some requests don't actually have or require a responseo to the client, though,
        // the response channel isn't always there.  Thus, we check each request and extract its
        // response channel if it has one.  The requests are then queued up for processing, and the
        // response channels wrapped in a future that can be shipped back to the caller.
        let response = req
            .as_mut_slice()
            .iter_mut()
            .map(|x| x.get_response_rx())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<_>>();

        self.enqueue(req);
        ResponseFuture::new(response)
    }
}

/// Managed connections to a backend server.
///
/// This backend is serviced by a Tokio task, which processes all work requests to backend servers,
/// and the connections that constitute this backend server.
///
/// Backends are, in essence, proxy objects to their respective Tokio task, which is doing the
/// actual heavy lifting.  They exist purely as a facade to the underlying channels which shuttle
/// work back and forth between the backend connections and client connections.
///
/// Backends maintain a given number of connections to their underlying service, and track error
/// states, recycling connections and pausing work when required.
pub struct Backend<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
{
    identifier: String,
    health: BackendHealth,
    conns: Vec<BackendConnection<P>>,
    conns_index: usize,
    sink: MetricSink<&'static str>,
}

impl<P> Backend<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
{
    pub fn new(
        address: SocketAddr, identifier: String, processor: P, mut options: HashMap<String, String>, noreply: bool,
        sink: MetricSink<&'static str>,
    ) -> Result<Backend<P>, CreationError>
    where
        P: Processor + Clone + Send + 'static,
        P::Message: Message + Send + 'static,
    {
        let sink = sink.scoped("backend");

        let conn_limit_raw = options.entry("conns".to_owned()).or_insert_with(|| "1".to_owned());
        let conn_limit = usize::from_str(conn_limit_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.conns".to_string()))?;
        debug!("[listener] using connection limit of '{}'", conn_limit);

        let cooloff_enabled_raw = options
            .entry("cooloff_enabled".to_owned())
            .or_insert_with(|| "true".to_owned());
        let cooloff_enabled = bool::from_str(cooloff_enabled_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.cooloff_enabled".to_string()))?;

        let cooloff_timeout_ms_raw = options
            .entry("cooloff_timeout_ms".to_owned())
            .or_insert_with(|| "10000".to_owned());
        let cooloff_timeout_ms = u64::from_str(cooloff_timeout_ms_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.cooloff_timeout_ms".to_string()))?;

        let cooloff_error_limit_raw = options
            .entry("cooloff_error_limit".to_owned())
            .or_insert_with(|| "5".to_owned());
        let cooloff_error_limit = usize::from_str(cooloff_error_limit_raw.as_str())
            .map_err(|_| CreationError::InvalidParameter("options.cooloff_error_limit".to_string()))?;

        let health = BackendHealth::new(cooloff_enabled, cooloff_timeout_ms, cooloff_error_limit);

        // TODO: where the hell did the actual backend timeout value go? can't hard-code this
        let conns = (0..conn_limit)
            .map(|_| BackendConnection::new(address, processor.clone(), 500, noreply, sink.clone()))
            .collect();

        Ok(Backend {
            identifier,
            health,
            conns,
            conns_index: 0,
            sink,
        })
    }

    pub fn health(&self) -> &BackendHealth { &self.health }

    pub fn get_descriptor(&mut self) -> BackendDescriptor {
        BackendDescriptor {
            idx: 0,
            identifier: self.identifier.clone(),
            healthy: self.health.is_healthy(),
        }
    }
}

impl<P> DirectService<EnqueuedRequests<P::Message>> for Backend<P>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
{
    type Error = BackendError;
    type Future = ResponseFuture<P, Self::Error>;
    type Response = AssignedResponses<P::Message>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        if self.health.is_healthy() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn poll_service(&mut self) -> Poll<(), Self::Error> {
        for conn in &mut self.conns {
            if conn.poll_service().is_err() {
                self.health.increment_error();
            }
        }

        Ok(Async::Ready(()))
    }

    fn poll_close(&mut self) -> Poll<(), Self::Error> { Ok(Async::Ready(())) }

    fn call(&mut self, req: EnqueuedRequests<P::Message>) -> Self::Future {
        let result = self.conns[self.conns_index].call(req);

        self.conns_index += 1;
        self.conns_index %= self.conns.len();

        result
    }
}

pub struct ResponseFuture<P, E>
where
    P: Processor + Send + 'static,
    P::Message: Message + Send + 'static,
    E: From<oneshot::error::RecvError>,
{
    responses: JoinAll<PendingResponses<P::Message>>,
    _processor: PhantomData<P>,
    _error: PhantomData<E>,
}

impl<P, E> ResponseFuture<P, E>
where
    P: Processor + Send + 'static,
    P::Message: Message + Send + 'static,
    E: From<oneshot::error::RecvError>,
{
    pub fn new(responses: PendingResponses<P::Message>) -> ResponseFuture<P, E> {
        ResponseFuture {
            responses: join_all(responses),
            _processor: PhantomData,
            _error: PhantomData,
        }
    }
}

impl<P, E> Future for ResponseFuture<P, E>
where
    P: Processor + Send + 'static,
    P::Message: Message + Send + 'static,
    E: From<oneshot::error::RecvError>,
{
    type Error = E;
    type Item = AssignedResponses<P::Message>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> { self.responses.poll().map_err(|e| e.into()) }
}
