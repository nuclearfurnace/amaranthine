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

pub use self::errors::BackendError;

use backend::{
    distributor::BackendDescriptor, health::BackendHealth, message_queue::QueuedMessage, processor::RequestProcessor,
};
use errors::CreationError;
use futures::{
    future::{ok, Either},
    prelude::*,
    sync::mpsc,
    Poll,
};
use protocol::errors::ProtocolError;
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use tokio::{
    net::tcp::TcpStream,
    timer::{timeout::Error as TimeoutError, Timeout},
};
use util::{WorkQueue, Worker};

type BackendWorkQueue<T> = Worker<Vec<QueuedMessage<T>>>;
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

/// Commands sent by backend connections to their backend supervisor.
pub enum BackendCommand {
    /// The connection has encountered an error.
    ///
    /// This lets the backend supervisor know that the connection has terminated and will need to
    /// be replaced, etc.
    Error,
}

/// A backend connection.
///
/// This represents a one-to-one mapping with a TCP connection to the given backend server.  This
/// connection will independently poll the work queue for the backend and run requests when
/// available.
///
/// If a backend connection encounters an error, it will terminate and notify its backend
/// supervisor, so that it can be replaced.
struct BackendConnection<P, C>
where
    P: RequestProcessor,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Send,
{
    processor: P,
    worker: BackendWorkQueue<P::Message>,
    close: C,
    command_tx: mpsc::UnboundedSender<BackendCommand>,
    address: SocketAddr,
    timeout_ms: u64,

    socket: Option<TcpStream>,
    current: Option<MaybeTimeout<P::Future>>,
}

impl<P, C> Future for BackendConnection<P, C>
where
    P: RequestProcessor,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Send,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // If we're supposed to close, do it now.
            if let Ok(Async::Ready(_)) = self.close.poll() {
                return Ok(Async::Ready(()));
            }

            // First, check if we have an operation running.  If we do, poll it to drive it towards
            // completion.  If it's done, we'll reclaim the socket and then fallthrough to trying to
            // find another piece of work to run.
            if let Some(task) = self.current.as_mut() {
                match task.poll() {
                    Ok(Async::Ready(socket)) => {
                        // The operation finished, and gave us the connection back.
                        self.socket = Some(socket);
                        self.current = None;
                    },
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(_) => {
                        // On error, we kill ourselves but notify the supervisor first so it can
                        // replace us down the line.  This includes both errors with the underlying
                        // call itself or timing out during the call.
                        let _ = self.command_tx.unbounded_send(BackendCommand::Error);
                        return Err(());
                    },
                }
            }

            // If we're here, we have no current operation to drive, so see if anything is in our work
            // queue that we can grab.
            match self.worker.poll() {
                Ok(Async::Ready(Some(batch))) => {
                    let socket = match self.socket.take() {
                        Some(socket) => Either::A(ok(socket)),
                        None => Either::B(TcpStream::connect(&self.address)),
                    };

                    let inner = self.processor.process(batch, socket);
                    let work = if self.timeout_ms == 0 {
                        Either::A(NotTimeout { inner })
                    } else {
                        Either::B(Timeout::new(inner, Duration::from_millis(self.timeout_ms)))
                    };
                    self.current = Some(work);
                },
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(_) => {
                    let _ = self.command_tx.unbounded_send(BackendCommand::Error);
                    return Err(());
                },
            }
        }
    }
}

impl<P, C> Drop for BackendConnection<P, C>
where
    P: RequestProcessor,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Send,
{
    fn drop(&mut self) {
        trace!("[backend connection] dropping");
    }
}

/// A state machine that drives the pooling of backend connections and the requests that require
/// them.
///
/// Rather than using explicit synchronization, all connections and work requests flow to this
/// state machine via channels, and this future must be launched as an independent task when a new
/// backend is created.
///
/// There is an implicit requirement that a backend be created with a sibling state machine, and
/// each given the appropriate tx/rx sides of a channel that allows them to communicate with each
/// other.
pub struct BackendSupervisor<P, C>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Clone + Send + 'static,
{
    processor: P,
    worker: BackendWorkQueue<P::Message>,
    health: Arc<BackendHealth>,
    updates_tx: mpsc::UnboundedSender<()>,
    command_rx: mpsc::UnboundedReceiver<BackendCommand>,
    command_tx: mpsc::UnboundedSender<BackendCommand>,

    address: SocketAddr,
    conn_count: usize,
    conn_limit: usize,
    timeout_ms: u64,

    close: C,
}

impl<P, C> Future for BackendSupervisor<P, C>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Clone + Send + 'static,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // If we're supposed to close, do it now.
        if let Ok(Async::Ready(_)) = self.close.poll() {
            return Ok(Async::Ready(()));
        }

        // Process any commands.
        loop {
            match self.command_rx.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    match cmd {
                        BackendCommand::Error => {
                            self.conn_count -= 1;
                            self.health.increment_error();
                        },
                    }
                },
                Ok(Async::NotReady) => break,
                _ => return Err(()),
            }
        }

        if !self.health.is_healthy() {
            let _ = self.updates_tx.unbounded_send(());
        }

        // Make sure all connections have been spawned.
        while self.conn_count < self.conn_limit {
            let connection = BackendConnection {
                processor: self.processor.clone(),
                worker: self.worker.clone(),
                close: self.close.clone(),
                address: self.address,
                command_tx: self.command_tx.clone(),
                timeout_ms: self.timeout_ms,
                current: None,
                socket: None,
            };

            tokio::spawn(connection);

            self.conn_count += 1;
        }

        Ok(Async::NotReady)
    }
}

impl<P, C> Drop for BackendSupervisor<P, C>
where
    P: RequestProcessor + Clone + Send + 'static,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Clone + Send + 'static,
{
    fn drop(&mut self) {
        trace!("[backend supervisor] dropping");
    }
}

fn new_supervisor<P, C>(
    processor: P, addr: SocketAddr, mut options: HashMap<String, String>, worker: BackendWorkQueue<P::Message>,
    health: Arc<BackendHealth>, updates_tx: mpsc::UnboundedSender<()>, close: C,
) -> Result<BackendSupervisor<P, C>, CreationError>
where
    P: RequestProcessor + Clone + Send,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    C: Future + Clone + Send + 'static,
{
    let conn_limit_raw = options.entry("conns".to_owned()).or_insert_with(|| "1".to_owned());
    let conn_limit = usize::from_str(conn_limit_raw.as_str())
        .map_err(|_| CreationError::InvalidParameter("options.conns".to_string()))?;
    debug!("[listener] using connection limit of '{}'", conn_limit);

    let timeout_ms_raw = options
        .entry("timeout_ms".to_owned())
        .or_insert_with(|| "1000".to_owned());
    let timeout_ms = u64::from_str(timeout_ms_raw.as_str())
        .map_err(|_| CreationError::InvalidParameter("options.timeout_ms".to_string()))?;

    let (command_tx, command_rx) = mpsc::unbounded();

    Ok(BackendSupervisor {
        processor,
        worker,
        health,
        updates_tx,

        command_rx,
        command_tx,

        address: addr,
        conn_count: 0,
        conn_limit,
        timeout_ms,

        close,
    })
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
    P: RequestProcessor + Clone + Send,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    health: Arc<BackendHealth>,
    work_queue: WorkQueue<Vec<QueuedMessage<P::Message>>>,
}

impl<P> Backend<P>
where
    P: RequestProcessor + Clone + Send,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    pub fn new<C>(
        addr: SocketAddr, processor: P, mut options: HashMap<String, String>, updates_tx: mpsc::UnboundedSender<()>,
        close: C,
    ) -> Result<(Backend<P>, BackendSupervisor<P, C>), CreationError>
    where
        P: RequestProcessor + Clone + Send,
        P::Message: Send,
        P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
        C: Future + Clone + Send,
    {
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

        let health = Arc::new(BackendHealth::new(
            cooloff_enabled,
            cooloff_timeout_ms,
            cooloff_error_limit,
            updates_tx.clone(),
        ));

        let work_queue = WorkQueue::new();
        let worker = work_queue.worker();
        let backend = Backend {
            work_queue,
            health: health.clone(),
        };
        let runner = new_supervisor(processor, addr, options, worker, health, updates_tx, close)?;

        Ok((backend, runner))
    }

    pub fn submit(&self, batch: Vec<QueuedMessage<P::Message>>) { self.work_queue.send(batch) }

    pub fn is_healthy(&self) -> bool { self.health.is_healthy() }

    pub fn get_descriptor(&self) -> BackendDescriptor { BackendDescriptor {} }
}

impl<P> Drop for Backend<P>
where
    P: RequestProcessor + Clone + Send,
    P::Message: Send,
    P::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    fn drop(&mut self) {
        trace!("[backend] dropping");
    }
}
