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
use crate::{
    backend::{
        pool::{BackendPool, BackendPoolBuilder},
        processor::Processor,
        redis::RedisProcessor,
    },
    common::{AssignedRequests, AssignedResponse, EnqueuedRequests, Message},
    conf::ListenerConfiguration,
    errors::CreationError,
    protocol::errors::ProtocolError,
    routing::{FixedRouter, ShadowRouter},
    service::{Pipeline, PipelineError},
    util::FutureExt,
};
use bytes::BytesMut;
use futures::{
    future::{lazy, ok, Shared},
    prelude::*,
};
use futures_turnstyle::Waiter;
use metrics_runtime::Sink as MetricSink;
use net2::TcpBuilder;
use std::{collections::HashMap, fmt::Display, net::SocketAddr};
use tokio::{io, net::TcpListener, reactor};
use tokio_evacuate::{Evacuate, Warden};
use tokio_executor::DefaultExecutor;
use tower_buffer::{Buffer, DirectServiceRef};
use tower_service::Service;

type GenericRuntimeFuture = Box<Future<Item = (), Error = ()> + Send + 'static>;
type BufferedPool<T, M> = Buffer<DirectServiceRef<BackendPool<T>>, EnqueuedRequests<M>>;

/// Creates a listener from the given configuration.
///
/// The listener will spawn a socket for accepting client connections, and when a client connects,
/// spawn a task to process all of the messages from that client until the client disconnects or
/// there is an unrecoverable connection/protocol error.
pub fn from_config(
    version: usize, name: String, config: ListenerConfiguration, close: Shared<Waiter>, sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError> {
    // Create the actual listener proper.
    let listen_address = config.address.clone();
    let listener = get_listener(&listen_address).expect("failed to create the TCP listener");

    // Now build our handler: this is what's actually going to do the real work.
    let protocol = config.protocol.to_lowercase();
    let handler = match protocol.as_str() {
        "redis" => routing_from_config(name, config, listener, close.clone(), RedisProcessor::new(), sink),
        s => Err(CreationError::InvalidResource(format!("unknown cache protocol: {}", s))),
    }?;

    // Make sure our handlers close out when told.
    let listen_address2 = listen_address.clone();
    let wrapped = lazy(move || {
        info!("[listener] starting listener '{}' (v{})", listen_address, version);
        ok(())
    })
    .and_then(|_| handler)
    .select2(close)
    .then(move |_| {
        info!("[listener] shutting down listener '{}' (v{})", listen_address2, version);
        ok(())
    });
    Ok(Box::new(wrapped))
}

fn routing_from_config<P, C>(
    name: String, config: ListenerConfiguration, listener: TcpListener, close: C, processor: P, sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
    P::Transport:
        Sink<SinkItem = BytesMut, SinkError = std::io::Error> + Stream<Item = P::Message, Error = ProtocolError> + Send,
    C: Future + Clone + Send + 'static,
{
    let reload_timeout_ms = config.reload_timeout_ms.unwrap_or_else(|| 5000);

    // Build our evacuator and wrap it as shared.  This lets us soft close everything.
    let (warden, evacuate) = Evacuate::new(close, reload_timeout_ms);
    let closer = evacuate.shared();

    // Get our scoped metric sink.
    let mut sink = sink.clone();
    sink.add_default_labels(&[("listener", name)]);

    // Extract all the configured pools and build a backend pool for them.
    let mut pools = HashMap::new();
    let pool_configs = config.pools.clone();
    for (pool_name, pool_config) in pool_configs {
        debug!(
            "[listener] configuring backend pool '{}' for address '{}'",
            &pool_name,
            config.address.clone()
        );

        let pool = BackendPoolBuilder::new(pool_name.clone(), processor.clone(), pool_config, sink.clone()).build()?;
        let buffered_pool = Buffer::new_direct(pool, 32, &DefaultExecutor::current()).map_err(|_| {
            CreationError::InvalidResource(format!(
                "error while building pool '{}': failed to spawn task",
                pool_name
            ))
        })?;
        pools.insert(pool_name, buffered_pool);
    }

    // Figure out what sort of routing we're doing so we can grab the right handler.
    let mut routing = config.routing;
    let route_type = routing
        .entry("type".to_owned())
        .or_insert_with(|| "fixed".to_owned())
        .to_lowercase();
    match route_type.as_str() {
        "fixed" => get_fixed_router(listener, pools, processor, warden, closer, sink),
        "shadow" => get_shadow_router(listener, pools, processor, warden, closer, sink),
        x => Err(CreationError::InvalidResource(format!("unknown route type '{}'", x))),
    }
}

fn get_fixed_router<P, C>(
    listener: TcpListener, pools: HashMap<String, BufferedPool<P, P::Message>>, processor: P, warden: Warden, close: C,
    sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
    P::Transport:
        Sink<SinkItem = BytesMut, SinkError = std::io::Error> + Stream<Item = P::Message, Error = ProtocolError> + Send,
    C: Future + Clone + Send + 'static,
{
    // Construct an instance of our router.
    let default_pool = pools
        .get("default")
        .ok_or_else(|| CreationError::InvalidResource("no default pool configured for fixed router".to_string()))?
        .clone();
    let router = FixedRouter::new(processor.clone(), default_pool);

    build_router_chain(listener, processor, router, warden, close, sink)
}

fn get_shadow_router<P, C>(
    listener: TcpListener, pools: HashMap<String, BufferedPool<P, P::Message>>, processor: P, warden: Warden, close: C,
    sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
    P::Transport:
        Sink<SinkItem = BytesMut, SinkError = std::io::Error> + Stream<Item = P::Message, Error = ProtocolError> + Send,
    C: Future + Clone + Send + 'static,
{
    // Construct an instance of our router.
    let default_pool = pools
        .get("default")
        .ok_or_else(|| CreationError::InvalidResource("no default pool configured for shadow router".to_string()))?
        .clone();

    let shadow_pool = pools
        .get("shadow")
        .ok_or_else(|| CreationError::InvalidResource("no shadow pool configured for shadow router".to_string()))?
        .clone();

    let router = ShadowRouter::new(processor.clone(), default_pool, shadow_pool);

    build_router_chain(listener, processor, router, warden, close, sink)
}

fn build_router_chain<P, R, C>(
    listener: TcpListener, processor: P, router: R, warden: Warden, close: C, mut sink: MetricSink,
) -> Result<GenericRuntimeFuture, CreationError>
where
    P: Processor + Clone + Send + 'static,
    P::Message: Message + Clone + Send + 'static,
    P::Transport:
        Sink<SinkItem = BytesMut, SinkError = std::io::Error> + Stream<Item = P::Message, Error = ProtocolError> + Send,
    R: Service<AssignedRequests<P::Message>> + Clone + Send + 'static,
    R::Error: Display + Send + Sync,
    R::Response: IntoIterator<Item = AssignedResponse<P::Message>> + Send,
    R::Future: Future + Send,
    C: Future + Clone + Send + 'static,
{
    let close2 = close.clone();
    let task = listener
        .incoming()
        .for_each(move |client| {
            warden.increment();
            sink.record_counter("clients_connected", 1);

            let router = router.clone();
            let processor = processor.clone();
            let close = close.clone();
            let warden2 = warden.clone();
            let mut sink2 = sink.clone();
            let client_addr = client.peer_addr().unwrap();
            debug!("[client] {} connected", client_addr);

            let transport = processor.get_transport(client);
            let task = Pipeline::new(transport, router, processor, sink.clone())
                .then(move |result| {
                    match result {
                        Ok(_) => {
                            debug!("[client] {} disconnected", client_addr);
                        },
                        Err(e) => {
                            match e {
                                // If we got a protocol error from a client, that's bad.  Otherwise,
                                // clients closing their connection is a normal thing.
                                PipelineError::TransportReceive(ie) => {
                                    if !ie.client_closed() {
                                        sink2.record_counter("client_errors", 1);
                                        error!("[client] transport error from {}: {}", client_addr, ie);
                                    }
                                },
                                e => error!("[client] error from {}: {}", client_addr, e),
                            }
                        },
                    }

                    warden2.decrement();

                    ok::<(), ()>(())
                })
                .select2(close);

            tokio::spawn(task.untyped());

            ok(())
        })
        .map_err(|e| error!("[listener] caught error while accepting connections: {:?}", e))
        .select2(close2);

    Ok(Box::new(task.untyped()))
}

fn get_listener(addr_str: &str) -> io::Result<TcpListener> {
    let addr = addr_str.parse().unwrap();
    let builder = match addr {
        SocketAddr::V4(_) => TcpBuilder::new_v4()?,
        SocketAddr::V6(_) => TcpBuilder::new_v6()?,
    };
    configure_builder(&builder)?;
    builder.reuse_address(true)?;
    builder.bind(addr)?;
    builder
        .listen(1024)
        .and_then(|l| TcpListener::from_std(l, &reactor::Handle::default()))
}

#[cfg(unix)]
fn configure_builder(builder: &TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    builder.reuse_port(true)?;
    Ok(())
}

#[cfg(windows)]
fn configure_builder(_builder: &TcpBuilder) -> io::Result<()> { Ok(()) }
