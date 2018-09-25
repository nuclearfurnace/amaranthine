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
use backend::{
    distributor, hasher, message_queue::MessageQueue, pool::BackendPool, processor::RequestProcessor,
    redis::RedisRequestProcessor,
};
use common::Message;
use conf::ListenerConfiguration;
use futures::{
    future::{lazy, ok, Shared},
    prelude::*,
};
use futures_turnstyle::Waiter;
use metrics::{self, Metrics};
use net2::TcpBuilder;
use protocol::errors::ProtocolError;
use routing::{FixedRouter, Router};
use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use tokio::{
    io::{self, AsyncRead},
    net::{TcpListener, TcpStream},
    reactor,
};
use util::StreamExt;

type GenericRuntimeFuture = Box<Future<Item = (), Error = ()> + Sync + Send + 'static>;

/// Creates a listener from the given configuration.
///
/// The listener will spawn a socket for accepting client connections, and when a client connects,
/// spawn a task to process all of the messages from that client until the client disconnects or
/// there is an unrecoverable connection/protocol error.
pub fn from_config(config: ListenerConfiguration, close: Shared<Waiter>) -> Result<GenericRuntimeFuture, Error> {
    // Create the actual listener proper.
    let listen_address = config.address.clone();
    let listener = get_listener(&listen_address).expect("failed to create the TCP listener");

    // Get the correct handler based on protocol.
    let protocol = config.protocol.to_lowercase();
    let handler = match protocol.as_str() {
        "redis" => routing_from_config(config, listener, close.clone(), RedisRequestProcessor::new())?,
        s => panic!("unknown protocol type: {}", s),
    };

    // Make sure our handlers close out when told.
    let listen_address2 = listen_address.clone();
    let wrapped = lazy(move || {
        info!("[listener] starting listener '{}'...", listen_address);
        ok(())
    }).and_then(|_| handler)
    .select2(close)
    .then(move |_| {
        info!("[pool] shutting down listener '{}'", listen_address2);
        ok(())
    });
    Ok(Box::new(wrapped))
}

fn routing_from_config<T>(
    config: ListenerConfiguration, listener: TcpListener, close: Shared<Waiter>, processor: T,
) -> Result<GenericRuntimeFuture, Error>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Message + Send + 'static,
    T::ClientReader: Stream<Item = T::Message, Error = ProtocolError> + Send + 'static,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    let mut pools = HashMap::new();
    let pool_configs = config.pools.clone();
    for (pool_name, pool_config) in pool_configs {
        debug!(
            "[listener] configuring backend pool '{}' for address '{}'",
            &pool_name,
            config.address.clone()
        );

        let mut opts = pool_config.options.unwrap_or_else(HashMap::new);

        let dist_type = opts
            .entry("distribution".to_owned())
            .or_insert_with(|| "modulo".to_owned())
            .to_lowercase();
        let distributor = distributor::configure_distributor(&dist_type);
        debug!("[listener] using distributor '{}'", dist_type);

        let hash_type = opts
            .entry("hash".to_owned())
            .or_insert_with(|| "fnv1a_64".to_owned())
            .to_lowercase();
        let hasher = hasher::configure_hasher(&hash_type);
        debug!("[listener] using hasher '{}'", hash_type);

        let conn_limit_raw = opts.entry("conns".to_owned()).or_insert_with(|| "1".to_owned());
        let conn_limit = usize::from_str(conn_limit_raw.as_str())
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to parse connection limit value"))?;

        debug!("[listener] using connection limit of '{}'", conn_limit);

        let pool = Arc::new(BackendPool::new(
            &pool_config.addresses,
            processor.clone(),
            distributor,
            hasher,
            conn_limit,
            close.clone(),
        ));
        pools.insert(pool_name, pool);
    }

    // Figure out what sort of routing we're doing so we can grab the right handler.
    let mut routing = config.routing;
    let route_type = routing
        .entry("type".to_owned())
        .or_insert_with(|| "fixed".to_owned())
        .to_lowercase();
    match route_type.as_str() {
        "fixed" => get_fixed_router(listener, pools, processor, close.clone()),
        x => panic!("unknown route type '{}'", x),
    }
}

fn get_fixed_router<T>(
    listener: TcpListener, pools: HashMap<String, Arc<BackendPool<T>>>, processor: T, close: Shared<Waiter>,
) -> Result<GenericRuntimeFuture, Error>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Message + Send + 'static,
    T::ClientReader: Stream<Item = T::Message, Error = ProtocolError> + Send + 'static,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
{
    // Construct an instance of our router.
    let default_pool = pools
        .get("default")
        .ok_or_else(|| Error::new(ErrorKind::Other, "no default pool configured for fixed router"))?;
    let router = FixedRouter::new(processor.clone(), default_pool.clone());

    build_router_chain(listener, processor, router, close)
}

fn build_router_chain<T, R>(
    listener: TcpListener, processor: T, router: R, close: Shared<Waiter>,
) -> Result<GenericRuntimeFuture, Error>
where
    T: RequestProcessor + Clone + Sync + Send + 'static,
    T::Message: Message + Send + 'static,
    T::ClientReader: Stream<Item = T::Message, Error = ProtocolError> + Send + 'static,
    T::Future: Future<Item = TcpStream, Error = ProtocolError> + Send + 'static,
    R: Router<T> + Clone + Sync + Send + 'static,
{
    let close2 = close.clone();

    let task = listener
        .incoming()
        .for_each(move |client| {
            let mut metrics = metrics::get_sink();
            metrics.increment(Metrics::ClientsConnected);

            let router = router.clone();
            let processor = processor.clone();
            let close = close.clone();
            let client_addr = client.peer_addr().unwrap();

            // Spin up our protocol read stream and our outbound message queue.
            let (client_rx, client_tx) = client.split();
            let proto_rx = processor.get_read_stream(client_rx);
            let (mq, mqcp) = MessageQueue::new(processor, client_tx);
            tokio::spawn(mq);

            // Run the client.
            let client_proto = proto_rx
                .map_err(move |e| {
                    if e.client_closed() {
                        // If the client closed normally, we don't need to log anything but we
                        // do need to decrement the client count.  Since we use `fold` to
                        // enumerate over the message batches, we're working with a future at
                        // that point, which terminates when an error occurs, unlike with a
                        // stream.  Since we have `and_then` to handle normal "client done"
                        // activities, we never hit that if an error is thrown.  Further, we've
                        // changed to a RouterError in the `fold`, completely losing the
                        // `ProtocolError` we get directly from the message stream.
                        //
                        // Long story short, we handle it here.  No biggie. :)
                        metrics::get_sink().decrement(Metrics::ClientsConnected);
                    } else {
                        // This is a "real" error that we may or may not care about.  Technically
                        // it could be a legitimate protocol error i.e. malformed message
                        // structure, which could spam the logs... but there's a good chance we
                        // actually want to know if a ton of clients are sending malformed
                        // messages.
                        error!(
                            "[client] [{:?}] caught error while reading from client: {:?}",
                            client_addr, e
                        )
                    }
                }).batch(128)
                .fold((router, mqcp, metrics), |(router, mut mqcp, mut metrics), req| {
                    metrics.update_count(Metrics::ServerMessagesReceived, req.len() as i64);

                    let batch_start = Instant::now();
                    mqcp.enqueue(req)
                        .and_then(move |qmsgs| {
                            router
                                .route(qmsgs)
                                .map(|_| router)
                                .map_err(|e| error!("[client] error during routing: {}", e))
                        }).map(move |router| {
                            let batch_end = Instant::now();
                            metrics.update_latency(Metrics::ClientMessageBatchServiced, batch_start, batch_end);

                            (router, mqcp, metrics)
                        })
                }).and_then(|(_, _, mut metrics)| {
                    info!("[client] disconnected");
                    metrics.decrement(Metrics::ClientsConnected);
                    ok(())
                }).select2(close)
                .map(|_| ())
                .map_err(|_| ());

            tokio::spawn(client_proto);

            ok(())
        }).map_err(|e| error!("[listener] caught error while accepting connections: {:?}", e))
        .select2(close2)
        .map(|_| ())
        .map_err(|_| ());

    Ok(Box::new(task))
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
        .and_then(|l| TcpListener::from_std(l, &reactor::Handle::current()))
}

#[cfg(unix)]
fn configure_builder(builder: &TcpBuilder) -> io::Result<()> {
    use net2::unix::*;

    builder.reuse_port(true)?;
    Ok(())
}

#[cfg(windows)]
fn configure_builder(_builder: &TcpBuilder) -> io::Result<()> { Ok(()) }
