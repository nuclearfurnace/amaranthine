use std::net::SocketAddr;
use futures::prelude::*;
use hotmic::Controller;
use warp::Filter;

pub fn build_with_graceful_shutdown(
    addr: SocketAddr,
    control: Controller,
    signal: impl Future<Item = ()> + Send + 'static,
) -> impl Future<Item = (), Error = ()> {
    let chain = warp::path("stats")
        .and_then(move || control.get_snapshot().map_err(warp::reject::custom))
        .map(|val| warp::reply::json(&val));

    let (_, server) = warp::serve(chain)
        .bind_with_graceful_shutdown(addr, signal);
    server
}
