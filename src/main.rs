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
#![feature(test)]
#![feature(nll)]
#![recursion_limit = "1024"]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate crossbeam;
extern crate parking_lot;

extern crate config;
extern crate crypto;
extern crate fnv;
extern crate pruefung;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate slab;

extern crate libc;
extern crate signal_hook;

extern crate tokio;
extern crate tokio_io;
extern crate tokio_io_pool;
#[macro_use]
extern crate futures;
extern crate futures_turnstyle;
extern crate net2;

use futures::{
    future::{lazy, ok},
    sync::{mpsc, oneshot},
};
use futures_turnstyle::{Turnstyle, Waiter};
use signal_hook::iterator::Signals;
use std::{
    net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr},
    thread,
};
use tokio::{io, net::TcpListener, prelude::*};

#[macro_use]
extern crate log;
#[macro_use(slog_o)]
extern crate slog;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use slog::Drain;

extern crate btoi;
extern crate bytes;
extern crate hotmic;
extern crate itoa;
extern crate rand;

#[cfg(test)]
extern crate test;

#[cfg(test)]
extern crate spectral;

extern crate tokio_evacuate;

mod backend;
mod common;
mod conf;
mod errors;
mod listener;
mod metrics;
mod protocol;
mod routing;
mod service;
mod util;

use conf::{Configuration, LevelExt};
use errors::CreationError;
use util::typeless;

enum SupervisorCommand {
    Launch,
    Reload,
    Shutdown,
}

fn main() {
    // Set up our signal handling before anything else.
    let (supervisor_tx, supervisor_rx) = mpsc::unbounded();
    let signals = Signals::new(&[libc::SIGINT, libc::SIGUSR1]).expect("failed to register signal handlers");
    thread::spawn(move || {
        // Do an initial send of the launch command to trigger actually spawning the listeners at
        // startup.
        let _ = supervisor_tx.unbounded_send(SupervisorCommand::Launch);

        for signal in signals.forever() {
            info!("[core] signal received: {:?}", signal);

            match signal {
                libc::SIGUSR1 => {
                    let _ = supervisor_tx.unbounded_send(SupervisorCommand::Reload);
                },
                libc::SIGINT => {
                    let _ = supervisor_tx.unbounded_send(SupervisorCommand::Shutdown);
                    break;
                },
                _ => {}, // we don't care about the rest
            }
        }
    });

    let configuration = Configuration::new().expect("failed to parse configuration");

    // Configure our logging.  This gives us fully asynchronous logging to the terminal
    // which is also level filtered.  As well, we've replaced the global std logger
    // and pulled in helper macros that correspond to the various logging levels.
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(
        slog::LevelFilter::new(drain, slog::Level::from_str(&configuration.logging.level)).fuse(),
        slog_o!("version" => env!("GIT_HASH")),
    );

    let _scope_guard = slog_scope::set_global_logger(logger);
    slog_stdlog::init().unwrap();
    info!("[core] logging configured");

    tokio_io_pool::run(lazy(move || {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        launch_supervisor(supervisor_rx, shutdown_tx);
        launch_stats(configuration.stats_port, shutdown_rx);

        info!("[core] synchrotron running");

        ok(())
    }))
}

fn launch_supervisor(supervisor_rx: mpsc::UnboundedReceiver<SupervisorCommand>, shutdown_tx: oneshot::Sender<()>) {
    let turnstyle = Turnstyle::new();
    let supervisor = supervisor_rx
        .map_err(|_| CreationError::ListenerSpawnFailed)
        .fold(turnstyle, |ts, command| {
            match command {
                SupervisorCommand::Launch => {
                    let (version, waiter) = ts.join();
                    launch_listeners(version, waiter)?;
                },
                SupervisorCommand::Reload => {
                    let (version, waiter) = ts.join();
                    launch_listeners(version, waiter)?;
                    ts.turn();
                },
                SupervisorCommand::Shutdown => {
                    ts.turn();
                },
            }

            Ok(ts)
        })
        .then(move |result| {
            if let Err(e) = result {
                error!("[core supervisor] caught an error during launch/reload: {}", e);
            }

            shutdown_tx.send(())
        });

    tokio::spawn(typeless(supervisor));
}

fn launch_listeners(version: usize, close: Waiter) -> Result<(), CreationError> {
    let configuration = Configuration::new().expect("failed to parse configuration");
    let closer = close.shared();
    let listeners = configuration
        .listeners
        .into_iter()
        .map(|x| {
            let close = closer.clone();
            let config = x.clone();

            listener::from_config(version, config, close)
        })
        .collect::<Vec<_>>();

    let mut errors = Vec::new();
    for listener in &listeners {
        let result = listener.as_ref();
        if result.is_err() {
            let error = result.err().unwrap();
            errors.push(error.to_string());
        }
    }

    if !errors.is_empty() {
        error!("[core] encountered errors while spawning listeners:");
        for error in errors {
            error!("[core] - {}", error);
        }

        return Err(CreationError::ListenerSpawnFailed);
    }

    // Launch all these listeners into the runtime.
    for listener in listeners {
        tokio::spawn(listener.unwrap());
    }

    Ok(())
}

// TODO: this should 100% be using either tower-web or warp.  it's super janky as-is.  it should
// also move to the metrics module where we can simply spawn it after getting the service from a
// builder function.
fn launch_stats(port: u16, close: oneshot::Receiver<()>) {
    // Touch the global registry for the first time to initialize it.
    let facade = metrics::get_facade();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let listener = TcpListener::bind(&addr).unwrap();
    let processor = listener
        .incoming()
        .map_err(|e| eprintln!("failed to accept stats connection: {:?}", e))
        .inspect(|_| debug!("got new stats connection"))
        .for_each(move |socket| {
            facade
                .get_snapshot()
                .into_future()
                .map_err(|e| eprintln!("failed to get metrics snapshot: {:?}", e))
                .and_then(|snapshot| {
                    let mut output = "{".to_owned();

                    for (stat, value) in snapshot.signed_data {
                        output = output + &format!("\"{}\":{},", stat, value);
                    }
                    for (stat, value) in snapshot.unsigned_data {
                        output = output + &format!("\"{}\":{},", stat, value);
                    }
                    if output.len() > 1 {
                        output.pop();
                    }
                    output += "}";

                    ok(output)
                })
                .and_then(|buf| io::write_all(socket, buf.into_bytes()).map_err(|_| ()))
                .and_then(|(c, _)| {
                    c.shutdown(Shutdown::Both).unwrap();
                    ok(())
                })
        })
        .select2(close)
        .and_then(|_| {
            trace!("[stats] closing stats listener");
            ok(())
        });

    tokio::spawn(typeless(processor));
}
