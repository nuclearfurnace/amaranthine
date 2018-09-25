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
#![feature(associated_type_defaults)]
#![feature(duration_as_u128)]
#![feature(extern_prelude)]
#![feature(option_replace)]
#![recursion_limit = "1024"]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate crossbeam;

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
#[macro_use]
extern crate futures;
extern crate futures_turnstyle;
extern crate net2;

use futures::future::{lazy, ok, Shared};
use futures_turnstyle::{Turnstyle, Waiter};
use signal_hook::iterator::Signals;
use std::{error::Error, process, thread};
use std::net::{Shutdown, SocketAddr, IpAddr, Ipv4Addr};
use tokio::{io, net::TcpListener, prelude::*, runtime};

#[macro_use]
extern crate log;
#[macro_use(slog_o, slog_kv)]
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

mod backend;
mod common;
mod conf;
mod listener;
mod metrics;
mod protocol;
mod routing;
mod util;

use conf::{Configuration, LevelExt};

fn run() -> i32 {
    // Set up our signal handling before anything else.
    let signals = Signals::new(&[libc::SIGINT, libc::SIGUSR1]).expect("failed to register signal handlers");
    let turnstyle = Turnstyle::new();
    let closer = turnstyle.join().shared();
    thread::spawn(move || {
        for signal in signals.forever() {
            info!("[core] signal received: {:?}", signal);

            match signal {
                libc::SIGUSR1 => {}, // signal to spawn new process
                libc::SIGINT => {
                    // signal to close this process
                    turnstyle.turn();
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

    // Now run.
    // let mut threadpool_builder = thread_pool::Builder::new();
    // threadpool_builder.name_prefix("synchrotron-worker-").pool_size(4);

    // let mut runtime = runtime::Builder::new()
    //    .threadpool_builder(threadpool_builder)
    //    .build()
    //    .expect("failed to create tokio runtime");
    let mut runtime = runtime::current_thread::Runtime::new().unwrap();

    let initial = lazy(move || {
        let listeners = configuration
            .listeners
            .into_iter()
            .map(|x| {
                let close = closer.clone();
                let config = x.clone();

                listener::from_config(config, close)
            }).collect::<Vec<_>>();

        let mut errors = Vec::new();
        for listener in &listeners {
            let result = listener.as_ref();
            if result.is_err() {
                let error = result.err().unwrap();
                errors.push(error.description().to_owned());
            }
        }

        if !errors.is_empty() {
            error!("[core] encountered errors while spawning listeners:");

            for error in errors {
                error!("[core] - {}", error);
            }

            return Err(());
        }

        // Launch all these listeners into the runtime.
        for listener in listeners {
            tokio::spawn(listener.unwrap());
        }

        // Launch our stats port.
        launch_stats(configuration.stats_port, closer.clone());

        Ok(())
    });

    runtime.spawn(initial);

    info!("[core] synchrotron running");

    // runtime.shutdown_on_idle().wait().unwrap();
    runtime.run().unwrap();
    0
}

// TODO: this should 100% be using either tower-web or warp.  it's super janky as-is.  it should
// also move to the metrics module where we can simply spawn it after getting the service from a
// builder function.
fn launch_stats(port: u16, close: Shared<Waiter>) {
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
                }).and_then(|buf| io::write_all(socket, buf.into_bytes()).map_err(|_| ()))
                .and_then(|(c, _)| {
                    c.shutdown(Shutdown::Both).unwrap();
                    ok(())
                })
        }).select2(close)
        .and_then(|_| {
            debug!("[stats] closing stats listener");
            ok(())
        }).map(|_| ())
        .map_err(|_| ());

    tokio::spawn(processor);
}

fn main() {
    let code = run();
    process::exit(code);
}
