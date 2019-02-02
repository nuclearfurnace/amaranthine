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
use hotmic::{Controller, Receiver, Sink};
use std::thread;

lazy_static! {
    static ref METRICS: MetricsFacade = {
        let mut receiver = Receiver::builder().build();
        let facade = MetricsFacade::new(&receiver);

        // Spawn our actual processing loop.
        thread::spawn(move || receiver.run());

        facade
    };
}

pub fn get_facade() -> &'static MetricsFacade { &METRICS }

pub fn get_sink() -> Sink<&'static str> {
    let facade = get_facade();
    facade.get_sink()
}

pub struct MetricsFacade {
    sink: Sink<&'static str>,
    controller: Controller,
}

impl MetricsFacade {
    pub fn new(receiver: &Receiver<&'static str>) -> MetricsFacade {
        MetricsFacade {
            sink: receiver.get_sink(),
            controller: receiver.get_controller(),
        }
    }

    pub fn get_sink(&self) -> Sink<&'static str> { self.sink.clone() }

    pub fn get_controller(&self) -> Controller { self.controller.clone() }
}
