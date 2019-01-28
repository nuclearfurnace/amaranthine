use std::thread;
use hotmic::{Receiver, Sink, Controller};

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

    pub fn get_sink(&self) -> Sink<&'static str> {
        self.sink.clone()
    }

    pub fn get_controller(&self) -> Controller {
        self.controller.clone()
    }
}
