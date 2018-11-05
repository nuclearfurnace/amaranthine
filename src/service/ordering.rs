use std::marker::PhantdomData;
use bytes::BytesMut;
use futures::{Sink, Stream};
use futures::sync::mpsc;
use tower_service::Service;
use backend::processor::Processor;
use backend::MessageQueue;

enum MaybeResponse<T, F> {
    Pending(F),
    Ready(T),
}

pub struct OrderedPipeline<P, T, S>
where
    P: Processor,
    T: Sink<SinkItem = BytesMut> + Stream<Item = P::Message>,
    S: Service,
{
    transport: Batch<T>,
    service: S,

    queue: MessageQueue<P>,
    responses: VecDeque<MaybeResponse<S::Response, S::Future>>,

    _processor: PhantomData<P>,
}

impl<P, T, S> OrderedPipeline<P, T, S>
where
    P: Processor,
    T: Sink<SinkItem = BytesMut> + Stream<Item = P::Message>,
    S: Service,
{
    pub fn new(processor: P, transport: T, service: S) -> Self {
        let (responses_tx, responses_rx) = mpsc::bounded(1024);
        let queue = MessageQueue::new(processor);

        OrderedPipeline {
            transport: Batch::new(transport, 128),
            service,
            queue,
            responses: Vec::new(),
            _processor: PhantomData,
        }
    }
}

impl<P, T, S> Future for OrderedPipeline<P, T, S>
where
    P: Processor,
    T: Sink<SinkItem = BytesMut> + Stream<Item = P::Message>,
    S: Service<Request = AssignedBatch<P::Message>, Response = FulfilledBatch<P::Message>>,
{
    type Error = ();
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let sendable = Vec::new();

        loop {
            // Go through our pending calls, driving the underlying futures.
            while let Some(r) = self.responses.pop_front() {
                match r {
                    MaybeResponse::Pending(mut f) => match f.poll() {
                        Ok(Async::Ready(resp)) => {
                            self.responses.push_front(MaybeResponse::Ready(resp));
                        }
                        Ok(Async::NotReady) => {
                            self.responses.push_front(MaybeResponse::Pending(f));
                            break;
                        }
                        Err(e) => {
                            return Err(Error::from_service_error(e));
                        }
                    },
                    MaybeResponse::Ready(resp) => self.queue.fulfill(resp),
                }
            }

            // TODO: ask the queue for the next largest available buffer we can write,
            // and figure oiut how to store it under the underlying sink isn't ready

            // See if we can extract a request batch from the transport.
            let batch = try_ready!(self.transport.poll().map_err(Error::from_stream_error));
            if let Some(batch) = batch {
                let abatch = self.queue.enqueue(batch);
                let fut = self.service.call(abatch);
                self.responses.push_back(MaybeResponse::Pending(fut));
            }
        }
    }
}
