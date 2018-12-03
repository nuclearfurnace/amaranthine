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
use backend::{message_queue::MessageQueue, processor::Processor};
use bytes::BytesMut;
use common::{AssignedRequests, AssignedResponse, Message};
use futures::prelude::*;
use service::{DirectService, PipelineError};
use std::collections::VecDeque;
use util::Batch;

enum MaybeResponse<T, F> {
    Pending(F),
    Ready(T),
}

/// Pipeline-capable service base.
///
/// `Pipeline` can simultaenously drive a `Transport` and an underlying `Service`,
/// opportunistically batching messages from the client transport and handing them off for
/// processing while waiting to send back to the responses.
pub struct Pipeline<T, S, P>
where
    T: Sink + Stream<Item = P::Message>,
    S: DirectService<AssignedRequests<P::Message>>,
    S::Response: IntoIterator<Item = AssignedResponse<P::Message>>,
    P: Processor,
    P::Message: Message + Clone,
{
    responses: VecDeque<MaybeResponse<S::Response, S::Future>>,
    transport: Batch<T>,
    service: S,
    queue: MessageQueue<P>,

    send_buf: Option<BytesMut>,
    finish: bool,
}

impl<T, S, P> Pipeline<T, S, P>
where
    T: Sink<SinkItem = BytesMut> + Stream<Item = P::Message>,
    S: DirectService<AssignedRequests<P::Message>>,
    S::Response: IntoIterator<Item = AssignedResponse<P::Message>>,
    P: Processor,
    P::Message: Message + Clone,
{
    /// Creates a new `Pipeline`.
    pub fn new(transport: T, service: S, processor: P) -> Self {
        Pipeline {
            responses: VecDeque::new(),
            transport: Batch::new(transport, 128),
            service,
            queue: MessageQueue::new(processor),
            send_buf: None,
            finish: false,
        }
    }
}

impl<T, S, P> Future for Pipeline<T, S, P>
where
    T: Sink<SinkItem = BytesMut> + Stream<Item = P::Message>,
    S: DirectService<AssignedRequests<P::Message>>,
    S::Response: IntoIterator<Item = AssignedResponse<P::Message>>,
    P: Processor,
    P::Message: Message + Clone,
{
    type Error = PipelineError<T, S, AssignedRequests<P::Message>>;
    type Item = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // Make sure we're driving the inner service.
            let _ = self.service.poll_service();

            // In order, drive the response futures we're waiting on.  Keep pulling from the
            // front to keep things in order, and as soon as we hit something that isn't ready or
            // isn't ready to flush to the message queue.
            while let Some(r) = self.responses.pop_front() {
                match r {
                    MaybeResponse::Pending(mut f) => {
                        match f.poll() {
                            Ok(Async::Ready(rsp)) => {
                                self.responses.push_front(MaybeResponse::Ready(rsp));
                            },
                            Ok(Async::NotReady) => {
                                self.responses.push_front(MaybeResponse::Pending(f));
                                break;
                            },
                            Err(e) => {
                                return Err(PipelineError::from_service_error(e));
                            },
                        }
                    },
                    MaybeResponse::Ready(batch) => {
                        self.queue.fulfill(batch);
                    },
                }
            }

            // Now that we've polled and fulfilled any completed batches, see if we have a buffer
            // to send: first, we might be holding on to a buffer we got from the queue that
            // hasn't been sendable, or we might be trying to get a buffer to send period.
            if self.send_buf.is_some() {
                let buf = self.send_buf.take().unwrap();
                if let AsyncSink::NotReady(buf) =
                    self.transport.start_send(buf).map_err(PipelineError::from_sink_error)?
                {
                    self.send_buf = Some(buf);
                    return Ok(Async::NotReady);
                }
            }

            if let Some(buf) = self.queue.get_sendable_buf() {
                if let AsyncSink::NotReady(buf) =
                    self.transport.start_send(buf).map_err(PipelineError::from_sink_error)?
                {
                    self.send_buf = Some(buf);
                    return Ok(Async::NotReady);
                }
            }

            // Drive our transport to flush any buffers we have.
            if let Async::Ready(()) = self.transport.poll_complete().map_err(PipelineError::from_sink_error)? {
                // If we're finished and have nothing else to send, then we're done!
                if self.finish && self.responses.is_empty() {
                    return Ok(Async::Ready(()));
                }
            }

            // Don't try and grab anything else from the transport if we're finished, we just need
            // to flush the rest of our responses and that's it.
            if self.finish {
                return Ok(Async::NotReady);
            }

            // Make sure the underlying service is ready to be called.
            try_ready!(self.service.poll_ready().map_err(PipelineError::from_service_error));

            // See if we can pull a batch from the transport.
            match try_ready!(self.transport.poll().map_err(PipelineError::from_stream_error)) {
                Some(batch) => {
                    let batch = self.queue.enqueue(batch)?;
                    if !batch.is_empty() {
                        let fut = self.service.call(batch);
                        self.responses.push_back(MaybeResponse::Pending(fut));
                    }
                },
                None => {
                    // Our transport has signalled no more messages are going to come in, so mark
                    // ourselves as finished so we can begin the closing process.
                    assert!(!self.finish);
                    self.finish = true;
                },
            }
        }
    }
}
