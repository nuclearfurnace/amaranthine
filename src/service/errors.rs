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
use crate::backend::processor::ProcessorError;
use futures::prelude::*;
use std::fmt;
use tower_service::Service;

/// Error type for `Pipeline`.
pub enum PipelineError<T, S, R>
where
    T: Sink + Stream,
    S: Service<R>,
{
    /// The underlying transport failed to produce a request.
    TransportReceive(<T as Stream>::Error),

    /// The underlying transport failed while attempting to send a response.
    TransportSend(<T as Sink>::SinkError),

    /// The underlying service failed to process a request.
    Service(S::Error),
}

impl<T, S, R> fmt::Display for PipelineError<T, S, R>
where
    T: Sink + Stream,
    <T as Sink>::SinkError: fmt::Display,
    <T as Stream>::Error: fmt::Display,
    S: Service<R>,
    <S as Service<R>>::Error: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PipelineError::TransportReceive(ref se) => fmt::Display::fmt(se, f),
            PipelineError::TransportSend(ref se) => fmt::Display::fmt(se, f),
            PipelineError::Service(ref se) => fmt::Display::fmt(se, f),
        }
    }
}

impl<T, S, R> fmt::Debug for PipelineError<T, S, R>
where
    T: Sink + Stream,
    <T as Sink>::SinkError: fmt::Debug,
    <T as Stream>::Error: fmt::Debug,
    S: Service<R>,
    <S as Service<R>>::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PipelineError::TransportReceive(ref se) => write!(f, "TransportRecv({:?})", se),
            PipelineError::TransportSend(ref se) => write!(f, "TransportSend({:?})", se),
            PipelineError::Service(ref se) => write!(f, "Service({:?})", se),
        }
    }
}

impl<T, S, R> PipelineError<T, S, R>
where
    T: Sink + Stream,
    S: Service<R>,
{
    pub fn from_sink_error(e: <T as Sink>::SinkError) -> Self { PipelineError::TransportSend(e) }

    pub fn from_stream_error(e: <T as Stream>::Error) -> Self { PipelineError::TransportReceive(e) }

    pub fn from_service_error(e: <S as Service<R>>::Error) -> Self { PipelineError::Service(e) }
}

impl<T, S, R> From<ProcessorError> for PipelineError<T, S, R>
where
    T: Sink + Stream,
    S: Service<R>,
{
    fn from(e: ProcessorError) -> PipelineError<T, S, R> { e.into() }
}
