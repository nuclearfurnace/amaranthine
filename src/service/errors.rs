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
use backend::processor::ProcessorError;
use futures::prelude::*;
use service::DirectService;

pub enum PipelineError<T, S, R>
where
    T: Sink + Stream,
    S: DirectService<R>,
{
    /// The underlying transport failed to produce a request.
    TransportReceive(<T as Stream>::Error),

    /// The underlying transport failed while attempting to send a response.
    TransportSend(<T as Sink>::SinkError),

    /// The underlying service failed to process a request.
    Service(S::Error),
}

impl<T, S, R> PipelineError<T, S, R>
where
    T: Sink + Stream,
    S: DirectService<R>,
{
    pub fn from_sink_error(e: <T as Sink>::SinkError) -> Self { PipelineError::TransportSend(e) }

    pub fn from_stream_error(e: <T as Stream>::Error) -> Self { PipelineError::TransportReceive(e) }

    pub fn from_service_error(e: <S as DirectService<R>>::Error) -> Self { PipelineError::Service(e) }
}

impl<T, S, R> From<ProcessorError> for PipelineError<T, S, R>
where
    T: Sink + Stream,
    S: DirectService<R>,
{
    fn from(e: ProcessorError) -> PipelineError<T, S, R> { e.into() }
}
