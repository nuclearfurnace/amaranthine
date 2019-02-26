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
mod errors;
pub use self::errors::ProcessorError;

use backend::message_queue::MessageState;
use common::{EnqueuedRequests, Message};
use futures::future::{Either, FutureResult};
use protocol::errors::ProtocolError;
use std::{error::Error, net::SocketAddr};
use tokio::net::tcp::TcpStream;
use util::ProcessFuture;

/// An existing or pending TcpStream.
pub type TcpStreamFuture = Either<FutureResult<TcpStream, ProtocolError>, ProcessFuture>;

/// Cache-specific logic for processing requests and interacting with backends.
pub trait Processor
where
    Self::Message: Message + Clone,
{
    type Message;
    type Transport;

    /// Fragments a client's requests into, potentially, multiple subrequests.
    ///
    /// This allows multi-operation requests -- multi-key lookups, etc -- to be sharded to the
    /// correct backend server when routed.
    fn fragment_messages(&self, Vec<Self::Message>) -> Result<Vec<(MessageState, Self::Message)>, ProcessorError>;

    /// Defragments a client's subrequests into a single request.
    ///
    /// This is used to do any coalesing necessary to assemble multiple subrequests -- generated by
    /// `fragment_messages` -- back into a cohesive response that the client will understand.
    fn defragment_messages(&self, Vec<(MessageState, Self::Message)>) -> Result<Self::Message, ProcessorError>;

    /// Converts the given error into a corresponding format that can be sent to the client.
    fn get_error_message(&self, Box<Error>) -> Self::Message;

    /// Converts the given error string into a corresponding format the can be sent to the client.
    fn get_error_message_str(&self, &str) -> Self::Message;

    /// Wraps the given TCP stream with a protocol-specific transport layer, allowing the caller to
    /// extract protocol-specific messages, as well as send them, via the `Stream` and `Sink`
    /// implementations.
    fn get_transport(&self, TcpStream) -> Self::Transport;

    /// Connects to the given address via TCP and performs any necessary processor-specific
    /// initialization.
    fn preconnect(&self, &SocketAddr, bool) -> ProcessFuture;

    /// Processes a batch of requests, running the necessary operations against the given TCP
    /// stream.
    fn process(&self, EnqueuedRequests<Self::Message>, TcpStreamFuture) -> ProcessFuture;
}
