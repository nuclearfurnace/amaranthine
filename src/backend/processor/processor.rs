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
use backend::{
    message_queue::{MessageState, QueuedMessage},
    processor::ProcessorError,
};
use futures::future::{Either, FutureResult};
use std::{error::Error, io};
use tokio::{
    io::ReadHalf,
    net::tcp::{ConnectFuture, TcpStream},
};

/// An existing or pending TcpStream.
pub type TcpStreamFuture = Either<FutureResult<TcpStream, io::Error>, ConnectFuture>;

/// Processors a request into a response by generating a future which will consume a given
/// TcpStream to an underlying server, do its work, and hand back both the stream and the results.
pub trait RequestProcessor {
    type Message;
    type ClientReader;
    type Future;

    fn fragment_messages(&self, Vec<Self::Message>) -> Result<Vec<(MessageState, Self::Message)>, ProcessorError>;
    fn defragment_messages(&self, Vec<(MessageState, Self::Message)>) -> Result<Self::Message, ProcessorError>;

    fn get_error_message(&self, Box<Error>) -> Self::Message;
    fn get_error_message_str(&self, &str) -> Self::Message;

    fn get_read_stream(&self, ReadHalf<TcpStream>) -> Self::ClientReader;

    fn process(&self, Vec<QueuedMessage<Self::Message>>, TcpStreamFuture) -> Self::Future;
}
