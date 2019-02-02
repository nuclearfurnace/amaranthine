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
use std::{error, fmt, io};
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum ProtocolError {
    IoError(io::Error),
    InvalidProtocol,
    BackendClosedPrematurely,
}

impl ProtocolError {
    pub fn client_closed(&self) -> bool {
        match self {
            ProtocolError::IoError(e) => {
                match e.kind() {
                    io::ErrorKind::ConnectionReset => true,
                    _ => false,
                }
            },
            _ => false,
        }
    }
}

impl error::Error for ProtocolError {
    fn description(&self) -> &str {
        match *self {
            ProtocolError::IoError(ref e) => e.description(),
            ProtocolError::InvalidProtocol => "invalid protocol",
            ProtocolError::BackendClosedPrematurely => "backend closed prematurely",
        }
    }

    fn cause(&self) -> Option<&error::Error> { None }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ProtocolError::IoError(ref ie) => fmt::Display::fmt(ie, f),
            ProtocolError::InvalidProtocol => write!(f, "invalid protocol"),
            ProtocolError::BackendClosedPrematurely => write!(f, "backend closed prematurely"),
        }
    }
}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> ProtocolError { ProtocolError::IoError(e) }
}

impl From<oneshot::error::RecvError> for ProtocolError {
    fn from(_: oneshot::error::RecvError) -> ProtocolError {
        // It's not the most descriptive, but we really only get receiver errors when
        // we fail to finish a batch to respond back with either the value or the error
        // from the backend, so it sort of fits.
        ProtocolError::BackendClosedPrematurely
    }
}
