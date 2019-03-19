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
use crate::protocol::errors::ProtocolError;
use std::{
    error::{self, Error},
    fmt, io,
};
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum BackendError {
    Internal(String),
    Protocol(ProtocolError),
    Io(io::Error),
}

#[derive(Debug)]
pub enum PoolError {
    Internal(String),
    Backend(BackendError),
}

impl From<ProcessorError> for BackendError {
    fn from(e: ProcessorError) -> Self {
        let desc = e.to_string();
        match e {
            ProcessorError::FragmentError(_) => BackendError::Internal(desc),
            ProcessorError::DefragmentError(_) => BackendError::Internal(desc),
        }
    }
}

impl From<ProtocolError> for BackendError {
    fn from(e: ProtocolError) -> Self { BackendError::Protocol(e) }
}

impl Into<io::Error> for BackendError {
    fn into(self) -> io::Error {
        match self {
            BackendError::Internal(s) => io::Error::new(io::ErrorKind::Other, s),
            BackendError::Protocol(e) => {
                match e {
                    ProtocolError::IoError(ie) => ie,
                    x => io::Error::new(io::ErrorKind::Other, x.description()),
                }
            },
            BackendError::Io(e) => e,
        }
    }
}

impl From<io::Error> for BackendError {
    fn from(e: io::Error) -> BackendError { BackendError::Io(e) }
}

impl From<oneshot::error::RecvError> for BackendError {
    fn from(_: oneshot::error::RecvError) -> BackendError { BackendError::Internal("receive failed".to_owned()) }
}

impl error::Error for BackendError {
    fn description(&self) -> &str {
        match self {
            BackendError::Internal(s) => s.as_str(),
            BackendError::Protocol(e) => e.description(),
            BackendError::Io(e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> { None }
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::error::Error;

        match self {
            BackendError::Internal(s) => write!(f, "internal error: {}", s.as_str()),
            BackendError::Protocol(pe) => write!(f, "protocol: {}", pe),
            BackendError::Io(e) => write!(f, "internal error: {}", e.description()),
        }
    }
}

impl From<BackendError> for PoolError {
    fn from(e: BackendError) -> PoolError { PoolError::Backend(e) }
}

impl From<oneshot::error::RecvError> for PoolError {
    fn from(_: oneshot::error::RecvError) -> PoolError { PoolError::Internal("receive failed".to_owned()) }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PoolError::Internal(s) => write!(f, "internal error: {}", s.as_str()),
            PoolError::Backend(be) => write!(f, "backend: {}", be),
        }
    }
}
