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
use std::{error, fmt, io};

#[derive(Debug)]
pub enum RouterError {
    BadRequest(String),
    BadResponse(String),
}

impl RouterError {
    pub fn from_processor(e: &ProcessorError) -> Self {
        let desc = e.to_string();
        match e {
            ProcessorError::FragmentError(_) => RouterError::BadRequest(desc),
            ProcessorError::DefragmentError(_) => RouterError::BadResponse(desc),
        }
    }
}

impl From<ProcessorError> for RouterError {
    fn from(e: ProcessorError) -> Self { RouterError::from_processor(&e) }
}

impl Into<io::Error> for RouterError {
    fn into(self) -> io::Error {
        let desc = match self {
            RouterError::BadRequest(s) => s,
            RouterError::BadResponse(s) => s,
        };

        io::Error::new(io::ErrorKind::Other, desc)
    }
}

impl error::Error for RouterError {
    fn description(&self) -> &str {
        match self {
            RouterError::BadRequest(s) => s.as_str(),
            RouterError::BadResponse(s) => s.as_str(),
        }
    }

    fn cause(&self) -> Option<&error::Error> { None }
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            RouterError::BadRequest(s) => write!(f, "bad request: {}", s.as_str()),
            RouterError::BadResponse(s) => write!(f, "bad response: {}", s.as_str()),
        }
    }
}
