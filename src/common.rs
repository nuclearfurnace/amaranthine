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
use bytes::BytesMut;
use futures::sync::oneshot::{channel, Receiver, Sender};

pub trait Message {
    fn key(&self) -> &[u8];
    fn is_inline(&self) -> bool;
    fn into_buf(self) -> BytesMut;
}

/// Message response types for a queued message.
#[derive(Debug)]
pub enum MessageResponse<T> {
    /// The message ultimately "failed".  This happens if a queued message is dropped before having
    /// a response sent for it, which may happen if an error occurs during the backend read, etc.
    Failed,

    /// The message was processed and a response was received.
    Complete(T),
}

// Core types.  These define the transformation between raw messages that come in over
// the transport and the interstitial types as they're batched, fragmented, etc.
pub type AssignedRequest<T> = (usize, T);
pub type AssignedResponse<T> = (usize, MessageResponse<T>);
pub type AssignedRequests<T> = Vec<AssignedRequest<T>>;
pub type AssignedResponses<T> = Vec<AssignedResponse<T>>;

pub type PendingResponse<T> = Receiver<AssignedResponse<T>>;
pub type EnqueuedRequests<T> = Vec<EnqueuedRequest<T>>;

pub struct EnqueuedRequest<T> {
    id: usize,
    request: Option<T>,
    done: bool,
    tx: Option<Sender<AssignedResponse<T>>>,
}

impl<T> EnqueuedRequest<T> {
    pub fn new(id: usize, request: T) -> (PendingResponse<T>, EnqueuedRequest<T>) {
        let (tx, rx) = channel();
        let er = EnqueuedRequest {
            id,
            request: Some(request),
            tx: Some(tx),
            done: false,
        };

        (rx, er)
    }

    pub fn with_noop(request: T) -> EnqueuedRequest<T> {
        EnqueuedRequest {
            id: 0,
            request: Some(request),
            tx: None,
            done: true,
        }
    }

    pub fn consume(&mut self) -> T { self.request.take().unwrap() }

    pub fn fulfill(&mut self, response: T) {
        if !self.done {
            let _ = self
                .tx
                .take()
                .unwrap()
                .send((self.id, MessageResponse::Complete(response)));
            self.done = true;
        }
    }
}

impl<T> Drop for EnqueuedRequest<T> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.tx.take().unwrap().send((self.id, MessageResponse::Failed));
        }
    }
}
