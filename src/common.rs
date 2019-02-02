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
use tokio::sync::oneshot::{channel, Receiver, Sender};
use util::Sizable;

pub trait Message: Sizable {
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

// Core types.
//
// These define the transformation between raw messages that come in over the transport and the
// interstitial types as they're batched, fragmented, etc.
pub type AssignedRequest<T> = (usize, T);
pub type AssignedResponse<T> = (usize, MessageResponse<T>);
pub type AssignedRequests<T> = Vec<AssignedRequest<T>>;
pub type AssignedResponses<T> = Vec<AssignedResponse<T>>;

pub type PendingResponse<T> = Receiver<AssignedResponse<T>>;
pub type PendingResponses<T> = Vec<PendingResponse<T>>;
pub type EnqueuedRequests<T> = Vec<EnqueuedRequest<T>>;

pub struct EnqueuedRequest<T: Clone + Message> {
    id: usize,
    request: Option<T>,
    has_response: bool,
    done: bool,
    tx: Option<Sender<AssignedResponse<T>>>,
}

impl<T: Clone + Message> EnqueuedRequest<T> {
    pub fn new(id: usize, request: T) -> EnqueuedRequest<T> {
        EnqueuedRequest {
            id,
            request: Some(request),
            tx: None,
            has_response: true,
            done: false,
        }
    }

    pub fn without_response(request: T) -> EnqueuedRequest<T> {
        EnqueuedRequest {
            id: 0,
            request: Some(request),
            tx: None,
            has_response: false,
            done: true,
        }
    }

    pub fn key(&self) -> &[u8] {
        // Pass-through for `Message::key` because we really don't want to expose the
        // entire Message trait over ourselves, as one of the methods allows taking
        // the request by consuming self.
        self.request.as_ref().expect("tried to get key for empty request").key()
    }

    pub fn consume(&mut self) -> T { self.request.take().unwrap() }

    pub fn fulfill(&mut self, response: T) {
        if self.done {
            return;
        }

        let _ = self
            .tx
            .take()
            .expect("tried to send response to uninitialized receiver")
            .send((self.id, MessageResponse::Complete(response)));
        self.done = true;
    }

    pub fn get_response_rx(&mut self) -> Option<PendingResponse<T>> {
        if self.has_response {
            let (tx, rx) = channel();
            self.tx = Some(tx);
            self.done = false;
            self.has_response = false;

            return Some(rx);
        }

        None
    }
}

impl<T: Clone + Message> Drop for EnqueuedRequest<T> {
    fn drop(&mut self) {
        // The drop guard is used to make sure we always send back a response to the upper
        // layers even if a backend has an error that kills an entire batch of requests.
        if !self.done {
            let _ = self.tx.take().unwrap().send((self.id, MessageResponse::Failed));
        }
    }
}
