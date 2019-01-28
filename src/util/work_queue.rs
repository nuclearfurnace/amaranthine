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
use crossbeam::{
    channel::{unbounded, Receiver as ChannelReceiver, Sender as ChannelSender},
    queue::MsQueue,
};
use futures::{
    prelude::*,
    task::{self, Task},
};
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

/// A single-producer-multiple-consumer queue used for distributing work to multiple workers.
pub struct WorkQueue<T> {
    work_tx: ChannelSender<T>,
    work_rx: ChannelReceiver<T>,
    wait_tx: ChannelSender<Weak<Option<Task>>>,
    wait_rx: ChannelReceiver<Weak<Option<Task>>>,
    waiters: MsQueue<Weak<Option<Task>>>,
}

/// Worker side of `WorkQueue`.
///
/// Workers can pull items from the queue.  If no items are available, the worker will be parked
/// and notified when an item is available, in FIFO order.
pub struct Worker<T> {
    work_rx: ChannelReceiver<T>,
    wait_tx: ChannelSender<Weak<Option<Task>>>,
    waiter: Arc<Option<Task>>,
}

impl<T> WorkQueue<T> {
    pub fn new() -> WorkQueue<T> {
        let (work_tx, work_rx) = unbounded();
        let (wait_tx, wait_rx) = unbounded();

        WorkQueue {
            work_tx,
            work_rx,
            wait_tx,
            wait_rx,
            waiters: MsQueue::new(),
        }
    }

    pub fn send(&self, item: T) {
        self.work_tx.send(item);
        let _ = self.notify_one();
    }

    fn notify_one(&self) -> bool {
        while let Ok(waiter) = self.wait_rx.try_recv() {
            self.waiters.push(waiter)
        }

        loop {
            match self.waiters.try_pop() {
                // No receivers waiting.  They'll check the work queue at least once during their
                // initial `poll`, so we're not losing wakeups here.
                None => return false,
                Some(waiter) => {
                    match waiter.upgrade() {
                        Some(maybe_task) => {
                            match maybe_task.deref() {
                                Some(task) => {
                                    // The receiver hasn't dropped yet, so notify it.
                                    task.notify();
                                    return true;
                                },
                                None => {
                                    // This shouldn't happen because it means we were able to upgrade
                                    // our weak reference to the task handle, but that the internal
                                    // value was changed... and we should only get get a strong
                                    // reference if the value was set and then passed over to us....
                                    unreachable!()
                                },
                            }
                        },
                        // Looks like the receiver dropped before we had a chance to notify them,
                        // so just continue looping to notify the next one in line.
                        None => continue,
                    }
                },
            }
        }
    }

    pub fn worker(&self) -> Worker<T> {
        Worker {
            work_rx: self.work_rx.clone(),
            wait_tx: self.wait_tx.clone(),
            waiter: Arc::new(None),
        }
    }
}

impl<T> Drop for WorkQueue<T> {
    fn drop(&mut self) { while self.notify_one() {} }
}

impl<T> Worker<T> {
    fn park(&mut self) {
        self.waiter = Arc::new(Some(task::current()));
        self.wait_tx.send(Arc::downgrade(&self.waiter));
    }
}

impl<T> Clone for Worker<T> {
    fn clone(&self) -> Worker<T> {
        Worker {
            work_rx: self.work_rx.clone(),
            wait_tx: self.wait_tx.clone(),
            waiter: Arc::new(None),
        }
    }
}

impl<T> Stream for Worker<T> {
    type Error = ();
    type Item = T;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        select! {
            recv(self.work_rx) -> msg => match msg {
                Ok(x) => Ok(Async::Ready(Some(x))),
                Err(_) => Ok(Async::Ready(None)),
            },
            default => {
                self.park();
                Ok(Async::NotReady)
            },
        }
    }
}
