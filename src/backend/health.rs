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
use futures::{future::ok, sync::mpsc, Future};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::timer::Delay;
use util::typeless;

pub(crate) struct BackendHealth {
    cooloff_enabled: bool,
    cooloff_period_ms: u64,
    error_limit: usize,
    error_count: Arc<AtomicUsize>,
    in_cooloff: Arc<AtomicBool>,
    updates_tx: mpsc::UnboundedSender<()>,
}

impl BackendHealth {
    pub fn new(
        cooloff_enabled: bool, cooloff_period_ms: u64, error_limit: usize, updates_tx: mpsc::UnboundedSender<()>,
    ) -> BackendHealth {
        debug!(
            "[backend health] cooloff enabled: {}, cooloff period (ms): {}, error limit: {}",
            cooloff_enabled, cooloff_period_ms, error_limit
        );

        BackendHealth {
            cooloff_enabled,
            cooloff_period_ms,
            error_limit,
            error_count: Arc::new(AtomicUsize::new(0)),
            in_cooloff: Arc::new(AtomicBool::new(false)),
            updates_tx,
        }
    }

    pub fn is_healthy(&self) -> bool { !self.cooloff_enabled || !self.in_cooloff.load(SeqCst) }

    pub fn increment_error(&self) {
        if !self.cooloff_enabled {
            return;
        }

        let mut final_error_count = 0;
        loop {
            // Keep looping until we increment the counter.
            let curr_count = self.error_count.load(SeqCst);
            final_error_count = curr_count + 1;
            if self.error_count.compare_and_swap(curr_count, final_error_count, SeqCst) == curr_count {
                break;
            }
        }

        // If we're over the error threshold, put us into cooloff.  Someone else may have beaten us
        // to the punch, so just check first.
        if final_error_count >= self.error_limit {
            debug!("[health] error count over limit, setting cooloff");
            let cooloff = self.in_cooloff.load(SeqCst);
            if !cooloff && !self.in_cooloff.compare_and_swap(false, true, SeqCst) {
                // If we're here, we weren't in cooloff _and_ nobody else was contending us.  Set
                // up the cooloff check task.
                debug!("[health] firing cooloff check");
                self.fire_cooloff_check();
            }
        }
    }

    fn fire_cooloff_check(&self) {
        let in_cooloff = self.in_cooloff.clone();
        let error_count = self.error_count.clone();
        let updates_tx = self.updates_tx.clone();
        let deadline = Instant::now() + Duration::from_millis(self.cooloff_period_ms);
        let delay = Delay::new(deadline).then(move |_| {
            debug!("[health] resetting cooloff");
            in_cooloff.store(false, SeqCst);
            error_count.store(0, SeqCst);
            let _ = updates_tx.unbounded_send(());
            ok::<_, ()>(())
        });

        tokio::spawn(typeless(delay));
    }
}
