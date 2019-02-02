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
use futures::{future::ok, task, Future};
use std::time::{Duration, Instant};
use tokio::timer::Delay;
use util::typeless;

pub struct BackendHealth {
    cooloff_enabled: bool,
    cooloff_period_ms: u64,
    error_limit: usize,
    error_count: usize,
    in_cooloff: bool,
    epoch: u64,
    cooloff_done_at: Instant,
}

impl BackendHealth {
    pub fn new(cooloff_enabled: bool, cooloff_period_ms: u64, error_limit: usize) -> BackendHealth {
        debug!(
            "[backend health] cooloff enabled: {}, cooloff period (ms): {}, error limit: {}",
            cooloff_enabled, cooloff_period_ms, error_limit
        );

        BackendHealth {
            cooloff_enabled,
            cooloff_period_ms,
            error_limit,
            error_count: 0,
            in_cooloff: false,
            epoch: 0,
            cooloff_done_at: Instant::now(),
        }
    }

    pub fn is_healthy(&mut self) -> bool {
        if !self.cooloff_enabled || !self.in_cooloff {
            return true;
        }

        if self.cooloff_done_at < Instant::now() {
            self.error_count = 0;
            self.in_cooloff = false;
            self.epoch += 1;

            return true;
        }

        false
    }

    pub fn epoch(&self) -> u64 { self.epoch }

    pub fn increment_error(&mut self) {
        if !self.cooloff_enabled {
            return;
        }

        self.error_count += 1;

        // If we're over the error threshold, put ourselves into cooloff.
        if self.error_count >= self.error_limit && !self.in_cooloff {
            debug!("[health] error count over limit, setting cooloff");
            self.in_cooloff = true;
            self.epoch += 1;
            self.fire_cooloff_check();
        }
    }

    fn fire_cooloff_check(&mut self) {
        // Mark when our cooloff period should be lifted, and trigger a task notification to fire
        // once that deadline has passed: our health will be checked, and thus we can reenable
        // ourselves.
        let deadline = Instant::now() + Duration::from_millis(self.cooloff_period_ms);
        self.cooloff_done_at = deadline;

        let this = task::current();
        let delay = Delay::new(deadline).then(move |_| {
            debug!("[health] resetting cooloff");
            this.notify();
            ok::<_, ()>(())
        });

        tokio::spawn(typeless(delay));
    }
}
