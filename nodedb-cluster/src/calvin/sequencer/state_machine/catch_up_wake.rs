// SPDX-License-Identifier: BUSL-1.1

//! The wake that tells a vShard's scheduler its catch-up owes a replay.
//!
//! While a vShard's catch-up is armed, the sequencer defers every input for
//! it to the replay. The scheduler drains the replay when this wake fires,
//! so a deferred input reaches it as soon as its entry applied, not at the
//! scheduler's next stall tick.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// One wake per vShard, created on first use by either side.
#[derive(Debug, Default)]
pub struct CatchUpWakes {
    wakes: Mutex<HashMap<u32, Arc<Notify>>>,
}

impl CatchUpWakes {
    /// The wake of `vshard`. The scheduler waits on it.
    pub fn handle(&self, vshard: u32) -> Arc<Notify> {
        let mut wakes = self.wakes.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(wakes.entry(vshard).or_default())
    }

    /// Wake the scheduler of `vshard`. A wake with no waiter is kept until
    /// the scheduler next waits, so none is lost.
    pub fn wake(&self, vshard: u32) {
        self.handle(vshard).notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_wake_before_the_wait_is_kept() {
        let wakes = CatchUpWakes::default();
        let handle = wakes.handle(7);
        wakes.wake(7);
        tokio::time::timeout(std::time::Duration::from_secs(1), handle.notified())
            .await
            .expect("the kept wake fires");
    }
}
