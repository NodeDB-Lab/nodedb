// SPDX-License-Identifier: BUSL-1.1

//! How far a Data Plane core has emitted events onto its ring.
//!
//! The core numbers every event it emits with a contiguous per-core sequence,
//! and stores the highest one here after each push, whether the push succeeded
//! or the ring was full. The Control Plane reads it to learn which events a
//! barrier must wait for: every event numbered at or below a value read here
//! was emitted before the read.
//!
//! The store happens after the push. So an event whose number a reader sees
//! here is already on the ring, or was dropped from it.

use std::sync::atomic::{AtomicU64, Ordering};

/// The emitted-event counter of one core.
#[derive(Debug, Default)]
pub struct CoreEmitProgress {
    emitted: AtomicU64,
}

impl CoreEmitProgress {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the event numbered `sequence` left the core. Called by the
    /// core's producer only, after the push.
    pub fn note_emitted(&self, sequence: u64) {
        self.emitted.fetch_max(sequence, Ordering::Release);
    }

    /// The highest sequence the core has emitted.
    pub fn emitted(&self) -> u64 {
        self.emitted.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_counter_keeps_the_highest_sequence() {
        let progress = CoreEmitProgress::new();
        assert_eq!(progress.emitted(), 0);
        progress.note_emitted(3);
        progress.note_emitted(2);
        assert_eq!(progress.emitted(), 3);
    }
}
