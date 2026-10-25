// SPDX-License-Identifier: BUSL-1.1

//! Counters for the sequencer state machine apply path.
//!
//! Reported by the cluster metrics endpoint. Every counter is monotonic and
//! `Relaxed` — they are observed, never used to make a decision — so they can
//! be bumped from the Raft apply thread without ordering cost.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Atomic counters for the sequencer state machine apply path.
pub struct StateMachineMetrics {
    /// Total epoch batches successfully applied.
    pub epochs_applied: AtomicU64,
    /// Total transactions fanned out to vshard channels.
    pub txns_fanned_out: AtomicU64,
    /// Transactions dropped because the vshard channel was full.
    pub txns_dropped_backpressure: AtomicU64,
    /// Epochs that arrived ahead of the expected one (entries missing locally).
    /// The arriving batch is still fanned out; the counter records the hole.
    pub epochs_skipped_gap: AtomicU64,
    /// Epoch batches refused because they re-used an already-consumed epoch.
    /// Non-zero means this node halted its sequencer state machine.
    pub epochs_refused_regression: AtomicU64,
    /// Committed entries delivered a second time at an index this replica had
    /// already applied, and therefore skipped as no-ops. Routine on restart —
    /// this is an activity counter, not a fault counter.
    pub entries_redelivered: AtomicU64,
    /// Committed entries the catch-up replay decoded from a log read. Each
    /// entry decodes once per node while the replay cache holds it.
    pub replay_decodes: AtomicU64,
}

impl StateMachineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl Default for StateMachineMetrics {
    fn default() -> Self {
        Self {
            epochs_applied: AtomicU64::new(0),
            txns_fanned_out: AtomicU64::new(0),
            txns_dropped_backpressure: AtomicU64::new(0),
            epochs_skipped_gap: AtomicU64::new(0),
            epochs_refused_regression: AtomicU64::new(0),
            entries_redelivered: AtomicU64::new(0),
            replay_decodes: AtomicU64::new(0),
        }
    }
}
