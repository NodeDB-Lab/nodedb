// SPDX-License-Identifier: BUSL-1.1

//! Node-global Calvin observability counters.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Node-global Calvin observability counters, incremented by the per-vShard
/// schedulers and the data-group apply loops, and read by tests and metrics
/// without reaching into the `!Send` Data-Plane state. All start at 0 and stay
/// 0 in single-node / no-Calvin deployments.
pub struct CalvinCounters {
    /// Count of committed Calvin slices whose stamped redo installed on this
    /// node. The install records the slice's write versions into the
    /// per-core write-version index.
    pub write_versions_recorded: Arc<AtomicU64>,
    /// Count of committed Calvin applies whose participant reported that its
    /// slice of the transaction's reads was no longer current at apply time.
    /// Observation only — the apply still committed.
    pub read_set_validation_failures: Arc<AtomicU64>,
    /// Count of staged Calvin transactions the scheduler resolved to COMMIT.
    /// Each commit's stamped redo installs its staged writes.
    pub commits_flushed: Arc<AtomicU64>,
    /// Count of staged Calvin transactions the scheduler resolved to ABORT
    /// by dispatching a drop of their commit-pending buffer, mirroring
    /// [`CalvinCounters::commits_flushed`].
    pub commits_dropped: Arc<AtomicU64>,
    /// Count of stamped Calvin redo copies that applied on this node and
    /// installed nothing: their position was applied, or another copy held
    /// its claim.
    pub redo_copies_skipped: Arc<AtomicU64>,
}
