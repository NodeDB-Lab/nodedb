//! Control plane meta-operations dispatched to the Data Plane.

use crate::types::RequestId;

/// Meta / maintenance physical operations.
#[derive(Debug, Clone)]
pub enum MetaOp {
    /// WAL append (write path).
    WalAppend { payload: Vec<u8> },

    /// Cancellation signal. Data Plane stops the target at next safe point.
    Cancel { target_request_id: RequestId },

    /// Atomic transaction batch: execute all sub-plans atomically.
    TransactionBatch { plans: Vec<super::PhysicalPlan> },

    /// Create a snapshot: export all engine state for this core.
    CreateSnapshot,

    /// On-demand compaction.
    Compact,

    /// Checkpoint: flush all engine state to disk, report LSN.
    Checkpoint,
}
