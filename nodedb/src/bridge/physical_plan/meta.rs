//! Control plane meta-operations dispatched to the Data Plane.

use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
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

    /// Register a continuous aggregate definition on this core's manager.
    RegisterContinuousAggregate { def: ContinuousAggregateDef },

    /// Remove a continuous aggregate from this core's manager.
    UnregisterContinuousAggregate { name: String },

    /// List all continuous aggregates from this core's manager.
    /// Returns JSON-serialized `Vec<AggregateInfo>`.
    ListContinuousAggregates,

    /// Refresh a materialized view: scan source collection, write to target.
    RefreshMaterializedView {
        /// View name (also the target collection name).
        view_name: String,
        /// Source collection to scan.
        source_collection: String,
    },
}
