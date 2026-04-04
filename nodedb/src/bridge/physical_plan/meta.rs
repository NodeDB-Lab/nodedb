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

    /// Convert a collection's storage mode.
    ///
    /// Scans all documents in the collection and re-encodes them for the
    /// target type. The catalog update happens on the Control Plane after
    /// the Data Plane confirms success.
    ///
    /// `target_type`: "document", "strict", "kv".
    /// `schema_json`: for "strict"/"kv", JSON-serialized column definitions.
    ConvertCollection {
        collection: String,
        target_type: String,
        schema_json: String,
    },

    /// Snapshot a tenant's data from the sparse engine.
    /// Returns serialized `(documents, indexes)` as JSON payload.
    CreateTenantSnapshot { tenant_id: u32 },

    /// Restore a tenant's data to the sparse engine.
    /// Writes documents and indexes from the serialized payload.
    RestoreTenantSnapshot {
        tenant_id: u32,
        /// JSON-serialized `Vec<(String, Vec<u8>)>` of (key, value) pairs.
        documents: Vec<u8>,
        /// JSON-serialized `Vec<(String, Vec<u8>)>` of index entries.
        indexes: Vec<u8>,
    },

    /// Refresh a materialized view: scan source collection, write to target.
    RefreshMaterializedView {
        /// View name (also the target collection name).
        view_name: String,
        /// Source collection to scan.
        source_collection: String,
    },

    /// Purge ALL data for a tenant across every engine and cache.
    ///
    /// Deletes documents, indexes, vectors, graph edges, timeseries,
    /// KV entries, CRDT state, inverted index terms, and cache entries.
    /// Idempotent: safe to re-run after a crash.
    PurgeTenant { tenant_id: u32 },

    /// Enforce retention on a timeseries collection: drop segments older than
    /// the cutoff. Called by the retention policy enforcement loop.
    EnforceTimeseriesRetention { collection: String, max_age_ms: i64 },

    /// Apply retention to continuous aggregate buckets managed by
    /// the aggregate manager. Drops materialized buckets older than
    /// each aggregate's configured retention_period_ms.
    ApplyContinuousAggRetention,
}
