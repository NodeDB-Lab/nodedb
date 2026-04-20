//! Control plane meta-operations dispatched to the Data Plane.

use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
use crate::types::RequestId;

/// Meta / maintenance physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
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

    /// Restore a tenant's data across all engines from a snapshot.
    /// `snapshot` is a MessagePack-serialized `TenantDataSnapshot`.
    RestoreTenantSnapshot { tenant_id: u32, snapshot: Vec<u8> },

    /// Pre-computed response payload. The Data Plane echoes it back without
    /// touching any engine. Used for constant queries (SELECT 1 AS value).
    RawResponse { payload: Vec<u8> },

    /// Purge ALL data for a tenant across every engine and cache.
    ///
    /// Deletes documents, indexes, vectors, graph edges, timeseries,
    /// KV entries, CRDT state, inverted index terms, and cache entries.
    /// Idempotent: safe to re-run after a crash.
    PurgeTenant { tenant_id: u32 },

    /// Purge a single collection across every engine on this core.
    ///
    /// Runs the per-collection equivalent of `PurgeTenant`: each
    /// engine (columnar, vector, FTS, spatial, document/strict/kv,
    /// graph, CRDT, timeseries) reclaims its L1 segment files,
    /// memtables, in-flight compactions, and snapshot references
    /// for this one collection, then a WAL `CollectionTombstoned`
    /// record is appended so replay after purge does not resurrect
    /// rows. The quiesce-drain primitive (see `bridge::quiesce`) is
    /// invoked first so in-flight scans complete before file unlink.
    ///
    /// `purge_lsn` is the metadata-raft commit LSN at which the
    /// `PurgeCollection` entry committed; used by the WAL tombstone
    /// filter and surfaced in the audit trail.
    ///
    /// Idempotent: re-running after partial completion picks up
    /// where the previous run left off.
    UnregisterCollection {
        tenant_id: u32,
        name: String,
        purge_lsn: u64,
    },

    /// Reclaim every local Data Plane resource for a single
    /// materialized view. Mirrors `UnregisterCollection` one level
    /// up: an MV has its own columnar segment files (populated by the
    /// CDC refresh loop) that outlive the MV's catalog row when the
    /// MV is dropped, unless reclaim runs on every follower. Drops
    /// the MV's in-memory refresh state + unlinks its segment files.
    ///
    /// Idempotent: missing in-memory state is a no-op; missing files
    /// are a no-op. Runs on every node.
    UnregisterMaterializedView { tenant_id: u32, name: String },

    /// Estimate the on-core data size (in bytes) for a single
    /// `(tenant_id, collection)` pair. Sums per-engine in-memory
    /// state: KV hash-table bytes, columnar flushed-segment byte
    /// count, vector-index byte count, and sparse-redb document
    /// range. Response payload is a u64 LE byte count.
    ///
    /// Used by `_system.dropped_collections.size_bytes_estimate` to
    /// surface "how much storage will a hard-delete reclaim?"
    /// without waiting for a purge cycle.
    QueryCollectionSize { tenant_id: u32, name: String },

    /// Enforce retention on a timeseries collection: drop segments older than
    /// the cutoff. Called by the retention policy enforcement loop.
    EnforceTimeseriesRetention { collection: String, max_age_ms: i64 },

    /// Apply retention to continuous aggregate buckets managed by
    /// the aggregate manager. Drops materialized buckets older than
    /// each aggregate's configured retention_period_ms.
    ApplyContinuousAggRetention,

    /// Query the watermark for a named continuous aggregate.
    /// Returns JSON-serialized `WatermarkState`.
    QueryAggregateWatermark { aggregate_name: String },

    /// Query all entries from a collection's last-value cache.
    /// Returns JSON-serialized `Vec<(u64, i64, f64)>` — (series_id, ts, value).
    QueryLastValues { collection: String },

    /// Query a single series from a collection's last-value cache.
    /// Returns JSON-serialized `Option<(i64, f64)>` — (ts, value).
    QueryLastValue { collection: String, series_id: u64 },
}
