// SPDX-License-Identifier: BUSL-1.1

//! Aggregate stats returned to the client at the end of a RESTORE TENANT.

use serde::Serialize;

/// Aggregate stats returned to the client at the end of a restore.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RestoreStats {
    pub tenant_id: u64,
    pub dry_run: bool,
    pub sections: u16,
    pub source_vshard_count: u16,
    pub documents: usize,
    pub indexes: usize,
    pub edges: usize,
    pub vectors: usize,
    pub kv_tables: usize,
    pub crdt_state: usize,
    pub timeseries: usize,
    pub columnar_engines: usize,
    pub flushed_ts_segments: usize,
    /// Number of timeseries collections re-issued durably (Raft/WAL) on restore.
    pub timeseries_reissued: usize,
    /// Number of CRDT tenant-snapshot imports re-issued durably (Raft/WAL) on
    /// restore — one per distinct data group that owns any CRDT collection.
    pub crdt_reissued: usize,
    /// Number of individual vectors re-issued durably (Raft/WAL) on restore.
    pub vectors_reissued: usize,
    /// Number of individual KV rows re-issued durably (Raft/WAL) on restore.
    pub kv_reissued: usize,
    /// Number of (collection, field) vector-index HNSW/PQ/IVF configs
    /// re-issued durably (Raft/WAL) on restore.
    pub vector_params_reissued: usize,
    /// Number of PK→surrogate identity bindings rebound into the catalog.
    pub surrogate_pk: usize,
    /// Document sub-records re-issued: one per current row, one per version
    /// of a `bitemporal=true` row.
    pub documents_reissued: usize,
    /// Edge versions re-issued.
    pub edges_reissued: usize,
    /// Redo records the document and edge re-issue committed.
    pub redo_records: usize,
}
