// SPDX-License-Identifier: BUSL-1.1

//! Aggregate stats returned to the client at the end of a RESTORE TENANT.

use serde::Serialize;

use crate::types::TenantDataSnapshot;

/// Aggregate stats returned to the client at the end of a restore.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RestoreStats {
    pub tenant_id: u64,
    pub dry_run: bool,
    pub sections: u16,
    /// Number of databases the backup covers.
    pub databases: usize,
    /// Number of those databases the restore created on this cluster.
    pub databases_created: usize,
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
    /// Number of KV keys re-issued durably on restore, each with every
    /// version the backup holds.
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
    /// Array catalog rows created or applied to an existing array.
    pub arrays: usize,
    /// Array cell versions re-issued.
    pub array_cells_reissued: usize,
    /// Collection parts whose destination row count and digest matched the
    /// backup's. `0` on a dry run, which verifies nothing.
    pub verified_collections: usize,
    /// Rows those collection parts hold.
    pub verified_rows: u64,
    /// Rows per restored collection. A restore lists the rows it verified. A
    /// dry run lists the rows the envelope holds.
    pub collection_rows: Vec<CollectionRows>,
}

/// The rows one collection of a restore holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectionRows {
    /// The destination database, `None` on a dry run for a database this
    /// cluster lacks.
    pub database_id: Option<u64>,
    pub collection: String,
    pub rows: u64,
}

impl RestoreStats {
    /// Add the section sizes of one database's merged snapshot. The
    /// columnar count is the number re-issued, so a restore adds it as it
    /// re-issues and a dry run adds the section size.
    pub fn count_sections(&mut self, snap: &TenantDataSnapshot) {
        self.documents += snap.documents_versioned.len();
        self.indexes += snap.indexes_versioned.len();
        self.edges += snap.edges.len();
        self.vectors += snap.vectors.len();
        self.kv_tables += snap.kv_tables.len();
        // CRDT state is one entry per (tenant, collection).
        self.crdt_state += snap.crdt_state.len();
        self.timeseries += snap.timeseries.len();
        self.flushed_ts_segments += snap.flushed_ts_segments.len();
        self.surrogate_pk += snap.surrogate_pk.len();
    }

    /// Add the counts of another tenant's restore. A database restore sums
    /// its tenants. `tenant_id`, `dry_run` and `source_vshard_count` stay.
    pub fn absorb(&mut self, other: &RestoreStats) {
        // Destructure exhaustively so a new count is not dropped here.
        let RestoreStats {
            tenant_id: _,
            dry_run: _,
            sections,
            databases,
            databases_created,
            source_vshard_count: _,
            documents,
            indexes,
            edges,
            vectors,
            kv_tables,
            crdt_state,
            timeseries,
            columnar_engines,
            flushed_ts_segments,
            timeseries_reissued,
            crdt_reissued,
            vectors_reissued,
            kv_reissued,
            vector_params_reissued,
            surrogate_pk,
            documents_reissued,
            edges_reissued,
            redo_records,
            arrays,
            array_cells_reissued,
            verified_collections,
            verified_rows,
            collection_rows,
        } = other;
        self.sections = self.sections.saturating_add(*sections);
        self.databases = self.databases.max(*databases);
        self.databases_created += databases_created;
        self.documents += documents;
        self.indexes += indexes;
        self.edges += edges;
        self.vectors += vectors;
        self.kv_tables += kv_tables;
        self.crdt_state += crdt_state;
        self.timeseries += timeseries;
        self.columnar_engines += columnar_engines;
        self.flushed_ts_segments += flushed_ts_segments;
        self.timeseries_reissued += timeseries_reissued;
        self.crdt_reissued += crdt_reissued;
        self.vectors_reissued += vectors_reissued;
        self.kv_reissued += kv_reissued;
        self.vector_params_reissued += vector_params_reissued;
        self.surrogate_pk += surrogate_pk;
        self.documents_reissued += documents_reissued;
        self.edges_reissued += edges_reissued;
        self.redo_records += redo_records;
        self.arrays += arrays;
        self.array_cells_reissued += array_cells_reissued;
        self.verified_collections += verified_collections;
        self.verified_rows += verified_rows;
        self.collection_rows.extend(collection_rows.iter().cloned());
    }
}
