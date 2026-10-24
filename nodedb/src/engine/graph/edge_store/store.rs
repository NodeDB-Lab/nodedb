// SPDX-License-Identifier: BUSL-1.1

//! EdgeStore root — types, redb table definitions, open/close.
//!
//! Query paths (`get_edge`, `scan_*`, `put_edge_raw`) live in `scan.rs`.
//! Node-level edge reads and journalled cascade replay live in `cascade.rs`. Bitemporal
//! write/read primitives live in `temporal/`.

use std::path::Path;
use std::sync::Arc;

use nodedb_types::{DatabaseId, TenantId};
use redb::{Database, TableDefinition};

use super::node_labels::{NODE_LABEL_LSN, NODE_LABEL_VERSIONS};
use super::stats::GRAPH_STATS;
use super::temporal::visibility::{EDGE_APPLIED, EDGE_CUTS};
use crate::engine::durability_gate::GatedDatabase;

/// `(collection, src, label, dst)` — a base-edge identity (no version suffix).
pub(super) type BaseKey = (String, String, String, String);

/// Database- and tenant-qualified `BaseKey`. Used when scanning across tenants.
pub(super) type TenantBaseKey = (u64, u64, String, String, String, String);

/// Edge table: composite key `(db, tid, "collection\x00src\x00label\x00dst\x00{system_from:020}")` → value.
///
/// Value is either an `EdgeValuePayload` (zerompk fixarray-5) or a single-byte
/// sentinel (`TOMBSTONE_SENTINEL`, `GDPR_ERASURE_SENTINEL`).
pub(super) const EDGES: TableDefinition<(u64, u64, &str), &[u8]> = TableDefinition::new("edges");

/// Reverse edge index: same versioned key shape as `EDGES` but with
/// `dst`/`src` swapped. Value is empty for live edges, or a sentinel for
/// soft-deleted / erased edges (symmetry with forward).
pub(super) const REVERSE_EDGES: TableDefinition<(u64, u64, &str), &[u8]> =
    TableDefinition::new("reverse_edges");

/// Node identity table: `(db, tid, node_name)` → global surrogate.
///
/// The CSR is rebuilt from `EDGES` on every open, and the edge record names its
/// endpoints but says nothing about their global identity. Without this table
/// the rebuilt index has no surrogate bound to any node until live writes
/// repopulate it, so every cross-engine read — which meets other engines on the
/// surrogate — silently sees an empty graph side after a restart. One row per
/// node, not per edge: identity belongs to the node.
pub(super) const NODE_SURROGATES: TableDefinition<(u64, u64, &str), u32> =
    TableDefinition::new("node_surrogates");

pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("{ctx}: {e}"),
    }
}

// Re-export shared Direction from nodedb-types.
pub use nodedb_types::graph::Direction;

/// Decoded edge record yielded by `EdgeStore::scan_all_edges_decoded`:
/// `(database, tenant, collection, src, label, dst, properties)`. Current-state
/// only — each base key resolves to its newest version no TRUNCATE hides, and
/// a base whose version is a tombstone or a GDPR erasure is left out.
pub type EdgeRecord = (
    DatabaseId,
    TenantId,
    String,
    String,
    String,
    String,
    Vec<u8>,
);

/// A single edge with its properties.
#[derive(Debug, Clone)]
pub struct Edge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub properties: Vec<u8>,
}

/// redb-backed edge storage for the Knowledge Graph engine.
///
/// Keys are `(TenantId, versioned_composite_key)` tuples — tenant routing
/// is structural, not lexical. Each Data Plane core owns its own
/// `EdgeStore` instance; no cross-core sharing.
///
/// The database sits behind a durability gate (see
/// [`crate::engine::durability_gate`]).
pub struct EdgeStore {
    pub(super) db: Arc<GatedDatabase>,
    /// The newest WAL LSN a node-label version recorded when the store
    /// opened. Boot replay writes a label version only above it.
    pub(super) label_lsn_floor: u64,
}

impl EdgeStore {
    /// Open or create the edge store database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        let write_txn = db.begin_write().map_err(|e| redb_err("begin_write", e))?;
        {
            let _ = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let _ = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
            let _ = write_txn
                .open_table(GRAPH_STATS)
                .map_err(|e| redb_err("open graph_stats", e))?;
            let _ = write_txn
                .open_table(NODE_SURROGATES)
                .map_err(|e| redb_err("open node_surrogates", e))?;
            let _ = write_txn
                .open_table(EDGE_CUTS)
                .map_err(|e| redb_err("open edge_cuts", e))?;
            let _ = write_txn
                .open_table(EDGE_APPLIED)
                .map_err(|e| redb_err("open edge_applied", e))?;
            let _ = write_txn
                .open_table(NODE_LABEL_VERSIONS)
                .map_err(|e| redb_err("open node_label_versions", e))?;
            let _ = write_txn
                .open_table(NODE_LABEL_LSN)
                .map_err(|e| redb_err("open node_label_lsn", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        let label_lsn_floor = Self::read_label_lsn(&db)?;

        Ok(Self {
            db: Arc::new(GatedDatabase::new(db)),
            label_lsn_floor,
        })
    }

    /// The underlying redb database.
    pub fn db(&self) -> &Arc<GatedDatabase> {
        &self.db
    }

    /// Persist every commit the durability gate deferred.
    pub fn persist(&self) -> crate::Result<()> {
        self.db
            .persist()
            .map_err(|e| redb_err("persist deferred commits", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_tables_in_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let _ = EdgeStore::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let _a = EdgeStore::open(&path).unwrap();
        drop(_a);
        let _b = EdgeStore::open(&path).unwrap();
    }
}
