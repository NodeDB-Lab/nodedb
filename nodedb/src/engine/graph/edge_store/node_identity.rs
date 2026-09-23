// SPDX-License-Identifier: BUSL-1.1

//! Durable node → `Surrogate` bindings.
//!
//! The CSR index is rebuilt from the edge table on every open. An edge record
//! names its endpoints but carries no global identity, so a rebuilt index knows
//! the graph's shape and nothing about how it meets the other engines — which
//! all key on the surrogate. This table is where that binding survives.
//!
//! Writes go through the edge-write transaction (see
//! `temporal::write::put_edge_versioned_with_stats`) so an edge and its
//! endpoints' identities commit together. This module owns the read and the
//! removal.

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::store::{EdgeStore, NODE_SURROGATES, redb_err};

/// One durable identity binding: `(database, tenant, node name, surrogate)`.
pub type NodeSurrogateRecord = (DatabaseId, TenantId, String, u32);

impl EdgeStore {
    /// Every node identity binding in this store, for CSR rebuild.
    ///
    /// One pass over a table with one row per node — the rebuild already walks
    /// every edge, and this adds a walk over a strictly smaller table rather
    /// than a per-node point lookup.
    pub fn scan_all_node_surrogates(&self) -> crate::Result<Vec<NodeSurrogateRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = match read_txn.open_table(NODE_SURROGATES) {
            Ok(t) => t,
            // A store written before this table existed has no bindings to
            // restore. That is the pre-existing state, not an error.
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err("open node_surrogates", e)),
        };

        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| redb_err("iter node_surrogates", e))?;
        for entry in iter {
            let (key, value) = entry.map_err(|e| redb_err("node surrogate entry", e))?;
            let (db, tid, node) = key.value();
            let raw = value.value();
            if raw == 0 {
                continue;
            }
            out.push((
                DatabaseId::new(db),
                TenantId::new(tid),
                node.to_string(),
                raw,
            ));
        }
        Ok(out)
    }

    /// Drop a node's identity binding.
    ///
    /// Called when the node itself goes away. Leaving the row behind would
    /// resurrect the binding on the next rebuild and hand a live surrogate to a
    /// node that no longer exists.
    pub fn delete_node_surrogate(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
    ) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut table = write_txn
                .open_table(NODE_SURROGATES)
                .map_err(|e| redb_err("open node_surrogates", e))?;
            table
                .remove((db.as_u64(), tid.as_u64(), node))
                .map_err(|e| redb_err("remove node surrogate", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Bind `node` to the surrogate `raw`, replacing any binding it had.
    ///
    /// A rolled-back node delete uses it to put back the binding the delete
    /// dropped.
    pub fn bind_node_surrogate(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
        raw: u32,
    ) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut table = write_txn
                .open_table(NODE_SURROGATES)
                .map_err(|e| redb_err("open node_surrogates", e))?;
            table
                .insert((db.as_u64(), tid.as_u64(), node), raw)
                .map_err(|e| redb_err("insert node surrogate", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::graph::edge_store::EdgeRef;

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn edge<'a>(src: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "people",
            src,
            "knows",
            dst,
        )
    }

    #[test]
    fn an_edge_write_persists_both_endpoints_identities() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                edge("a", "b").with_surrogates(Surrogate::new(10), Surrogate::new(20)),
                b"{}",
                100,
                100,
                i64::MAX,
            )
            .unwrap();

        let mut out = store.scan_all_node_surrogates().unwrap();
        out.sort_by(|l, r| l.2.cmp(&r.2));
        assert_eq!(out.len(), 2);
        assert_eq!((out[0].2.as_str(), out[0].3), ("a", 10));
        assert_eq!((out[1].2.as_str(), out[1].3), ("b", 20));
    }

    /// A write with no identities to record must not invent one — the ZERO
    /// sentinel is "unset", and storing it would put a node in the table that
    /// can never intersect anything.
    #[test]
    fn a_write_without_surrogates_records_nothing() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(edge("a", "b"), b"{}", 100, 100, i64::MAX)
            .unwrap();
        assert!(store.scan_all_node_surrogates().unwrap().is_empty());
    }

    #[test]
    fn rebinding_a_node_overwrites_rather_than_duplicates() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                edge("a", "b").with_surrogates(Surrogate::new(10), Surrogate::new(20)),
                b"{}",
                100,
                100,
                i64::MAX,
            )
            .unwrap();
        store
            .put_edge_versioned(
                edge("a", "c").with_surrogates(Surrogate::new(10), Surrogate::new(30)),
                b"{}",
                200,
                200,
                i64::MAX,
            )
            .unwrap();

        let out = store.scan_all_node_surrogates().unwrap();
        assert_eq!(out.len(), 3, "one row per node, not per edge: {out:?}");
        assert_eq!(out.iter().filter(|r| r.2 == "a").count(), 1);
    }

    #[test]
    fn deleting_a_node_drops_its_binding() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                edge("a", "b").with_surrogates(Surrogate::new(10), Surrogate::new(20)),
                b"{}",
                100,
                100,
                i64::MAX,
            )
            .unwrap();
        store
            .delete_node_surrogate(DatabaseId::DEFAULT, TenantId::new(1), "a")
            .unwrap();

        let out = store.scan_all_node_surrogates().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, "b");
    }
}
