// SPDX-License-Identifier: BUSL-1.1

//! Node-level cascade: soft-delete every edge incident on a node.

use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;

use super::store::{BaseKey, EDGES, EdgeStore, NODE_SURROGATES, redb_err};
use super::temporal::write::write_sentinel_in;
use super::temporal::{
    EdgeRef, EdgeValuePayload, EdgeVersionWrite, TOMBSTONE_SENTINEL, is_sentinel,
    parse_versioned_edge_key,
};
use nodedb_types::{DatabaseId, TenantId};

/// A single cascaded edge removal captured for transactional rollback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRestore {
    pub collection: String,
    pub src: String,
    pub label: String,
    pub dst: String,
    /// The edge's properties before the tombstone. The CSR restore reads its
    /// weight from them.
    pub old_properties: Vec<u8>,
    /// The tombstone version the cascade added. A rollback removes it.
    pub tombstone: EdgeVersionWrite,
}

impl EdgeStore {
    /// Soft-delete every edge incident on `node` (as either src or dst) in
    /// the caller's tenant, across all collections, and drop the node's
    /// identity binding. Emits a tombstone version at `system_from` for each
    /// distinct base edge that has a live (non-sentinel) latest version.
    ///
    /// One transaction holds every tombstone and the binding removal, so the
    /// cascade lands whole or not at all.
    ///
    /// Returns the edges actually soft-deleted, each with its pre-delete
    /// properties and its tombstone, so a transactional caller can push one
    /// `UndoEntry::EdgeWrite` per edge and fully reverse the cascade on
    /// rollback. Already-tombstoned bases are skipped and not returned.
    pub fn delete_edges_for_node(
        &self,
        db: u64,
        tid: TenantId,
        node: &str,
        system_from: i64,
    ) -> crate::Result<Vec<EdgeRestore>> {
        // Snapshot all live bases touching `node` in a read txn first.
        let bases = self.live_bases_touching_node(db, tid, node)?;
        let database = DatabaseId::new(db);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let mut removed = Vec::with_capacity(bases.len());
        for ((collection, src, label, dst), old_properties) in bases {
            let tombstone = write_sentinel_in(
                &write_txn,
                EdgeRef::new(database, tid, &collection, &src, &label, &dst),
                system_from,
                TOMBSTONE_SENTINEL,
                true,
            )?;
            removed.push(EdgeRestore {
                collection,
                src,
                label,
                dst,
                old_properties,
                tombstone,
            });
        }
        // The node itself is going away, so its identity binding goes with it.
        // Only this node's: the neighbours survive and keep theirs. A rolled-back
        // delete restores the binding along with the edges (see the transaction
        // undo path), so this is not a one-way loss.
        {
            let mut surrogates = write_txn
                .open_table(NODE_SURROGATES)
                .map_err(|e| redb_err("open node_surrogates", e))?;
            surrogates
                .remove((db, tid.as_u64(), node))
                .map_err(|e| redb_err("remove node surrogate", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit node edge cascade", e))?;
        Ok(removed)
    }

    /// Every base edge in this `(database, tenant)` whose latest version
    /// touches `node` as src or dst and is live, with that version's
    /// properties.
    fn live_bases_touching_node(
        &self,
        db: u64,
        tid: TenantId,
        node: &str,
    ) -> crate::Result<Vec<(BaseKey, Vec<u8>)>> {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Latest version per base: its system time and its raw value.
        let mut latest: HashMap<BaseKey, (i64, Vec<u8>)> = HashMap::new();
        // DB-scoped range: a node-delete in database A must NOT cascade into
        // the same tenant's edges in database B.
        let range = table
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("iter", e))?;
        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("iter entry", e))?;
            let composite = k.value().2;
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if src != node && dst != node {
                continue;
            }
            let base = (
                coll.to_string(),
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            );
            let value = v.value();
            match latest.get_mut(&base) {
                Some((cur, bytes)) if sys > *cur => {
                    *cur = sys;
                    *bytes = value.to_vec();
                }
                Some(_) => {}
                None => {
                    latest.insert(base, (sys, value.to_vec()));
                }
            }
        }
        let mut live = Vec::with_capacity(latest.len());
        for (base, (_sys, bytes)) in latest {
            if is_sentinel(&bytes) {
                continue;
            }
            let properties = EdgeValuePayload::decode(&bytes)?.properties;
            live.push((base, properties));
        }
        Ok(live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::OrdinalClock;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn put(store: &EdgeStore, clock: &OrdinalClock, src: &str, label: &str, dst: &str, p: &[u8]) {
        let ord = clock.next_ordinal();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, COLL, src, label, dst),
                p,
                ord,
                ord,
                i64::MAX,
            )
            .unwrap();
    }

    #[test]
    fn delete_edges_for_node_soft_deletes_all_incident() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"1");
        put(&store, &clock, "alice", "KNOWS", "carol", b"2");
        put(&store, &clock, "dave", "KNOWS", "alice", b"3");
        put(&store, &clock, "eve", "KNOWS", "frank", b"4");

        let purge_ord = clock.next_ordinal();
        let removed = store
            .delete_edges_for_node(D, T, "alice", purge_ord)
            .unwrap();
        // Three live bases touch alice (alice→bob, alice→carol, dave→alice),
        // each returned with its captured pre-delete properties.
        assert_eq!(removed.len(), 3);
        assert!(
            removed
                .iter()
                .any(|r| r.src == "alice" && r.dst == "bob" && r.old_properties == b"1")
        );
        assert!(
            removed
                .iter()
                .any(|r| r.src == "dave" && r.dst == "alice" && r.old_properties == b"3")
        );

        assert!(
            store
                .get_edge(D, T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(D, T, COLL, "alice", "KNOWS", "carol")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(D, T, COLL, "dave", "KNOWS", "alice")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.get_edge(D, T, COLL, "eve", "KNOWS", "frank").unwrap(),
            Some(b"4".to_vec())
        );
    }

    #[test]
    fn delete_edges_for_node_skips_already_tombstoned() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"1");
        store
            .soft_delete_edge(
                EdgeRef::new(DB, T, COLL, "alice", "KNOWS", "bob"),
                clock.next_ordinal(),
            )
            .unwrap();

        // Should be a no-op — no live bases to cascade through.
        let removed = store
            .delete_edges_for_node(D, T, "alice", clock.next_ordinal())
            .unwrap();
        assert!(removed.is_empty());
        assert!(
            store
                .get_edge(D, T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
    }
}
