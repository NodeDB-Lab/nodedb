// SPDX-License-Identifier: BUSL-1.1

//! Node-level edge reads, and the replay of journalled node cascades.
//!
//! A live delete never cascades here: its transaction tombstones the edges
//! it removes with `EdgeDelete`, or cuts the collection with `TruncateEdges`
//! for a TRUNCATE. A node cascade record an older WAL journalled replays at
//! its own ordinals through [`EdgeStore::apply_node_cascade`].

use redb::ReadableTable;

use super::store::{Direction, EDGES, EdgeStore, NODE_SURROGATES, redb_err};
use super::temporal::NeighborsAsOfParams;
use super::temporal::write::{VersionStamp, write_version_in};
use super::temporal::{EdgeRef, TOMBSTONE_SENTINEL, is_sentinel};
use nodedb_types::{DatabaseId, TenantId};

impl EdgeStore {
    /// Write the tombstones a node cascade journalled: one for each edge of
    /// `edges` at the edge's journalled ordinal, then drop `node`'s identity
    /// binding, all in one transaction. A key that already holds a tombstone
    /// is left as it is, so re-applying a cascade changes nothing and counts
    /// no edge twice.
    pub fn apply_node_cascade(
        &self,
        db: u64,
        tid: TenantId,
        node: &str,
        edges: &[crate::wal::CascadedEdge],
    ) -> crate::Result<()> {
        let database = DatabaseId::new(db);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        for edge in edges {
            let key = super::temporal::versioned_edge_key(
                &edge.collection,
                &edge.src,
                &edge.label,
                &edge.dst,
                edge.system_from,
            )?;
            let tombstoned = {
                let table = write_txn
                    .open_table(EDGES)
                    .map_err(|e| redb_err("open edges", e))?;
                table
                    .get((db, tid.as_u64(), key.as_str()))
                    .map_err(|e| redb_err("read cascaded edge", e))?
                    .is_some_and(|value| is_sentinel(value.value()))
            };
            if tombstoned {
                continue;
            }
            write_version_in(
                &write_txn,
                EdgeRef::new(
                    database,
                    tid,
                    &edge.collection,
                    &edge.src,
                    &edge.label,
                    &edge.dst,
                ),
                VersionStamp::at(edge.system_from),
                TOMBSTONE_SENTINEL,
                true,
            )?;
        }
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
            .map_err(|e| redb_err("commit journalled node cascade", e))
    }

    /// Every live edge of `collection` with `node` as source or destination,
    /// as `(src, label, dst)` in sorted order. A node delete's guard compares
    /// it against the edges the planner read with a current-state
    /// `TemporalNeighbors`, which reads through [`EdgeStore::node_edges_as_of`]
    /// too.
    pub fn live_edges_of_node(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        node: &str,
    ) -> crate::Result<Vec<(String, String, String)>> {
        self.node_edges_as_of(
            NeighborsAsOfParams {
                db,
                tid,
                collection,
                node,
                label_filter: None,
                cutoff_ordinal: i64::MAX,
                valid_at_ms: None,
            },
            Direction::Both,
        )
    }

    /// The edges of `params.node` in `direction` that are visible at the
    /// cutoffs of `params`, as `(src, label, dst)` in sorted order. A
    /// self-loop appears once.
    pub fn node_edges_as_of(
        &self,
        params: NeighborsAsOfParams<'_>,
        direction: Direction,
    ) -> crate::Result<Vec<(String, String, String)>> {
        let mut edges = std::collections::BTreeSet::new();
        if matches!(direction, Direction::Out | Direction::Both) {
            for edge in self.neighbors_out_as_of(params)? {
                edges.insert((edge.src_id, edge.label, edge.dst_id));
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for edge in self.neighbors_in_as_of(params)? {
                edges.insert((edge.src_id, edge.label, edge.dst_id));
            }
        }
        Ok(edges.into_iter().collect())
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

    fn put_in(store: &EdgeStore, clock: &OrdinalClock, coll: &str, src: &str, dst: &str) -> i64 {
        let ord = clock.next_ordinal();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, coll, src, "KNOWS", dst),
                b"p",
                ord,
                ord,
                i64::MAX,
            )
            .unwrap();
        ord
    }

    fn edge(src: &str, dst: &str) -> (String, String, String) {
        (src.to_string(), "KNOWS".to_string(), dst.to_string())
    }

    /// A node's live edges are the edges of its own collection that name it
    /// at either end. A tombstoned edge and another collection's edge are
    /// not among them.
    #[test]
    fn a_nodes_live_edges_stay_in_its_collection() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put_in(&store, &clock, COLL, "alice", "bob");
        put_in(&store, &clock, COLL, "dave", "alice");
        put_in(&store, &clock, COLL, "alice", "carol");
        put_in(&store, &clock, "social", "alice", "erin");
        put_in(&store, &clock, COLL, "eve", "frank");
        store
            .soft_delete_edge(
                EdgeRef::new(DB, T, COLL, "alice", "KNOWS", "carol"),
                clock.next_ordinal(),
            )
            .unwrap();

        assert_eq!(
            store.live_edges_of_node(D, T, COLL, "alice").unwrap(),
            vec![edge("alice", "bob"), edge("dave", "alice")]
        );
        assert_eq!(
            store.live_edges_of_node(D, T, "social", "alice").unwrap(),
            vec![edge("alice", "erin")]
        );
    }

    /// A journalled cascade re-applies each edge's own tombstone ordinal.
    #[test]
    fn a_journalled_cascade_writes_each_edge_at_its_ordinal() {
        let (store, _dir) = make_store();
        for dst in ["bob", "carol"] {
            store
                .put_edge_versioned(
                    EdgeRef::new(DB, T, COLL, "alice", "KNOWS", dst),
                    b"p",
                    100,
                    100,
                    i64::MAX,
                )
                .unwrap();
        }
        let edges: Vec<crate::wal::CascadedEdge> = [("bob", 200), ("carol", 300)]
            .into_iter()
            .map(|(dst, system_from)| crate::wal::CascadedEdge {
                collection: COLL.into(),
                src: "alice".into(),
                label: "KNOWS".into(),
                dst: dst.into(),
                system_from,
            })
            .collect();
        store.apply_node_cascade(D, T, "alice", &edges).unwrap();
        for (dst, ord) in [("bob", 200), ("carol", 300)] {
            assert_eq!(
                store
                    .latest_version_ordinal(EdgeRef::new(DB, T, COLL, "alice", "KNOWS", dst))
                    .unwrap(),
                Some(ord)
            );
        }
    }
}
