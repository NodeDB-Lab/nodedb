// SPDX-License-Identifier: BUSL-1.1

//! Node-level edge reads.
//!
//! A delete never cascades here: its transaction tombstones the edges it
//! removes with `EdgeDelete`, or cuts the collection with `TruncateEdges`
//! for a TRUNCATE.

use super::store::{Direction, EdgeStore};
use super::temporal::NeighborsAsOfParams;
use nodedb_types::TenantId;

impl EdgeStore {
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
    use super::super::temporal::EdgeRef;
    use super::*;
    use nodedb_types::{DatabaseId, OrdinalClock};

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
}
