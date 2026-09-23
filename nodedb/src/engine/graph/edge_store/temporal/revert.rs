// SPDX-License-Identifier: BUSL-1.1

//! Removal of one versioned edge write, for a rollback.
//!
//! A write adds a version at its `system_from`. A rollback removes that
//! version, so no read at any system time sees it. It also restores what the
//! write changed beside the version: the edge counters, the zero summary row
//! and the endpoint identity bindings.

use crate::engine::graph::edge_store::stats::table::{GRAPH_STATS, summary_key};
use crate::engine::graph::edge_store::stats::update::{
    EdgeStatsKey, decrement_counts, increment_counts,
};
use crate::engine::graph::edge_store::store::{
    EDGES, EdgeStore, NODE_SURROGATES, REVERSE_EDGES, redb_err,
};

use super::keys::{EdgeRef, versioned_edge_key};

/// How one write changed the edge counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeCountChange {
    /// The write made the edge live: it counted one more edge.
    Added,
    /// The write ended a live edge: it counted one edge fewer.
    Removed,
}

/// What one versioned edge write changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeVersionWrite {
    /// The system-time key of the version the write added.
    pub system_from: i64,
    /// The forward value the key held before the write, `None` when absent.
    pub prior_forward: Option<Vec<u8>>,
    /// The reverse value the key held before the write, `None` when absent.
    pub prior_reverse: Option<Vec<u8>>,
    /// The counter change the write made, `None` when it made none.
    pub counted: Option<EdgeCountChange>,
    /// Whether the write created the collection's zero summary row.
    pub summary_created: bool,
    /// Every endpoint binding the write set, with the binding it held before.
    pub prior_bindings: Vec<(String, Option<u32>)>,
}

impl EdgeVersionWrite {
    /// A write at `system_from` that changed nothing yet.
    pub(super) fn at(system_from: i64) -> Self {
        Self {
            system_from,
            prior_forward: None,
            prior_reverse: None,
            counted: None,
            summary_created: false,
            prior_bindings: Vec::new(),
        }
    }
}

impl EdgeStore {
    /// Remove the version `written` describes and restore what the write
    /// changed beside it, in one transaction.
    ///
    /// The caller removes later writes to the same edge first. Rollback runs
    /// in reverse write order, so it does.
    pub fn remove_edge_version(
        &self,
        edge: EdgeRef<'_>,
        written: &EdgeVersionWrite,
    ) -> crate::Result<()> {
        let sys = written.system_from;
        let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, sys)?;
        let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, sys)?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let restored = match &written.prior_forward {
                Some(prior) => edges
                    .insert((d, t, fwd.as_str()), prior.as_slice())
                    .map(drop),
                None => edges.remove((d, t, fwd.as_str())).map(drop),
            };
            restored.map_err(|e| redb_err("remove edge version", e))?;
            drop(edges);

            let mut reverse = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            let restored = match &written.prior_reverse {
                Some(prior) => reverse
                    .insert((d, t, rev.as_str()), prior.as_slice())
                    .map(drop),
                None => reverse.remove((d, t, rev.as_str())).map(drop),
            };
            restored.map_err(|e| redb_err("remove reverse edge version", e))?;
            drop(reverse);

            let key = EdgeStatsKey {
                db: d,
                tid: t,
                collection: edge.collection,
                label: edge.label,
                src: edge.src,
                dst: edge.dst,
            };
            match written.counted {
                Some(EdgeCountChange::Added) => decrement_counts(&write_txn, key)?,
                Some(EdgeCountChange::Removed) => increment_counts(&write_txn, key)?,
                None => {}
            }
            if written.summary_created {
                let summary = summary_key(edge.collection);
                let mut stats = write_txn
                    .open_table(GRAPH_STATS)
                    .map_err(|e| redb_err("open graph_stats", e))?;
                stats
                    .remove((d, t, summary.as_str()))
                    .map_err(|e| redb_err("remove zero graph summary", e))?;
            }

            let mut surrogates = write_txn
                .open_table(NODE_SURROGATES)
                .map_err(|e| redb_err("open node_surrogates", e))?;
            for (node, prior) in &written.prior_bindings {
                let restored = match prior {
                    Some(raw) => surrogates.insert((d, t, node.as_str()), *raw).map(drop),
                    None => surrogates.remove((d, t, node.as_str())).map(drop),
                };
                restored.map_err(|e| redb_err("restore node surrogate", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge version removal", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, Surrogate, TenantId};
    use redb::{ReadableDatabase, ReadableTable};

    use super::*;
    use crate::engine::graph::edge_store::stats::table::CollectionStats;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open edge store");
        (store, dir)
    }

    fn e<'a>(src: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DB, T, COLL, src, "L", dst)
    }

    fn version_count(store: &EdgeStore) -> usize {
        let txn = store.db.begin_read().expect("begin read");
        let edges = txn.open_table(EDGES).expect("open edges");
        let reverse = txn.open_table(REVERSE_EDGES).expect("open reverse");
        let forward = edges.iter().expect("iter edges").count();
        let backward = reverse.iter().expect("iter reverse").count();
        assert_eq!(forward, backward, "every version has its reverse entry");
        forward
    }

    fn stats(store: &EdgeStore) -> CollectionStats {
        store
            .collection_stats(DB.as_u64(), T, COLL, None)
            .expect("collection stats")
    }

    #[test]
    fn removing_an_inserted_version_leaves_no_trace_at_any_system_time() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "b"), b"v1", 100, 100, i64::MAX)
            .expect("seed");
        let before = stats(&store);

        let written = store
            .put_edge_version_recorded(
                e("a", "b").with_surrogates(Surrogate::new(7), Surrogate::new(8)),
                b"v2",
                200,
                200,
                i64::MAX,
                true,
            )
            .expect("write");
        store
            .remove_edge_version(e("a", "b"), &written)
            .expect("remove");

        assert_eq!(version_count(&store), 1, "only the seeded version remains");
        for as_of in [150, 200, 250, i64::MAX] {
            assert_eq!(
                store
                    .ceiling_resolve_edge(e("a", "b"), as_of, None)
                    .expect("read"),
                Some(b"v1".to_vec()),
                "the edge reads as seeded at system time {as_of}"
            );
        }
        assert_eq!(stats(&store), before);
        assert!(
            store
                .scan_all_node_surrogates()
                .expect("scan bindings")
                .is_empty(),
            "the bindings the write set are gone"
        );
    }

    #[test]
    fn removing_a_tombstone_restores_the_live_edge_and_its_counters() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "b"), b"v1", 100, 100, i64::MAX)
            .expect("seed");
        let before = stats(&store);

        let written = store
            .soft_delete_edge_recorded(e("a", "b"), 200, true)
            .expect("tombstone");
        assert_eq!(written.counted, Some(EdgeCountChange::Removed));
        store
            .remove_edge_version(e("a", "b"), &written)
            .expect("remove");

        assert_eq!(version_count(&store), 1);
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "b"), 300, None)
                .expect("read"),
            Some(b"v1".to_vec())
        );
        assert_eq!(stats(&store), before);
    }

    #[test]
    fn removing_a_first_insert_uncounts_the_edge() {
        let (store, _dir) = make_store();
        let written = store
            .put_edge_version_recorded(e("a", "b"), b"v1", 100, 100, i64::MAX, true)
            .expect("write");
        assert_eq!(written.counted, Some(EdgeCountChange::Added));
        store
            .remove_edge_version(e("a", "b"), &written)
            .expect("remove");

        assert_eq!(version_count(&store), 0);
        let after = stats(&store);
        assert_eq!(after.edge_count, 0);
        assert_eq!(after.distinct_node_count, 0);
        assert_eq!(after.distinct_label_count, 0);
    }

    #[test]
    fn removing_a_version_that_replaced_one_at_the_same_key_restores_it() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "b"), b"v1", 100, 100, i64::MAX)
            .expect("seed");
        let written = store
            .put_edge_version_recorded(e("a", "b"), b"v2", 100, 100, i64::MAX, true)
            .expect("overwrite");
        store
            .remove_edge_version(e("a", "b"), &written)
            .expect("remove");

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "b"), 100, None)
                .expect("read"),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn removing_a_write_restores_the_binding_it_replaced() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                e("a", "b").with_surrogates(Surrogate::new(1), Surrogate::new(2)),
                b"v1",
                100,
                100,
                i64::MAX,
            )
            .expect("seed");
        let written = store
            .put_edge_version_recorded(
                e("a", "c").with_surrogates(Surrogate::new(9), Surrogate::new(3)),
                b"v1",
                200,
                200,
                i64::MAX,
                true,
            )
            .expect("write");
        store
            .remove_edge_version(e("a", "c"), &written)
            .expect("remove");

        let mut bindings: Vec<(String, u32)> = store
            .scan_all_node_surrogates()
            .expect("scan bindings")
            .into_iter()
            .map(|record| (record.2, record.3))
            .collect();
        bindings.sort();
        assert_eq!(bindings, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    }

    #[test]
    fn removing_a_destination_replica_write_drops_the_zero_summary_it_created() {
        let (store, _dir) = make_store();
        let written = store
            .put_edge_version_recorded(e("a", "b"), b"v1", 100, 100, i64::MAX, false)
            .expect("write");
        assert!(written.summary_created);
        store
            .remove_edge_version(e("a", "b"), &written)
            .expect("remove");

        let txn = store.db.begin_read().expect("begin read");
        let table = txn.open_table(GRAPH_STATS).expect("open stats");
        assert_eq!(table.iter().expect("iter stats").count(), 0);
    }
}
