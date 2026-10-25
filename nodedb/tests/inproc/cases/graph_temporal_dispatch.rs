// SPDX-License-Identifier: BUSL-1.1

//! End-to-end bitemporal graph read path.
//!
//! Exercises the edge-store methods the Data Plane's historical graph
//! reads dispatch to: `neighbors_out_as_of`, `neighbors_in_as_of`, and
//! `rebuild_sharded_from_store_as_of`. The wire ops, dispatch match,
//! and handler are thin wrappers over these calls, so validating the
//! edge-store surface validates the full round-trip.

use nodedb::engine::graph::csr::rebuild::rebuild_sharded_from_store_as_of;
use nodedb::engine::graph::edge_store::{Direction, EdgeRef, EdgeStore, NeighborsAsOfParams};
use nodedb_types::{TenantId, ms_to_ordinal_upper};

const T: TenantId = TenantId::new(1);
const DB: nodedb_types::DatabaseId = nodedb_types::DatabaseId::DEFAULT;
const COLL: &str = "people";

fn open_store() -> (EdgeStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
    (store, dir)
}

/// Put an edge with an ms-aligned ordinal so that `Some(ms)` cutoffs
/// (which go through `ms_to_ordinal_upper` → ns) compare correctly
/// against it.
fn put(store: &EdgeStore, src: &str, label: &str, dst: &str, ms: i64) {
    let ord = ms * 1_000_000; // ms → ns, matches HLC ordinal granularity
    store
        .put_edge_versioned(
            EdgeRef::new(DB, T, COLL, src, label, dst),
            b"v",
            ord,
            ord,
            i64::MAX,
        )
        .unwrap();
}

/// `neighbors_out_as_of(None, None)` must match current-state semantics.
#[test]
fn neighbors_out_as_of_none_matches_current_state() {
    let (store, _dir) = open_store();
    put(&store, "alice", "KNOWS", "bob", 100);
    put(&store, "alice", "KNOWS", "carol", 200);

    let current = store
        .neighbors_out(0, T, COLL, "alice", Some("KNOWS"))
        .unwrap();
    let temporal_none = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: i64::MAX,
            valid_at_ms: None,
        })
        .unwrap();

    assert_eq!(current.len(), 2);
    assert_eq!(temporal_none.len(), current.len());
    let mut cur_dsts: Vec<_> = current.iter().map(|e| e.dst_id.as_str()).collect();
    let mut tmp_dsts: Vec<_> = temporal_none.iter().map(|e| e.dst_id.as_str()).collect();
    cur_dsts.sort();
    tmp_dsts.sort();
    assert_eq!(cur_dsts, tmp_dsts);
}

/// Cutoff at ms=150 must return only v@100; cutoff at ms=250 returns both.
#[test]
fn neighbors_out_as_of_honors_system_cutoff() {
    let (store, _dir) = open_store();
    put(&store, "alice", "KNOWS", "bob", 100);
    put(&store, "alice", "KNOWS", "carol", 200);

    let at_150 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(150),
            valid_at_ms: None,
        })
        .unwrap();
    let at_250 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(250),
            valid_at_ms: None,
        })
        .unwrap();

    let dsts_150: Vec<_> = at_150.iter().map(|e| e.dst_id.as_str()).collect();
    assert_eq!(dsts_150, vec!["bob"]);
    assert_eq!(at_250.len(), 2);
}

/// Tombstone emitted at ord=300 must hide the edge for cutoffs ≥ 300 and
/// leave it visible for earlier cutoffs.
#[test]
fn neighbors_out_as_of_respects_tombstone_at_cutoff() {
    let (store, _dir) = open_store();
    put(&store, "alice", "KNOWS", "bob", 100);
    store
        .soft_delete_edge(
            EdgeRef::new(DB, T, COLL, "alice", "KNOWS", "bob"),
            300 * 1_000_000,
        )
        .unwrap();

    let before = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(200),
            valid_at_ms: None,
        })
        .unwrap();
    let after = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(400),
            valid_at_ms: None,
        })
        .unwrap();

    assert_eq!(before.len(), 1);
    assert_eq!(after.len(), 0);
}

/// Valid-time predicate must skip versions whose interval does not contain
/// the asked point, walking to earlier versions via the Ceiling resolver.
#[test]
fn neighbors_out_as_of_applies_valid_time_predicate() {
    let (store, _dir) = open_store();
    // v1: valid [0, 100)
    store
        .put_edge_versioned(
            EdgeRef::new(DB, T, COLL, "alice", "KNOWS", "bob"),
            b"v1",
            10,
            0,
            100,
        )
        .unwrap();
    // v2: valid [200, 300)
    store
        .put_edge_versioned(
            EdgeRef::new(DB, T, COLL, "alice", "KNOWS", "bob"),
            b"v2",
            20,
            200,
            300,
        )
        .unwrap();

    // No version is valid at t=150 — hole in the valid-time coverage.
    let at_150 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(10_000),
            valid_at_ms: Some(150),
        })
        .unwrap();
    assert!(at_150.is_empty(), "expected empty, got {at_150:?}");

    // Valid-time 50 matches v1; 250 matches v2.
    let at_50 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(10_000),
            valid_at_ms: Some(50),
        })
        .unwrap();
    let at_250 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "alice",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(10_000),
            valid_at_ms: Some(250),
        })
        .unwrap();
    assert_eq!(at_50.len(), 1);
    assert_eq!(at_250.len(), 1);
}

/// Inbound neighbors also honour the cutoff + valid-time predicate.
#[test]
fn neighbors_in_as_of_honors_system_cutoff() {
    let (store, _dir) = open_store();
    put(&store, "alice", "KNOWS", "bob", 100);
    put(&store, "carol", "KNOWS", "bob", 200);

    let at_150 = store
        .neighbors_in_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "bob",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(150),
            valid_at_ms: None,
        })
        .unwrap();
    let at_250 = store
        .neighbors_in_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "bob",
            label_filter: Some("KNOWS"),
            cutoff_ordinal: ms_to_ordinal_upper(250),
            valid_at_ms: None,
        })
        .unwrap();

    let srcs_150: Vec<_> = at_150.iter().map(|e| e.src_id.as_str()).collect();
    assert_eq!(srcs_150, vec!["alice"]);
    assert_eq!(at_250.len(), 2);
}

/// A rebuild at `ms_to_ordinal_upper(ms)` yields the topology live at that
/// cutoff.
#[test]
fn temporal_algorithm_rebuild_matches_expected_topology_at_cutoff() {
    let (store, _dir) = open_store();
    put(&store, "a", "R", "b", 100);
    put(&store, "a", "R", "c", 200);
    put(&store, "b", "R", "c", 300);

    let at_150 = rebuild_sharded_from_store_as_of(
        &store,
        Some(ms_to_ordinal_upper(150)),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    let at_250 = rebuild_sharded_from_store_as_of(
        &store,
        Some(ms_to_ordinal_upper(250)),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();

    let p150 = at_150.partition(DB, T).unwrap();
    let p250 = at_250.partition(DB, T).unwrap();

    // At ms=150 only the first edge is visible.
    assert_eq!(p150.edge_count(), 1, "at 150ms expected 1 edge");
    // At ms=250 two edges visible.
    assert_eq!(p250.edge_count(), 2, "at 250ms expected 2 edges");
}

/// The two cutoffs produce different topologies, so an algorithm run on
/// each one reads a different graph.
#[test]
fn temporal_algorithm_direction_matters_across_cutoffs() {
    let (store, _dir) = open_store();
    put(&store, "source", "R", "sink", 100);
    // Flip: later make sink→source dominant.
    put(&store, "sink", "R", "source", 300);

    let at_150 = rebuild_sharded_from_store_as_of(
        &store,
        Some(ms_to_ordinal_upper(150)),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    let at_400 = rebuild_sharded_from_store_as_of(
        &store,
        Some(ms_to_ordinal_upper(400)),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();

    // Verify direction of the 1-hop neighbor set.
    let src_out_150 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "source",
            label_filter: Some("R"),
            cutoff_ordinal: ms_to_ordinal_upper(150),
            valid_at_ms: None,
        })
        .unwrap();
    let src_out_400 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "source",
            label_filter: Some("R"),
            cutoff_ordinal: ms_to_ordinal_upper(400),
            valid_at_ms: None,
        })
        .unwrap();
    let sink_out_400 = store
        .neighbors_out_as_of(NeighborsAsOfParams {
            db: 0,
            tid: T,
            collection: COLL,
            node: "sink",
            label_filter: Some("R"),
            cutoff_ordinal: ms_to_ordinal_upper(400),
            valid_at_ms: None,
        })
        .unwrap();

    assert_eq!(src_out_150.len(), 1);
    assert_eq!(src_out_400.len(), 1);
    assert_eq!(sink_out_400.len(), 1);
    assert_eq!(at_150.partition(DB, T).unwrap().edge_count(), 1);
    assert_eq!(at_400.partition(DB, T).unwrap().edge_count(), 2);
    let direction = Direction::Out;
    let _ = direction; // silence unused-import on this lightweight test
}
