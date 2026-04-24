//! End-to-end integration tests for bitemporal EdgeStore.
//!
//! Covers the write → update → soft-delete → GDPR-erase lifecycle through
//! the versioned key path, verifies Ceiling resolution at arbitrary ordinal
//! cutoffs, and asserts that CSR rebuild honors `system_as_of`.

use nodedb::engine::graph::csr::rebuild::{
    rebuild_sharded_from_store, rebuild_sharded_from_store_as_of,
};
use nodedb::engine::graph::edge_store::EdgeStore;
use nodedb_types::TenantId;

const T: TenantId = TenantId::new(1);
const COLL: &str = "people";

fn open_store() -> (EdgeStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
    (store, dir)
}

#[test]
fn insert_update_softdelete_ceiling_roundtrip() {
    let (store, _dir) = open_store();

    // v1 @ ordinal 100
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
            b"v1",
            100,
            100,
            i64::MAX,
        )
        .unwrap();
    // v2 @ ordinal 200 (overwrites)
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
            b"v2",
            200,
            200,
            i64::MAX,
        )
        .unwrap();
    // tombstone @ ordinal 300
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
            300,
        )
        .unwrap();

    // Ceiling at each cutoff
    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
                99,
                None
            )
            .unwrap(),
        None,
        "before any version exists"
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
                150,
                None
            )
            .unwrap()
            .unwrap(),
        b"v1".to_vec()
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
                250,
                None
            )
            .unwrap()
            .unwrap(),
        b"v2".to_vec()
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
                350,
                None
            )
            .unwrap(),
        None,
        "post-tombstone must be hidden"
    );

    // Current-state get_edge must see None (latest is tombstone).
    assert!(
        store
            .get_edge(T, COLL, "alice", "KNOWS", "bob")
            .unwrap()
            .is_none()
    );
}

#[test]
fn gdpr_erase_distinct_from_soft_delete() {
    let (store, _dir) = open_store();

    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "u1", "PAID", "vendor"),
            b"100usd",
            10,
            10,
            i64::MAX,
        )
        .unwrap();
    store
        .gdpr_erase_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "u1", "PAID", "vendor"),
            20,
        )
        .unwrap();

    // After erasure, current-state is empty.
    assert!(
        store
            .get_edge(T, COLL, "u1", "PAID", "vendor")
            .unwrap()
            .is_none()
    );
    // But the raw export still shows the erasure marker — audit preservation.
    let raw = store.scan_edges_for_tenant(T).unwrap();
    let found = raw.iter().any(|(_k, v)| v == &[0xFEu8]);
    assert!(found, "GDPR erasure marker must persist in storage");
}

#[test]
fn csr_rebuild_as_of_omits_post_cutoff_writes() {
    let (store, _dir) = open_store();

    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"x",
            100,
            100,
            i64::MAX,
        )
        .unwrap();
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "c"),
            b"y",
            200,
            200,
            i64::MAX,
        )
        .unwrap();

    // Current-state rebuild sees both edges.
    let csr_now = rebuild_sharded_from_store(&store).unwrap();
    let part_now = csr_now.partition(T).expect("partition exists");
    assert_eq!(part_now.node_count(), 3);

    // AS OF ordinal 150 only sees the first edge.
    let sharded = rebuild_sharded_from_store_as_of(&store, Some(150)).unwrap();
    let part = sharded.partition(T).expect("partition exists");
    assert_eq!(part.node_count(), 2, "only a + b at ordinal 150");
}

#[test]
fn soft_delete_then_reinsert_resurrects_current_state() {
    let (store, _dir) = open_store();

    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"v1",
            10,
            10,
            i64::MAX,
        )
        .unwrap();
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            20,
        )
        .unwrap();
    assert!(store.get_edge(T, COLL, "a", "L", "b").unwrap().is_none());

    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"v2",
            30,
            30,
            i64::MAX,
        )
        .unwrap();
    assert_eq!(
        store.get_edge(T, COLL, "a", "L", "b").unwrap().unwrap(),
        b"v2".to_vec()
    );
    // Historical read at ordinal 25 (between tombstone and re-insert)
    // correctly returns None.
    assert!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
                25,
                None
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn valid_time_filter_selects_applicable_version() {
    let (store, _dir) = open_store();

    // Two disjoint valid-time windows for the same system-time progression.
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"old",
            10,
            0,
            100,
        )
        .unwrap();
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"new",
            20,
            200,
            i64::MAX,
        )
        .unwrap();

    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
                i64::MAX,
                Some(50)
            )
            .unwrap()
            .unwrap(),
        b"old".to_vec()
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
                i64::MAX,
                Some(300)
            )
            .unwrap()
            .unwrap(),
        b"new".to_vec()
    );
    // Valid-time hole [100, 200) — no version covers t=150.
    assert!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
                i64::MAX,
                Some(150)
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn delete_edges_for_node_cascades_tombstones() {
    let (store, _dir) = open_store();

    for (n, (src, dst)) in [("alice", "bob"), ("alice", "carol"), ("dave", "alice")]
        .iter()
        .enumerate()
    {
        let ord = 10 + n as i64;
        store
            .put_edge_versioned(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, src, "KNOWS", dst),
                b"p",
                ord,
                ord,
                i64::MAX,
            )
            .unwrap();
    }
    // Unrelated edge must survive.
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "eve", "KNOWS", "frank"),
            b"p",
            100,
            100,
            i64::MAX,
        )
        .unwrap();

    store.delete_edges_for_node(T, "alice", 1_000).unwrap();

    assert!(
        store
            .get_edge(T, COLL, "alice", "KNOWS", "bob")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_edge(T, COLL, "alice", "KNOWS", "carol")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_edge(T, COLL, "dave", "KNOWS", "alice")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_edge(T, COLL, "eve", "KNOWS", "frank")
            .unwrap()
            .unwrap(),
        b"p".to_vec()
    );

    // Historical read before the cascade still sees the edges.
    assert_eq!(
        store
            .ceiling_resolve_edge(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "alice", "KNOWS", "bob"),
                500,
                None
            )
            .unwrap()
            .unwrap(),
        b"p".to_vec()
    );
}
