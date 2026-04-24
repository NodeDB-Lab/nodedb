//! Integration: bitemporal `CsrSnapshot::from_edge_store_as_of` +
//! algorithm correctness at different system-time ordinals.

use nodedb::engine::graph::algo::pagerank;
use nodedb::engine::graph::algo::params::AlgoParams;
use nodedb::engine::graph::csr::rebuild::rebuild_sharded_from_store_as_of;
use nodedb::engine::graph::edge_store::EdgeStore;
use nodedb::engine::graph::olap::snapshot::CsrSnapshot;
use nodedb_types::TenantId;

const T: TenantId = TenantId::new(1);
const COLL: &str = "g";

fn open_store() -> (EdgeStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
    (store, dir)
}

#[test]
fn snapshot_topology_differs_across_ordinals() {
    let (store, _dir) = open_store();

    // Ordinal 100: chain a → b → c
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"",
            100,
            100,
            i64::MAX,
        )
        .unwrap();
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "b", "L", "c"),
            b"",
            101,
            101,
            i64::MAX,
        )
        .unwrap();

    // Ordinal 200: add d → a so there's a back-edge cycle candidate later
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "d", "L", "a"),
            b"",
            200,
            200,
            i64::MAX,
        )
        .unwrap();

    // Snapshot at 150 — only 2 edges, 3 nodes.
    let snap_old = CsrSnapshot::from_edge_store_as_of(&store, T, Some(150)).unwrap();
    assert_eq!(snap_old.edge_count(), 2);
    assert_eq!(snap_old.node_count(), 3);

    // Snapshot at 250 — full 3 edges, 4 nodes.
    let snap_new = CsrSnapshot::from_edge_store_as_of(&store, T, Some(250)).unwrap();
    assert_eq!(snap_new.edge_count(), 3);
    assert_eq!(snap_new.node_count(), 4);

    // Current-state snapshot matches the latest.
    let snap_cur = CsrSnapshot::from_edge_store_as_of(&store, T, None).unwrap();
    assert_eq!(snap_cur.edge_count(), 3);
    assert_eq!(snap_cur.node_count(), 4);
}

#[test]
fn snapshot_honors_tombstones_and_gdpr_erasure() {
    let (store, _dir) = open_store();

    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            b"",
            10,
            10,
            i64::MAX,
        )
        .unwrap();
    store
        .put_edge_versioned(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "c"),
            b"",
            20,
            20,
            i64::MAX,
        )
        .unwrap();
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "b"),
            30,
        )
        .unwrap();
    store
        .gdpr_erase_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "c"),
            40,
        )
        .unwrap();

    // At ordinal 25: both edges live.
    let snap_mid = CsrSnapshot::from_edge_store_as_of(&store, T, Some(25)).unwrap();
    assert_eq!(snap_mid.edge_count(), 2);

    // At ordinal 50: both gone (tombstoned + erased).
    let snap_after = CsrSnapshot::from_edge_store_as_of(&store, T, Some(50)).unwrap();
    assert_eq!(snap_after.edge_count(), 0);
}

#[test]
fn pagerank_ranks_differ_across_temporal_rebuilds() {
    let (store, _dir) = open_store();

    // Ordinal 100: star centered on 'hub' — nodes a,b,c,d all point to hub.
    for (i, src) in ["a", "b", "c", "d"].iter().enumerate() {
        let ord = 100 + i as i64;
        store
            .put_edge_versioned(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, src, "L", "hub"),
                b"",
                ord,
                ord,
                i64::MAX,
            )
            .unwrap();
    }

    // Ordinal 200: topology flips — 'hub' now points outward; no inbound edges.
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "a", "L", "hub"),
            200,
        )
        .unwrap();
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "b", "L", "hub"),
            201,
        )
        .unwrap();
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "c", "L", "hub"),
            202,
        )
        .unwrap();
    store
        .soft_delete_edge(
            nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "d", "L", "hub"),
            203,
        )
        .unwrap();
    for (i, dst) in ["a", "b", "c", "d"].iter().enumerate() {
        let ord = 210 + i as i64;
        store
            .put_edge_versioned(
                nodedb::engine::graph::edge_store::EdgeRef::new(T, COLL, "hub", "L", dst),
                b"",
                ord,
                ord,
                i64::MAX,
            )
            .unwrap();
    }

    let params = AlgoParams::default();

    // Helper: run PageRank and return a HashMap<node, rank> by decoding the
    // algorithm's JSON projection.
    fn ranks(
        result: nodedb::engine::graph::algo::result::AlgoResultBatch,
    ) -> std::collections::HashMap<String, f64> {
        use sonic_rs::{JsonContainerTrait, JsonValueTrait};
        let json = result.to_json().unwrap();
        let v: sonic_rs::Value = sonic_rs::from_slice(&json).unwrap();
        let mut out = std::collections::HashMap::new();
        for row in v.as_array().unwrap() {
            let node = row["node_id"].as_str().unwrap().to_string();
            let rank = row["rank"].as_f64().unwrap();
            out.insert(node, rank);
        }
        out
    }

    // Old topology — 'hub' is a sink and should have the highest rank.
    let sharded_old = rebuild_sharded_from_store_as_of(&store, Some(150)).unwrap();
    let csr_old = sharded_old.partition(T).expect("partition at 150");
    let old_ranks = ranks(pagerank::run(csr_old, &params));
    let old_hub_rank = *old_ranks.get("hub").expect("hub present in old");
    let old_a_rank = *old_ranks.get("a").expect("a present in old");
    assert!(
        old_hub_rank > old_a_rank,
        "sink hub should dominate old rank: hub={old_hub_rank} a={old_a_rank}"
    );

    // New topology — hub is a source, leaves a/b/c/d should now dominate.
    let sharded_new = rebuild_sharded_from_store_as_of(&store, Some(250)).unwrap();
    let csr_new = sharded_new.partition(T).expect("partition at 250");
    let new_ranks = ranks(pagerank::run(csr_new, &params));
    let new_hub_rank = *new_ranks.get("hub").expect("hub present in new");
    let new_a_rank = *new_ranks.get("a").expect("a present in new");
    assert!(
        new_hub_rank < new_a_rank,
        "source hub should drop below leaves: hub={new_hub_rank} a={new_a_rank}"
    );
}
