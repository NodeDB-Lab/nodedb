// SPDX-License-Identifier: BUSL-1.1

//! Production Raft-snapshot builder→applier round-trip, single process.
//!
//! This drives the REAL snapshot SEND/RECEIVE path end to end without a live
//! cluster:
//!
//! 1. A SOURCE single-node `TestServer` creates a strict-document collection
//!    and inserts rows over pgwire, so surrogates are allocated and the
//!    pk→surrogate bindings land in the source catalog.
//! 2. The production [`DataPlaneSnapshotBuilder`] builds a group snapshot
//!    (group-filtered `TenantDataSnapshot` bytes).
//! 3. A FRESH TARGET `TestServer` pre-creates the identical schema, then the
//!    production [`DataPlaneSnapshotApplier`] installs the bytes.
//! 4. The target is verified through the normal query paths: `COUNT(*)`, a PK
//!    point-lookup (which exercises pk→surrogate resolution against the target
//!    catalog — proving the applier rebound the binding), and a direct catalog
//!    surrogate-equality check against the source.
//!
//! Routing: a single-node `TestServer` is normally `cluster_routing == None`,
//! which makes the builder ship an empty snapshot. Both nodes are started with
//! `RoutingTable::uniform(1, &[1], 1)`. With one data group, every vShard maps
//! to data group `1` (group `0` is metadata and owns no vShards), so the test
//! collection's vShard is guaranteed to land in group `1` — the group built and
//! applied here. The test asserts this membership explicitly.

use nodedb_test_support::pgwire_harness::TestServer;

use nodedb::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder;
use nodedb::types::TenantId;
use nodedb_cluster::SnapshotApplier;
use nodedb_cluster::SnapshotBuilder;
use nodedb_cluster::routing::vshard_for_collection;
use nodedb_types::id::DatabaseId;

use super::snapshot_rt_common::{DATA_GROUP_ID, first_value, single_node_routing};

#[tokio::test]
async fn snapshot_round_trip_builder_to_applier() {
    const COLL: &str = "snap_rt_docs";
    let pks = ["pk0", "pk1", "pk2", "pk3", "pk4"];

    // ── Sanity: the collection's vShard belongs to the data group we build. ───
    let vshard = vshard_for_collection(DatabaseId::DEFAULT, COLL);
    let routing = single_node_routing();
    assert!(
        routing.vshards_for_group(DATA_GROUP_ID).contains(&vshard),
        "collection vShard {vshard} must belong to data group {DATA_GROUP_ID}"
    );

    // ── SOURCE node: create collection + insert rows over pgwire. ─────────────
    let source = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*source.client;
        client
            .simple_query(&format!(
                "CREATE COLLECTION {COLL} \
                 (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')"
            ))
            .await
            .expect("CREATE COLLECTION on source");
        for pk in pks {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL} (id, val) VALUES ('{pk}', 'v_{pk}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {pk} on source: {e:?}"));
        }
    }

    // ── Discover the tenant the inserts actually bound under. ─────────────────
    // The pgwire connection resolves to a concrete tenant; rather than hard-code
    // it, read it from the source catalog so the surrogate assertions use the
    // exact tenant the builder captured.
    let source_catalog = source.shared.credentials.catalog().clone();
    let tenant_id = source_catalog
        .load_all_collections(DatabaseId::DEFAULT)
        .expect("load source collections")
        .into_iter()
        .find(|c| c.is_active && c.name == COLL)
        .map(|c| c.tenant_id)
        .expect("source collection descriptor present");
    let tid = TenantId::new(tenant_id);

    // The source must have a surrogate binding for pk0 (proves inserts allocated
    // identities the snapshot will carry).
    let source_surrogate = source_catalog
        .get_surrogate_for_pk(DatabaseId::DEFAULT, tid, COLL, pks[0].as_bytes())
        .expect("source get_surrogate_for_pk")
        .expect("source must have a surrogate for pk0");

    // ── Build the group snapshot via the PRODUCTION builder. ──────────────────
    let builder = DataPlaneSnapshotBuilder::new(source.shared.clone());
    let bytes = builder
        .build_group_snapshot(DATA_GROUP_ID, 0, 0)
        .await
        .expect("build_group_snapshot");
    assert!(
        !bytes.is_empty(),
        "production builder must produce a non-empty group snapshot"
    );

    // ── TARGET node: fresh server, same routing, identical schema pre-created. ─
    let target = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*target.client;
        client
            .simple_query(&format!(
                "CREATE COLLECTION {COLL} \
                 (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')"
            ))
            .await
            .expect("CREATE COLLECTION on target");
    }

    // ── Apply via the PRODUCTION applier. ─────────────────────────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET through the normal query paths. ──────────────────
    let client = &*target.client;

    // (a) All inserted rows are present.
    let count_msgs = client
        .simple_query(&format!("SELECT COUNT(*) FROM {COLL}"))
        .await
        .expect("SELECT COUNT(*) on target");
    assert_eq!(
        first_value(&count_msgs).as_deref(),
        Some(pks.len().to_string().as_str()),
        "target must contain all {} snapshot-installed rows",
        pks.len()
    );

    // (b) PK point-lookup resolves — exercises pk→surrogate resolution against
    //     the target catalog, proving the applier rebound the binding.
    let lookup_msgs = client
        .simple_query(&format!("SELECT val FROM {COLL} WHERE id = '{}'", pks[0]))
        .await
        .expect("SELECT val WHERE id = pk0 on target");
    assert_eq!(
        first_value(&lookup_msgs).as_deref(),
        Some(format!("v_{}", pks[0]).as_str()),
        "PK point-lookup on target must return the snapshot-installed value"
    );

    // (c) Direct catalog check: the target's surrogate for pk0 equals the
    //     source's — the identity map travelled with the data group and was
    //     rebound on apply.
    let target_catalog = target.shared.credentials.catalog().clone();
    let target_surrogate = target_catalog
        .get_surrogate_for_pk(DatabaseId::DEFAULT, tid, COLL, pks[0].as_bytes())
        .expect("target get_surrogate_for_pk")
        .expect("target must have a rebound surrogate for pk0");
    assert_eq!(
        target_surrogate, source_surrogate,
        "rebound target surrogate must equal the source surrogate for pk0"
    );
}

/// Builder→applier round-trip for the **TIMESERIES** snapshot section.
///
/// A handful of small inserts stay in the in-memory memtable (flush threshold
/// is ~64MB), so the timeseries snapshot section captures them directly. The
/// section key is `{db}:{tid}:{collection}` and the (now-fixed) builder filter
/// extracts the collection name correctly. The applier restores the memtable on
/// the target (flushing it to a segment), so `COUNT(*)` sees every row.
#[tokio::test]
async fn snapshot_round_trip_timeseries() {
    const COLL: &str = "ts_rt";
    // CREATE/INSERT/SELECT SQL copied from engine_surface_timeseries.rs.
    const CREATE: &str = "CREATE COLLECTION ts_rt \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')";
    // Small, increasing-ts rows — stay in the memtable, never flushed.
    let rows: &[(&str, u64, f64)] = &[
        ("p1", 1000, 10.0),
        ("p2", 2000, 20.0),
        ("p3", 3000, 30.0),
        ("p4", 4000, 40.0),
    ];

    // ── Sanity: the collection's vShard belongs to the data group we build. ───
    let vshard = vshard_for_collection(DatabaseId::DEFAULT, COLL);
    assert!(
        single_node_routing()
            .vshards_for_group(DATA_GROUP_ID)
            .contains(&vshard),
        "collection vShard {vshard} must belong to data group {DATA_GROUP_ID}"
    );

    // ── SOURCE node: create collection + insert rows over pgwire. ─────────────
    let source = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*source.client;
        client
            .simple_query(CREATE)
            .await
            .expect("CREATE COLLECTION on source");
        for (id, ts, value) in rows {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL} (id, ts, metric, value) \
                     VALUES ('{id}', {ts}, 'cpu', {value})"
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {id} on source: {e}"));
        }
    }

    // ── Build the group snapshot via the PRODUCTION builder. ──────────────────
    let builder = DataPlaneSnapshotBuilder::new(source.shared.clone());
    let bytes = builder
        .build_group_snapshot(DATA_GROUP_ID, 0, 0)
        .await
        .expect("build_group_snapshot");
    assert!(
        !bytes.is_empty(),
        "production builder must produce a non-empty group snapshot"
    );

    // ── TARGET node: fresh server, same routing, identical schema pre-created. ─
    let target = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*target.client;
        client
            .simple_query(CREATE)
            .await
            .expect("CREATE COLLECTION on target");
    }

    // ── Apply via the PRODUCTION applier. ─────────────────────────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET: all memtable rows round-tripped. ────────────────
    let count_msgs = target
        .client
        .simple_query(&format!("SELECT COUNT(*) FROM {COLL}"))
        .await
        .expect("SELECT COUNT(*) on target");
    assert_eq!(
        first_value(&count_msgs).as_deref(),
        Some(rows.len().to_string().as_str()),
        "target must contain all {} snapshot-installed timeseries rows",
        rows.len()
    );
}

/// Builder→applier round-trip for the **VECTOR** snapshot section.
///
/// A vector-primary collection only registers in the snapshot's vector section
/// after at least one vector write. With `vector_field='embedding'`, the section
/// key is `{db}:{tid}:vec_rt:embedding`; the fixed extractor takes the first
/// token after `{db}:{tid}:` → `vec_rt`, which is correct. The applier rebuilds
/// the index using the **target** collection's params, so the target schema must
/// match (dim/metric); `COUNT(*)` then sees every restored row.
#[tokio::test]
async fn snapshot_round_trip_vector() {
    const COLL: &str = "vec_rt";
    // CREATE/INSERT SQL copied from vector_primary_fast_path.rs.
    const CREATE: &str = "CREATE COLLECTION vec_rt \
          (id STRING PRIMARY KEY, embedding VECTOR(4)) \
         WITH (engine='vector', primary = 'vector', vector_field = 'embedding', dim = 4)";
    let vecs: &[(&str, [f32; 4])] = &[
        ("v1", [1.0, 0.0, 0.0, 0.0]),
        ("v2", [0.0, 1.0, 0.0, 0.0]),
        ("v3", [0.0, 0.0, 1.0, 0.0]),
        ("v4", [0.7, 0.7, 0.0, 0.0]),
    ];

    // ── Sanity: the collection's vShard belongs to the data group we build. ───
    let vshard = vshard_for_collection(DatabaseId::DEFAULT, COLL);
    assert!(
        single_node_routing()
            .vshards_for_group(DATA_GROUP_ID)
            .contains(&vshard),
        "collection vShard {vshard} must belong to data group {DATA_GROUP_ID}"
    );

    // ── SOURCE node: create collection + insert vectors over pgwire. ──────────
    let source = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*source.client;
        client
            .simple_query(CREATE)
            .await
            .expect("CREATE COLLECTION on source");
        for (id, e) in vecs {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL} (id, embedding) \
                     VALUES ('{id}', ARRAY[{}, {}, {}, {}])",
                    e[0], e[1], e[2], e[3]
                ))
                .await
                .unwrap_or_else(|err| panic!("INSERT {id} on source: {err}"));
        }
    }

    // ── Build the group snapshot via the PRODUCTION builder. ──────────────────
    let builder = DataPlaneSnapshotBuilder::new(source.shared.clone());
    let bytes = builder
        .build_group_snapshot(DATA_GROUP_ID, 0, 0)
        .await
        .expect("build_group_snapshot");
    assert!(
        !bytes.is_empty(),
        "production builder must produce a non-empty group snapshot"
    );

    // ── TARGET node: fresh server, same routing, identical vector params. ─────
    let target = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*target.client;
        client
            .simple_query(CREATE)
            .await
            .expect("CREATE COLLECTION on target");
    }

    // ── Apply via the PRODUCTION applier. ─────────────────────────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET: all vectors round-tripped. ──────────────────────
    let count_msgs = target
        .client
        .simple_query(&format!("SELECT COUNT(*) FROM {COLL}"))
        .await
        .expect("SELECT COUNT(*) on target");
    assert_eq!(
        first_value(&count_msgs).as_deref(),
        Some(vecs.len().to_string().as_str()),
        "target must contain all {} snapshot-installed vectors",
        vecs.len()
    );
}

/// Builder→applier round-trip for the **GRAPH EDGE** snapshot section.
///
/// Edges live in their own snapshot section (`snap.edges`) keyed by the
/// versioned composite key `{collection}\x00{src}\x00{label}\x00{dst}\x00{sys}`
/// — the collection is the first component, so the builder routes each edge
/// through the same vshard filter every other section uses. The builder must
/// not drop this section entirely — that would lose all edge data on a
/// snapshot-installed follower, and this test would fail with an empty
/// traversal.
///
/// SQL is copied verbatim from `graph_cross_core_bfs.rs`:
/// - `CREATE COLLECTION <name>`               (e.g. `bfs_nodes`)
/// - `GRAPH INSERT EDGE IN '<coll>' FROM '<src>' TO '<dst>' TYPE '<label>'`
/// - `GRAPH TRAVERSE IN '<coll>' FROM '<src>' DEPTH <n> LABEL '<label>' DIRECTION out`
#[tokio::test]
async fn snapshot_round_trip_edges() {
    const COLL: &str = "snap_rt_edges";
    // root → leaf_0..leaf_(FANOUT-1), label 'l'. Small fan-out keeps the test
    // fast; the traversal below is non-empty ONLY if the edges round-trip.
    const FANOUT: usize = 8;

    // ── Sanity: the collection's vShard belongs to the data group we build. ───
    let vshard = vshard_for_collection(DatabaseId::DEFAULT, COLL);
    assert!(
        single_node_routing()
            .vshards_for_group(DATA_GROUP_ID)
            .contains(&vshard),
        "collection vShard {vshard} must belong to data group {DATA_GROUP_ID}"
    );

    // ── SOURCE node: create collection + insert edges over pgwire. ────────────
    let source = TestServer::start_with_routing(single_node_routing()).await;
    source
        .exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION on source");
    for i in 0..FANOUT {
        source
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{COLL}' FROM 'root' TO 'leaf_{i}' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("GRAPH INSERT EDGE leaf_{i} on source: {e}"));
    }

    // (No source-side traversal sanity: with `cluster_routing` injected — which
    // the builder requires — `GRAPH TRAVERSE` attempts distributed graph dispatch
    // and needs a cluster gateway the single-node harness has no. The edge
    // INSERTs above are `.expect`-checked, so the edges are definitely present;
    // the round-trip is proven by the TARGET traversal below.)

    // ── Build the group snapshot via the PRODUCTION builder. ──────────────────
    let builder = DataPlaneSnapshotBuilder::new(source.shared.clone());
    let bytes = builder
        .build_group_snapshot(DATA_GROUP_ID, 0, 0)
        .await
        .expect("build_group_snapshot");
    assert!(
        !bytes.is_empty(),
        "production builder must produce a non-empty group snapshot"
    );

    // Decode the snapshot and assert the builder actually carried the edges
    // (group-filtered). This is the direct proof of the builder fix and is
    // independent of the apply/traversal path below.
    let decoded: nodedb::types::TenantDataSnapshot =
        zerompk::from_msgpack(&bytes).expect("decode group snapshot");
    assert_eq!(
        decoded.tenant_edges.len(),
        FANOUT,
        "builder must carry all {FANOUT} edges (tenant-aware) for the in-group \
         collection; got {}",
        decoded.tenant_edges.len()
    );

    // ── TARGET node: fresh server, identical schema pre-created. ──────────────
    // The target is started WITHOUT a routing table: the applier does not need
    // one (the snapshot bytes are already group-filtered), and its absence keeps
    // the verification `GRAPH TRAVERSE` below on the local (non-distributed) path
    // so it does not require a cluster gateway.
    let target = TestServer::start().await;
    target
        .exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION on target");

    // ── Apply via the PRODUCTION applier. ─────────────────────────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET: all edges round-tripped. ────────────────────────
    // This traversal is non-empty ONLY if the edges were carried in the
    // snapshot and the applier rebuilt the CSR. Without the builder fix the
    // edge section ships empty and this assertion fails.
    let tgt_traverse = target
        .query_text(&format!(
            "GRAPH TRAVERSE IN '{COLL}' FROM 'root' DEPTH 1 LABEL 'l' DIRECTION out"
        ))
        .await
        .expect("GRAPH TRAVERSE on target");
    let tgt_blob = tgt_traverse.join("");
    for i in 0..FANOUT {
        assert!(
            tgt_blob.contains(&format!("leaf_{i}")),
            "target must traverse leaf_{i} from snapshot-installed edges; got: {tgt_blob}"
        );
    }
}
