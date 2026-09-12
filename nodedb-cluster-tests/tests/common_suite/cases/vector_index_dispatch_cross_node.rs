// SPDX-License-Identifier: BUSL-1.1
//! `CREATE VECTOR INDEX` / `DROP INDEX` must reach the Data Plane of every
//! node, not only the node that ran the statement.
//!
//! The catalog row replicates on its own, so a test that reads the row passes
//! on a node whose cores never learned the index. These tests probe the
//! non-executing nodes' own cores instead: an `ExecuteRequest` carrying a
//! read-only `VectorOp` is executed by the receiving node locally, so the
//! answer is that node's `vector_collections` state and nothing else.
//!
//! A schemaless collection indexes a document field only when `SetParams`
//! installed that field's build parameters on the core. A node that missed
//! the dispatch therefore materializes no index at all, and both `QueryStats`
//! and `Search` answer `NotFound` on its cores. The Control Plane tolerates a
//! core's `NotFound` and drops that core's contribution, so such a node
//! answers with an empty result rather than an error — the probes treat an
//! empty gather payload as "no index" for that reason. Nothing restarts, so
//! the boot seed never gets a chance to install what the dispatch owed.

use crate::common;

use std::time::{Duration, Instant};

use common::cluster_harness::{TestCluster, TestClusterNode};

use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, VectorOp, wire as plan_wire};
use nodedb_types::vector_distance::DistanceMetric;
use nodedb_types::{DatabaseId, QualifiedCollection, SystemTimeScope, Value, VectorIndexStats};

/// Schemaless collection carrying the indexed embedding field.
const COLLECTION: &str = "vec_dispatch_docs";
/// Indexed document field.
const FIELD: &str = "embedding";
/// Index built by the first `CREATE VECTOR INDEX`.
const INDEX: &str = "vec_dispatch_idx";
/// Index built by the re-`CREATE` after the drop.
const INDEX_V2: &str = "vec_dispatch_idx_v2";
/// Tenant of the harness superuser.
const TENANT: u64 = 1;
/// Rows written at each declared width.
const ROWS: usize = 3;

/// Declared build parameters of [`INDEX`]. None match the engine defaults
/// (`m = 16`, `ef_construction = 200`), so a shape carrying them proves the
/// node built the index from the statement rather than from a default.
const DIM: usize = 8;
const M: usize = 32;
const EF_CONSTRUCTION: usize = 400;

/// Declared build parameters of [`INDEX_V2`]. Every one differs from
/// [`INDEX`]'s, so a shape carrying them proves the drop and the second
/// create both landed: `execute_set_vector_params` refuses a params change on
/// a key still present in `vector_collections`.
const DIM_V2: usize = 6;
const M_V2: usize = 48;
const EF_CONSTRUCTION_V2: usize = 500;

/// Deadline for a node's cores to reflect a replicated index change.
const CONVERGE: Duration = Duration::from_secs(20);
/// Poll step while waiting for that change.
const STEP: Duration = Duration::from_millis(100);

/// Build parameters one node's materialized index actually carries.
#[derive(Debug, PartialEq, Eq)]
struct IndexShape {
    dim: usize,
    m: usize,
    ef_construction: usize,
    live: usize,
}

/// Node id every node agrees is the metadata-group leader. `CREATE VECTOR
/// INDEX` and `DROP INDEX` are metadata DDL, so this is the executing node.
fn leader_id(cluster: &TestCluster) -> u64 {
    cluster
        .nodes
        .iter()
        .map(|n| n.metadata_group_leader())
        .find(|&id| id != 0)
        .expect("at least one node must report a non-zero leader id")
}

/// The nodes that did not run the DDL.
fn followers(cluster: &TestCluster) -> Vec<&TestClusterNode> {
    let executor = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .filter(|n| n.node_id != executor)
        .collect()
}

/// The node that runs the DDL and takes the writes.
fn leader(cluster: &TestCluster) -> &TestClusterNode {
    let executor = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .find(|n| n.node_id == executor)
        .expect("the metadata leader must be one of the spawned nodes")
}

/// Execute `plan` on `node`'s own cores.
///
/// `QueryStats` and `Search` are read ops, so the receiver runs them through
/// its local Data Plane instead of proposing them through Raft. That makes
/// the response this node's index state, never a peer's.
async fn execute_on_node(node: &TestClusterNode, plan: PhysicalPlan) -> ExecuteResponse {
    let transport = node
        .shared
        .cluster_transport
        .as_ref()
        .expect("cluster transport");
    let request = ExecuteRequest {
        plan_bytes: plan_wire::encode(&plan).expect("encode plan"),
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: 10_000,
        trace_id: [0u8; 16],
        // Empty: the probe binds no descriptor version, so nothing is fenced.
        descriptor_versions: Vec::new(),
        txn_id: None,
    };
    match transport
        .send_rpc_to_addr(node.listen_addr, RaftRpc::ExecuteRequest(request))
        .await
    {
        Ok(RaftRpc::ExecuteResponse(resp)) => resp,
        Ok(other) => panic!("expected ExecuteResponse, got {other:?}"),
        Err(e) => panic!("transport error: {e}"),
    }
}

/// Did every core drop out of this gather?
///
/// A core that holds nothing for the key answers `ErrorCode::NotFound`, and
/// the Control Plane maps that to a dropped contribution rather than an error,
/// so the caller receives a successful response carrying an empty array.
fn is_empty_gather(payload: &[u8]) -> bool {
    matches!(nodedb_types::value_from_msgpack(payload), Ok(Value::Array(items)) if items.is_empty())
}

/// Build parameters `node`'s cores hold for the indexed field.
///
/// `Err` carries the reason the node produced no shape. `NotFound` covers two
/// distinct states — `SetParams` never arrived, or it arrived and no vector
/// row has been indexed yet — so the report pairs it with the catalog row and
/// this node's local document count.
async fn index_probe(node: &TestClusterNode) -> Result<IndexShape, String> {
    let plan = PhysicalPlan::Vector(VectorOp::QueryStats {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        field_name: FIELD.to_string(),
    });
    let resp = execute_on_node(node, plan).await;
    if !resp.success {
        return Err(format!("{:?}", resp.error));
    }
    let payload = resp.payloads.first().ok_or("empty payload")?;
    // A core holding no index answers `NotFound`, which the Control Plane
    // tolerates rather than propagates, leaving an empty gathered array.
    if is_empty_gather(payload) {
        return Err("no index on this node (core answered NotFound)".to_string());
    }
    // `VectorIndexStats` derives `zerompk::FromMessagePack` with the default
    // array representation, and the single core's response survives the
    // gather's re-wrap unchanged, so the response type decodes its own bytes.
    let stats: VectorIndexStats = zerompk::from_msgpack(payload)
        .map_err(|e| format!("undecodable payload ({e}) {payload:02x?}"))?;
    Ok(IndexShape {
        dim: stats.dimensions,
        m: stats.hnsw_m,
        ef_construction: stats.hnsw_ef_construction,
        live: stats.live_count,
    })
}

/// Build parameters `node`'s cores hold, or `None` when it holds no index.
async fn local_index_shape(node: &TestClusterNode) -> Option<IndexShape> {
    index_probe(node).await.ok()
}

/// The replicated `_system.vector_index_params` row this node holds.
///
/// Independent of the post-apply dispatch: the row travels as a catalog
/// entry. Absent here means the failure is upstream of the dispatch.
fn catalog_params(node: &TestClusterNode) -> Option<(usize, usize, usize)> {
    node.shared
        .credentials
        .catalog()
        .get_vector_index_params(DatabaseId::DEFAULT.as_u64(), TENANT, COLLECTION, FIELD)
        .expect("read the vector index params row")
        .map(|row| (row.dim, row.m, row.ef_construction))
}

/// Rows `node`'s own cores hold for the collection.
///
/// A document scan is a read, so the receiving node runs it locally: this
/// separates "the vector rows never reached this node's Data Plane" from
/// "they reached it and were not indexed".
async fn local_doc_count(node: &TestClusterNode) -> Result<usize, String> {
    let plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        limit: 1000,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    let resp = execute_on_node(node, plan).await;
    if !resp.success {
        return Err(format!("{:?}", resp.error));
    }
    let payload = resp.payloads.first().ok_or("empty payload")?;
    match nodedb_types::value_from_msgpack(payload) {
        Ok(Value::Array(items)) => Ok(items.len()),
        other => Err(format!("undecodable scan payload: {other:?}")),
    }
}

/// One line per node: what it holds in the catalog, on its cores, and in its
/// own document store. The whole diagnosis is in this text.
async fn cluster_report(cluster: &TestCluster) -> String {
    let executor = leader_id(cluster);
    let mut lines = Vec::new();
    for node in &cluster.nodes {
        let role = if node.node_id == executor {
            "executor"
        } else {
            "follower"
        };
        let probe = index_probe(node).await;
        // Query at the width this node's own index carries, so a re-created
        // index of a different width is not reported as a missing one.
        let width = probe.as_ref().map(|shape| shape.dim).unwrap_or(DIM);
        let index = match &probe {
            Ok(shape) => format!("{shape:?}"),
            Err(reason) => format!("NO INDEX ({reason})"),
        };
        let hits = match search_probe(node, query_vector(width)).await {
            Ok(ids) => format!("{} hits", ids.len()),
            Err(reason) => format!("NO HITS ({reason})"),
        };
        let docs = match local_doc_count(node).await {
            Ok(n) => n.to_string(),
            Err(reason) => format!("scan failed ({reason})"),
        };
        lines.push(format!(
            "  node {} ({role}): catalog_row={:?} index={index} search={hits} local_docs={docs}",
            node.node_id,
            catalog_params(node),
        ));
    }
    lines.join("\n")
}

/// Ranked hit ids `node` returns for `query` from its own index.
///
/// An index-less node and an index holding no matching vector both answer
/// with an empty list — the core's `NotFound` is tolerated upstream — so a
/// hit COUNT discriminates, while emptiness alone does not.
async fn search_probe(node: &TestClusterNode, query: Vec<f32>) -> Result<Vec<u32>, String> {
    let plan = PhysicalPlan::Vector(VectorOp::Search {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        query_vector: query,
        top_k: ROWS,
        ef_search: 0,
        metric: DistanceMetric::Cosine,
        filter_bitmap: None,
        field_name: FIELD.to_string(),
        rls_filters: Vec::new(),
        inline_prefilter_plan: None,
        ann_options: nodedb_types::VectorAnnOptions::default(),
        skip_payload_fetch: true,
        payload_filters: Vec::new(),
    });
    let resp = execute_on_node(node, plan).await;
    if !resp.success {
        return Err(format!("{:?}", resp.error));
    }
    let payload = resp.payloads.first().ok_or("empty payload")?;
    let Ok(Value::Array(items)) = nodedb_types::value_from_msgpack(payload) else {
        return Err(format!("undecodable hits payload {payload:02x?}"));
    };
    // A bound vector hit carries its storage key as `id`.
    Ok(items
        .iter()
        .filter_map(|hit| match hit {
            Value::Object(map) => match map.get("id") {
                Some(Value::String(key)) => {
                    nodedb_types::StorageKey::parse(key).map(|k| k.surrogate().as_u32())
                }
                _ => None,
            },
            _ => None,
        })
        .collect())
}

/// Ranked hit ids, or `None` when this node holds no index for the key.
async fn local_search_hits(node: &TestClusterNode, query: Vec<f32>) -> Option<Vec<u32>> {
    search_probe(node, query).await.ok()
}

/// One row's `embedding` literal, `width` components wide.
fn insert_sql(id: &str, width: usize) -> String {
    let components: Vec<String> = (0..width).map(|i| format!("{}.0", i + 1)).collect();
    format!(
        "INSERT INTO {COLLECTION} (id, {FIELD}) VALUES ('{id}', ARRAY[{}])",
        components.join(",")
    )
}

/// A query vector of `width` components, pointing at the seeded rows.
fn query_vector(width: usize) -> Vec<f32> {
    (0..width).map(|i| (i + 1) as f32).collect()
}

/// Write [`ROWS`] rows at `width` through the DDL-executing node, then wait
/// until every replica has applied them.
async fn seed_rows(cluster: &TestCluster, prefix: &str, width: usize) {
    for i in 0..ROWS {
        leader(cluster)
            .client
            .simple_query(&insert_sql(&format!("{prefix}{i}"), width))
            .await
            .expect("insert a row at the declared width");
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
}

/// Wait until `node`'s cores hold exactly `expected`.
///
/// On timeout the panic carries every node's catalog row, index shape, search
/// result, and local document count, so the failure names which component is
/// missing rather than only that something is.
async fn wait_for_shape(cluster: &TestCluster, node: &TestClusterNode, expected: &IndexShape) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        if local_index_shape(node).await.as_ref() == Some(expected) {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {} never held the declared vector index {expected:?}\n{}",
                node.node_id,
                cluster_report(cluster).await
            );
        }
        tokio::time::sleep(STEP).await;
    }
}

/// Wait until `node`'s cores hold no index for the key.
async fn wait_for_no_index(cluster: &TestCluster, node: &TestClusterNode) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        if local_index_shape(node).await.is_none() {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {} never tore down the dropped vector index\n{}",
                node.node_id,
                cluster_report(cluster).await
            );
        }
        tokio::time::sleep(STEP).await;
    }
}

/// A 3-node cluster whose collection carries [`INDEX`] and [`ROWS`] rows.
async fn cluster_with_indexed_rows() -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");

    // Baseline: no node holds an index before the DDL, so the probe answering
    // later is the DDL's doing.
    for node in &cluster.nodes {
        assert!(
            local_index_shape(node).await.is_none(),
            "node {} must hold no vector index before CREATE VECTOR INDEX",
            node.node_id
        );
    }

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE VECTOR INDEX {INDEX} ON {COLLECTION} ({FIELD}) \
             METRIC cosine DIM {DIM} M {M} EF_CONSTRUCTION {EF_CONSTRUCTION}"
        ))
        .await
        .expect("CREATE VECTOR INDEX on the metadata leader");
    seed_rows(&cluster, "a", DIM).await;
    cluster
}

/// The declared build parameters must reach the cores of every node that
/// applied the entry, and those cores must index the replicated writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_vector_index_reaches_every_node() {
    let cluster = cluster_with_indexed_rows().await;
    let expected = IndexShape {
        dim: DIM,
        m: M,
        ef_construction: EF_CONSTRUCTION,
        live: ROWS,
    };

    for node in followers(&cluster) {
        wait_for_shape(&cluster, node, &expected).await;
        let shape = local_index_shape(node)
            .await
            .expect("the follower's cores must hold the replicated vector index");
        assert_eq!(
            shape, expected,
            "node {} built the index with parameters the statement never declared",
            node.node_id
        );

        let hits = local_search_hits(node, query_vector(DIM))
            .await
            .unwrap_or_else(|| panic!("vector search failed on node {}", node.node_id));
        assert_eq!(
            hits.len(),
            ROWS,
            "node {} must rank every replicated row from its own index",
            node.node_id
        );
        assert_eq!(
            local_doc_count(node)
                .await
                .expect("scan the follower's own document store"),
            ROWS,
            "node {} must hold every replicated row in its own store",
            node.node_id
        );
    }

    // The executing node holds the same index, so both sides of the contract
    // are asserted, not just the followers'.
    assert_eq!(
        local_index_shape(leader(&cluster)).await,
        Some(expected),
        "the executing node must hold the index it declared"
    );

    cluster.shutdown().await;
}

/// `DROP INDEX` must tear the index down on every node, and a re-`CREATE`
/// with different parameters must install those parameters everywhere.
///
/// The second shape is the discriminating one: a node that kept the dropped
/// index in `vector_collections` refuses the params change, so it would still
/// report the first index's width and connectivity.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drop_and_recreate_vector_index_reach_every_node() {
    let cluster = cluster_with_indexed_rows().await;
    let first = IndexShape {
        dim: DIM,
        m: M,
        ef_construction: EF_CONSTRUCTION,
        live: ROWS,
    };
    for node in followers(&cluster) {
        wait_for_shape(&cluster, node, &first).await;
    }

    cluster
        .exec_ddl_on_any_leader(&format!("DROP INDEX {INDEX}"))
        .await
        .expect("DROP INDEX on the metadata leader");
    // `wait_for_no_index` is the whole post-drop observation: the key is gone
    // from this node's `vector_collections` and `index_configs`. Search adds
    // nothing here — it reads the same map through a path that reports an
    // absent index and an empty index identically.
    for node in &cluster.nodes {
        wait_for_no_index(&cluster, node).await;
    }

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE VECTOR INDEX {INDEX_V2} ON {COLLECTION} ({FIELD}) \
             METRIC cosine DIM {DIM_V2} M {M_V2} EF_CONSTRUCTION {EF_CONSTRUCTION_V2}"
        ))
        .await
        .expect("re-CREATE VECTOR INDEX with different parameters");
    seed_rows(&cluster, "b", DIM_V2).await;

    let second = IndexShape {
        dim: DIM_V2,
        m: M_V2,
        ef_construction: EF_CONSTRUCTION_V2,
        live: ROWS,
    };
    for node in followers(&cluster) {
        wait_for_shape(&cluster, node, &second).await;
        let shape = local_index_shape(node)
            .await
            .expect("the follower's cores must hold the re-created vector index");
        assert_eq!(
            shape, second,
            "node {} kept the dropped index, so the new parameters never took",
            node.node_id
        );

        let hits = local_search_hits(node, query_vector(DIM_V2))
            .await
            .unwrap_or_else(|| panic!("vector search failed on node {}", node.node_id));
        assert_eq!(
            hits.len(),
            ROWS,
            "node {} must rank every row written under the new parameters",
            node.node_id
        );
        assert_eq!(
            local_doc_count(node)
                .await
                .expect("scan the follower's own document store"),
            ROWS * 2,
            "node {} must hold the rows from both widths in its own store",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
