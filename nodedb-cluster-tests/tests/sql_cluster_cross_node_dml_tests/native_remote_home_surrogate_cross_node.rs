// SPDX-License-Identifier: BUSL-1.1

//! Native writes issued on a node outside a collection's home group plan with
//! the surrogate the home binds.
//!
//! With a replication factor of 1, each data group lives on one node. Each
//! write below goes over the native protocol to a node that does not replicate
//! the collection's home group:
//!
//! - a document batch insert (`DocumentBatchInsert`)
//! - a KV put (`PointPut` on a KV collection)
//! - a columnar ingest (`ColumnarInsert`)
//! - an edge insert (`EdgePut`), whose endpoints are keys of the edge's
//!   collection
//!
//! For every key the home binds one surrogate, and the issuing node keeps that
//! same winner in its own catalog.

use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request};
use nodedb_types::protocol::{BatchDocument, OpCode, ResponseStatus, TextFields};
use nodedb_types::{CollectionKey, DatabaseId, TenantId};
use tokio::net::TcpStream;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const DOCS: &str = "nrh_docs";
const KV: &str = "nrh_kv";
const COLS: &str = "nrh_cols";
const EDGES: &str = "nrh_edges";
/// The trust superuser the harness bootstraps.
const SUPERUSER: &str = "nodedb";
/// The tenant the trust superuser writes as.
const TENANT: TenantId = TenantId::new(1);

/// A native session on `port`, authenticated as the trust superuser.
async fn native_session(port: u16) -> TcpStream {
    open_trust_session(port, SUPERUSER).await
}

/// The collection's home node and a node outside its home group, once
/// exactly one node replicates that group.
async fn home_and_remote(cluster: &TestCluster, collection: &str) -> (usize, usize) {
    let group_id = cluster.nodes[0]
        .group_id_for_collection(collection)
        .unwrap_or_else(|| panic!("the data group of '{collection}'"));
    wait_for(
        &format!("exactly one node replicates the group of '{collection}'"),
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .filter(|node| node.replicates_data_group(group_id))
                .count()
                == 1
        },
    )
    .await;
    let home = cluster
        .nodes
        .iter()
        .position(|node| node.replicates_data_group(group_id))
        .unwrap_or_else(|| panic!("the home node of '{collection}'"));
    let remote = cluster
        .nodes
        .iter()
        .position(|node| !node.replicates_data_group(group_id))
        .unwrap_or_else(|| panic!("a node outside the home group of '{collection}'"));
    (home, remote)
}

async fn write(stream: &mut TcpStream, seq: u64, op: OpCode, fields: TextFields) {
    let response = send_request(stream, seq, op, fields).await;
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "native {op:?} on a remote node must succeed: {response:?}"
    );
}

/// The surrogate `node`'s catalog binds `pk` to in `collection`.
fn bound(node: &TestClusterNode, collection: &str, pk: &str) -> Option<u32> {
    node.shared
        .surrogate_assigner
        .lookup_bound(
            CollectionKey::from_bare(DatabaseId::DEFAULT, collection),
            TENANT,
            pk.as_bytes(),
        )
        .unwrap_or_else(|e| panic!("lookup of '{pk}' in '{collection}': {e}"))
        .map(|surrogate| surrogate.as_u32())
}

/// Every key in `pks` is bound at the home, to a surrogate the issuing node
/// also holds, and distinct keys hold distinct surrogates.
fn assert_home_winners(
    cluster: &TestCluster,
    collection: &str,
    pks: &[&str],
    home: usize,
    remote: usize,
) {
    let mut seen = std::collections::HashSet::new();
    for pk in pks {
        let at_home = bound(&cluster.nodes[home], collection, pk).unwrap_or_else(|| {
            panic!("the home of '{collection}' must bind '{pk}' written on a remote node")
        });
        assert_ne!(
            at_home, 0,
            "'{pk}' in '{collection}' must not bind the zero surrogate"
        );
        assert_eq!(
            bound(&cluster.nodes[remote], collection, pk),
            Some(at_home),
            "the issuing node must keep the home's winner for '{pk}' in '{collection}'"
        );
        assert!(
            seen.insert(at_home),
            "'{pk}' in '{collection}' shares surrogate {at_home} with another key"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_writes_on_a_remote_node_plan_with_the_home_surrogate() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    let ddl = [
        format!("CREATE COLLECTION {DOCS}"),
        format!("CREATE COLLECTION {KV} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"),
        format!("CREATE COLLECTION {COLS} COLUMNS (id TEXT, v BIGINT) WITH (engine='columnar')"),
        format!("CREATE COLLECTION {EDGES}"),
    ];
    for statement in &ddl {
        cluster
            .exec_ddl_on_any_leader(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    wait_for(
        "all 3 nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 4)
        },
    )
    .await;

    // Document batch insert.
    let (home, remote) = home_and_remote(&cluster, DOCS).await;
    let doc_ids = ["d0", "d1", "d2", "d3"];
    let mut session = native_session(cluster.nodes[remote].native_port).await;
    write(
        &mut session,
        2,
        OpCode::DocumentBatchInsert,
        TextFields {
            collection: Some(DOCS.to_string()),
            documents: Some(
                doc_ids
                    .iter()
                    .map(|id| BatchDocument {
                        id: (*id).to_string(),
                        fields: nodedb_types::Value::Object(
                            [(
                                "name".to_string(),
                                nodedb_types::Value::String((*id).to_string()),
                            )]
                            .into(),
                        ),
                    })
                    .collect(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_home_winners(&cluster, DOCS, &doc_ids, home, remote);

    // KV put.
    let (home, remote) = home_and_remote(&cluster, KV).await;
    let mut session = native_session(cluster.nodes[remote].native_port).await;
    write(
        &mut session,
        2,
        OpCode::PointPut,
        TextFields {
            collection: Some(KV.to_string()),
            document_id: Some("k0".to_string()),
            data: Some(
                nodedb_types::json_to_msgpack(&serde_json::json!({ "n": 0 })).expect("KV value"),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_home_winners(&cluster, KV, &["k0"], home, remote);

    // Columnar ingest.
    let (home, remote) = home_and_remote(&cluster, COLS).await;
    let col_ids = ["c0", "c1", "c2"];
    let payload = sonic_rs::to_vec(
        &col_ids
            .iter()
            .enumerate()
            .map(|(i, id)| serde_json::json!({ "id": id, "v": i }))
            .collect::<Vec<_>>(),
    )
    .expect("columnar payload");
    let mut session = native_session(cluster.nodes[remote].native_port).await;
    write(
        &mut session,
        2,
        OpCode::ColumnarInsert,
        TextFields {
            collection: Some(COLS.to_string()),
            payload: Some(payload),
            format: Some("json".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert_home_winners(&cluster, COLS, &col_ids, home, remote);

    // Edge insert: both endpoints are keys of the edge's collection.
    let (home, remote) = home_and_remote(&cluster, EDGES).await;
    let mut session = native_session(cluster.nodes[remote].native_port).await;
    write(
        &mut session,
        2,
        OpCode::EdgePut,
        TextFields {
            collection: Some(EDGES.to_string()),
            from_node: Some("e_src".to_string()),
            to_node: Some("e_dst".to_string()),
            edge_type: Some("rel".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert_home_winners(&cluster, EDGES, &["e_src", "e_dst"], home, remote);

    cluster.shutdown().await;
}
