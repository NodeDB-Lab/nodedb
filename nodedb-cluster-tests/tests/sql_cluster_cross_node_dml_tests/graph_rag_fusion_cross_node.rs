// SPDX-License-Identifier: BUSL-1.1

//! GraphRAG fusion in a cluster answers what a single node answers.
//!
//! With a replication factor of 1 the collection's vector and text indexes
//! sit on its owner, and graph edges spread over every data group, because
//! the node keys cover every group. The same collection on a single node is
//! the reference. Each surface is compared on every cluster node:
//!
//! - `GRAPH RAG FUSION` over pgwire, two-source and three-source (BM25);
//! - the same SQL over the native protocol;
//! - the native `GraphRagFusion` opcode (two-source).
//!
//! Results compare exactly: node, RRF score, vector rank and distance, hop
//! distance, and the metadata counts. Only the watermark differs by design.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request};
use nodedb_types::id::VShardId;
use nodedb_types::protocol::{NativeResponse, OpCode, ResponseStatus, TextFields};
use tokio::net::TcpStream;

use crate::common::cluster_harness::{TestCluster, wait_for};
use crate::common::pgwire_harness::TestServer;

const COLL: &str = "rag_xnode";
const MIN_NODES: usize = 24;
const SUPERUSER: &str = "nodedb";
const QUERY: [f64; 3] = [1.0, 0.0, 0.0];

/// Names `d0, d1, …`, enough that their key vShards cover every data group.
fn node_names(groups: &BTreeSet<u64>, group_of: &HashMap<u32, u64>) -> Vec<String> {
    let mut out = Vec::new();
    let mut covered = BTreeSet::new();
    let mut i = 0usize;
    while out.len() < MIN_NODES || covered != *groups {
        let name = format!("d{i}");
        let vshard = VShardId::from_key(name.as_bytes()).as_u32();
        covered.insert(group_of.get(&vshard).copied().unwrap_or(0));
        out.push(name);
        i += 1;
    }
    out
}

/// Every statement that builds the collection: indexes, one document per
/// name with a distinct embedding, and a `hop` ring plus `skip` chords.
fn setup_statements(names: &[String]) -> Vec<String> {
    let mut out = vec![
        format!("CREATE COLLECTION {COLL}"),
        format!("CREATE VECTOR INDEX idx_{COLL}_emb ON {COLL} METRIC cosine DIM 3"),
        format!("CREATE SEARCH INDEX idx_{COLL}_fts ON {COLL} FIELDS body ANALYZER 'standard'"),
    ];
    let n = names.len();
    for (i, name) in names.iter().enumerate() {
        let angle = i as f64 * 0.11;
        let body = if i % 3 == 0 {
            "alpha omega"
        } else {
            "beta gamma"
        };
        out.push(format!(
            "INSERT INTO {COLL} (id, body, embedding) VALUES ('{name}', '{body}', \
             ARRAY[{:.6}, {:.6}, {:.6}])",
            angle.cos(),
            angle.sin(),
            0.01 * i as f64
        ));
    }
    for i in 0..n {
        out.push(format!(
            "GRAPH INSERT EDGE IN '{COLL}' FROM '{}' TO '{}' TYPE 'hop'",
            names[i],
            names[(i + 1) % n]
        ));
        if i % 4 == 0 {
            out.push(format!(
                "GRAPH INSERT EDGE IN '{COLL}' FROM '{}' TO '{}' TYPE 'skip'",
                names[i],
                names[(i + 5) % n]
            ));
        }
    }
    out
}

fn query_array() -> String {
    format!("ARRAY[{:.1}, {:.1}, {:.1}]", QUERY[0], QUERY[1], QUERY[2])
}

/// A fusion whose walk hits its visit cap: the admitted nodes must match.
const CAPPED: usize = 3;

/// The SQL fusions compared: two-source over one label and over every label,
/// three-source with BM25, and a two-source walk cut by `MAX_VISITED`
/// (entry [`CAPPED`]).
fn fusion_sql() -> Vec<String> {
    let q = query_array();
    vec![
        format!(
            "GRAPH RAG FUSION ON {COLL} QUERY {q} VECTOR_FIELD 'embedding' VECTOR_TOP_K 3 \
             EXPANSION_DEPTH 2 EDGE_LABEL 'hop' FINAL_TOP_K 10 RRF_K (60.0, 10.0)"
        ),
        format!(
            "GRAPH RAG FUSION ON {COLL} QUERY {q} VECTOR_FIELD 'embedding' VECTOR_TOP_K 4 \
             EXPANSION_DEPTH 3 FINAL_TOP_K 20 RRF_K (40.0, 20.0)"
        ),
        format!(
            "GRAPH RAG FUSION ON {COLL} QUERY {q} VECTOR_FIELD 'embedding' VECTOR_TOP_K 3 \
             BM25 'alpha' ON 'body' EXPANSION_DEPTH 2 EDGE_LABEL 'hop' FINAL_TOP_K 15 \
             RRF_K (60.0, 35.0, 50.0)"
        ),
        format!(
            "GRAPH RAG FUSION ON {COLL} QUERY {q} VECTOR_FIELD 'embedding' VECTOR_TOP_K 3 \
             EXPANSION_DEPTH 4 MAX_VISITED 7 FINAL_TOP_K 30 RRF_K (60.0, 10.0)"
        ),
    ]
}

/// A fusion answer with every `watermark_lsn` removed and every JSON text
/// cell parsed, so two answers compare by content.
fn normalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) => match sonic_rs::from_str::<serde_json::Value>(&s) {
            Ok(parsed @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => {
                normalize(parsed)
            }
            _ => serde_json::Value::String(s),
        },
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(normalize).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .filter(|(key, _)| key != "watermark_lsn")
                .map(|(key, v)| (key, normalize(v)))
                .collect(),
        ),
        other => other,
    }
}

/// The single `result` cell of a pgwire fusion answer.
async fn pgwire_fusion(client: &tokio_postgres::Client, sql: &str) -> serde_json::Value {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let cell = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no result row"));
    normalize(serde_json::Value::String(cell))
}

fn native_rows(response: &NativeResponse) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = response
        .rows
        .iter()
        .flatten()
        .map(|row| {
            serde_json::Value::Array(row.iter().cloned().map(serde_json::Value::from).collect())
        })
        .collect();
    normalize(serde_json::Value::Array(rows))
}

/// A native session authenticated as the trust superuser, in JSON framing.
async fn native_session(port: u16) -> TcpStream {
    open_trust_session(port, SUPERUSER).await
}

async fn native_call(
    stream: &mut TcpStream,
    seq: u64,
    op: OpCode,
    fields: TextFields,
) -> serde_json::Value {
    let response = send_request(stream, seq, op, fields).await;
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "native {op:?} must succeed: {response:?}"
    );
    native_rows(&response)
}

fn opcode_fields() -> TextFields {
    TextFields {
        collection: Some(COLL.to_string()),
        query_vector: Some(QUERY.iter().map(|v| *v as f32).collect()),
        vector_top_k: Some(3),
        edge_labels: Some(vec!["hop".to_string()]),
        expansion_depth: Some(2),
        final_top_k: Some(10),
        vector_k: Some(60.0),
        graph_k: Some(10.0),
        vector_field: Some("embedding".to_string()),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rag_fusion_across_shards_matches_a_single_node() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    let (groups, group_of): (BTreeSet<u64>, HashMap<u32, u64>) = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let groups: BTreeSet<u64> = routing
            .group_ids()
            .into_iter()
            .filter(|g| *g != 0)
            .collect();
        let mut map = HashMap::new();
        for &g in &groups {
            for vs in routing.vshards_for_group(g) {
                map.insert(vs, g);
            }
        }
        (groups, map)
    };
    wait_for(
        "each data group has exactly one replica",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            groups.iter().all(|&g| {
                cluster
                    .nodes
                    .iter()
                    .filter(|node| node.replicates_data_group(g))
                    .count()
                    == 1
            })
        },
    )
    .await;

    let names = node_names(&groups, &group_of);
    let statements = setup_statements(&names);
    let reference = TestServer::start().await;
    for statement in &statements {
        reference
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("reference {statement}: {e}"));
    }
    let (ddl, writes) = statements.split_at(3);
    for statement in ddl {
        cluster
            .exec_ddl_on_any_leader(statement)
            .await
            .unwrap_or_else(|e| panic!("cluster {statement}: {e}"));
    }
    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    // The writes rotate over the nodes: a key's surrogate comes from its
    // collection home whichever node writes it.
    for (i, statement) in writes.iter().enumerate() {
        cluster.nodes[i % cluster.nodes.len()]
            .client
            .simple_query(statement)
            .await
            .unwrap_or_else(|e| panic!("cluster {statement}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let sql = fusion_sql();
    let mut expected_sql = Vec::new();
    for statement in &sql {
        let answer = pgwire_fusion(&reference.client, statement).await;
        assert!(
            answer["results"].as_array().is_some_and(|r| !r.is_empty()),
            "the reference fusion returns results: {answer}"
        );
        expected_sql.push(answer);
    }
    assert_eq!(
        expected_sql[CAPPED]["metadata"]["truncated"],
        serde_json::Value::Bool(true),
        "the capped reference fusion hits its visit cap: {}",
        expected_sql[CAPPED]
    );
    let mut single = native_session(reference.native_port).await;
    let expected_opcode =
        native_call(&mut single, 2, OpCode::GraphRagFusion, opcode_fields()).await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        for (statement, expected) in sql.iter().zip(&expected_sql) {
            assert_eq!(
                &pgwire_fusion(&node.client, statement).await,
                expected,
                "node {idx}: pgwire `{statement}` must equal the single-node fusion"
            );
            let native = node
                .native_client()
                .query(statement)
                .await
                .unwrap_or_else(|e| panic!("node {idx}: native `{statement}`: {e}"));
            let cell = native
                .rows
                .first()
                .and_then(|row| row.first())
                .cloned()
                .unwrap_or_else(|| panic!("node {idx}: native `{statement}` returned no row"));
            assert_eq!(
                &normalize(serde_json::Value::from(cell)),
                expected,
                "node {idx}: native SQL `{statement}` must equal the single-node fusion"
            );
        }
        let mut clustered = native_session(node.native_port).await;
        assert_eq!(
            native_call(&mut clustered, 2, OpCode::GraphRagFusion, opcode_fields()).await,
            expected_opcode,
            "node {idx}: the native fusion opcode must equal the single-node answer"
        );
    }

    cluster.shutdown().await;
}
