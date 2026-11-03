// SPDX-License-Identifier: BUSL-1.1

//! Search responses name rows by primary key on a node outside the home group.
//!
//! With a replication factor of 1, one node replicates the collection's home
//! group. Every row is written through a second node, so the third node's
//! catalog binds none of the collection's keys. That node answers each
//! search by asking the home leader for the keys its catalog lacks:
//!
//! - `GRAPH RAG FUSION`, two-source and three-source, over pgwire, native
//!   SQL and the native `GraphRagFusion` opcode, on every node;
//! - a body-less vector search (`SELECT id, vector_distance(...)`);
//! - a hybrid search (`rrf_score(vector_distance(...), bm25_score(...))`).

use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request};
use nodedb_types::protocol::{OpCode, ResponseStatus, TextFields};

use crate::common::cluster_harness::{TestCluster, wait_for};

const COLL: &str = "rag_naming";
const STRICT: &str = "rag_naming_strict";
const SUPERUSER: &str = "nodedb";
/// Every key the collection holds.
const KEYS: [&str; 4] = ["lonely", "wordy", "a", "b"];

/// The statements that build the schemaless collection: `lonely` is the
/// vector-nearest row in no edge, `wordy` is the only `entangled` match and
/// far from the query, and `a -hop-> b` is the graph.
fn collection_writes() -> Vec<String> {
    vec![
        format!(
            "INSERT INTO {COLL} (id, body, embedding) VALUES ('lonely', 'plain prose', \
             ARRAY[1.0, 0.0])"
        ),
        format!(
            "INSERT INTO {COLL} (id, body, embedding) VALUES ('wordy', 'entangled photons', \
             ARRAY[-1.0, 0.0])"
        ),
        format!("INSERT INTO {COLL} (id, body, embedding) VALUES ('a', 'beta', ARRAY[0.0, 1.0])"),
        format!(
            "INSERT INTO {COLL} (id, body, embedding) VALUES ('b', 'gamma', ARRAY[-0.5, -0.5])"
        ),
        format!("GRAPH INSERT EDGE IN '{COLL}' FROM 'a' TO 'b' TYPE 'hop'"),
    ]
}

/// The first column of every row a native SQL answer returns: each hit's
/// `node_id`.
fn native_node_ids(response: &nodedb_types::QueryResult) -> Vec<String> {
    response
        .rows
        .iter()
        .filter_map(|row| row.first())
        .filter_map(|cell| {
            serde_json::Value::from(cell.clone())
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}

/// The first column of every row a pgwire query returns.
async fn first_column(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// Every name is one of `keys`, and `expected` is among them.
fn assert_named(names: &[String], keys: &[&str], expected: &str, what: &str) {
    assert!(
        names.iter().any(|n| n == expected),
        "{what}: `{expected}` must answer its key; got {names:?}"
    );
    for name in names {
        assert!(
            keys.contains(&name.as_str()),
            "{what}: every row answers a primary key, never a decimal; got {names:?}"
        );
    }
}

fn two_source() -> String {
    format!(
        "GRAPH RAG FUSION ON {COLL} QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 2 EXPANSION_DEPTH 1 EDGE_LABEL 'hop' FINAL_TOP_K 10"
    )
}

fn three_source() -> String {
    format!(
        "GRAPH RAG FUSION ON {COLL} QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 2 BM25 'entangled' ON 'body' EXPANSION_DEPTH 1 EDGE_LABEL 'hop' \
         FINAL_TOP_K 10 RRF_K (60.0, 60.0, 60.0)"
    )
}

fn opcode_fields() -> TextFields {
    TextFields {
        collection: Some(COLL.to_string()),
        query_vector: Some(vec![1.0, 0.0]),
        vector_top_k: Some(2),
        edge_labels: Some(vec!["hop".to_string()]),
        expansion_depth: Some(1),
        final_top_k: Some(10),
        vector_k: Some(60.0),
        graph_k: Some(60.0),
        vector_field: Some("embedding".to_string()),
        ..Default::default()
    }
}

/// A three-node RF1 cluster, each data group on exactly one node.
async fn rf1_cluster() -> TestCluster {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    let groups: Vec<u64> = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing
            .group_ids()
            .into_iter()
            .filter(|g| *g != 0)
            .collect()
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
    cluster
}

/// The indexes of the node that writes and the node that only reads:
/// neither replicates `collection`'s home group.
fn writer_and_reader(cluster: &TestCluster, collection: &str) -> (usize, usize) {
    let group = cluster.nodes[0]
        .group_id_for_collection(collection)
        .expect("the collection's data group");
    let outside: Vec<usize> = cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| !node.replicates_data_group(group))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(outside.len(), 2, "two nodes sit outside the home group");
    (outside[0], outside[1])
}

/// Run `ddl` through any leader and wait until every node sees it.
async fn create(cluster: &TestCluster, ddl: &[String], collections: usize) {
    for statement in ddl {
        cluster
            .exec_ddl_on_any_leader(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    wait_for(
        "every node sees the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= collections)
        },
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn hit_in_no_edge_returns_its_primary_key_on_every_node() {
    let cluster = rf1_cluster().await;
    create(
        &cluster,
        &[
            format!("CREATE COLLECTION {COLL}"),
            format!("CREATE VECTOR INDEX idx_{COLL}_emb ON {COLL} METRIC cosine DIM 2"),
            format!("CREATE SEARCH INDEX idx_{COLL}_fts ON {COLL} FIELDS body ANALYZER 'standard'"),
        ],
        1,
    )
    .await;
    let (writer, reader) = writer_and_reader(&cluster, COLL);
    for statement in collection_writes() {
        cluster.nodes[writer]
            .client
            .simple_query(&statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        for (sql, expected) in [(two_source(), "lonely"), (three_source(), "wordy")] {
            let ids = first_column(&node.client, &sql).await;
            assert!(!ids.is_empty(), "node {idx}: `{sql}` returned no row");
            assert_named(&ids, &KEYS, expected, &format!("node {idx} pgwire `{sql}`"));
            let native = node
                .native_client()
                .query(&sql)
                .await
                .unwrap_or_else(|e| panic!("node {idx}: native `{sql}`: {e}"));
            let native_ids = native_node_ids(&native);
            assert!(
                !native_ids.is_empty(),
                "node {idx}: native `{sql}` returned no row"
            );
            assert_named(
                &native_ids,
                &KEYS,
                expected,
                &format!("node {idx} native `{sql}`"),
            );
        }
        let mut session = open_trust_session(node.native_port, SUPERUSER).await;
        let response = send_request(&mut session, 2, OpCode::GraphRagFusion, opcode_fields()).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "node {idx}: native fusion opcode: {response:?}"
        );
        let answer = format!("{:?}", response.rows);
        assert!(
            answer.contains("lonely"),
            "node {idx}: the opcode answer names `lonely`: {answer}"
        );
    }

    // The reader's catalog binds no key of the collection: a body-less
    // vector search and a hybrid search still answer primary keys.
    let reader_client = &cluster.nodes[reader].client;
    let vector_ids = first_column(
        reader_client,
        &format!(
            "SELECT id, vector_distance(embedding, ARRAY[1.0, 0.0]) AS d FROM {COLL} \
             ORDER BY d LIMIT 2"
        ),
    )
    .await;
    assert_named(
        &vector_ids,
        &KEYS,
        "lonely",
        "reader body-less vector search",
    );
    let hybrid_ids = first_column(
        reader_client,
        &format!(
            "SELECT id, rrf_score(vector_distance(embedding, ARRAY[1.0, 0.0]), \
             bm25_score(body, 'entangled')) AS score FROM {COLL} ORDER BY score DESC LIMIT 5"
        ),
    )
    .await;
    assert_named(&hybrid_ids, &KEYS, "wordy", "reader hybrid search");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn strict_collection_hit_returns_its_declared_key_on_every_node() {
    let cluster = rf1_cluster().await;
    create(
        &cluster,
        &[
            format!(
                "CREATE COLLECTION {STRICT} (code TEXT PRIMARY KEY, embedding VECTOR(2)) \
                 WITH (engine = 'document_strict')"
            ),
            format!("CREATE VECTOR INDEX idx_{STRICT} ON {STRICT} (embedding) METRIC cosine DIM 2"),
        ],
        1,
    )
    .await;
    let (writer, _) = writer_and_reader(&cluster, STRICT);
    for (code, v) in [("lonely", "[1.0, 0.0]"), ("far", "[-1.0, 0.0]")] {
        let statement = format!("INSERT INTO {STRICT} (code, embedding) VALUES ('{code}', '{v}')");
        cluster.nodes[writer]
            .client
            .simple_query(&statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let sql = format!(
        "GRAPH RAG FUSION ON {STRICT} QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 2 EXPANSION_DEPTH 1 FINAL_TOP_K 10"
    );
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let ids = first_column(&node.client, &sql).await;
        assert!(!ids.is_empty(), "node {idx}: `{sql}` returned no row");
        assert_named(
            &ids,
            &["lonely", "far"],
            "lonely",
            &format!("node {idx} strict fusion"),
        );
    }

    cluster.shutdown().await;
}
