// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH RAG FUSION` names every row by its primary key.
//!
//! A row in no edge has no graph node name. The fusion keys it by its
//! surrogate, and the response boundary names it through the resolver every
//! search response uses. A client then joins each `node_id` to its document.
//! Naming runs after fusion: a key equal to another row's surrogate decimal
//! never fuses with that row.

use crate::harness::TestServer;

/// The hits of a fusion answer, one JSON object per row.
async fn fusion_results(server: &TestServer, sql: &str) -> Vec<serde_json::Value> {
    server
        .fusion_hits(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Each result's `node_id`, in rank order.
fn node_ids(results: &[serde_json::Value]) -> Vec<String> {
    results
        .iter()
        .map(|r| {
            r.get("node_id")
                .and_then(|n| n.as_str())
                .unwrap_or_else(|| panic!("a result carries no node_id: {r}"))
                .to_string()
        })
        .collect()
}

/// The single result named `node_id`.
fn result_named<'a>(results: &'a [serde_json::Value], node_id: &str) -> &'a serde_json::Value {
    let named: Vec<&serde_json::Value> = results
        .iter()
        .filter(|r| r.get("node_id").and_then(|n| n.as_str()) == Some(node_id))
        .collect();
    assert_eq!(
        named.len(),
        1,
        "exactly one result must be named {node_id:?}; got {results:?}"
    );
    named[0]
}

/// Every `node_id` names a row the client can read back by its key.
async fn assert_every_node_id_joins(server: &TestServer, collection: &str, ids: &[String]) {
    for id in ids {
        let rows = server
            .query_text(&format!("SELECT id FROM {collection} WHERE id = '{id}'"))
            .await
            .unwrap_or_else(|e| panic!("select {id}: {e}"));
        assert_eq!(
            rows.len(),
            1,
            "node_id {id:?} must join to exactly one row of {collection}; got {rows:?}"
        );
    }
}

async fn vector_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} METRIC cosine DIM 2"
        ))
        .await
        .unwrap();
}

async fn text_collection(server: &TestServer, name: &str) {
    vector_collection(server, name).await;
    server
        .exec(&format!(
            "CREATE SEARCH INDEX idx_{name}_fts ON {name} FIELDS body ANALYZER 'standard'"
        ))
        .await
        .unwrap();
}

/// The vector-nearest row is in no edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_hit_in_no_edge_returns_its_primary_key() {
    let server = TestServer::start().await;
    vector_collection(&server, "ragn_vec").await;
    for (id, v) in [
        ("lonely", "1.0, 0.0"),
        ("a", "0.0, 1.0"),
        ("b", "-1.0, 0.0"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO ragn_vec (id, embedding) VALUES ('{id}', ARRAY[{v}])"
            ))
            .await
            .unwrap();
    }
    server
        .exec("GRAPH INSERT EDGE IN 'ragn_vec' FROM 'a' TO 'b' TYPE 'hop'")
        .await
        .unwrap();

    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_vec QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 2 EXPANSION_DEPTH 1 EDGE_LABEL 'hop' FINAL_TOP_K 10",
    )
    .await;
    let ids = node_ids(&results);
    assert!(
        ids.contains(&"lonely".to_string()),
        "the hit in no edge must answer its key; got {ids:?}"
    );
    assert!(
        results.iter().all(|r| r.get("surrogate").is_none()),
        "the naming marker never reaches a client: {results:?}"
    );
    assert_every_node_id_joins(&server, "ragn_vec", &ids).await;
}

/// A text-only hit in no edge, three sources.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_hit_in_no_edge_returns_its_primary_key() {
    let server = TestServer::start().await;
    text_collection(&server, "ragn_text").await;
    server
        .exec(
            "INSERT INTO ragn_text (id, body, embedding) \
             VALUES ('near', 'plain prose', ARRAY[1.0, 0.0])",
        )
        .await
        .unwrap();
    // `wordy` is far from the query vector and the only `entangled` match.
    server
        .exec(
            "INSERT INTO ragn_text (id, body, embedding) \
             VALUES ('wordy', 'entangled photons', ARRAY[-1.0, 0.0])",
        )
        .await
        .unwrap();

    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_text QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 1 BM25 'entangled' ON 'body' EXPANSION_DEPTH 1 FINAL_TOP_K 10 \
         RRF_K (60.0, 60.0, 60.0)",
    )
    .await;
    let ids = node_ids(&results);
    assert!(
        ids.contains(&"wordy".to_string()),
        "the text-only hit must answer its key; got {ids:?}"
    );
    assert_every_node_id_joins(&server, "ragn_text", &ids).await;
}

/// Both legs hit one row in no edge: one named entry with both contributions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_and_text_hit_in_no_edge_returns_one_named_row() {
    let server = TestServer::start().await;
    text_collection(&server, "ragn_both").await;
    server
        .exec(
            "INSERT INTO ragn_both (id, body, embedding) \
             VALUES ('both', 'entangled photons', ARRAY[1.0, 0.0])",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO ragn_both (id, body, embedding) \
             VALUES ('other', 'plain prose', ARRAY[0.9, 0.1])",
        )
        .await
        .unwrap();

    // Text rank 0 alone is 0.5 under text_k = 1; both legs pass 0.5.
    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_both QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 5 BM25 'entangled' ON 'body' EXPANSION_DEPTH 1 FINAL_TOP_K 10 \
         RRF_K (60.0, 1.0, 60.0)",
    )
    .await;
    let both = result_named(&results, "both");
    let score = both
        .get("rrf_score")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    assert!(score > 0.5, "both legs must fuse onto `both`: {results:?}");
}

/// A text hit on a graph node fuses with that node's vector and graph entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_hit_on_a_graph_node_fuses_with_its_node() {
    let server = TestServer::start().await;
    text_collection(&server, "ragn_node").await;
    server
        .exec(
            "INSERT INTO ragn_node (id, body, embedding) \
             VALUES ('a', 'entangled photons', ARRAY[1.0, 0.0])",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO ragn_node (id, body, embedding) \
             VALUES ('b', 'plain prose', ARRAY[-1.0, 0.0])",
        )
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'ragn_node' FROM 'a' TO 'b' TYPE 'hop'")
        .await
        .unwrap();

    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_node QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 1 BM25 'entangled' ON 'body' EXPANSION_DEPTH 1 EDGE_LABEL 'hop' \
         FINAL_TOP_K 10 RRF_K (60.0, 1.0, 60.0)",
    )
    .await;
    let a = result_named(&results, "a");
    assert_eq!(
        a.get("hop_distance").and_then(serde_json::Value::as_u64),
        Some(0),
        "the fused entry is the graph node itself: {results:?}"
    );
    let score = a
        .get("rrf_score")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    assert!(
        score > 0.5,
        "the text leg must fuse with the node's entry: {results:?}"
    );
}

/// A strict collection's row in no edge answers its declared key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_collection_hit_returns_its_declared_key() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ragn_strict (code TEXT PRIMARY KEY, embedding VECTOR(2)) \
             WITH (engine = 'document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_ragn_strict ON ragn_strict (embedding) METRIC cosine DIM 2")
        .await
        .unwrap();
    for (code, v) in [("lonely", "[1.0, 0.0]"), ("far", "[-1.0, 0.0]")] {
        server
            .exec(&format!(
                "INSERT INTO ragn_strict (code, embedding) VALUES ('{code}', '{v}')"
            ))
            .await
            .unwrap();
    }

    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_strict QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 2 EXPANSION_DEPTH 1 FINAL_TOP_K 10",
    )
    .await;
    let ids = node_ids(&results);
    assert!(
        ids.contains(&"lonely".to_string()) && ids.contains(&"far".to_string()),
        "a strict row answers its declared key; got {ids:?}"
    );
}

/// A row inserted with no id answers the decimal identity `SELECT id` reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_row_returns_its_decimal_identity() {
    let server = TestServer::start().await;
    vector_collection(&server, "ragn_minted").await;
    server
        .exec("INSERT INTO ragn_minted (embedding) VALUES (ARRAY[1.0, 0.0])")
        .await
        .unwrap();
    let ids = server
        .query_text("SELECT id FROM ragn_minted")
        .await
        .expect("select id");
    assert_eq!(ids.len(), 1, "one minted row; got {ids:?}");

    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_minted QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 1 EXPANSION_DEPTH 1 FINAL_TOP_K 10",
    )
    .await;
    assert_eq!(
        node_ids(&results),
        ids,
        "the minted row answers the identity SELECT id reads"
    );
}

/// A key equal to another row's surrogate decimal stays a separate entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decimal_key_stays_apart_from_an_equal_surrogate() {
    let server = TestServer::start().await;
    vector_collection(&server, "ragn_decimal").await;
    server
        .exec("INSERT INTO ragn_decimal (id, embedding) VALUES ('x', ARRAY[1.0, 0.0])")
        .await
        .unwrap();
    let surrogate = server
        .query_text(
            "SELECT s._surrogate FROM \
             (SELECT * FROM ragn_decimal \
              ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0]) LIMIT 1) s",
        )
        .await
        .expect("read x's surrogate")
        .into_iter()
        .next()
        .expect("x has a surrogate");
    // Row `<surrogate>` is a graph node one hop from `y`; `x` is in no edge.
    server
        .exec(&format!(
            "INSERT INTO ragn_decimal (id, embedding) VALUES ('{surrogate}', ARRAY[-1.0, 0.0])"
        ))
        .await
        .unwrap();
    server
        .exec("INSERT INTO ragn_decimal (id, embedding) VALUES ('y', ARRAY[0.0, 1.0])")
        .await
        .unwrap();
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN 'ragn_decimal' FROM 'y' TO '{surrogate}' TYPE 'hop'"
        ))
        .await
        .unwrap();

    let results = fusion_results(
        &server,
        "GRAPH RAG FUSION ON ragn_decimal QUERY ARRAY[1.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 2 EXPANSION_DEPTH 1 EDGE_LABEL 'hop' FINAL_TOP_K 10",
    )
    .await;
    let x = result_named(&results, "x");
    assert!(
        x.get("hop_distance").is_none_or(serde_json::Value::is_null),
        "x is in no edge: {results:?}"
    );
    let node = result_named(&results, &surrogate);
    assert_eq!(
        node.get("hop_distance").and_then(serde_json::Value::as_u64),
        Some(1),
        "the decimal-keyed node is reached by expansion: {results:?}"
    );
    assert!(
        node.get("vector_rank")
            .is_none_or(serde_json::Value::is_null),
        "the decimal-keyed node never takes x's vector rank: {results:?}"
    );
}
