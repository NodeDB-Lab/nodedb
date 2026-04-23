//! Regression coverage for the `GRAPH RAG FUSION` DSL and the `SEARCH ... USING
//! FUSION` handler's RRF_K handling.
//!
//! The broken invariant: every graph DSL statement accessible via pgwire MUST be
//! wired through `graph_parse.rs` → `NodedbStatement` variant → `graph_ops::
//! dispatch_typed`. `GRAPH RAG FUSION` violates this — it exists only at the
//! executor level (`GraphOp::RagFusion`) and is only reachable via the separate
//! `SEARCH … USING FUSION` syntax, which also hard-codes `rrf_k: (60.0, 60.0)`
//! and cannot accept user-specified RRF_K values.
//!
//! Sibling failures covered here (all share the same missing-wiring flaw):
//! 1. `GRAPH RAG FUSION ON <col>` syntax is not in the parser → SQLSTATE 42601.
//! 2. `RRF_K (k1, k2)` pair cannot be parsed; tokenizer strips `()`; second
//!    value is silently discarded.
//! 3. `VECTOR_TOP_K` cap (22023) is unreachable because the syntax never routes
//!    to `clamped_param`; the cap check gets the same 42601 as everything else.
//! 4. Two `GRAPH RAG FUSION` queries with radically different `graph_k` values
//!    produce identical scores because the hard-coded 60.0 overrides both.

mod common;

use common::pgwire_harness::TestServer;

// ── 1. GRAPH RAG FUSION syntax is routed, not rejected as syntax error ───────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_syntax_reaches_executor() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ragf_route").await.unwrap();

    // Minimal form — no vector index, so results will be empty, but the
    // statement must NOT be rejected by the SQL parser with SQLSTATE 42601.
    // The spec: "unknown DSL command" is 42601; "empty results" is success.
    let result = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_route \
             QUERY ARRAY[0.1, 0.2] \
             VECTOR_TOP_K 5 \
             FINAL_TOP_K 3",
        )
        .await;

    if let Err(msg) = &result {
        assert!(
            !msg.to_lowercase().contains("42601") && !msg.to_lowercase().contains("syntax"),
            "GRAPH RAG FUSION must route to executor, not produce a syntax error; got: {msg}"
        );
    }
}

// ── 2. RRF_K (k1, k2) pair is parsed and both values forwarded ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_rrf_k_pair_is_accepted() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ragf_rrfk").await.unwrap();

    // The tokenizer strips `(` and `)`, collapsing `RRF_K (60.0, 35.0)` to
    // three consecutive tokens. Without a dedicated `float_pair_after` helper
    // the second value (graph_k = 35.0) is silently dropped and the default
    // (10.0) used. The statement must not error on the pair syntax.
    let result = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_rrfk \
             QUERY ARRAY[0.1, 0.2] \
             VECTOR_TOP_K 5 \
             EXPANSION_DEPTH 2 \
             EDGE_LABEL 'related_to' \
             FINAL_TOP_K 10 \
             RRF_K (60.0, 35.0)",
        )
        .await;

    if let Err(msg) = &result {
        assert!(
            !msg.to_lowercase().contains("42601") && !msg.to_lowercase().contains("syntax"),
            "RRF_K (k1, k2) pair syntax must be accepted without a parse error; got: {msg}"
        );
    }
}

// ── 3. VECTOR_TOP_K cap enforcement reaches the handler ──────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_vector_top_k_cap_enforced() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ragf_cap").await.unwrap();

    // Once `GRAPH RAG FUSION` is routed through the handler, `VECTOR_TOP_K`
    // values exceeding the cap must be rejected with SQLSTATE 22023 — the same
    // guard that `SEARCH … USING FUSION` already applies. Today this gets 42601
    // because the syntax is never routed; after the fix it must get 22023.
    server
        .expect_error(
            "GRAPH RAG FUSION ON ragf_cap \
             QUERY ARRAY[0.1] \
             VECTOR_TOP_K 999999 \
             FINAL_TOP_K 3",
            "22023",
        )
        .await;
}

// ── 4. Result shape: rrf_score + node_id + metadata ──────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_returns_structured_result() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ragf_shape").await.unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX idx_ragf_shape ON ragf_shape \
             METRIC cosine DIM 3",
        )
        .await
        .unwrap();

    // Insert two nodes and an edge so the executor has something to traverse.
    server
        .exec("INSERT INTO ragf_shape (id, embedding) VALUES ('n1', ARRAY[1.0, 0.0, 0.0])")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ragf_shape (id, embedding) VALUES ('n2', ARRAY[0.5, 0.5, 0.0])")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'ragf_shape' FROM 'n1' TO 'n2' TYPE 'related_to'")
        .await
        .unwrap();

    let rows = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_shape \
             QUERY ARRAY[1.0, 0.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 5 \
             EXPANSION_DEPTH 1 \
             EDGE_LABEL 'related_to' \
             FINAL_TOP_K 10 \
             RRF_K (60.0, 35.0)",
        )
        .await
        .expect("GRAPH RAG FUSION must succeed and return a structured result");

    let blob = rows.join("");
    assert!(
        blob.contains("rrf_score"),
        "result must contain 'rrf_score' field; got: {blob}"
    );
    assert!(
        blob.contains("node_id"),
        "result must contain 'node_id' field; got: {blob}"
    );
    assert!(
        blob.contains("metadata"),
        "result must contain 'metadata' field; got: {blob}"
    );
}

// ── 5. Different RRF_K graph_k values produce different scores ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_graph_k_affects_expanded_node_score() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ragf_rrfk_score")
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX idx_ragf_rrfk ON ragf_rrfk_score \
             METRIC cosine DIM 2",
        )
        .await
        .unwrap();

    // n1 is the vector-nearest node to the query; n2 is only reachable via BFS.
    // With graph_k=1.0 (low) n2 gets a large graph RRF contribution: 1/(1+0+1)=0.5.
    // With graph_k=10000.0 (high) n2's graph contribution is negligible: ≈0.0001.
    // Since `search_fusion.rs` hard-codes rrf_k=(60.0,60.0), both queries today
    // return identical scores — proving the user-supplied RRF_K is discarded.
    server
        .exec("INSERT INTO ragf_rrfk_score (id, embedding) VALUES ('n1', ARRAY[1.0, 0.0])")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ragf_rrfk_score (id, embedding) VALUES ('n2', ARRAY[-1.0, 0.0])")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'ragf_rrfk_score' FROM 'n1' TO 'n2' TYPE 'hop'")
        .await
        .unwrap();

    // graph_k = 1.0 — graph hop rank strongly influences final score.
    let rows_low_k = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_rrfk_score \
             QUERY ARRAY[1.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 1 \
             EXPANSION_DEPTH 1 \
             EDGE_LABEL 'hop' \
             FINAL_TOP_K 5 \
             RRF_K (60.0, 1.0)",
        )
        .await
        .expect("low graph_k query must succeed");

    // graph_k = 10000.0 — graph hop rank is nearly irrelevant.
    let rows_high_k = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_rrfk_score \
             QUERY ARRAY[1.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 1 \
             EXPANSION_DEPTH 1 \
             EDGE_LABEL 'hop' \
             FINAL_TOP_K 5 \
             RRF_K (60.0, 10000.0)",
        )
        .await
        .expect("high graph_k query must succeed");

    // n2 is only reachable via BFS (not in vector top-1). Its rrf_score must differ
    // between the two queries because graph_k changes its graph-component weight.
    // Regression guard: if both blobs are identical, the user's RRF_K is being silently
    // discarded in favour of a hard-coded constant.
    assert_ne!(
        rows_low_k.join(""),
        rows_high_k.join(""),
        "rrf_score for graph-expanded node n2 must differ between \
         RRF_K (60.0, 1.0) and RRF_K (60.0, 10000.0); identical output \
         means the graph_k value is being ignored"
    );
}
