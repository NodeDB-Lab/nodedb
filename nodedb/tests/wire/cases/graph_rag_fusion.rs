// SPDX-License-Identifier: BUSL-1.1

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

use crate::harness::TestServer;

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
    // three consecutive tokens. A reader that stops at the first value drops
    // the second (graph_k = 35.0) and the default applies. The statement must
    // not error on the pair syntax.
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

    // `GRAPH RAG FUSION` must route through the handler so `VECTOR_TOP_K`
    // values exceeding the cap are rejected with SQLSTATE 22023 — the same
    // guard that `SEARCH … USING FUSION` already applies, not 42601 from an
    // unrouted syntax.
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

// ── 5. Fusion meets on surrogate identity, not on node names ────────────────

/// A vector hit that is not a graph node must still rank, under a key that can
/// meet the other legs.
///
/// Expansion must seed from the surrogate directly, not mint a node *name*
/// from each hit's surrogate purely to start the walk. Minting a name falls
/// back to a `__local_<hnsw_id>` sentinel when the surrogate is bound to no
/// graph node — an index-internal id that can never equal another leg's key,
/// so such a row could never fuse, and it would leak into the response's
/// `node_id`. The reporting key falls back to the document identity instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_only_hit_ranks_without_an_index_local_sentinel() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ragf_surrogate")
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX idx_ragf_surrogate ON ragf_surrogate \
             METRIC cosine DIM 2",
        )
        .await
        .unwrap();

    // `lonely` is the vector-nearest row and participates in no edge, so it has
    // no graph node binding — exactly the case a local-id sentinel would cover.
    server
        .exec("INSERT INTO ragf_surrogate (id, embedding) VALUES ('lonely', ARRAY[1.0, 0.0])")
        .await
        .unwrap();

    let rows = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_surrogate \
             QUERY ARRAY[1.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 3 \
             EXPANSION_DEPTH 1 \
             FINAL_TOP_K 5",
        )
        .await
        .expect("a vector-only hit must still produce a ranked result");

    let blob = rows.join("");
    assert!(
        !blob.contains("__local_"),
        "an index-local id must never surface as a node_id; got: {blob}"
    );
}

/// Graph expansion is seeded by the vector hits' identity, so a node reachable
/// only by traversal still joins the ranking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expansion_reaches_neighbours_of_vector_hits() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ragf_seed").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_ragf_seed ON ragf_seed METRIC cosine DIM 2")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ragf_seed (id, embedding) VALUES ('near', ARRAY[1.0, 0.0])")
        .await
        .unwrap();
    // `far` is the vector opposite of the query, so it enters the ranking only
    // by being one hop from the seed.
    server
        .exec("INSERT INTO ragf_seed (id, embedding) VALUES ('far', ARRAY[-1.0, 0.0])")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'ragf_seed' FROM 'near' TO 'far' TYPE 'hop'")
        .await
        .unwrap();

    let blob = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_seed \
             QUERY ARRAY[1.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 1 \
             EXPANSION_DEPTH 1 \
             EDGE_LABEL 'hop' \
             FINAL_TOP_K 5",
        )
        .await
        .expect("fusion must succeed")
        .join("");

    assert!(
        blob.contains("\"far\""),
        "the seed's neighbour must be reached by expansion; got: {blob}"
    );
}

/// A row that is both the vector-nearest hit and the only text match must fuse
/// into **one** ranked entry carrying **both** contributions.
///
/// This is the assertion that the reporting-key change actually bought
/// something. The vector leg keys a surrogate-bound-but-nodeless hit by its
/// document identity; the text leg keys every hit by that same document
/// identity. If the two keys did not agree, RRF would see two separate
/// documents and the row would carry only one leg's contribution — the failure
/// is invisible to a "no `__local_` sentinel appears" check, because a sentinel
/// key also produces a plausible-looking result set.
///
/// The k-constants make the two cases numerically distinguishable. With
/// `RRF_K (60.0, 1.0, 60.0)`:
///   - text rank 0 alone            → 1/(1+0+1)      = 0.5
///   - vector rank 0 alone          → 1/(60+0+1)     ≈ 0.0164
///   - both, fused on one key       → 0.5 + 0.0164   ≈ 0.5164
/// So a top score strictly above 0.5 is reachable only by the two legs meeting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_and_text_hits_on_one_row_fuse_into_a_single_entry() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ragf_crossleg")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_ragf_crossleg_emb ON ragf_crossleg METRIC cosine DIM 2")
        .await
        .unwrap();
    server
        .exec(
            "CREATE SEARCH INDEX idx_ragf_crossleg_fts ON ragf_crossleg \
             FIELDS body ANALYZER 'standard'",
        )
        .await
        .unwrap();

    // `both` is the vector-nearest row AND the only row whose body contains
    // `entangled` — the row the two legs must agree on.
    server
        .exec(
            "INSERT INTO ragf_crossleg (id, body, embedding) \
             VALUES ('both', 'entangled photons', ARRAY[1.0, 0.0])",
        )
        .await
        .unwrap();
    // `vector_only` also ranks in the vector leg, but matches no text.
    server
        .exec(
            "INSERT INTO ragf_crossleg (id, body, embedding) \
             VALUES ('vector_only', 'unrelated prose', ARRAY[0.9, 0.1])",
        )
        .await
        .unwrap();

    let blob = server
        .query_text(
            "GRAPH RAG FUSION ON ragf_crossleg \
             QUERY ARRAY[1.0, 0.0] \
             VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 5 \
             BM25 'entangled' ON 'body' \
             EXPANSION_DEPTH 1 \
             FINAL_TOP_K 10 \
             RRF_K (60.0, 1.0, 60.0)",
        )
        .await
        .expect("three-source fusion must succeed")
        .join("");

    let parsed: serde_json::Value =
        serde_json::from_str(&blob).unwrap_or_else(|e| panic!("response is not JSON: {e}: {blob}"));
    let results = parsed
        .get("results")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("response carries no results array: {blob}"));

    let scores: Vec<f64> = results
        .iter()
        .filter_map(|r| r.get("rrf_score").and_then(serde_json::Value::as_f64))
        .collect();
    let best = scores.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    assert!(
        best > 0.5,
        "the vector and text legs must fuse onto one key: a top score of {best} \
         is at most one leg's contribution (text-alone is exactly 0.5, \
         vector-alone ≈0.0164); got: {blob}"
    );

    // The same row must not also appear under a second key — that is the shape
    // a key disagreement takes when both legs happen to rank the same document.
    let above_half = scores.iter().filter(|s| **s > 0.4).count();
    assert_eq!(
        above_half, 1,
        "exactly one entry may carry the text leg's contribution; \
         more than one means the legs keyed the same row differently: {blob}"
    );
}

// ── Mistyped fusion options are refused, never defaulted ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_rejects_a_mistyped_option_keyword() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ragf_typo").await.unwrap();

    // `VECTOR_TOPK` is not a clause. Seeking `VECTOR_TOP_K`, finding none, and
    // defaulting the cap left `VECTOR_TOPK 5` unread: the statement ran with a
    // cap the text did not ask for, and reported success.
    server
        .expect_error(
            "GRAPH RAG FUSION ON ragf_typo QUERY ARRAY[0.1, 0.2] VECTOR_TOPK 5",
            "VECTOR_TOPK",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_rag_fusion_rejects_an_unreadable_option_value() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ragf_bad_value")
        .await
        .unwrap();

    // The clause is present but its value is not a number: a silent default
    // would answer a different question than the text asks.
    server
        .expect_error(
            "GRAPH RAG FUSION ON ragf_bad_value QUERY ARRAY[0.1, 0.2] VECTOR_TOP_K five",
            "VECTOR_TOP_K",
        )
        .await;
}
