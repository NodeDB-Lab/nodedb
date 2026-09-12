// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for `rrf_score()` SQL function projection.
//!
//! The bug class fixed here spanned four layers, each silently dropping or
//! misnaming the score that the user asked for:
//!
//! 1. Planner: `apply_order_by` only looked at the literal ORDER BY expression
//!    for a `rrf_score(...)` call; an ORDER BY referencing a SELECT alias
//!    (`ORDER BY score DESC` against `SELECT … rrf_score(…) AS score`) never
//!    matched the trigger, so the plan stayed a plain Scan and the column
//!    resolved to NULL via scalar evaluation that has no implementation.
//! 2. Planner: there was no SELECT-projection path — `SELECT id, rrf_score(…)
//!    AS score FROM c WHERE … LIMIT N` (no ORDER BY at all) had no entry path
//!    into hybrid-search detection.
//! 3. Plan + executor: `SqlPlan::HybridSearch` and `TextOp::HybridSearch`
//!    carried no alias field; even when the trigger fired, `HybridSearchHit`
//!    serialized the score under the fixed name `rrf_score` regardless of the
//!    caller's `AS <alias>`.
//! 4. pgwire shaping: `TextOp::Search` and `TextOp::HybridSearch` were missing
//!    from the `MultiRow` `PlanKind`, so search responses fell through to
//!    `PlanKind::Execution` — the row payload was discarded entirely and the
//!    client received an "OK" execution tag with no rows.
//!
//! Plus a fifth invariant: the no-args shape `rrf_score()` previously fell
//! through to scalar evaluation (no implementation) and surfaced NULL silently.
//! It now returns a typed `InvalidFunction` error at parse time.

use crate::harness::TestServer;

async fn create_hybrid_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} METRIC cosine DIM 4"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE SEARCH INDEX idx_{name}_fts ON {name} FIELDS content ANALYZER 'standard'"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {name} (id, tenant_id, content, embedding) \
             VALUES ('a', 't1', 'consensus algorithm', ARRAY[0.1, 0.2, 0.3, 0.4])"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {name} (id, tenant_id, content, embedding) \
             VALUES ('b', 't1', 'distributed consensus', ARRAY[0.2, 0.3, 0.4, 0.5])"
        ))
        .await
        .unwrap();
}

/// Returns the score column from a hybrid-search query result, parsed to f64.
/// Treats empty/missing as NULL → returns `None`.
fn parse_score(cell: &str) -> Option<f64> {
    let trimmed = cell.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

// ── 1. No-args rrf_score() must surface a typed error, not silent NULL ─────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_no_args_returns_typed_error() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hs_noargs").await;

    // The previous behaviour: `rrf_score()` fell through `plan_hybrid_from_sort`
    // (args.len() < 2), reached scalar evaluation that has no implementation,
    // and the aliased column quietly returned NULL. The fix surfaces an error
    // at parse time so the bad shape is loud, not invisible.
    //
    // The WHERE clause must NOT contain `text_match`, `vector_distance`, or any
    // other search-trigger function — those are pre-empted by the WHERE-side
    // search detector and would replace the entire plan before the ORDER BY
    // path inspects `rrf_score()`. A plain equality keeps the plan as a Scan
    // so `apply_order_by` is the layer that sees the no-args call.
    let result = server
        .query_rows(
            "SELECT id, rrf_score() AS score \
             FROM hs_noargs \
             WHERE tenant_id = 't1' \
             ORDER BY score DESC LIMIT 5",
        )
        .await;

    let err = result.expect_err("rrf_score() with no arguments must error");
    assert!(
        err.to_lowercase().contains("rrf_score") && err.to_lowercase().contains("argument"),
        "error must name rrf_score and indicate the argument problem; got: {err}"
    );
}

// ── 2. Alias in ORDER BY must resolve and trigger HybridSearch ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_alias_in_order_by_triggers_hybrid_search() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hs_alias_ob").await;

    let rows = server
        .query_rows(
            "SELECT id, \
                    rrf_score(\
                      vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                      bm25_score(content, 'consensus')\
                    ) AS score \
             FROM hs_alias_ob \
             ORDER BY score DESC LIMIT 5",
        )
        .await
        .expect("rrf_score(...) AS score ... ORDER BY score DESC must succeed");

    // Regression guard #1: a missing pgwire MultiRow mapping for TextOp made
    // the response stream empty even though the executor produced fused rows.
    assert!(
        !rows.is_empty(),
        "alias-in-ORDER-BY query must return fused rows; \
         empty result means the response was discarded as PlanKind::Execution"
    );
    // Regression guard #2: the score must be present and non-NULL — empty cell
    // means alias resolution did not promote ORDER BY into HybridSearch and the
    // expression resolved through scalar eval (no implementation → NULL).
    for row in &rows {
        assert_eq!(row.len(), 2, "expected 2 columns (id, score); got {row:?}");
        let score = parse_score(&row[1]);
        assert!(
            score.is_some(),
            "score column must be a non-NULL number; \
             empty/NULL means alias resolution failed to fire HybridSearch: {row:?}"
        );
    }
}

// ── 3. Caller's alias must appear as the response field name ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_caller_alias_honoured_in_response() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hs_resp_alias").await;

    // ORDER BY uses the literal rrf_score(...) call (so the trigger fires
    // unconditionally). SELECT names the score `relevance`. The fix in
    // `HybridSearchHit::ToMessagePack` writes the score under the caller's
    // alias instead of the hardcoded `rrf_score`. The pgwire projection layer
    // looks up the field by alias, so the user-chosen column name must reach
    // the response with a non-NULL value.
    let rows = server
        .query_rows(
            "SELECT id, \
                    rrf_score(\
                      vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                      bm25_score(content, 'consensus')\
                    ) AS relevance \
             FROM hs_resp_alias \
             ORDER BY rrf_score(\
                      vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                      bm25_score(content, 'consensus')\
             ) DESC LIMIT 5",
        )
        .await
        .expect("query with alias in SELECT and literal call in ORDER BY must succeed");

    assert!(
        !rows.is_empty(),
        "literal-rrf_score-in-ORDER-BY query must return fused rows"
    );
    for row in &rows {
        assert_eq!(
            row.len(),
            2,
            "expected 2 columns (id, relevance); got {row:?}"
        );
        let relevance = parse_score(&row[1]);
        assert!(
            relevance.is_some(),
            "alias 'relevance' must resolve to a non-NULL score; \
             NULL means the response codec wrote 'rrf_score' as the field name \
             instead of honouring the SELECT alias: {row:?}"
        );
    }
}

// ── 4. SELECT-projection path: rrf_score(...) without ORDER BY ──────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_in_select_without_order_by_returns_score() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hs_select_only").await;

    // The `SELECT id, rrf_score(...) AS score FROM c WHERE ... LIMIT N` shape
    // is the canonical SQL form for a hybrid-search read with no ranking
    // change. Without the SELECT-projection trigger path the `rrf_score(...)`
    // expression resolved through scalar evaluation and the column was NULL.
    let rows = server
        .query_rows(
            "SELECT id, \
                    rrf_score(\
                      vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                      bm25_score(content, 'consensus')\
                    ) AS score \
             FROM hs_select_only \
             LIMIT 5",
        )
        .await
        .expect("rrf_score(...) in SELECT without ORDER BY must succeed");

    assert!(
        !rows.is_empty(),
        "SELECT-only query must return fused rows; \
         empty result means try_hybrid_from_projection did not fire"
    );
    for row in &rows {
        assert_eq!(row.len(), 2, "expected 2 columns (id, score); got {row:?}");
        let score = parse_score(&row[1]);
        assert!(
            score.is_some(),
            "score column must be a non-NULL number when rrf_score(...) is in \
             the SELECT list; NULL means the SELECT-projection trigger path \
             did not fire HybridSearch: {row:?}"
        );
    }
}

// ── 5. `id` must be the user's primary key, not the internal surrogate ─────

/// Pre-fix, `TextOp::HybridSearch` hits carried only `doc_id` (the DP-side
/// `StorageKey::for_surrogate(surrogate)` hex string) with no surrogate->PK
/// translation on the Text/Hybrid plan path (only the Vector plan path had
/// one) — so `SELECT id` against a hybrid query had no `id` field to read at
/// all and the cell came back empty. `create_hybrid_collection` inserts rows
/// with PKs `'a'` and `'b'`; the fused rows here must show exactly those,
/// never an empty cell or an 8-hex-digit surrogate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_search_id_is_user_primary_key_not_surrogate_hex() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hs_id_pk").await;

    let rows = server
        .query_rows(
            "SELECT id, \
                    rrf_score(\
                      vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                      bm25_score(content, 'consensus')\
                    ) AS score \
             FROM hs_id_pk \
             ORDER BY score DESC LIMIT 5",
        )
        .await
        .expect("hybrid query must succeed");

    assert!(!rows.is_empty(), "hybrid query must return fused rows");
    for row in &rows {
        let id = &row[0];
        assert!(
            id == "a" || id == "b",
            "id must be the user-facing primary key ('a' or 'b'), not empty \
             or an internal surrogate; got {id:?} in row {row:?}"
        );
    }
}
