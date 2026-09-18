// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for `SEARCH ... USING VECTOR(...)` in subquery
//! position.
//!
//! A `SEARCH` result is a relation, so it has to compose: usable as a derived
//! table, joinable, and reachable from an `IN (...)` predicate. Without that,
//! every hybrid vector-plus-relational query needs two round trips and the
//! relational filter can only be applied after the k-NN cut, which silently
//! shrinks the result set a caller asked for.

use crate::harness::TestServer;

/// Fixture rows, nearest-first for the query vector used below: `r0` (exact
/// match), `r1`, then `r2`. `tag` splits them so a filter over the k-NN result
/// can be told apart from a filter applied before the cut.
async fn create_vector_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} (embedding) METRIC cosine DIM 4"
        ))
        .await
        .unwrap();
    for (id, tag, v) in [
        ("r0", "keep", [0.10f32, 0.20, 0.30, 0.40]),
        ("r1", "drop", [0.11, 0.21, 0.31, 0.41]),
        ("r2", "keep", [0.90, 0.80, 0.70, 0.60]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, tag, embedding) VALUES \
                 ('{id}', '{tag}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_is_usable_as_a_derived_table() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_derived").await;

    let rows = server
        .query_text(
            "SELECT id FROM \
             (SEARCH vec_derived USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 2)) s",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["r0".to_string(), "r1".to_string()],
        "SEARCH in FROM position must yield the same k rows, in distance order, as the top-level form"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_limit_narrows_the_search_result() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_outer_limit").await;

    let rows = server
        .query_text(
            "SELECT id FROM \
             (SEARCH vec_outer_limit USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 3)) s \
             LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["r0".to_string()],
        "an outer LIMIT must cut the k-NN result, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relational_filter_applies_over_search_results() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_filtered").await;

    // The predicate reaches the engine as a search filter, so the k-NN cut is
    // taken over matching rows: k = 2 yields the two nearest rows tagged
    // 'keep' (r0, r2), not the tagged subset of the two nearest (r0 alone).
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SEARCH vec_filtered USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 2)) s \
             WHERE s.tag = 'keep'",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["r0".to_string(), "r2".to_string()],
        "the WHERE clause must run inside the engine, and k counts matching rows, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_result_projects_a_single_column() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_projected").await;

    let rows = server
        .query_rows(
            "SELECT s.id FROM \
             (SEARCH vec_projected USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 2)) s",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "got: {rows:?}");
    for row in &rows {
        assert_eq!(
            row.len(),
            1,
            "the outer projection must narrow the SEARCH output, got: {row:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_accepts_a_quoted_collection_name() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_quoted").await;

    let bare = server
        .query_text("SEARCH vec_quoted USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 2)")
        .await
        .unwrap();
    let quoted = server
        .query_text("SEARCH \"vec_quoted\" USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 2)")
        .await
        .unwrap();
    assert_eq!(
        quoted, bare,
        "a quoted collection name must search the same collection as the bare form"
    );

    let quoted_subquery = server
        .query_text(
            "SELECT id FROM \
             (SEARCH \"vec_quoted\" USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 2)) s",
        )
        .await
        .unwrap();
    assert_eq!(quoted_subquery, vec!["r0".to_string(), "r1".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_search_subquery_is_rejected() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_bad_args").await;

    // One argument is neither the two-arg (vector, k) nor the three-arg
    // (field, vector, k) form — it must not be rewritten into a SELECT.
    server
        .expect_error(
            "SELECT * FROM (SEARCH vec_bad_args USING VECTOR(ARRAY[0.1, 0.2, 0.3, 0.4])) s",
            "parse error",
        )
        .await;
}

// ── Post-processing over the k-NN result (QueryOp::PostProcess) ──────────────
//
// An outer ORDER BY / OFFSET / DISTINCT (and a LIMIT that must apply after a
// reorder) cannot be absorbed by the vector-search leaf. Before the
// post-processing operator these were silently dropped, returning the raw
// distance-ordered k rows. These tests pin the corrected behaviour end to end.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_payload_column_reorders_search_result() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_order_payload").await;

    // The three nearest are r0, r1, r2 (distance order). An outer ORDER BY on a
    // *document* column (`id`) must re-sort them — proving payload columns are
    // materialized from the hit body, not left nested.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SEARCH vec_order_payload USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 3)) s \
             ORDER BY s.id DESC",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["r2".to_string(), "r1".to_string(), "r0".to_string()],
        "outer ORDER BY on a payload column must reorder the k-NN result, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_offset_skips_leading_search_rows() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_offset").await;

    // No explicit ORDER BY: rows stay in distance order (r0, r1, r2); OFFSET 1
    // drops the nearest.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SEARCH vec_offset USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 3)) s \
             OFFSET 1",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["r1".to_string(), "r2".to_string()],
        "outer OFFSET must skip leading rows of the k-NN result, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_distinct_dedups_search_result() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_distinct").await;

    // Tags across the three nearest are keep / drop / keep — DISTINCT on the
    // projected column must collapse to two rows.
    let mut rows = server
        .query_text(
            "SELECT DISTINCT tag FROM \
             (SEARCH vec_distinct USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 3)) s",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec!["drop".to_string(), "keep".to_string()],
        "outer DISTINCT must dedup the projected column, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_distance_then_limit_takes_farthest() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_order_distance").await;

    // ORDER BY distance DESC reverses the k-NN order, so LIMIT 1 takes the
    // FARTHEST of the three nearest (r2) — a LIMIT that must apply after the
    // reorder, not fold into the search top_k.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SEARCH vec_order_distance USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3, 0.4], 3)) s \
             ORDER BY s.distance DESC LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["r2".to_string()],
        "LIMIT after an outer ORDER BY must cut the reordered rows, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_vector_distance_declares_distance() {
    let server = TestServer::start().await;
    create_vector_collection(&server, "vec_implicit").await;

    // A hand-written `ORDER BY vector_distance(...)` derived table (no SEARCH
    // keyword) takes the same sort-trigger rewrite as the SEARCH form, so the
    // synthetic `distance` column must resolve AND carry a value: declaring
    // the column without producing a cell would make `s.distance` a phantom.
    let rows = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM vec_implicit \
              ORDER BY vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]) LIMIT 2) s",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two rows expected, got: {rows:?}");
    let first: f64 = rows[0]
        .parse()
        .unwrap_or_else(|_| panic!("distance must be numeric, got: {rows:?}"));
    let second: f64 = rows[1]
        .parse()
        .unwrap_or_else(|_| panic!("distance must be numeric, got: {rows:?}"));
    assert!(
        first <= second,
        "distance must be ordered nearest-first, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_vector_distance_without_index_stays_consistent() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vec_no_index").await.unwrap();
    for (id, v) in [
        ("r0", [0.10f32, 0.20, 0.30, 0.40]),
        ("r1", [0.11, 0.21, 0.31, 0.41]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vec_no_index (id, embedding) VALUES ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // The sort-trigger rewrite does not consult the index, so the plan is the
    // same `VectorSearch` a collection with an index gets — but the search
    // itself serves no hits without one. The pinned outcome is an empty
    // result: a declared `distance` cell is never NULL, and a row only ever
    // appears with a value.
    let rows = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM vec_no_index \
              ORDER BY vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]) LIMIT 2) s",
        )
        .await
        .unwrap_or_else(|e| panic!("the rewrite serves distances without an index: {e}"));
    assert!(
        rows.is_empty(),
        "no index serves no hits; a returned row would carry a numeric distance: {rows:?}"
    );
}

/// The filed defect: a closed-schema source declares no `distance` column, so
/// the synthetic cell the response layer appends resolves only when
/// derived-relation inference adds the name. On an open source the same
/// projection always resolved, which is why this case pins the closed schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_over_a_closed_schema_resolves_a_synthetic_distance_column() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION sp_strict (id TEXT PRIMARY KEY, embedding VECTOR(3)) \
             WITH (engine = 'document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_sp_strict ON sp_strict (embedding) METRIC COSINE DIM 3")
        .await
        .unwrap();
    // The string form: an `ARRAY[...]` literal on a strict schema is a separate
    // defect and not what this case measures.
    server
        .exec("INSERT INTO sp_strict (id, embedding) VALUES ('s1', '[0.1, 0.2, 0.3]')")
        .await
        .unwrap();

    let rows = server
        .query_text(
            "SELECT s.distance \
             FROM (SEARCH sp_strict USING VECTOR(embedding, ARRAY[0.1, 0.2, 0.3], 2)) s",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one row expected, got: {rows:?}");
    rows[0]
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("distance must be numeric, got: {rows:?}"));
}

/// A closed-schema collection with a vector index and one row: the shape every
/// distance-column case below needs.
async fn create_closed_vector(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, embedding VECTOR(3)) \
             WITH (engine = 'document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} (embedding) METRIC COSINE DIM 3"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {name} (id, embedding) VALUES ('s1', '[0.1, 0.2, 0.3]')"
        ))
        .await
        .unwrap();
}

/// Every function that routes an ORDER BY to a vector search appends a
/// `distance` cell, so each one must resolve over a closed schema — not only
/// `vector_distance`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_vector_search_form_resolves_the_distance_column() {
    let server = TestServer::start().await;
    create_closed_vector(&server, "sp_forms").await;

    for order_by in [
        "vector_cosine_distance(embedding, ARRAY[0.1, 0.2, 0.3])",
        "vector_neg_inner_product(embedding, ARRAY[0.1, 0.2, 0.3])",
        "embedding <=> ARRAY[0.1, 0.2, 0.3]",
    ] {
        let sql = format!(
            "SELECT s.distance FROM \
             (SELECT * FROM sp_forms ORDER BY {order_by} LIMIT 2) s"
        );
        let rows = server.query_text(&sql).await.unwrap_or_else(|e| {
            panic!("ORDER BY {order_by} must resolve s.distance: {e}");
        });
        assert_eq!(rows.len(), 1, "ORDER BY {order_by}: one row, got {rows:?}");
        rows[0].parse::<f64>().unwrap_or_else(|_| {
            panic!("ORDER BY {order_by}: distance must be numeric, got {rows:?}")
        });
    }
}

/// A search also answers with `_surrogate`. A WHERE operator form routes to a
/// search (the preprocessor rewrites it to a bare call that
/// `try_extract_where_search` dispatches), so its cells are declared; a
/// comparison wrapped around the call does not route, so the projection must
/// refuse `42703` instead of reading a NULL cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_projections_resolve_surrogate_and_a_where_trigger() {
    let server = TestServer::start().await;
    create_closed_vector(&server, "sp_cells").await;

    let rows = server
        .query_text(
            "SELECT s._surrogate FROM \
             (SELECT * FROM sp_cells ORDER BY vector_distance(embedding, ARRAY[0.1, 0.2, 0.3]) LIMIT 2) s",
        )
        .await
        .unwrap_or_else(|e| panic!("s._surrogate must resolve over a closed schema: {e}"));
    assert_eq!(rows.len(), 1, "one row, got {rows:?}");
    rows[0]
        .parse::<i64>()
        .unwrap_or_else(|_| panic!("_surrogate must be an integer id, got {rows:?}"));

    let rows = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM sp_cells \
              WHERE embedding <=> ARRAY[0.1, 0.2, 0.3] LIMIT 2) s",
        )
        .await
        .unwrap_or_else(|e| panic!("a routing WHERE form must declare s.distance: {e}"));
    assert_eq!(rows.len(), 1, "one row expected, got {rows:?}");
    rows[0]
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("distance must be numeric, got {rows:?}"));

    let error = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM sp_cells \
              WHERE vector_cosine_distance(embedding, ARRAY[0.1, 0.2, 0.3]) < 2.0) s",
        )
        .await
        .expect_err("a comparison around the call routes no search");
    assert!(
        error.contains("42703"),
        "the undeclared cell must refuse, got: {error}"
    );
}

/// A sparse trigger routes an ORDER BY to `SqlPlan::SparseSearch`, so
/// `s.distance` resolves with a value per returned hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sparse_order_by_resolves_the_distance_column() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TABLE sp_sparse (id TEXT PRIMARY KEY, terms SPARSEVECTOR)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO sp_sparse (id, terms) VALUES ('s1', '{3: 1.0, 7: 0.5}')")
        .await
        .unwrap();

    let sql = "SELECT s.distance FROM \
               (SELECT * FROM sp_sparse ORDER BY sparse_score(terms, '{3: 1.0}') LIMIT 2) s";
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("a sparse ORDER BY declares s.distance: {e}"));
    assert_eq!(rows.len(), 1, "one row expected, got {rows:?}");
    rows[0]
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("distance must be numeric, got {rows:?}"));
}

/// A WHERE `sparse_score(...)` is a scalar fallback in the planner
/// (`where_search`'s dispatch sends every non-WHERE trigger to `Ok(None)`),
/// so the plan carries no search cells. The projection must refuse `42703`
/// instead of declaring a cell whose rows read NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sparse_where_trigger_refuses_the_distance_cell() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TABLE sp_sparse_where (id TEXT PRIMARY KEY, terms SPARSEVECTOR)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO sp_sparse_where (id, terms) VALUES ('s1', '{3: 1.0, 7: 0.5}')")
        .await
        .unwrap();

    let error = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM sp_sparse_where WHERE sparse_score(terms, '{3: 1.0}') > 0.1) s",
        )
        .await
        .expect_err("the scalar fallback carries no distance cell");
    assert!(
        error.contains("42703"),
        "expected 42703 for the undeclared cell, got: {error}"
    );
}

/// A one-argument `vector_distance` does not route to a search
/// (`order_by/triggers.rs` returns `Ok(None)` below two arguments), so no
/// cell is declared and `s.distance` must refuse `42703`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_argument_order_by_refuses_the_distance_cell() {
    let server = TestServer::start().await;
    create_closed_vector(&server, "sp_one_arg").await;

    let error = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM sp_one_arg ORDER BY vector_distance(embedding) LIMIT 2) s",
        )
        .await
        .expect_err("a one-argument call routes no search");
    assert!(
        error.contains("42703") || error.contains("42883"),
        "expected 42703 (undeclared cell) or 42883 (arity), got: {error}"
    );
}

/// A body whose FROM is a derived relation gives `try_extract_sort_search` no
/// `Scan` or `Join` to read (`Ok(None)`), so its order-by trigger routes no
/// search and the outer projection must refuse `42703`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_off_a_derived_relation_refuses_the_distance_cell() {
    let server = TestServer::start().await;
    create_closed_vector(&server, "sp_nested").await;

    let error = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM (SELECT * FROM sp_nested LIMIT 2) inner_s \
              ORDER BY vector_distance(embedding, ARRAY[0.1, 0.2, 0.3]) LIMIT 2) s",
        )
        .await
        .expect_err("a non-Scan body routes no search");
    assert!(
        error.contains("42703") || error.contains("42883"),
        "expected 42703 (undeclared cell) or 42883 (arity), got: {error}"
    );
}

/// `WHERE multi_vector_search(field, query)` plans `SqlPlan::MultiVectorSearch`,
/// whose rows carry the search cells: `s.distance` is declared, so planning
/// passes and any refusal belongs to the lowering step, never `42703`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_vector_where_trigger_declares_the_distance_cell() {
    let server = TestServer::start().await;
    create_closed_vector(&server, "sp_multi").await;

    let error = server
        .query_text(
            "SELECT s.distance FROM \
             (SELECT * FROM sp_multi \
              WHERE multi_vector_search(embedding, ARRAY[0.1, 0.2, 0.3])) s",
        )
        .await
        .expect_err("the variant is not lowered yet; the cell declaration still happens");
    assert!(
        !error.contains("42703"),
        "the cell follows the plan and must resolve; got: {error}"
    );
    assert!(
        error.contains("42601") && error.contains("MultiVectorSearch"),
        "expected the lowering refusal (42601, variant), got: {error}"
    );
}
