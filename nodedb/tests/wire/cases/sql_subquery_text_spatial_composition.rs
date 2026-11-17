// SPDX-License-Identifier: BUSL-1.1

//! Post-processing (outer ORDER BY / OFFSET / DISTINCT) over NON-vector
//! subquery bodies: full-text and spatial.
//!
//! The vector case (`sql_search_subquery_composition.rs`) needs a hit-specific
//! flatten because a vector hit nests the document `body`. Full-text hits use
//! the `{id, data}` document envelope and spatial rows are flat document maps,
//! so both flow through the ordinary storage→relational flatten — these tests
//! lock that the outer relational constraints actually apply over them, by
//! asserting the OUTCOME (correct reorder / dedup) independent of which plan
//! variant the body lowers to.

use crate::harness::TestServer;

// ── Full-text subquery bodies ────────────────────────────────────────────────

async fn create_fts_collection(server: &TestServer) {
    server
        .exec("CREATE COLLECTION fts_sub WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec("CREATE SEARCH INDEX ON fts_sub (content)")
        .await
        .unwrap();
    // Every row matches 'database' so the inner ranked search returns all three;
    // `tag` splits them for DISTINCT and `id` gives a deterministic outer order.
    for (id, tag, content) in [
        ("t1", "keep", "database alpha"),
        ("t2", "drop", "database beta"),
        ("t3", "keep", "database gamma"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO fts_sub {{ id: '{id}', tag: '{tag}', content: '{content}' }}"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_reorders_fts_subquery() {
    let server = TestServer::start().await;
    create_fts_collection(&server).await;

    // Inner: a bm25-ranked full-text search as a derived table. Outer: reorder
    // by a payload column (`id`) descending — the outer ORDER BY must reach the
    // post-processor, not be dropped.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SELECT id, tag, bm25_score(content, 'database') AS score \
              FROM fts_sub ORDER BY score DESC LIMIT 3) s \
             ORDER BY s.id DESC",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["t3".to_string(), "t2".to_string(), "t1".to_string()],
        "outer ORDER BY must reorder the full-text subquery result, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_distinct_dedups_fts_subquery() {
    let server = TestServer::start().await;
    create_fts_collection(&server).await;

    let mut rows = server
        .query_text(
            "SELECT DISTINCT tag FROM \
             (SELECT id, tag, bm25_score(content, 'database') AS score \
              FROM fts_sub ORDER BY score DESC LIMIT 3) s",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec!["drop".to_string(), "keep".to_string()],
        "outer DISTINCT must dedup the full-text subquery's projected column, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_offset_skips_fts_subquery_rows() {
    let server = TestServer::start().await;
    create_fts_collection(&server).await;

    // Inner orders by id ascending; OFFSET 1 over the three rows drops the
    // first (t1), leaving t2, t3.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SELECT id FROM fts_sub WHERE text_match(content, 'database') ORDER BY id) s \
             OFFSET 1",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["t2".to_string(), "t3".to_string()],
        "outer OFFSET must skip leading rows of the full-text subquery, got: {rows:?}"
    );
}

// ── Spatial subquery bodies ──────────────────────────────────────────────────

async fn create_spatial_collection(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION geo_sub COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();
    // Points at increasing distance from the origin; `name` gives a
    // deterministic outer order distinct from distance order.
    for (id, lon, lat, name) in [
        ("g1", 0.0f64, 0.0f64, "charlie"),
        ("g2", 0.1, 0.1, "bravo"),
        ("g3", 0.2, 0.2, "alpha"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO geo_sub (id, location, name) \
                 VALUES ('{id}', ST_Point({lon}, {lat}), '{name}')"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_reorders_spatial_subquery() {
    let server = TestServer::start().await;
    create_spatial_collection(&server).await;

    // Inner: nearest-first spatial ordering as a derived table (distance order
    // g1, g2, g3). Outer: reorder by `name` ascending → alpha(g3), bravo(g2),
    // charlie(g1). Proves the outer ORDER BY reaches the post-processor and the
    // spatial payload column is materialized.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SELECT id, name FROM geo_sub \
              ORDER BY ST_Distance(location, ST_Point(0.0, 0.0)) LIMIT 3) s \
             ORDER BY s.name ASC",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["g3".to_string(), "g2".to_string(), "g1".to_string()],
        "outer ORDER BY must reorder the spatial subquery by a payload column, got: {rows:?}"
    );
}

// ── Body-less hit bodies: `id` must resolve to the user PK, not the surrogate ─
//
// Sparse and hybrid hits carry no document body — they emit the internal
// surrogate (as `id` / `doc_id`), and the Control-Plane translator normally
// resolves it to the user PK at the response boundary. The post-processor runs
// that same translator over the gathered rows, so `SELECT id` over such a
// subquery must return the user PK (a string), and an outer ORDER BY on it must
// sort by that PK — not by the raw surrogate.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_id_over_sparse_subquery_returns_user_pk() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TABLE sparse_sub (id TEXT PRIMARY KEY, terms SPARSEVECTOR)")
        .await
        .unwrap();
    for (id, terms) in [
        ("a", "{3: 1.0, 7: 1.0}"),
        ("b", "{3: 1.0}"),
        ("c", "{7: 1.0}"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO sparse_sub (id, terms) VALUES ('{id}', '{terms}')"
            ))
            .await
            .unwrap();
    }

    // Inner ranks a, b, c by descending dot product; the outer ORDER BY id DESC
    // must reorder to c, b, a — which only holds if `id` is the user PK string,
    // not the internal surrogate integer.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SELECT id FROM sparse_sub \
              ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC LIMIT 3) s \
             ORDER BY s.id DESC",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["c".to_string(), "b".to_string(), "a".to_string()],
        "outer ORDER BY id over a sparse subquery must sort by the user PK, got: {rows:?}"
    );
}

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
    for (id, content, emb) in [
        ("a", "consensus algorithm", "ARRAY[0.1, 0.2, 0.3, 0.4]"),
        ("b", "distributed consensus", "ARRAY[0.2, 0.3, 0.4, 0.5]"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, tenant_id, content, embedding) \
                 VALUES ('{id}', 't1', '{content}', {emb})"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_order_by_id_over_hybrid_subquery_returns_user_pk() {
    let server = TestServer::start().await;
    create_hybrid_collection(&server, "hyb_sub").await;

    // The hybrid hit carries `doc_id` (the surrogate hex) and a fused score, no
    // document body. `SELECT id` therefore reads NULL unless the surrogate is
    // resolved to the user PK; the outer ORDER BY id DESC must then yield b, a.
    let rows = server
        .query_text(
            "SELECT id FROM \
             (SELECT id, rrf_score(\
                vector_distance(embedding, ARRAY[0.1, 0.2, 0.3, 0.4]), \
                bm25_score(content, 'consensus')\
              ) AS score \
              FROM hyb_sub ORDER BY score DESC LIMIT 5) s \
             ORDER BY s.id DESC",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["b".to_string(), "a".to_string()],
        "outer ORDER BY id over a hybrid subquery must sort by the resolved user PK, got: {rows:?}"
    );
}
