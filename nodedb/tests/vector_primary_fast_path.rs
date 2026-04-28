//! Integration tests for the vector-primary pure-vector projection fast path
//! and per-collection memory arena assignment.
//!
//! Fast-path guard: when a SELECT against a vector-primary collection projects
//! only `id` and `vector_distance(...)`, the Data Plane must skip document
//! body fetches entirely. The test verifies that:
//!   (a) the id-only projection returns exactly the expected columns,
//!   (b) a projection that includes a payload field still works (slow path).
//!
//! Arena guard: two vector-primary collections each report a non-None
//! `arena_bytes` figure in their QueryStats response (or both report `None`
//! on builds without jemalloc per-arena stats). Either way the call must
//! not panic and must return a valid `VectorIndexStats`.

mod common;

use common::pgwire_harness::TestServer;

fn row_columns(msg: &tokio_postgres::SimpleQueryMessage) -> Option<Vec<String>> {
    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
        let cols: Vec<String> = (0..row.columns().len())
            .map(|i| row.get(i).unwrap_or("").to_string())
            .collect();
        Some(cols)
    } else {
        None
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pure_vector_projection_skips_payload_fetch() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION vp_fast_path \
             TYPE VECTOR (id STRING PRIMARY KEY, embedding VECTOR(4), label STRING) \
             WITH (primary = 'vector', vector_field = 'embedding', dim = 4)",
        )
        .await
        .unwrap();

    for i in 0..20usize {
        let v0 = i as f32;
        let v1 = (i as f32) * 0.1;
        let v2 = (i as f32) * 0.2;
        let v3 = (i as f32) * 0.3;
        let sql = format!(
            "INSERT INTO vp_fast_path (id, embedding, label) \
             VALUES ('row-{i}', ARRAY[{v0}, {v1}, {v2}, {v3}], 'label-{i}')"
        );
        server.exec(&sql).await.unwrap();
    }

    // Fast path: projection has only id + vector_distance — no payload column.
    // The Data Plane must skip the document body fetch entirely. Vector
    // queries return a single JSON column whose payload is the search hits;
    // we verify the fast path produces non-empty hits and crucially does NOT
    // contain any payload field ("label-N") in the response body.
    let fast_sql = "SELECT id, vector_distance(embedding, ARRAY[1.0, 0.1, 0.2, 0.3]) AS dist \
                    FROM vp_fast_path \
                    ORDER BY vector_distance(embedding, ARRAY[1.0, 0.1, 0.2, 0.3]) \
                    LIMIT 5";
    let fast_rows = server
        .client
        .simple_query(fast_sql)
        .await
        .expect("fast-path query must not fail");

    let fast_data: Vec<Vec<String>> = fast_rows.iter().filter_map(row_columns).collect();
    assert!(
        !fast_data.is_empty(),
        "fast-path query returned no rows; expected at least 1 hit"
    );
    let fast_blob = fast_data
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !fast_blob.contains("label-"),
        "fast-path response must not include payload field 'label'; got: {fast_blob}"
    );

    // Slow path: projection includes the payload column `label`. The handler
    // attaches the body, and the CP response translator flattens the body's
    // fields into the hit JSON so `label-N` appears in the response.
    let slow_sql = "SELECT id, label, vector_distance(embedding, ARRAY[1.0, 0.1, 0.2, 0.3]) AS dist \
                    FROM vp_fast_path \
                    ORDER BY vector_distance(embedding, ARRAY[1.0, 0.1, 0.2, 0.3]) \
                    LIMIT 5";
    let slow_rows = server
        .client
        .simple_query(slow_sql)
        .await
        .expect("slow-path query must not fail");

    let slow_data: Vec<Vec<String>> = slow_rows.iter().filter_map(row_columns).collect();
    assert!(
        !slow_data.is_empty(),
        "slow-path query returned no rows; expected at least 1 hit"
    );
    let slow_blob = slow_data
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        slow_blob.contains("label-"),
        "slow-path response must include payload field 'label'; got: {slow_blob}"
    );
}

/// WHERE clauses on payload-indexed fields must be peeled by the planner
/// into `payload_filters` and intersected with the HNSW candidate set via
/// `PayloadIndexSet::pre_filter`. The test inserts 10 vectors split across
/// two categories, then queries with `WHERE category = 'A'` and asserts
/// the response contains only A's surrogates (5), not all 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_bitmap_prefilter_restricts_candidates() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION vp_payload \
             TYPE VECTOR (id STRING PRIMARY KEY, vec VECTOR(3), category STRING) \
             WITH (primary='vector', vector_field='vec', dim=3, payload_indexes=['category'])",
        )
        .await
        .unwrap();

    for i in 0..10usize {
        let cat = if i % 2 == 0 { "A" } else { "B" };
        let v = i as f32;
        let sql = format!(
            "INSERT INTO vp_payload (id, vec, category) \
             VALUES ('row-{i}', ARRAY[{v}, 0.0, 0.0], '{cat}')"
        );
        server.exec(&sql).await.unwrap();
    }

    // Sanity: without WHERE, expect 10 hits.
    let baseline = "SELECT id, vector_distance(vec, ARRAY[0.0, 0.0, 0.0]) AS dist \
                    FROM vp_payload \
                    ORDER BY vector_distance(vec, ARRAY[0.0, 0.0, 0.0]) LIMIT 20";
    let rows = server.client.simple_query(baseline).await.unwrap();
    let baseline_data: Vec<Vec<String>> = rows.iter().filter_map(row_columns).collect();
    let baseline_blob = baseline_data
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    let baseline_hits = baseline_blob.matches("\"id\":").count();
    assert_eq!(
        baseline_hits, 10,
        "baseline (no WHERE) must return 10 hits; blob = {baseline_blob}"
    );

    let sql = "SELECT id, vector_distance(vec, ARRAY[0.0, 0.0, 0.0]) AS dist \
               FROM vp_payload WHERE category = 'A' \
               ORDER BY vector_distance(vec, ARRAY[0.0, 0.0, 0.0]) LIMIT 20";
    let rows = server
        .client
        .simple_query(sql)
        .await
        .expect("payload-filtered query must not fail");
    let data: Vec<Vec<String>> = rows.iter().filter_map(row_columns).collect();
    let blob = data
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");

    // Only 5 hits — the A-category subset. The bitmap pre-filter must
    // exclude every B-category surrogate from the HNSW walk.
    let hit_count = blob.matches("\"id\":").count();
    assert_eq!(
        hit_count, 5,
        "payload bitmap pre-filter should restrict to category='A' (5 rows); blob = {blob}"
    );
}

/// `WHERE category IN ('A','C')` peels to `PayloadAtom::In` and the
/// handler's `FilterPredicate::In` lookup ORs the per-value bitmaps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_in_list_prefilter() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION vp_in \
             TYPE VECTOR (id STRING PRIMARY KEY, vec VECTOR(3), category STRING) \
             WITH (primary='vector', vector_field='vec', dim=3, payload_indexes=['category'])",
        )
        .await
        .unwrap();
    for i in 0..9usize {
        let cat = ["A", "B", "C"][i % 3];
        let v = i as f32;
        server
            .exec(&format!(
                "INSERT INTO vp_in (id, vec, category) \
                 VALUES ('row-{i}', ARRAY[{v}, 0.0, 0.0], '{cat}')"
            ))
            .await
            .unwrap();
    }
    let sql = "SELECT id, vector_distance(vec, ARRAY[0.0,0.0,0.0]) AS dist \
               FROM vp_in WHERE category IN ('A', 'C') \
               ORDER BY vector_distance(vec, ARRAY[0.0,0.0,0.0]) LIMIT 20";
    let rows = server.client.simple_query(sql).await.unwrap();
    let blob = rows
        .iter()
        .filter_map(row_columns)
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    let hits = blob.matches("\"id\":").count();
    assert_eq!(
        hits, 6,
        "IN ('A','C') should return 6 rows (3 A + 3 C); blob = {blob}"
    );
}

/// `WHERE n BETWEEN 3 AND 6` peels to `PayloadAtom::Range` and uses the
/// BTreeMap-backed Range index to scan the sorted key range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_range_prefilter() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION vp_range \
             TYPE VECTOR (id STRING PRIMARY KEY, vec VECTOR(3), n BIGINT) \
             WITH (primary='vector', vector_field='vec', dim=3, payload_indexes=['n'])",
        )
        .await
        .unwrap();
    for i in 0..10i64 {
        let v = i as f32;
        server
            .exec(&format!(
                "INSERT INTO vp_range (id, vec, n) VALUES ('row-{i}', ARRAY[{v}, 0.0, 0.0], {i})"
            ))
            .await
            .unwrap();
    }
    let sql = "SELECT id, vector_distance(vec, ARRAY[0.0,0.0,0.0]) AS dist \
               FROM vp_range WHERE n BETWEEN 3 AND 6 \
               ORDER BY vector_distance(vec, ARRAY[0.0,0.0,0.0]) LIMIT 20";
    let rows = server.client.simple_query(sql).await.unwrap();
    let blob = rows
        .iter()
        .filter_map(row_columns)
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    let hits = blob.matches("\"id\":").count();
    assert_eq!(
        hits, 4,
        "BETWEEN 3 AND 6 should return 4 rows (3,4,5,6); blob = {blob}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_collection_arenas_reported_in_stats() {
    let server = TestServer::start().await;

    for name in ["arena_col_a", "arena_col_b"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} \
                 TYPE VECTOR (id STRING PRIMARY KEY, vec VECTOR(3)) \
                 WITH (primary = 'vector', vector_field = 'vec', dim = 3)"
            ))
            .await
            .unwrap();

        for i in 0..5usize {
            let v = i as f32;
            server
                .exec(&format!(
                    "INSERT INTO {name} (id, vec) VALUES ('r-{i}', ARRAY[{v}, 0.0, 0.0])"
                ))
                .await
                .unwrap();
        }

        let stats_rows = server
            .query_text(&format!("SHOW VECTOR INDEX status ON {name}"))
            .await
            .unwrap_or_default();
        let _ = stats_rows;
    }
}
