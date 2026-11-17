// SPDX-License-Identifier: BUSL-1.1

//! Query shapes a shadowed clone refuses instead of answering wrongly. A
//! read over a shadowed clone either reads through to the source or refuses
//! with a typed error — never target-only rows, never two partial answers
//! silently concatenated. A shape with no source-side rewrite refuses until
//! materialized. A set operation reads each branch through the clone.

use crate::harness::TestServer;

/// Substring shared by every clone-read refusal.
const REFUSAL: &str = "cannot be read through an unmaterialized clone";

async fn exec_all(server: &TestServer, stmts: &[&str]) {
    for sql in stmts {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Aggregating scan on the `timeseries` engine. Lowers to
/// `TimeseriesOp::Scan` carrying `aggregates`, not `QueryOp::Aggregate` —
/// the same unsound concatenation by a different plan shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_aggregate_over_shadowed_clone_is_refused() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_ts_src",
            "USE DATABASE crs_ts_src",
            "CREATE COLLECTION metrics \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, sensor TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        ],
    )
    .await;
    for i in 0..5u32 {
        let ts = u64::from(i) * 1000 + 1000;
        server
            .exec(&format!(
                "INSERT INTO metrics (id, ts, sensor, value) \
                 VALUES ('m{i}', {ts}, 's1', {i}.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert m{i}: {e}"));
    }
    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "CLONE DATABASE crs_ts_clone FROM crs_ts_src",
            "USE DATABASE crs_ts_clone",
        ],
    )
    .await;

    server
        .expect_error("SELECT COUNT(*) FROM metrics", REFUSAL)
        .await;

    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "ALTER DATABASE crs_ts_clone MATERIALIZE",
            "USE DATABASE crs_ts_clone",
        ],
    )
    .await;
    let rows = server
        .query_rows("SELECT COUNT(*) FROM metrics")
        .await
        .expect("COUNT(*) on a materialized timeseries clone");
    assert_eq!(
        rows.first().and_then(|r| r.first()).map(String::as_str),
        Some("5"),
        "materialized clone must count every source row: {rows:?}"
    );
}

/// Join. `extract_collection` reports a join's LEFT collection, and the
/// resolver also checks the right side, so a clone on either side refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_over_shadowed_clone_is_refused() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_join_src",
            "USE DATABASE crs_join_src",
            "CREATE COLLECTION cj_left (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
            "CREATE COLLECTION cj_right (id TEXT PRIMARY KEY, w TEXT) \
             WITH (engine='document_strict')",
            "INSERT INTO cj_left (id, v) VALUES ('k1', 'left1')",
            "INSERT INTO cj_right (id, w) VALUES ('k1', 'right1')",
            "USE DATABASE default",
            "CLONE DATABASE crs_join_clone FROM crs_join_src",
            "USE DATABASE crs_join_clone",
        ],
    )
    .await;

    server
        .expect_error(
            "SELECT a.id FROM cj_left a JOIN cj_right b ON a.id = b.id",
            REFUSAL,
        )
        .await;

    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "ALTER DATABASE crs_join_clone MATERIALIZE",
            "USE DATABASE crs_join_clone",
        ],
    )
    .await;
    let rows = server
        .query_rows("SELECT a.id FROM cj_left a JOIN cj_right b ON a.id = b.id")
        .await
        .expect("join on a materialized clone");
    assert_eq!(rows.len(), 1, "join must return the matching row: {rows:?}");
}

/// Vector ANN search (`VectorOp::Search`). Every graph op that names a
/// collection (`RagFusion`, a scoped traversal) refuses through the identical
/// default arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_search_over_shadowed_clone_is_refused() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_vec_src",
            "USE DATABASE crs_vec_src",
            "CREATE COLLECTION vecs WITH (engine='document_schemaless')",
            "CREATE INDEX ON vecs (embedding)",
            "INSERT INTO vecs { id: 'v1', embedding: [1.0,0.0,0.0,0.0] }",
            "INSERT INTO vecs { id: 'v2', embedding: [0.0,1.0,0.0,0.0] }",
            "USE DATABASE default",
            "CLONE DATABASE crs_vec_clone FROM crs_vec_src",
            "USE DATABASE crs_vec_clone",
        ],
    )
    .await;

    let query = "SELECT id FROM vecs \
                 ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 2";
    server.expect_error(query, REFUSAL).await;

    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "ALTER DATABASE crs_vec_clone MATERIALIZE",
            "USE DATABASE crs_vec_clone",
        ],
    )
    .await;
    // Row identity depends on the target's index rebuild, which this refusal
    // does not govern; what must hold is that the refusal lifts.
    server
        .query_rows(query)
        .await
        .expect("vector search on a materialized clone must not refuse");
}

/// Full-text search (`TextOp::Search`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_search_over_shadowed_clone_is_refused() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_fts_src",
            "USE DATABASE crs_fts_src",
            "CREATE COLLECTION docs (id TEXT PRIMARY KEY, content TEXT) \
             WITH (engine='document_strict')",
            "CREATE SEARCH INDEX idx_crs_fts ON docs FIELDS content",
            "INSERT INTO docs (id, content) VALUES \
             ('d1', 'consensus algorithm distributed'), \
             ('d2', 'vector search memory')",
            "USE DATABASE default",
            "CLONE DATABASE crs_fts_clone FROM crs_fts_src",
            "USE DATABASE crs_fts_clone",
        ],
    )
    .await;

    let query = "SELECT id FROM docs WHERE text_match(content, 'consensus')";
    server.expect_error(query, REFUSAL).await;

    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "ALTER DATABASE crs_fts_clone MATERIALIZE",
            "USE DATABASE crs_fts_clone",
        ],
    )
    .await;
    // As with vector: the assertion is that the refusal lifts, not that the
    // target's search index was rebuilt with identical scoring.
    server
        .query_rows(query)
        .await
        .expect("text search on a materialized clone must not refuse");
}

/// Spatial predicate scan (`SpatialOp::Scan`). A PLAIN scan over a spatial
/// collection lowers to `ColumnarOp::Scan` and still reads through — only the
/// `ST_*` predicate form takes the spatial plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_predicate_over_shadowed_clone_is_refused() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_sp_src",
            "USE DATABASE crs_sp_src",
            "CREATE COLLECTION places \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
            "INSERT INTO places (id, location, name) \
             VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')",
            "INSERT INTO places (id, location, name) \
             VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')",
            "USE DATABASE default",
            "CLONE DATABASE crs_sp_clone FROM crs_sp_src",
            "USE DATABASE crs_sp_clone",
        ],
    )
    .await;

    let query = "SELECT name FROM places WHERE \
                 ST_DWithin(location, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)";
    server.expect_error(query, REFUSAL).await;

    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "ALTER DATABASE crs_sp_clone MATERIALIZE",
            "USE DATABASE crs_sp_clone",
        ],
    )
    .await;
    let rows = server
        .query_rows(query)
        .await
        .expect("spatial predicate on a materialized clone");
    assert_eq!(
        rows.len(),
        1,
        "exactly one place is within 5 km of Times Square: {rows:?}"
    );
}

/// `UNION DISTINCT` / `INTERSECT` / `EXCEPT` read each branch through the
/// clone on its own, then merge the complete branch rows: `EXCEPT` subtracts
/// source-only rows, and `UNION` dedups them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_operation_over_shadowed_clone_merges_complete_branches() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_set_src",
            "USE DATABASE crs_set_src",
            "CREATE COLLECTION so_items (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
            "INSERT INTO so_items (id, v) VALUES ('s1', 'a')",
            "INSERT INTO so_items (id, v) VALUES ('s2', 'b')",
            "USE DATABASE default",
            "CLONE DATABASE crs_set_clone FROM crs_set_src",
            "USE DATABASE crs_set_clone",
        ],
    )
    .await;

    let union = "SELECT id FROM so_items UNION SELECT id FROM so_items";
    let except = "SELECT id FROM so_items EXCEPT SELECT id FROM so_items WHERE v = 'a'";
    let sorted_ids = |rows: Vec<Vec<String>>| {
        let mut ids: Vec<String> = rows.into_iter().map(|row| row[0].clone()).collect();
        ids.sort_unstable();
        ids
    };

    for stage in ["shadowed", "materialized"] {
        let rows = server
            .query_rows(union)
            .await
            .unwrap_or_else(|e| panic!("UNION on a {stage} clone: {e}"));
        assert_eq!(
            sorted_ids(rows),
            vec!["s1", "s2"],
            "UNION of a collection with itself is its distinct rows on a {stage} clone"
        );
        let rows = server
            .query_rows(except)
            .await
            .unwrap_or_else(|e| panic!("EXCEPT on a {stage} clone: {e}"));
        assert_eq!(
            sorted_ids(rows),
            vec!["s2"],
            "EXCEPT subtracts the source-only row on a {stage} clone"
        );
        if stage == "shadowed" {
            exec_all(
                &server,
                &[
                    "USE DATABASE default",
                    "ALTER DATABASE crs_set_clone MATERIALIZE",
                    "USE DATABASE crs_set_clone",
                ],
            )
            .await;
        }
    }
}

/// Each `UNION ALL` branch reads its own collection through the clone, so no
/// branch answers target-only rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_collection_read_over_shadowed_clone_resolves_each_branch() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE crs_multi_src",
            "USE DATABASE crs_multi_src",
            "CREATE COLLECTION ua_one (id TEXT PRIMARY KEY) WITH (engine='document_strict')",
            "CREATE COLLECTION ua_two (id TEXT PRIMARY KEY) WITH (engine='document_strict')",
            "INSERT INTO ua_one (id) VALUES ('o1')",
            "INSERT INTO ua_two (id) VALUES ('t1')",
            "USE DATABASE default",
            "CLONE DATABASE crs_multi_clone FROM crs_multi_src",
            "USE DATABASE crs_multi_clone",
        ],
    )
    .await;

    let query = "SELECT id FROM ua_one UNION ALL SELECT id FROM ua_two";

    // Values, not just count: 2 duplicate rows from one collection would
    // also pass a bare length check.
    let rows = server
        .query_rows(query)
        .await
        .expect("UNION ALL over a Shadowed clone must read through, not refuse");
    let mut ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["o1", "t1"],
        "each branch must independently resolve its own collection's source-only row: {rows:?}"
    );

    // Control: materializing must not change the answer.
    exec_all(
        &server,
        &[
            "USE DATABASE default",
            "ALTER DATABASE crs_multi_clone MATERIALIZE",
            "USE DATABASE crs_multi_clone",
        ],
    )
    .await;
    let rows = server
        .query_rows(query)
        .await
        .expect("UNION ALL on a materialized clone");
    let mut ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["o1", "t1"],
        "materialized clone must still return one row from each collection: {rows:?}"
    );
}
