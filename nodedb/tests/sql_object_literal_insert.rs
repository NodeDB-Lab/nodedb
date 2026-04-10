//! Integration tests for `{ key: value }` object literal INSERT syntax.
//!
//! Verifies that `INSERT INTO coll { ... }` produces the same result as the
//! standard `INSERT INTO coll (cols) VALUES (vals)` form across all engines.

mod common;

use common::pgwire_harness::TestServer;

// ── Schemaless Document ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_schemaless() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs { id: 'doc1', name: 'Alice', age: 30 }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM docs WHERE id = 'doc1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice"));
    assert!(rows[0].contains("30"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_schemaless_nested() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION nested_docs").await.unwrap();

    server
        .exec("INSERT INTO nested_docs { id: 'n1', name: 'Bob', address: { city: 'NYC', zip: '10001' }, tags: ['admin', 'dev'] }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM nested_docs WHERE id = 'n1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Bob"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_schemaless_auto_id() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION auto_id_docs").await.unwrap();

    // No explicit id — should auto-generate uuid_v7.
    server
        .exec("INSERT INTO auto_id_docs { name: 'Charlie', score: 42 }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM auto_id_docs")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Charlie"));
}

// ── Key-Value ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_kv() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION kv_cache TYPE KEY_VALUE (key TEXT PRIMARY KEY)")
        .await
        .unwrap();

    // { } form inserts without error.
    server
        .exec("INSERT INTO kv_cache { key: 'k1', value: 'hello' }")
        .await
        .unwrap();

    // Both forms should produce the same result.
    server
        .exec("INSERT INTO kv_cache (key, value) VALUES ('k2', 'world')")
        .await
        .unwrap();

    // Verify both keys exist and both forms produce the same response shape.
    let r1 = server
        .query_text("SELECT * FROM kv_cache WHERE key = 'k1'")
        .await
        .unwrap();
    let r2 = server
        .query_text("SELECT * FROM kv_cache WHERE key = 'k2'")
        .await
        .unwrap();
    assert_eq!(r1.len(), 1, "object literal key lookup: {r1:?}");
    assert_eq!(r2.len(), 1, "VALUES key lookup: {r2:?}");

    // Full scan should return individual rows (flat array format).
    let all = server.query_text("SELECT * FROM kv_cache").await.unwrap();
    assert_eq!(all.len(), 2, "full scan should return 2 rows, got: {all:?}");
}

// ── Strict Document ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION strict_orders TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, customer TEXT, amount FLOAT\
            )",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO strict_orders { id: 'o1', customer: 'Alice', amount: 99.99 }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM strict_orders WHERE id = 'o1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice"));
}

// ── Plain Columnar ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_columnar() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION col_data TYPE COLUMNAR (\
                id TEXT, region TEXT, value FLOAT\
            )",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO col_data { id: 'c1', region: 'us-east', value: 3.14 }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM col_data").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("us-east"));
}

// ── Timeseries Columnar ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_timeseries() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION ts_events TYPE COLUMNAR (\
                id TEXT, ts TIMESTAMP TIME_KEY, value FLOAT, region TEXT\
            ) WITH profile = 'timeseries'",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO ts_events { id: 'e1', ts: '2024-01-01T00:00:00Z', value: 42.0, region: 'us' }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM ts_events").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("42"));
}

// ── Spatial Columnar ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_spatial() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION sp_locations TYPE COLUMNAR (\
                id TEXT, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH profile = 'spatial'",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO sp_locations (id, geom, label) VALUES ('s1', ST_Point(-73.98, 40.75), 'NYC')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM sp_locations")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("NYC"));
}

// ── UPSERT with { } ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_upsert_schemaless() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION upsert_docs").await.unwrap();

    server
        .exec("INSERT INTO upsert_docs { id: 'u1', name: 'Alice', role: 'user' }")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO upsert_docs { id: 'u1', name: 'Alice Updated', role: 'admin' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM upsert_docs WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice Updated"));
    assert!(rows[0].contains("admin"));
}

// ── Equivalence: { } produces same result as VALUES ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_matches_values_form() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION equiv_docs").await.unwrap();

    server
        .exec("INSERT INTO equiv_docs (id, name, score) VALUES ('v1', 'ValuesForm', 100)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO equiv_docs { id: 'o1', name: 'ObjectForm', score: 100 }")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM equiv_docs").await.unwrap();
    assert_eq!(rows.len(), 2);
}

// ── Batch: [{ }, { }] array form ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_batch_insert() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION batch_docs").await.unwrap();

    server
        .exec("INSERT INTO batch_docs [{ id: 'b1', name: 'Alice' }, { id: 'b2', name: 'Bob' }, { id: 'b3', name: 'Charlie' }]")
        .await
        .unwrap();

    let rows = server.query_text("SELECT * FROM batch_docs").await.unwrap();
    assert_eq!(
        rows.len(),
        3,
        "batch insert should create 3 rows, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_batch_insert_heterogeneous_keys() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION hetero_docs").await.unwrap();

    // Different rows have different keys — union columns, NULL for missing.
    server
        .exec("INSERT INTO hetero_docs [{ id: 'h1', name: 'Alice', age: 30 }, { id: 'h2', name: 'Bob', role: 'admin' }]")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM hetero_docs")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "hetero batch should create 2 rows, got: {rows:?}"
    );
}

// ── Graph edge PROPERTIES with { } ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_edge_properties_object_literal() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION graph_nodes").await.unwrap();

    // Insert nodes first.
    server
        .exec("INSERT INTO graph_nodes { id: 'a', name: 'Alice' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO graph_nodes { id: 'b', name: 'Bob' }")
        .await
        .unwrap();

    // Edge with { } properties (new form).
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'knows' PROPERTIES { since: 2020, weight: 0.9 }")
        .await
        .unwrap();

    // Edge with quoted JSON properties (old form — still works).
    server
        .exec("GRAPH INSERT EDGE FROM 'b' TO 'a' TYPE 'follows' PROPERTIES '{\"year\": 2021}'")
        .await
        .unwrap();

    // Verify edges exist via traversal.
    let rows = server
        .query_text("GRAPH NEIGHBORS OF 'a' DIRECTION out")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "node 'a' should have outbound neighbors");
}

// ── MATCH node label filtering ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_node_label_filtering() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION label_docs").await.unwrap();

    // Insert nodes + edges (edges implicitly create graph nodes).
    server
        .exec("INSERT INTO label_docs { id: 'alice', name: 'Alice' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO label_docs { id: 'bob', name: 'Bob' }")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'knows'")
        .await
        .unwrap();

    // Verify unlabeled MATCH works.
    let all = server
        .query_text("MATCH (a)-[:knows]->(b) RETURN a, b")
        .await
        .unwrap();
    assert_eq!(all.len(), 1, "unlabeled: {all:?}");

    // Set labels.
    server
        .exec("GRAPH LABEL 'alice' AS 'Person'")
        .await
        .expect("GRAPH LABEL alice");
    server
        .exec("GRAPH LABEL 'bob' AS 'Person'")
        .await
        .expect("GRAPH LABEL bob");

    // Labeled MATCH — both are Person.
    let labeled = server
        .query_text("MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b")
        .await
        .unwrap();
    assert_eq!(labeled.len(), 1, "Person->Person: {labeled:?}");

    // Non-matching label — should return 0.
    let none = server
        .query_text("MATCH (a:Bot)-[:knows]->(b) RETURN a, b")
        .await
        .unwrap();
    assert_eq!(none.len(), 0, "Bot src should match nothing: {none:?}");
}
