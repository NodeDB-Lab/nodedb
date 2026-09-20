// SPDX-License-Identifier: BUSL-1.1

//! Pins the cross-engine `TRUNCATE` contract: after seeding 2-3 rows,
//! `TRUNCATE <coll>` answers the bare `TRUNCATE` command tag (no row count —
//! see `command_complete_tag_conformance::truncate_table_reports_no_row_count`
//! for that half of the contract), a following read of the collection
//! reports zero rows, and a further `INSERT` + read returns exactly the new
//! row. Every peer engine owes this: `plan_truncate_stmt` resolves the
//! collection's engine through the catalog and routes through
//! `EngineRules::plan_truncate`, so each engine's own store is cleared.
//! The columnar family's spatial-index and transaction contracts live in
//! `truncate_engine_conformance_columnar_family`.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Every `CommandComplete` tag in `sql`'s simple-query response, in wire
/// order.
pub(super) async fn command_tags(server: &TestServer, sql: &str) -> Vec<u64> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .collect()
}

/// Assert `TRUNCATE <collection>` answers exactly one command tag whose
/// fallback-parsed count is 0 — the bare `TRUNCATE` tag Postgres sends
/// (`extract_row_affected` parses a tag with no trailing integer as `0`).
pub(super) async fn assert_truncate_tag(server: &TestServer, collection: &str) {
    let tags = command_tags(server, &format!("TRUNCATE {collection}")).await;
    assert_eq!(
        tags,
        vec![0],
        "TRUNCATE {collection} must answer one bare tag (fallback-parses to 0)"
    );
}

/// Number of rows `SELECT COUNT(*) FROM <collection>` reports.
pub(super) async fn row_count(server: &TestServer, collection: &str) -> u64 {
    server
        .query_rows(&format!("SELECT COUNT(*) FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("count {collection}: {e}"))
        .first()
        .unwrap_or_else(|| panic!("count query returned no row for {collection}"))[0]
        .parse()
        .unwrap_or_else(|e| panic!("count query returned a non-integer for {collection}: {e}"))
}

/// Document (schemaless): TRUNCATE empties the collection; a post-truncate
/// INSERT + SELECT returns exactly the new row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_document_schemaless_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_doc_schemaless (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless')",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for id in ["a", "b", "c"] {
        srv.exec(&format!(
            "INSERT INTO trunc_doc_schemaless (id, v) VALUES ('{id}', 'x')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_doc_schemaless").await, 3);

    assert_truncate_tag(&srv, "trunc_doc_schemaless").await;
    assert_eq!(
        row_count(&srv, "trunc_doc_schemaless").await,
        0,
        "TRUNCATE must remove every row from a schemaless document collection"
    );

    srv.exec("INSERT INTO trunc_doc_schemaless (id, v) VALUES ('z', 'new')")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id, v FROM trunc_doc_schemaless")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string(), "new".to_string()]]);
}

/// Document (strict, `FIELDS (...)` form): TRUNCATE empties the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_document_strict_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_doc_strict \
         FIELDS (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for id in ["a", "b", "c"] {
        srv.exec(&format!(
            "INSERT INTO trunc_doc_strict (id, v) VALUES ('{id}', 'x')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_doc_strict").await, 3);

    assert_truncate_tag(&srv, "trunc_doc_strict").await;
    assert_eq!(
        row_count(&srv, "trunc_doc_strict").await,
        0,
        "TRUNCATE must remove every row from a strict document collection"
    );

    srv.exec("INSERT INTO trunc_doc_strict (id, v) VALUES ('z', 'new')")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id, v FROM trunc_doc_strict")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string(), "new".to_string()]]);
}

/// Key-Value: TRUNCATE empties the collection; verified both by aggregate
/// count and by a point `SELECT ... WHERE k = ...` on the truncated key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_kv_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION trunc_kv (k TEXT PRIMARY KEY, v TEXT) WITH (engine='kv')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    for k in ["a", "b", "c"] {
        srv.exec(&format!("INSERT INTO trunc_kv (k, v) VALUES ('{k}', 'x')"))
            .await
            .unwrap_or_else(|e| panic!("seed {k}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_kv").await, 3);

    assert_truncate_tag(&srv, "trunc_kv").await;
    assert_eq!(
        row_count(&srv, "trunc_kv").await,
        0,
        "TRUNCATE must remove every row from a kv collection"
    );
    let point = srv
        .query_rows("SELECT v FROM trunc_kv WHERE k = 'a'")
        .await
        .unwrap_or_else(|e| panic!("point read after truncate: {e}"));
    assert!(
        point.is_empty(),
        "a point SELECT on a truncated key must return no rows: {point:?}"
    );

    srv.exec("INSERT INTO trunc_kv (k, v) VALUES ('z', 'new')")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT k, v FROM trunc_kv WHERE k = 'z'")
        .await
        .unwrap_or_else(|e| panic!("post-truncate point read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string(), "new".to_string()]]);
}

/// Columnar: TRUNCATE empties the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_columnar_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_columnar \
         COLUMNS (id TEXT, n INT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for i in 0..3u32 {
        srv.exec(&format!(
            "INSERT INTO trunc_columnar (id, n) VALUES ('c{i}', {i})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {i}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_columnar").await, 3);

    assert_truncate_tag(&srv, "trunc_columnar").await;
    assert_eq!(
        row_count(&srv, "trunc_columnar").await,
        0,
        "TRUNCATE must remove every row from a columnar collection"
    );

    srv.exec("INSERT INTO trunc_columnar (id, n) VALUES ('z', 99)")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id, n FROM trunc_columnar")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string(), "99".to_string()]]);
}

/// Timeseries: TRUNCATE empties the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_timeseries_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_ts \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (id, ts, v) in [("a", 1000, 10), ("b", 2000, 20), ("c", 3000, 30)] {
        srv.exec(&format!(
            "INSERT INTO trunc_ts (id, ts, v) VALUES ('{id}', {ts}, {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_ts").await, 3);

    assert_truncate_tag(&srv, "trunc_ts").await;
    assert_eq!(
        row_count(&srv, "trunc_ts").await,
        0,
        "TRUNCATE must remove every row from a timeseries collection"
    );

    srv.exec("INSERT INTO trunc_ts (id, ts, v) VALUES ('z', 4000, 99)")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id, v FROM trunc_ts")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string(), "99".to_string()]]);
}

/// Spatial: TRUNCATE empties the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_spatial_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_spatial \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (id, x, y) in [("a", 1.0, 1.0), ("b", 2.0, 2.0), ("c", 3.0, 3.0)] {
        srv.exec(&format!(
            "INSERT INTO trunc_spatial (id, loc) VALUES ('{id}', ST_MakePoint({x}, {y}))"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_spatial").await, 3);

    assert_truncate_tag(&srv, "trunc_spatial").await;
    assert_eq!(
        row_count(&srv, "trunc_spatial").await,
        0,
        "TRUNCATE must remove every row from a spatial collection"
    );

    srv.exec("INSERT INTO trunc_spatial (id, loc) VALUES ('z', ST_MakePoint(9.0, 9.0))")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id FROM trunc_spatial")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string()]]);
}

/// Vector-primary: TRUNCATE empties the collection, and a
/// `vector_distance` search over the truncated collection returns nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_vector_primary_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_vec (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
         WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
               payload_indexes=['owner'])",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (id, x, y, z) in [
        ("a", 1.0, 0.0, 0.0),
        ("b", 0.0, 1.0, 0.0),
        ("c", 0.0, 0.0, 1.0),
    ] {
        srv.exec(&format!(
            "INSERT INTO trunc_vec (id, vec, owner) VALUES \
             ('{id}', ARRAY[{x}, {y}, {z}], 'alice')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_vec").await, 3);

    assert_truncate_tag(&srv, "trunc_vec").await;
    assert_eq!(
        row_count(&srv, "trunc_vec").await,
        0,
        "TRUNCATE must remove every row from a vector-primary collection"
    );
    let search = srv
        .query_rows(
            "SELECT id FROM trunc_vec \
             ORDER BY vector_distance(vec, ARRAY[1.0, 0.0, 0.0]) LIMIT 5",
        )
        .await
        .unwrap_or_else(|e| panic!("post-truncate search: {e}"));
    assert!(
        search.is_empty(),
        "a vector_distance search over a truncated collection must return no rows: {search:?}"
    );

    srv.exec("INSERT INTO trunc_vec (id, vec, owner) VALUES ('z', ARRAY[1.0, 0.0, 0.0], 'bob')")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id FROM trunc_vec")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string()]]);
}

/// CRDT (`crdt='true'`): TRUNCATE empties the collection through the
/// materialized document-store read path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_crdt_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION trunc_crdt (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (id, v) in [("a", 1), ("b", 2), ("c", 3)] {
        srv.exec(&format!(
            "INSERT INTO trunc_crdt (id, v) VALUES ('{id}', {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_crdt").await, 3);

    assert_truncate_tag(&srv, "trunc_crdt").await;
    assert_eq!(
        row_count(&srv, "trunc_crdt").await,
        0,
        "TRUNCATE must remove every row from a CRDT collection"
    );

    srv.exec("INSERT INTO trunc_crdt (id, v) VALUES ('z', 99)")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let rows = srv
        .query_rows("SELECT id, v FROM trunc_crdt")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(rows, vec![vec!["z".to_string(), "99".to_string()]]);
}

/// Array: `ArrayRules::plan_truncate` refuses `TRUNCATE` with a typed error
/// that names the array surface to use instead (`DROP ARRAY`). The cells
/// stay in place, checked through `ARRAY_SLICE` over the array's full
/// coordinate range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_array_is_refused_with_a_typed_error() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY trunc_arr \
         DIMS (x INT64 [0..15]) \
         ATTRS (v INT64) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await
    .unwrap_or_else(|e| panic!("create array: {e}"));
    for (coord, v) in [(0, 10), (1, 11), (2, 12)] {
        srv.exec(&format!(
            "INSERT INTO ARRAY trunc_arr COORDS ({coord}) VALUES ({v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed coord {coord}: {e}"));
    }
    let slice = "SELECT * FROM ARRAY_SLICE('trunc_arr', '{\"x\":[0,15]}', '*', 100)";
    let before = srv
        .query_rows(slice)
        .await
        .unwrap_or_else(|e| panic!("pre-truncate slice: {e}"));
    assert_eq!(before.len(), 3, "seed must have landed: {before:?}");

    srv.expect_error("TRUNCATE trunc_arr", "DROP ARRAY").await;

    let after = srv
        .query_rows(slice)
        .await
        .unwrap_or_else(|e| panic!("post-truncate slice: {e}"));
    assert_eq!(
        after.len(),
        3,
        "a refused TRUNCATE must leave every array cell in place: {after:?}"
    );
}

/// Vector-primary: `TRUNCATE ... RESTART IDENTITY` empties the collection
/// and restarts the `SERIAL` key's sequence, so the next insert takes 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_vector_primary_restart_identity_restarts_serial_key() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_vec_serial (id SERIAL PRIMARY KEY, vec VECTOR(3), owner STRING) \
         WITH (engine='vector', primary='vector', vector_field='vec', dim=3)",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (x, y, z) in [(1.0, 0.0, 0.0), (0.0, 1.0, 0.0)] {
        srv.exec(&format!(
            "INSERT INTO trunc_vec_serial (vec, owner) VALUES (ARRAY[{x}, {y}, {z}], 'alice')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed: {e}"));
    }
    let ids = srv
        .query_rows("SELECT id FROM trunc_vec_serial ORDER BY id")
        .await
        .unwrap_or_else(|e| panic!("seed read: {e}"));
    assert_eq!(ids, vec![vec!["1".to_string()], vec!["2".to_string()]]);

    let tags = command_tags(&srv, "TRUNCATE trunc_vec_serial RESTART IDENTITY").await;
    assert_eq!(tags, vec![0], "TRUNCATE must answer one bare tag");
    assert_eq!(row_count(&srv, "trunc_vec_serial").await, 0);

    srv.exec("INSERT INTO trunc_vec_serial (vec, owner) VALUES (ARRAY[0.0, 0.0, 1.0], 'bob')")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    let ids = srv
        .query_rows("SELECT id FROM trunc_vec_serial")
        .await
        .unwrap_or_else(|e| panic!("post-truncate read: {e}"));
    assert_eq!(
        ids,
        vec![vec!["1".to_string()]],
        "RESTART IDENTITY must restart the SERIAL key at 1"
    );
}

/// A KV `TRUNCATE` inside an explicit transaction empties the collection
/// only once the transaction commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_inside_transaction_kv_then_commit_empties_collection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION trunc_kv_txn (k TEXT PRIMARY KEY, v TEXT) WITH (engine='kv')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    for k in ["a", "b", "c"] {
        srv.exec(&format!(
            "INSERT INTO trunc_kv_txn (k, v) VALUES ('{k}', 'x')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {k}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_kv_txn").await, 3);

    srv.exec("BEGIN")
        .await
        .unwrap_or_else(|e| panic!("begin: {e}"));
    assert_truncate_tag(&srv, "trunc_kv_txn").await;
    srv.exec("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("commit: {e}"));

    assert_eq!(
        row_count(&srv, "trunc_kv_txn").await,
        0,
        "TRUNCATE inside a transaction must empty the collection once committed"
    );
}
