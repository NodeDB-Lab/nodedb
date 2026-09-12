// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

// ── Natural-key PRIMARY KEY on a non-`id` column ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_natural_key_pk_on_non_id_column_keeps_distinct_rows() {
    // A PRIMARY KEY declared on a non-`id` column must drive row identity on
    // columnar too. Two rows with DISTINCT natural keys must both stay visible
    // — the built-in synthetic `id` (NULL for every row) must NOT make them
    // collide into a single tombstoned row (silent data loss).
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics (\
                sku TEXT PRIMARY KEY, region TEXT, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX metrics_pk ON metrics (sku)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO metrics (sku, region, value) VALUES ('a', 'us-east', 1.0)")
        .await
        .unwrap();
    // Distinct natural key: must NOT collide on an empty synthetic `id`.
    server
        .exec("INSERT INTO metrics (sku, region, value) VALUES ('b', 'us-west', 2.0)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT sku, region FROM metrics ORDER BY sku")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both distinct natural-key rows must stay visible, got: {rows:?}"
    );
    assert_eq!(rows[0][0], "a", "got: {rows:?}");
    assert_eq!(rows[1][0], "b", "got: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_natural_key_pk_on_non_id_column_keeps_distinct_rows() {
    // Spatial inherits columnar identity: a non-`id` PRIMARY KEY must keep
    // distinct natural-key rows distinct rather than colliding on synthetic id.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places (\
                code TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX places_pk ON places (code)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO places (code, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO places (code, geom, label) VALUES ('p2', ST_Point(1.0, 1.0), 'other')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT code, label FROM places ORDER BY code")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both distinct natural-key rows must stay visible, got: {rows:?}"
    );
    assert_eq!(rows[0][0], "p1", "got: {rows:?}");
    assert_eq!(rows[1][0], "p2", "got: {rows:?}");
}

// ── Declared non-`id` PRIMARY KEY is a uniqueness constraint ────────────────
//
// The natural-key tests above create a `UNIQUE INDEX` alongside the
// declaration, so they exercise the index rather than the declaration.
// `PRIMARY KEY` implies uniqueness on every engine, and declaring it is
// enough — these tests carry no index.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_declared_non_id_primary_key_refuses_duplicate() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics_pk (\
                sku TEXT PRIMARY KEY, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO metrics_pk (sku, value) VALUES ('a', 1.0)")
        .await
        .unwrap();

    match server
        .client
        .simple_query("INSERT INTO metrics_pk (sku, value) VALUES ('a', 2.0)")
        .await
    {
        Ok(_) => panic!("expected unique_violation on the declared primary key, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }

    // The refused insert left one row under the key. A duplicate that commits
    // makes every later point read and keyed DML touch an unbounded row count.
    let rows = server
        .query_rows("SELECT value FROM metrics_pk WHERE sku = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly one row may exist per declared primary key, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_declared_non_id_primary_key_refuses_duplicate() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places_pk (\
                code TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO places_pk (code, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')",
        )
        .await
        .unwrap();

    match server
        .client
        .simple_query(
            "INSERT INTO places_pk (code, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved')",
        )
        .await
    {
        Ok(_) => panic!("expected unique_violation on the declared primary key, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }

    let rows = server
        .query_rows("SELECT label FROM places_pk WHERE code = 'p1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly one row may exist per declared primary key, got: {rows:?}"
    );
}

/// `ON CONFLICT` resolves against the declared key. The upsert path derives
/// identity through the same helper as the insert path, so it must recognise
/// the same conflict rather than appending a second row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_declared_non_id_primary_key_upsert_updates_in_place() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics_up (\
                sku TEXT PRIMARY KEY, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO metrics_up (sku, value) VALUES ('a', 1.0)")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO metrics_up (sku, value) VALUES ('a', 7.0) \
             ON CONFLICT (sku) DO UPDATE SET value = EXCLUDED.value",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT value FROM metrics_up WHERE sku = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the upsert must resolve against the declared key, got: {rows:?}"
    );
    assert_eq!(
        rows[0][0], "7.0",
        "expected the EXCLUDED value, got: {:?}",
        rows[0]
    );
}
