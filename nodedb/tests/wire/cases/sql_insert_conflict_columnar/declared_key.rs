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
        rows[0][0], "7",
        "expected the EXCLUDED value, got: {:?}",
        rows[0]
    );
}

// ── `ON CONFLICT` target validation, exhaustive per engine ──────────────────
//
// `check_conflict_target` reads each column's own `is_primary_key` flag, and
// `convert_collection_type`'s columnar arm sets that flag on a declared
// natural key rather than on the synthetic surrogate `id` column whenever
// one is declared. These cases pin that resolution directly, both when no
// key is declared (the surrogate is the only candidate) and when one is.
//
// Timeseries is not covered here: `timeseries/create.rs` always records
// `declared_primary_key: None`, so it can never reach the declared-key arm
// these tests exercise, and `EngineRules::plan_upsert` refuses every
// timeseries `UPSERT` / `ON CONFLICT DO UPDATE` outright before a target's
// name would matter (`"UPSERT is not supported on timeseries collections
// (append-only)"`). `ON CONFLICT (x) DO NOTHING` still passes the planner on
// timeseries (its `if_absent` flag is accepted but unused by
// `TimeseriesRules::plan_insert`), a pre-existing gap unrelated to the
// declared-key resolution these tests cover.

/// A refused DB error's SQLSTATE, read the way the sibling tests above read
/// `23505`.
async fn refused_sqlstate(server: &TestServer, sql: &str) -> String {
    match server.client.simple_query(sql).await {
        Ok(_) => panic!("expected the statement to be refused: {sql}"),
        Err(e) => e
            .as_db_error()
            .unwrap_or_else(|| panic!("expected DbError for: {sql}"))
            .code()
            .code()
            .to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_no_declared_key_on_conflict_surrogate_id_is_accepted() {
    // No `PRIMARY KEY` at all: the synthetic surrogate `id` column is the
    // only candidate, and naming it is accepted.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m_nokey (v INT) WITH (engine='columnar')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m_nokey (v) VALUES (1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m_nokey (v) VALUES (2) ON CONFLICT (id) DO NOTHING")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m_nokey (v) VALUES (3) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v")
        .await
        .unwrap();

    let rows = server.query_text("SELECT v FROM m_nokey").await.unwrap();
    assert_eq!(rows.len(), 3, "every row mints a fresh id, got: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_no_declared_key_on_conflict_other_column_is_refused() {
    // No `PRIMARY KEY` declared: a target naming any column other than the
    // surrogate `id` is refused, `DO NOTHING` included.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m_nokey2 (v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m_nokey2 (v) VALUES (1)")
        .await
        .unwrap();

    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO m_nokey2 (v) VALUES (2) ON CONFLICT (v) DO NOTHING",
        )
        .await,
        "0A000",
    );
    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO m_nokey2 (v) VALUES (2) ON CONFLICT (v) DO UPDATE SET v = EXCLUDED.v",
        )
        .await,
        "0A000",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_declared_non_id_primary_key_on_conflict_do_nothing_on_declared_key_is_accepted() {
    // The `DO NOTHING` branch gained the same target check `DO UPDATE`
    // already had: naming the real declared key is still accepted, and the
    // conflicting row is left unchanged.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics_dn (\
                sku TEXT PRIMARY KEY, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO metrics_dn (sku, value) VALUES ('a', 1.0)")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO metrics_dn (sku, value) VALUES ('a', 999.0) ON CONFLICT (sku) DO NOTHING",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT value FROM metrics_dn WHERE sku = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "got: {rows:?}");
    assert_eq!(
        rows[0][0], "1",
        "DO NOTHING must leave the row, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_declared_non_id_primary_key_on_conflict_do_nothing_other_column_is_refused() {
    // Before this fix, `DO NOTHING` ran no target check at all: a target
    // naming the wrong column was silently accepted and the row was deduped
    // by the real key regardless of what the clause named.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics_dn2 (\
                sku TEXT PRIMARY KEY, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO metrics_dn2 (sku, value) VALUES ('a', 1.0)")
        .await
        .unwrap();

    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO metrics_dn2 (sku, value) VALUES ('a', 2.0) \
             ON CONFLICT (value) DO NOTHING",
        )
        .await,
        "0A000",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_declared_non_id_primary_key_on_conflict_surrogate_id_is_refused() {
    // A declared natural key is "the" primary key: the surrogate `id`
    // column's `is_primary_key` is cleared, so naming `id` instead of the
    // declared key is refused exactly like naming any other wrong column.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics_sid (\
                sku TEXT PRIMARY KEY, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO metrics_sid (sku, value) VALUES ('a', 1.0)")
        .await
        .unwrap();

    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO metrics_sid (sku, value) VALUES ('a', 2.0) \
             ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        )
        .await,
        "0A000",
    );
    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO metrics_sid (sku, value) VALUES ('a', 2.0) ON CONFLICT (id) DO NOTHING",
        )
        .await,
        "0A000",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_declared_non_id_primary_key_on_conflict_on_constraint_is_refused() {
    // `ON CONFLICT ON CONSTRAINT` is refused for every target, unchanged by
    // this fix.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics_oc (\
                sku TEXT PRIMARY KEY, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO metrics_oc (sku, value) VALUES ('a', 1.0)")
        .await
        .unwrap();

    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO metrics_oc (sku, value) VALUES ('a', 2.0) \
             ON CONFLICT ON CONSTRAINT metrics_oc_pk DO UPDATE SET value = EXCLUDED.value",
        )
        .await,
        "0A000",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_declared_non_id_primary_key_on_conflict_surrogate_id_is_refused() {
    // Spatial shares columnar's identity arm in `convert_collection_type`;
    // the same surrogate-vs-declared-key resolution applies.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places_sid (\
                code TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO places_sid (code, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')",
        )
        .await
        .unwrap();

    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO places_sid (code, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved') \
             ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label",
        )
        .await,
        "0A000",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_declared_non_id_primary_key_on_conflict_do_nothing_other_column_is_refused() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places_dn (\
                code TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO places_dn (code, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')",
        )
        .await
        .unwrap();

    assert_eq!(
        refused_sqlstate(
            &server,
            "INSERT INTO places_dn (code, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved') \
             ON CONFLICT (label) DO NOTHING",
        )
        .await,
        "0A000",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_declared_non_id_primary_key_on_conflict_declared_key_do_nothing_is_accepted() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places_dn2 (\
                code TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO places_dn2 (code, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO places_dn2 (code, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved') \
             ON CONFLICT (code) DO NOTHING",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT label FROM places_dn2 WHERE code = 'p1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "got: {rows:?}");
    assert_eq!(
        rows[0][0], "origin",
        "DO NOTHING must leave the row, got: {rows:?}"
    );
}
