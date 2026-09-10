// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

// ── Plain columnar ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_duplicate_pk_keeps_latest() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics (\
                id TEXT PRIMARY KEY, region TEXT, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX metrics_pk ON metrics (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO metrics (id, region, value) VALUES ('m1', 'us-east', 1.0)")
        .await
        .unwrap();

    // Duplicate PK — must NOT raise 23505 on columnar (OLAP-shaped), but
    // must also NOT produce two visible rows (the silent-duplicate bug).
    server
        .exec("INSERT INTO metrics (id, region, value) VALUES ('m1', 'us-west', 2.0)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, region, value FROM metrics WHERE id = 'm1'")
        .await
        .unwrap();

    // Regression guard: the original bug was two rows visible for one PK.
    assert_eq!(
        rows.len(),
        1,
        "duplicate PK must not produce two visible rows, got: {rows:?}"
    );

    // Latest-write-wins on the tombstoned prior row. row[0]=id, row[1]=region, row[2]=value.
    assert_eq!(
        rows[0][1], "us-west",
        "expected latest write (us-west), got: {:?}",
        rows[0]
    );
    assert!(
        rows[0][2].contains('2'),
        "expected value 2.0, got: {:?}",
        rows[0]
    );
    assert_ne!(
        rows[0][1], "us-east",
        "prior row must be tombstoned, got: {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_full_scan_hides_tombstoned_duplicate() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES ('b', 20)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, v FROM m ORDER BY id")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        3,
        "full scan must return 3 rows after dup, got: {rows:?}"
    );
    // ORDER BY id → a, b, c with latest-wins on b. row[0]=id, row[1]=v.
    assert_eq!(
        rows[0][0], "a",
        "first row after ORDER BY id must be 'a', got: {rows:?}"
    );
    assert_eq!(rows[1][0], "b", "second row id must be 'b', got: {rows:?}");
    assert_eq!(
        rows[1][1], "20",
        "second row must be b=20 (latest wins), got: {rows:?}"
    );
    assert_eq!(
        rows[2][0], "c",
        "third row after ORDER BY id must be 'c', got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_on_conflict_do_nothing_keeps_original() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    // DO NOTHING must be a silent no-op (no error, no overwrite).
    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 999) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT v FROM m WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains('1') && !rows[0].contains("999"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_on_conflict_do_update_merges_excluded() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT, note TEXT) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v, note) VALUES ('a', 1, 'orig')")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO m (id, v, note) VALUES ('a', 7, 'new') \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT v, note FROM m WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // row[0]=v, row[1]=note
    assert_eq!(
        rows[0][0], "7",
        "expected v=7 (from EXCLUDED), got: {:?}",
        rows[0]
    );
    assert_eq!(
        rows[0][1], "orig",
        "expected note=orig (unchanged), got: {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_upsert_keyword_overwrites_on_pk() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("UPSERT INTO m (id, v) VALUES ('a', 42)")
        .await
        .unwrap();

    let rows = server.query_text("SELECT v FROM m").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("42") && !rows[0].contains(" 1 "));
}

// ── Explicit ORDER BY sort key (no PK) ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_order_by_sort_key_accepted() {
    let server = TestServer::start().await;

    // ORDER BY declares a sort key without PK semantics: duplicates on the
    // sort column are allowed and all rows remain visible.
    server
        .exec(
            "CREATE COLLECTION events (\
                bucket TEXT, payload TEXT\
             ) WITH (engine='columnar') ORDER BY (bucket)",
        )
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO events (bucket, payload) VALUES \
             ('b', 'first'), ('a', 'second'), ('b', 'third')",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT bucket, payload FROM events ORDER BY bucket, payload")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "ORDER BY without PK must not dedup; got: {rows:?}"
    );
}
