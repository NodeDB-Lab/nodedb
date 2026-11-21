// SPDX-License-Identifier: BUSL-1.1

//! Two-phase Building→Ready backfill and UNIQUE enforcement.

use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_unique_index_rejects_duplicate_insert() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_unique_enforce")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX ON idx_unique_enforce(email)")
        .await
        .unwrap();

    // First insert must succeed.
    server
        .exec("INSERT INTO idx_unique_enforce { id: 'a', email: 'x@y.z' }")
        .await
        .unwrap();

    // Second insert with the same indexed value must be rejected. Today the
    // UNIQUE keyword is parsed (`is_unique`) but never persisted anywhere,
    // so duplicates succeed silently — a correctness bug that is part of
    // the same design flaw as the reporter's point-lookup issue: CREATE
    // INDEX DDL modifiers are parsed but not dispatched to the config or
    // enforcement layer.
    server
        .expect_error(
            "INSERT INTO idx_unique_enforce { id: 'b', email: 'x@y.z' }",
            "unique",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_backfills_existing_rows() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION bf_col").await.unwrap();
    server
        .exec("INSERT INTO bf_col { id: 'a', email: 'one@x.com' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bf_col { id: 'b', email: 'two@x.com' }")
        .await
        .unwrap();

    // CREATE INDEX runs AFTER the rows exist. The two-phase
    // Building→Ready backfill pipeline must populate the index from the
    // pre-existing documents before flipping to Ready; otherwise a
    // subsequent lookup against the index would miss the rows (same
    // silent-miss class as the original reporter's bug).
    server.exec("CREATE INDEX ON bf_col(email)").await.unwrap();

    let rows = server
        .query_text("SELECT id FROM bf_col WHERE email = 'one@x.com'")
        .await
        .expect("indexed SELECT must succeed");
    assert_eq!(
        rows.len(),
        1,
        "indexed SELECT must return exactly one row, got: {rows:?}"
    );
    assert_eq!(
        rows[0], "a",
        "indexed SELECT row must reference doc id 'a', got: {}",
        rows[0]
    );
}

/// The backfill must decode existing rows with the collection's OWN storage
/// mode, not with the schemaless MessagePack decoder.
///
/// A `document_strict` collection stores Binary Tuples. Reading them with the
/// schemaless decoder fails on every row, so a backfill that skipped
/// undecodable rows built an EMPTY index and reported `CREATE INDEX` as
/// successful — after which every equality lookup on the indexed column
/// silently missed rows that are plainly there. The lookup below is the only
/// thing that can tell an index that was populated from one that merely exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_backfills_existing_strict_rows() {
    let server = TestServer::start().await;

    server
        .exec("CREATE TABLE bf_strict (id TEXT PRIMARY KEY, region TEXT, n INT)")
        .await
        .unwrap();
    for (id, region, n) in [("a", "us", 1), ("b", "eu", 2), ("c", "us", 3)] {
        server
            .exec(&format!(
                "INSERT INTO bf_strict (id, region, n) VALUES ('{id}', '{region}', {n})"
            ))
            .await
            .unwrap();
    }

    // Rows exist first, so this CREATE INDEX must go through the backfill path
    // rather than the incremental per-write index maintenance.
    server
        .exec("CREATE INDEX ON bf_strict (region)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM bf_strict WHERE region = 'us' ORDER BY id")
        .await
        .expect("indexed SELECT on a strict collection must succeed");
    assert_eq!(
        rows,
        vec!["a".to_string(), "c".to_string()],
        "the backfill must have populated the index from the Binary Tuple rows, got: {rows:?}"
    );

    // The ascending assertion above also passes when the sort is dropped and
    // the index happens to hand back its entries in key order. DESC cannot be
    // satisfied by accident.
    let rows = server
        .query_text("SELECT id FROM bf_strict WHERE region = 'us' ORDER BY id DESC")
        .await
        .expect("indexed SELECT with a descending sort must succeed");
    assert_eq!(
        rows,
        vec!["c".to_string(), "a".to_string()],
        "an equality on the indexed column must not cost the query its ORDER BY, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_unique_index_rejects_existing_duplicates() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION bf_unique").await.unwrap();
    server
        .exec("INSERT INTO bf_unique { id: 'a', code: 'ABC' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bf_unique { id: 'b', code: 'ABC' }")
        .await
        .unwrap();

    // CREATE UNIQUE INDEX on a collection that already contains
    // duplicates must fail at the backfill phase — detecting the
    // violation before the Ready flip so the catalog never advertises
    // an index that doesn't actually enforce uniqueness.
    let refused = server
        .client
        .simple_query("CREATE UNIQUE INDEX ON bf_unique(code)")
        .await
        .expect_err("a UNIQUE backfill over duplicates is refused");
    let db_error = refused.as_db_error().expect("a server error");
    assert_eq!(
        db_error.code(),
        &tokio_postgres::error::SqlState::UNIQUE_VIOLATION,
        "a duplicate found by the backfill is 23505: {db_error:?}"
    );
    assert!(
        db_error.message().to_lowercase().contains("unique"),
        "the refusal names the unique constraint: {db_error:?}"
    );
}
