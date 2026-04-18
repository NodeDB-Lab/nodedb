//! Two-phase Building→Ready backfill and UNIQUE enforcement.

use super::common::pgwire_harness::TestServer;

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
    assert!(
        rows[0].contains("\"a\""),
        "indexed SELECT row must reference doc id 'a', got: {}",
        rows[0]
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
    server
        .expect_error("CREATE UNIQUE INDEX ON bf_unique(code)", "unique")
        .await;
}
