// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for sequences: CREATE/DROP/ALTER/SHOW SEQUENCE, SERIAL.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_drop_sequence() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE order_seq START 1 INCREMENT 1")
        .await
        .unwrap();
    let rows = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(!rows.is_empty(), "SHOW SEQUENCES should list the sequence");
    server.exec("DROP SEQUENCE order_seq").await.unwrap();
    server
        .expect_error("DROP SEQUENCE order_seq", "does not exist")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_sequence_restart() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE s1 START 1 INCREMENT 1")
        .await
        .unwrap();
    server
        .exec("ALTER SEQUENCE s1 RESTART WITH 100")
        .await
        .unwrap();
    server.exec("DROP SEQUENCE s1").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequence_options() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE cyc START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 5 CYCLE CACHE 10")
        .await
        .unwrap();
    server.exec("DROP SEQUENCE cyc").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_creates_implicit_sequence() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION orders FIELDS (id SERIAL, name TEXT)")
        .await
        .unwrap();

    let rows = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        !rows.is_empty(),
        "SERIAL should create an implicit sequence"
    );

    server.exec("DROP COLLECTION orders").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_sequence_if_exists() {
    let server = TestServer::start().await;

    // DROP IF EXISTS on non-existent should not error.
    server
        .exec("DROP SEQUENCE IF EXISTS nonexistent")
        .await
        .unwrap();
}

/// `nextval('seq')` returns 1 on the first call and 2 on the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_returns_successive_values() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_nextval_succ")
        .await
        .unwrap();

    let first = server
        .query_text("SELECT nextval('seq_nextval_succ')")
        .await
        .unwrap();
    assert_eq!(first, vec!["1".to_string()], "first nextval must be 1");

    let second = server
        .query_text("SELECT nextval('seq_nextval_succ')")
        .await
        .unwrap();
    assert_eq!(second, vec!["2".to_string()], "second nextval must be 2");
}

/// `currval('seq')` returns the session's last `nextval` result, not a fresh
/// allocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn currval_returns_the_last_value_of_the_session() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_currval_session")
        .await
        .unwrap();

    server
        .query_text("SELECT nextval('seq_currval_session')")
        .await
        .unwrap();
    let first = server
        .query_text("SELECT currval('seq_currval_session')")
        .await
        .unwrap();
    assert_eq!(
        first,
        vec!["1".to_string()],
        "currval must echo the last nextval"
    );

    server
        .query_text("SELECT nextval('seq_currval_session')")
        .await
        .unwrap();
    let second = server
        .query_text("SELECT currval('seq_currval_session')")
        .await
        .unwrap();
    assert_eq!(
        second,
        vec!["2".to_string()],
        "currval must track the second nextval"
    );
}

/// `setval('seq', 10)` positions the sequence so the next `nextval` returns 11.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setval_positions_the_next_allocation() {
    let server = TestServer::start().await;

    server.exec("CREATE SEQUENCE seq_setval_pos").await.unwrap();

    server
        .query_text("SELECT setval('seq_setval_pos', 10)")
        .await
        .unwrap();
    let next = server
        .query_text("SELECT nextval('seq_setval_pos')")
        .await
        .unwrap();
    assert_eq!(
        next,
        vec!["11".to_string()],
        "nextval after setval(10) must be 11"
    );
}

/// `nextval` on a sequence that was never created must fail with `42704`
/// (undefined_object), not `42883` (undefined_function) — the function
/// exists, the object does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_on_an_unknown_sequence_errors() {
    let server = TestServer::start().await;

    server
        .expect_error("SELECT nextval('seq_that_was_never_created')", "42704")
        .await;
}

/// A `SERIAL` column allocates 1 then 2 across two inserts that omit it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_column_allocates_successive_keys() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION seq_serial_alloc FIELDS (n SERIAL, v TEXT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO seq_serial_alloc (v) VALUES ('a')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO seq_serial_alloc (v) VALUES ('b')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM seq_serial_alloc ORDER BY n")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two rows expected: {rows:?}");
    for row in &rows {
        let trimmed = row.trim();
        assert!(
            !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("null"),
            "SERIAL column must not be empty or NULL, got `{row}`"
        );
    }
    assert_eq!(rows, vec!["1".to_string(), "2".to_string()]);
}
