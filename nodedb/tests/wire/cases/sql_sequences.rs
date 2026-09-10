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

/// Preparing an INSERT must not consume a `nextval` allocation.
/// Only executing the statement advances the sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparing_an_insert_does_not_advance_a_sequence_default() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_plan_side_effect;")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION seq_plan_probe (\
                id BIGINT DEFAULT nextval('seq_plan_side_effect') PRIMARY KEY, \
                v TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    const INSERT: &str = "INSERT INTO seq_plan_probe (v) VALUES ('a')";

    // Parse/Describe only. Each round trip plans the statement without
    // executing it, so none of them can allocate a sequence value.
    for _ in 0..3 {
        server
            .client
            .prepare(INSERT)
            .await
            .expect("prepare INSERT with a nextval default must succeed");
    }

    server.exec(INSERT).await.unwrap();

    let rows = server
        .query_text("SELECT id FROM seq_plan_probe")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
    assert_eq!(
        rows[0].trim(),
        "1",
        "planning must leave the sequence at its start, got id `{}`",
        rows[0]
    );

    // The next allocation is 2 when exactly one value was consumed.
    let next = server
        .query_text("SELECT nextval('seq_plan_side_effect')")
        .await
        .unwrap();
    assert_eq!(
        next,
        vec!["2".to_string()],
        "one execution must consume exactly one value, got {next:?}"
    );
}

/// Describing an INSERT that omits a `nextval` DEFAULT reports its columns
/// without consuming a sequence value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describing_an_insert_leaves_the_sequence_untouched() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_describe_probe;")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION seq_describe_target (\
                id BIGINT DEFAULT nextval('seq_describe_probe') PRIMARY KEY, \
                v TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .client
        .prepare("INSERT INTO seq_describe_target (v) VALUES ($1) RETURNING id")
        .await
        .expect("prepare INSERT ... RETURNING with a nextval default must succeed");

    // The sequence was never executed against, so the first allocation is 1.
    let first = server
        .query_text("SELECT nextval('seq_describe_probe')")
        .await
        .unwrap();
    assert_eq!(
        first,
        vec!["1".to_string()],
        "describe must not allocate, got {first:?}"
    );
}

/// Create a two-row KV collection and a sequence, both named after `label`.
///
/// Returns `(sequence_name, collection_name)`.
async fn two_row_collection_with_sequence(server: &TestServer, label: &str) -> (String, String) {
    let sequence = format!("seq_row_ctx_{label}");
    let collection = format!("row_ctx_{label}");

    server
        .exec(&format!("CREATE SEQUENCE {sequence}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id BIGINT PRIMARY KEY, v TEXT) \
             WITH (engine = 'kv')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {collection} (id, v) VALUES (1, 'a')"))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {collection} (id, v) VALUES (2, 'b')"))
        .await
        .unwrap();

    (sequence, collection)
}

/// `nextval` in a SELECT list over a FROM relation raises `0A000`
/// (feature_not_supported). Per-row allocation is not implemented, and a
/// silent NULL for every row is worse than the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_a_select_list_over_a_scan_is_refused() {
    let server = TestServer::start().await;
    let (sequence, collection) = two_row_collection_with_sequence(&server, "nextval").await;

    server
        .expect_error(
            &format!("SELECT nextval('{sequence}') FROM {collection}"),
            "0A000",
        )
        .await;
}

/// `currval` in a SELECT list over a FROM relation raises `0A000`, the same
/// as `nextval`: both read session sequence state the row evaluator lacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn currval_in_a_select_list_over_a_scan_is_refused() {
    let server = TestServer::start().await;
    let (sequence, collection) = two_row_collection_with_sequence(&server, "currval").await;

    server
        .query_text(&format!("SELECT nextval('{sequence}')"))
        .await
        .unwrap();
    server
        .expect_error(
            &format!("SELECT currval('{sequence}') FROM {collection}"),
            "0A000",
        )
        .await;
}

/// A projection mixing a plain column with a sequence accessor is refused as
/// a whole. Returning the column and a NULL beside it would ship the silent
/// cell this refusal exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_projection_mixing_a_column_and_an_accessor_is_refused() {
    let server = TestServer::start().await;
    let (sequence, collection) = two_row_collection_with_sequence(&server, "mixed").await;

    server
        .expect_error(
            &format!("SELECT id, nextval('{sequence}') FROM {collection}"),
            "0A000",
        )
        .await;
}

/// A sequence accessor in a WHERE clause over a FROM relation is refused for
/// the same reason as one in the SELECT list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sequence_accessor_in_a_where_clause_is_refused() {
    let server = TestServer::start().await;
    let (sequence, collection) = two_row_collection_with_sequence(&server, "where").await;

    server
        .expect_error(
            &format!("SELECT id FROM {collection} WHERE id = nextval('{sequence}')"),
            "0A000",
        )
        .await;
}

/// The refusal yields no result set at all, so no row can carry an empty or
/// NULL cell, and the statement allocates nothing from the sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_accessor_produces_no_row_and_no_allocation() {
    let server = TestServer::start().await;
    let (sequence, collection) = two_row_collection_with_sequence(&server, "guard").await;

    let result = server
        .query_text(&format!("SELECT nextval('{sequence}') FROM {collection}"))
        .await;
    let message = match result {
        Ok(rows) => panic!("per-row nextval must not return rows, got {rows:?}"),
        Err(message) => message,
    };
    assert!(
        message.contains("0A000"),
        "refusal must carry SQLSTATE 0A000, got: {message}"
    );

    // A refused statement consumes nothing, so the first real allocation is 1.
    let first = server
        .query_text(&format!("SELECT nextval('{sequence}')"))
        .await
        .unwrap();
    assert_eq!(
        first,
        vec!["1".to_string()],
        "the refused statement must not have allocated, got {first:?}"
    );
}
