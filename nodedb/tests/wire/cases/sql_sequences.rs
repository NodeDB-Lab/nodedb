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

/// A from-less projection legally repeats an output name
/// (`SELECT nextval('s'), nextval('s')`). The constant row is a single JSON
/// object, so keying both cells by the display name collapsed them to the
/// last value and both wire columns rendered it. Each column must keep its
/// own cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_constant_column_names_keep_their_own_cells() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SEQUENCE dup_cell_seq START 1 INCREMENT 1")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT nextval('dup_cell_seq'), nextval('dup_cell_seq')")
        .await
        .expect("a from-less projection repeating an output name must return one row");

    assert_eq!(
        rows.len(),
        1,
        "expected exactly one constant row, got {rows:?}"
    );
    assert_eq!(
        rows[0],
        vec!["1".to_string(), "2".to_string()],
        "each duplicate column must keep its own cell; keying both by the \
         display name collapses them to the last value"
    );
}

/// The same repeated output name through the extended protocol
/// (Parse/Bind/Execute). Describe announces the columns, and the projection
/// built from it must keep the planner's per-column lookup keys, or both
/// columns read the first cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_constant_column_names_keep_their_own_cells_extended_protocol() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SEQUENCE dup_cell_ext_seq START 1 INCREMENT 1")
        .await
        .unwrap();

    let rows = server
        .client
        .query(
            "SELECT nextval('dup_cell_ext_seq'), nextval('dup_cell_ext_seq')",
            &[],
        )
        .await
        .expect("a prepared from-less projection repeating an output name must run");
    assert_eq!(rows.len(), 1, "expected exactly one constant row");
    let cells: (i64, i64) = (rows[0].get(0), rows[0].get(1));
    assert_eq!(
        cells,
        (1, 2),
        "each duplicate column must keep its own cell over the extended protocol"
    );
}

/// A join that projects two columns of one bare name, and a projection that
/// repeats an aggregate, keep their own cells over the extended protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_join_and_aggregate_names_keep_their_own_cells_extended_protocol() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION dup_w (id TEXT PRIMARY KEY, b_id TEXT) WITH (engine='document_strict')",
        "CREATE COLLECTION dup_b (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
        "INSERT INTO dup_b (id, n) VALUES ('b1', 5)",
        "INSERT INTO dup_b (id, n) VALUES ('b2', 7)",
        "INSERT INTO dup_w (id, b_id) VALUES ('w1', 'b1')",
    ] {
        server.exec(sql).await.unwrap();
    }

    let rows = server
        .client
        .query(
            "SELECT dup_w.id, dup_b.id FROM dup_w JOIN dup_b ON dup_w.b_id = dup_b.id",
            &[],
        )
        .await
        .expect("a prepared join repeating a bare column name must run");
    assert_eq!(rows.len(), 1, "expected one joined row");
    let ids: (String, String) = (rows[0].get(0), rows[0].get(1));
    assert_eq!(
        ids,
        ("w1".to_string(), "b1".to_string()),
        "each side of the join keeps its own id cell"
    );

    let rows = server
        .client
        .query("SELECT count(*), sum(n), count(*) FROM dup_b", &[])
        .await
        .expect("a prepared projection repeating an aggregate must run");
    assert_eq!(rows.len(), 1, "expected one aggregate row");
    let counts: (i64, i64) = (rows[0].get(0), rows[0].get(2));
    assert_eq!(counts, (2, 2), "both count(*) columns read the count");
}

/// Describing a statement plans it without executing it: Parse announces a
/// `nextval` column's type and consumes no value, so the first execution
/// still gets the sequence's start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describing_nextval_consumes_no_value() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SEQUENCE describe_only_seq START 1 INCREMENT 1")
        .await
        .unwrap();

    for _ in 0..2 {
        let statement = server
            .client
            .prepare("SELECT nextval('describe_only_seq')")
            .await
            .expect("prepare a nextval projection");
        assert_eq!(
            statement.columns().len(),
            1,
            "Describe announces the nextval column"
        );
        assert_eq!(
            statement.columns()[0].type_(),
            &tokio_postgres::types::Type::INT8
        );
    }

    let rows = server
        .query_rows("SELECT nextval('describe_only_seq')")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![vec!["1".to_string()]],
        "describing the statement consumed a sequence value"
    );
}

/// Describing a `nextval` of a sequence that does not exist reports the
/// missing sequence, as executing it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describing_nextval_of_a_missing_sequence_is_refused() {
    let server = TestServer::start().await;
    let refused = server
        .client
        .prepare("SELECT nextval('no_such_describe_seq')")
        .await;
    let error = refused.expect_err("Parse of a nextval on a missing sequence must be refused");
    let code = error.code().map(|c| c.code().to_string());
    assert_eq!(code.as_deref(), Some("42704"), "{error}");
}
