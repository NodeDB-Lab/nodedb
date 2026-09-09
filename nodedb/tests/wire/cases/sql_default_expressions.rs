// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for DEFAULT expression evaluation in INSERT.
//!
//! A declared DEFAULT expression evaluates on every engine, not only
//! `document_strict`. A DEFAULT the server cannot evaluate must be refused at
//! DDL time, never accepted and silently dropped at insert time.

use crate::harness::TestServer;

/// `DEFAULT upper('x')` — a scalar function call as a default value.
/// The planner should evaluate this rather than dropping the column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_scalar_function_upper() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_fn (\
                id TEXT PRIMARY KEY, \
                a TEXT DEFAULT upper('x')) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_fn (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT a FROM def_fn WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row should exist");
    // The default should produce 'X'. If the column was silently dropped,
    // the value will be null/absent.
    assert!(
        rows[0].contains('X'),
        "DEFAULT upper('x') should produce 'X', got {:?}",
        rows[0]
    );
}

/// `DEFAULT lower('HELLO')` — another scalar function.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_scalar_function_lower() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_lower (\
                id TEXT PRIMARY KEY, \
                tag TEXT DEFAULT lower('HELLO')) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_lower (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT tag FROM def_lower WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("hello"),
        "DEFAULT lower('HELLO') should produce 'hello', got {:?}",
        rows[0]
    );
}

/// `DEFAULT 1 + 2` — a binary arithmetic expression as default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_arithmetic_expression() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_arith (\
                id TEXT PRIMARY KEY, \
                v INT DEFAULT 1 + 2) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_arith (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT v FROM def_arith WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains('3'),
        "DEFAULT 1 + 2 should produce 3, got {:?}",
        rows[0]
    );
}

/// `DEFAULT concat('a', 'b')` — a multi-arg function.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_concat_function() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_concat (\
                id TEXT PRIMARY KEY, \
                label TEXT DEFAULT concat('hello', '_', 'world')) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_concat (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT label FROM def_concat WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("hello_world"),
        "DEFAULT concat should produce 'hello_world', got {:?}",
        rows[0]
    );
}

/// Verify that recognized defaults (literal string, NOW(), UUID_V7) still work.
/// This is a baseline — not a new bug, just ensures we don't regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_recognized_expressions_still_work() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_known (\
                id TEXT PRIMARY KEY, \
                status TEXT DEFAULT 'active', \
                uid TEXT DEFAULT UUID_V7) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_known (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT status FROM def_known WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("active"),
        "DEFAULT 'active' should work: got {:?}",
        rows[0]
    );
}

/// `DEFAULT nextval('seq')` fills a strict-engine primary key across two
/// inserts that omit the column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_nextval_fills_a_strict_primary_key() {
    let server = TestServer::start().await;

    server.exec("CREATE SEQUENCE seq_def_strict").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION def_seq_strict (\
                id BIGINT DEFAULT nextval('seq_def_strict') PRIMARY KEY, \
                v TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_seq_strict (v) VALUES ('a')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO def_seq_strict (v) VALUES ('b')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM def_seq_strict ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two rows expected: {rows:?}");
    assert_not_null(&rows[0], "first id");
    assert_not_null(&rows[1], "second id");
    assert_eq!(rows, vec!["1".to_string(), "2".to_string()]);
}

/// `DEFAULT nextval('seq')` fills a schemaless-engine primary key, the same
/// way it fills a strict one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_nextval_fills_a_schemaless_primary_key() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_def_schemaless")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION def_seq_schemaless (\
                id BIGINT DEFAULT nextval('seq_def_schemaless') PRIMARY KEY, \
                v TEXT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_seq_schemaless (v) VALUES ('a')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO def_seq_schemaless (v) VALUES ('b')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM def_seq_schemaless ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two rows expected: {rows:?}");
    assert_not_null(&rows[0], "first id");
    assert_not_null(&rows[1], "second id");
    assert_eq!(rows, vec!["1".to_string(), "2".to_string()]);
}

/// `DEFAULT nextval('seq')` fills a KV-engine key column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_nextval_fills_a_kv_key() {
    let server = TestServer::start().await;

    server.exec("CREATE SEQUENCE seq_def_kv").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION def_seq_kv (\
                id BIGINT DEFAULT nextval('seq_def_kv') PRIMARY KEY, \
                v TEXT) WITH (engine='kv')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_seq_kv (v) VALUES ('a')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO def_seq_kv (v) VALUES ('b')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM def_seq_kv ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two rows expected: {rows:?}");
    assert_not_null(&rows[0], "first id");
    assert_not_null(&rows[1], "second id");
    assert_eq!(rows, vec!["1".to_string(), "2".to_string()]);
}

/// `DEFAULT nextval('seq')` fills a columnar-engine column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_nextval_fills_a_columnar_column() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_def_columnar")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION def_seq_columnar (\
                id BIGINT DEFAULT nextval('seq_def_columnar') PRIMARY KEY, \
                v TEXT) WITH (engine='columnar')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_seq_columnar (v) VALUES ('a')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO def_seq_columnar (v) VALUES ('b')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM def_seq_columnar ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "two rows expected: {rows:?}");
    assert_not_null(&rows[0], "first id");
    assert_not_null(&rows[1], "second id");
    assert_eq!(rows, vec!["1".to_string(), "2".to_string()]);
}

/// `DEFAULT upper('x')` evaluates on a schemaless collection, isolating the
/// schemaless DEFAULT drop from the sequence-accessor problem.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_scalar_function_on_a_schemaless_collection() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_fn_schemaless (id TEXT PRIMARY KEY, a TEXT DEFAULT upper('x'))",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_fn_schemaless (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT a FROM def_fn_schemaless WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row should exist");
    assert_not_null(&rows[0], "a");
    assert!(
        rows[0].contains('X'),
        "DEFAULT upper('x') should produce 'X', got {:?}",
        rows[0]
    );
}

/// `DEFAULT UUID_V7()` evaluates on a schemaless collection and produces a
/// 36-character value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_uuid_on_a_schemaless_collection() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_uuid_schemaless (id TEXT PRIMARY KEY, b TEXT DEFAULT UUID_V7())",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_uuid_schemaless (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT b FROM def_uuid_schemaless WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row should exist");
    assert_not_null(&rows[0], "b");
    assert_eq!(
        rows[0].trim().len(),
        36,
        "UUID_V7() must render as 36 characters, got `{}`",
        rows[0]
    );
}

/// A DEFAULT expression the server cannot evaluate is refused at DDL time
/// with `42883`, never accepted and silently dropped at insert time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_that_cannot_be_evaluated_is_refused_at_ddl() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "CREATE COLLECTION def_unevaluable (\
                id TEXT PRIMARY KEY, \
                a TEXT DEFAULT no_such_function_here('x'))",
            "42883",
        )
        .await;
}

/// `DEFAULT currval('seq')` fills a strict-engine column with the session's
/// last `nextval` result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_currval_fills_a_column() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_def_currval")
        .await
        .unwrap();
    server
        .query_text("SELECT nextval('seq_def_currval')")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION def_currval_strict (\
                id TEXT PRIMARY KEY, \
                n BIGINT DEFAULT currval('seq_def_currval')) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_currval_strict (id) VALUES ('k1')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM def_currval_strict WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "row should exist");
    assert_not_null(&rows[0], "n");
    assert_eq!(
        rows[0].trim(),
        "1",
        "currval-backed default must be 1, got `{}`",
        rows[0]
    );
}

/// Asserts a rendered row carries a real value in place of an absent or NULL column.
fn assert_not_null(row: &str, label: &str) {
    let trimmed = row.trim();
    assert!(
        !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("null"),
        "{label}: expected a value, got `{row}`"
    );
}
