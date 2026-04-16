//! Integration coverage for DEFAULT expression evaluation in INSERT.
//!
//! The planner's `evaluate_default_expr` recognizes only a fixed keyword list
//! (UUID_V7, NOW(), NANOID, literals). Any other expression returns None,
//! causing the column to be silently omitted. These tests verify that
//! expression-based defaults are evaluated, not dropped.

mod common;

use common::pgwire_harness::TestServer;

/// `DEFAULT upper('x')` — a scalar function call as a default value.
/// The planner should evaluate this rather than dropping the column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_scalar_function_upper() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_fn TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                a TEXT DEFAULT upper('x'))",
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
            "CREATE COLLECTION def_lower TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                tag TEXT DEFAULT lower('HELLO'))",
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
            "CREATE COLLECTION def_arith TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                v INT DEFAULT 1 + 2)",
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
            "CREATE COLLECTION def_concat TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                label TEXT DEFAULT concat('hello', '_', 'world'))",
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
            "CREATE COLLECTION def_known TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                status TEXT DEFAULT 'active', \
                uid TEXT DEFAULT UUID_V7)",
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
