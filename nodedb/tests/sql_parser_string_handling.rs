//! Integration coverage for SQL parsers handling string literals correctly.
//!
//! Hand-rolled parsers must skip over single-quoted string literals when
//! scanning for structural delimiters (`BEGIN`, `)`, `,`, `WITH`, `{}`).
//! These tests verify that embedded keywords and special characters inside
//! string values do not corrupt the parse.

mod common;

use common::pgwire_harness::TestServer;

// ---------------------------------------------------------------------------
// 1. CREATE TRIGGER: `BEGIN` inside a string literal in WHEN clause
// ---------------------------------------------------------------------------

/// A WHEN clause containing the word 'BEGIN' inside a string literal must not
/// confuse the header/body split. The trigger should parse correctly and the
/// body should execute as written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_begin_inside_string_literal_does_not_corrupt_parse() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION items TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, label TEXT)")
        .await
        .unwrap();

    server.exec("CREATE COLLECTION audit_log").await.unwrap();

    // The WHEN clause references the string 'BEGIN' — the parser must not
    // treat this as the body start.
    let result = server
        .exec(
            "CREATE TRIGGER tr_begin BEFORE INSERT ON items FOR EACH ROW \
             WHEN (NEW.label = 'BEGIN' OR NEW.label = 'open') \
             BEGIN INSERT INTO audit_log { id: NEW.id, note: 'triggered' }; END",
        )
        .await;
    assert!(
        result.is_ok(),
        "CREATE TRIGGER with 'BEGIN' in WHEN string should succeed: {:?}",
        result
    );

    // The trigger should not interfere with a normal insert.
    server
        .exec("INSERT INTO items (id, label) VALUES ('i1', 'hello')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM items WHERE id = 'i1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "insert should succeed through the trigger");
}

// ---------------------------------------------------------------------------
// 2. INSERT: `)` inside a string literal in VALUES
// ---------------------------------------------------------------------------

/// A string value containing `)` must not break the VALUES pre-parse that
/// scans for the closing paren.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_paren_inside_string_value_parses_correctly() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION msgs TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                body TEXT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO msgs (id, body) VALUES ('m1', 'hello)world')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT body FROM msgs WHERE id = 'm1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "insert with ) in string should succeed");
    assert!(
        rows[0].contains("hello)world"),
        "value should preserve the literal paren: got {:?}",
        rows[0]
    );
}

/// Multiple special characters inside string values in a multi-column INSERT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_multiple_parens_in_strings() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION notes TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                a TEXT, \
                b TEXT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO notes (id, a, b) VALUES ('n1', 'a)b', 'c(d')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT a FROM notes WHERE id = 'n1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("a)b"), "got {:?}", rows[0]);
}

// ---------------------------------------------------------------------------
// 3. split_values: quote tracking inside brackets/arrays
// ---------------------------------------------------------------------------

/// An array literal containing strings with `)` must not confuse the value
/// splitter's bracket-depth tracking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_array_with_paren_in_string_element() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION arr_test TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                tags TEXT, \
                count INT)",
        )
        .await
        .unwrap();

    // ARRAY['x)y', 'z'] — the `)` inside the first element must not
    // decrement the bracket counter.
    server
        .exec("INSERT INTO arr_test (id, tags, count) VALUES ('a1', ARRAY['x)y', 'z'], 42)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT count FROM arr_test WHERE id = 'a1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "insert with array containing ')' should succeed"
    );
    assert!(
        rows[0].contains("42"),
        "second column value should be 42, got {:?}",
        rows[0]
    );
}

// ---------------------------------------------------------------------------
// 5. Object-literal `{ }` rewriter: `''`-escaped quotes
// ---------------------------------------------------------------------------

/// The `{ }` preprocessor must handle SQL-escaped single quotes (`''`).
/// `'it''s'` is a valid SQL string containing an apostrophe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_with_escaped_quote_parses_correctly() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION esc_test").await.unwrap();

    // The `{ note: 'it''s' }` syntax goes through `find_matching_brace`
    // which must correctly handle the `''` escape.
    server
        .exec("INSERT INTO esc_test { id: 'e1', note: 'it''s fine' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM esc_test WHERE id = 'e1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "insert with escaped quote in object literal should succeed"
    );
    assert!(
        rows[0].contains("it's fine") || rows[0].contains("it''s fine"),
        "value should contain the apostrophe: got {:?}",
        rows[0]
    );
}

/// Escaped quotes adjacent to the closing brace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_escaped_quote_near_brace() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION esc_test2").await.unwrap();

    server
        .exec("INSERT INTO esc_test2 { id: 'e2', val: 'end''s}' }")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM esc_test2 WHERE id = 'e2'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "escaped quote near brace should parse correctly"
    );
}

// ---------------------------------------------------------------------------
// 7. CREATE MATERIALIZED VIEW: `WITH` inside SELECT body
// ---------------------------------------------------------------------------

/// A materialized view whose SELECT contains a CTE (`WITH cte AS (...)`)
/// must not have the CTE keyword mistaken for the options clause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_view_with_cte_in_select() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION mv_src TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_src (id, val) VALUES ('r1', 10)")
        .await
        .unwrap();

    // The SELECT uses a CTE — `WITH s AS (...)`. The parser must not
    // truncate the query at the CTE's `WITH`.
    let result = server
        .exec(
            "CREATE MATERIALIZED VIEW mv_cte ON mv_src AS \
             WITH s AS (SELECT id, val FROM mv_src) SELECT id, val FROM s",
        )
        .await;
    assert!(
        result.is_ok(),
        "CREATE MATERIALIZED VIEW with CTE should succeed: {:?}",
        result
    );
}

/// A materialized view whose SELECT contains a column aliased with `with_`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_view_with_keyword_in_column_alias() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION mv_src2 TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, val INT)")
        .await
        .unwrap();

    // `with_tax` as a column alias should not be mistaken for `WITH` options.
    let result = server
        .exec(
            "CREATE MATERIALIZED VIEW mv_alias ON mv_src2 AS \
             SELECT id, val AS with_tax FROM mv_src2",
        )
        .await;
    assert!(
        result.is_ok(),
        "column alias starting with 'with_' should not break parse: {:?}",
        result
    );
}
