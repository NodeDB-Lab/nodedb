// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

// ── helpers ────────────────────────────────────────────────────────────────

/// Extract SQLSTATE from an error string produced by the harness.
/// The harness formats errors as: "SEVERITY: message (SQLSTATE XXXXX)"
fn sqlstate(err: &str) -> Option<&str> {
    let start = err.rfind("SQLSTATE ")?;
    let rest = &err[start + 9..];
    let end = rest.find(')').unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Assert: the result is either (a) Ok with at least one row, or
/// (b) Err with an expected SQLSTATE — never silent success with zero rows
/// when a non-empty result is required.
fn assert_ok_or_sqlstate(result: &Result<Vec<String>, String>, allowed_states: &[&str]) {
    match result {
        Ok(rows) => {
            // OK is always acceptable — caller asserts row content separately.
            let _ = rows;
        }
        Err(e) => {
            let state = sqlstate(e).unwrap_or("");
            assert!(
                allowed_states.contains(&state),
                "unexpected error — SQLSTATE {state:?} not in allowed {allowed_states:?}. Full error: {e}"
            );
        }
    }
}

// ── 1. Quoted identifiers ──────────────────────────────────────────────────

#[tokio::test]
async fn quoted_camelcase_column_returns_correct_field() {
    let srv = TestServer::start().await;
    srv.exec(r#"CREATE COLLECTION users (id INTEGER PRIMARY KEY, "userId" TEXT)"#)
        .await
        .expect("create collection");
    srv.exec(r#"INSERT INTO users (id, "userId") VALUES (1, 'alice')"#)
        .await
        .expect("insert");
    let rows = srv
        .query_text(r#"SELECT "userId" FROM users WHERE id = 1"#)
        .await
        .expect("select");
    assert_eq!(rows, vec!["alice"], "camelCase column value mismatch");
}

#[tokio::test]
async fn quoted_identifier_preserves_case() {
    let srv = TestServer::start().await;
    srv.exec(r#"CREATE COLLECTION casetest (id INTEGER PRIMARY KEY, "MyCol" TEXT, mycol TEXT)"#)
        .await
        .expect("create collection");
    srv.exec(r#"INSERT INTO casetest (id, "MyCol", mycol) VALUES (1, 'upper', 'lower')"#)
        .await
        .expect("insert");
    let upper = srv
        .query_text(r#"SELECT "MyCol" FROM casetest WHERE id = 1"#)
        .await
        .expect("select MyCol");
    let lower = srv
        .query_text(r#"SELECT mycol FROM casetest WHERE id = 1"#)
        .await
        .expect("select mycol");
    assert_eq!(upper, vec!["upper"]);
    assert_eq!(lower, vec!["lower"]);
}

#[tokio::test]
async fn mixed_quoted_unquoted_in_same_query() {
    let srv = TestServer::start().await;
    srv.exec(r#"CREATE COLLECTION mixed (id INTEGER PRIMARY KEY, "Name" TEXT, age INTEGER)"#)
        .await
        .expect("create collection");
    srv.exec(r#"INSERT INTO mixed (id, "Name", age) VALUES (1, 'Bob', 30)"#)
        .await
        .expect("insert");
    let rows = srv
        .query_text(r#"SELECT "Name" FROM mixed WHERE age = 30"#)
        .await
        .expect("select");
    assert_eq!(rows, vec!["Bob"]);
}

// ── 2. ILIKE ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn ilike_matches_case_insensitive() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION fruits (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO fruits (id, name) VALUES (1, 'Apple'), (2, 'banana'), (3, 'CHERRY')")
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT name FROM fruits WHERE name ILIKE 'apple'")
        .await;
    match &result {
        Ok(rows) => assert_eq!(rows, &["Apple"], "ILIKE must match case-insensitively"),
        Err(e) => {
            let state = sqlstate(e).unwrap_or("");
            // 42601=syntax error, 0A000=feature not supported, 42883=unknown function
            assert!(
                ["42601", "0A000", "42883"].contains(&state),
                "ILIKE failed with unexpected SQLSTATE {state}: {e}"
            );
        }
    }
}

#[tokio::test]
async fn ilike_pattern_with_underscore_and_percent() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION patterns (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO patterns (id, val) VALUES (1, 'Hello'), (2, 'hXllo'), (3, 'world')")
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT val FROM patterns WHERE val ILIKE 'h_llo'")
        .await;
    match &result {
        Ok(rows) => {
            assert!(
                rows.iter().any(|r| r.to_lowercase() == "hello"),
                "ILIKE h_llo should match Hello, got: {rows:?}"
            );
        }
        Err(e) => {
            let state = sqlstate(e).unwrap_or("");
            assert!(
                ["42601", "0A000", "42883"].contains(&state),
                "unexpected SQLSTATE {state}: {e}"
            );
        }
    }
}

// ── 3. Schema-qualified names ──────────────────────────────────────────────

#[tokio::test]
async fn public_schema_prefix_resolves_or_documented_error() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION schematest (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO schematest (id, val) VALUES (1, 'hi')")
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT val FROM public.schematest WHERE id = 1")
        .await;
    // Either resolves correctly OR returns a typed error (not panic/silent wrong result)
    match &result {
        Ok(rows) => assert_eq!(rows, &["hi"]),
        Err(e) => {
            let state = sqlstate(e).unwrap_or("");
            assert!(
                !state.is_empty(),
                "schema-qualified query must return typed SQLSTATE, got: {e}"
            );
        }
    }
}

#[tokio::test]
async fn unqualified_table_resolves_in_default_schema() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION default_schema_tbl (id INTEGER PRIMARY KEY, x INTEGER)")
        .await
        .expect("create");
    srv.exec("INSERT INTO default_schema_tbl (id, x) VALUES (42, 99)")
        .await
        .expect("insert");
    let rows = srv
        .query_text("SELECT x FROM default_schema_tbl WHERE id = 42")
        .await
        .expect("unqualified select must work");
    assert_eq!(rows, vec!["99"]);
}

// ── 4. Isolation level ─────────────────────────────────────────────────────

#[tokio::test]
async fn set_transaction_accepts_every_isolation_level() {
    let srv = TestServer::start().await;
    for level in [
        "SERIALIZABLE",
        "REPEATABLE READ",
        "READ COMMITTED",
        "READ UNCOMMITTED",
    ] {
        srv.exec(&format!("SET TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .unwrap_or_else(|e| panic!("{level} must be accepted: {e}"));
    }
}

// ── 5. JSON operators ──────────────────────────────────────────────────────

#[tokio::test]
async fn json_arrow_extracts_value() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION jsondocs (id INTEGER PRIMARY KEY, doc DOCUMENT)")
        .await
        .expect("create");
    srv.exec(r#"INSERT INTO jsondocs (id, doc) VALUES (1, '{"key": "hello"}')"#)
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT doc -> 'key' FROM jsondocs WHERE id = 1")
        .await;
    assert_ok_or_sqlstate(&result, &["42883", "42601", "0A000", "42703"]);
    if let Ok(rows) = &result {
        assert!(
            !rows.is_empty() && (rows[0].contains("hello") || rows[0].contains('"')),
            "json -> operator should return the value, got: {rows:?}"
        );
    }
}

#[tokio::test]
async fn json_double_arrow_extracts_text() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION jsondocs2 (id INTEGER PRIMARY KEY, doc DOCUMENT)")
        .await
        .expect("create");
    srv.exec(r#"INSERT INTO jsondocs2 (id, doc) VALUES (1, '{"name": "world"}')"#)
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT doc ->> 'name' FROM jsondocs2 WHERE id = 1")
        .await;
    assert_ok_or_sqlstate(&result, &["42883", "42601", "0A000", "42703"]);
    if let Ok(rows) = &result {
        assert!(
            !rows.is_empty() && rows[0] == "world",
            "json ->> operator should return text without quotes, got: {rows:?}"
        );
    }
}

#[tokio::test]
async fn json_contains_operator() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION jsondocs3 (id INTEGER PRIMARY KEY, doc DOCUMENT)")
        .await
        .expect("create");
    srv.exec(r#"INSERT INTO jsondocs3 (id, doc) VALUES (1, '{"a": 1, "b": 2}')"#)
        .await
        .expect("insert");
    let result = srv
        .query_text(r#"SELECT id FROM jsondocs3 WHERE doc @> '{"a": 1}'"#)
        .await;
    assert_ok_or_sqlstate(&result, &["42883", "42601", "0A000", "42703", "42804"]);
    if let Ok(rows) = &result {
        assert_eq!(rows, &["1"], "json @> should match containing document");
    }
}

#[tokio::test]
async fn json_path_existence() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION jsondocs4 (id INTEGER PRIMARY KEY, doc DOCUMENT)")
        .await
        .expect("create");
    srv.exec(r#"INSERT INTO jsondocs4 (id, doc) VALUES (1, '{"present": true}')"#)
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT id FROM jsondocs4 WHERE doc ? 'present'")
        .await;
    assert_ok_or_sqlstate(&result, &["42883", "42601", "0A000", "42703", "42804"]);
    if let Ok(rows) = &result {
        assert_eq!(rows, &["1"], "json ? key should find existing key");
    }
}

// ── 6. FTS Postgres compat ─────────────────────────────────────────────────

#[tokio::test]
async fn fts_double_at_operator_or_documented_rejection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ftsdocs (id INTEGER PRIMARY KEY, content TEXT)")
        .await
        .expect("create");
    srv.exec("INSERT INTO ftsdocs (id, content) VALUES (1, 'the quick brown fox')")
        .await
        .expect("insert");
    let result = srv
        .query_text("SELECT id FROM ftsdocs WHERE content @@ to_tsquery('quick')")
        .await;
    assert_ok_or_sqlstate(&result, &["42883", "42601", "0A000", "42703", "42804"]);
    if let Ok(rows) = &result {
        assert_eq!(rows, &["1"], "@@ operator should match FTS document");
    }
}

#[tokio::test]
async fn to_tsvector_function_or_documented_rejection() {
    let srv = TestServer::start().await;
    let result = srv
        .query_text("SELECT to_tsvector('english', 'hello world')")
        .await;
    assert_ok_or_sqlstate(&result, &["42883", "42601", "0A000"]);
    if let Ok(rows) = &result {
        assert!(
            !rows.is_empty(),
            "to_tsvector must return a result if accepted"
        );
    }
}

// ── 7. NUMERIC precision ───────────────────────────────────────────────────

#[tokio::test]
async fn numeric_large_value_round_trips_exact_string() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION numtest (id INTEGER PRIMARY KEY, val NUMERIC(20,2))")
        .await
        .expect("create");
    srv.exec("INSERT INTO numtest (id, val) VALUES (1, 999999999999999999.99)")
        .await
        .expect("insert");
    let rows = srv
        .query_text("SELECT val FROM numtest WHERE id = 1")
        .await
        .expect("select");
    assert_eq!(
        rows,
        vec!["999999999999999999.99"],
        "NUMERIC large value must round-trip exactly"
    );
}

#[tokio::test]
async fn numeric_negative_value_round_trips() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION numtest_neg (id INTEGER PRIMARY KEY, val NUMERIC(10,3))")
        .await
        .expect("create");
    srv.exec("INSERT INTO numtest_neg (id, val) VALUES (1, -12345.678)")
        .await
        .expect("insert");
    let rows = srv
        .query_text("SELECT val FROM numtest_neg WHERE id = 1")
        .await
        .expect("select");
    assert_eq!(rows, vec!["-12345.678"], "NUMERIC negative must round-trip");
}

#[tokio::test]
async fn numeric_arithmetic_no_float_drift() {
    let srv = TestServer::start().await;
    // 0.1 + 0.2 = 0.3 exactly in NUMERIC, unlike float
    let result = srv
        .query_text("SELECT CAST(0.1 AS NUMERIC(5,1)) + CAST(0.2 AS NUMERIC(5,1))")
        .await;
    match &result {
        Ok(rows) => {
            assert!(!rows.is_empty(), "NUMERIC arithmetic must return a result");
            let v = &rows[0];
            // Must not contain float noise like 0.30000000000000004
            assert!(
                v == "0.3" || v == "0.30",
                "NUMERIC 0.1+0.2 must equal 0.3 exactly, got: {v}"
            );
        }
        Err(e) => {
            let state = sqlstate(e).unwrap_or("");
            assert!(
                ["42883", "42601", "0A000"].contains(&state),
                "unexpected error: {state}: {e}"
            );
        }
    }
}

// ── 8. TIMESTAMPTZ ────────────────────────────────────────────────────────

#[tokio::test]
async fn current_timestamp_returns_a_value() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT current_timestamp")
        .await
        .expect("current_timestamp must succeed");
    assert!(!rows.is_empty(), "current_timestamp returned no rows");
    assert!(
        !rows[0].is_empty(),
        "current_timestamp returned empty string"
    );
}

#[tokio::test]
async fn now_returns_a_nonempty_timestamp() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT now()")
        .await
        .expect("now() must succeed");
    assert!(
        !rows.is_empty() && !rows[0].is_empty(),
        "now() returned empty"
    );
}

#[tokio::test]
async fn timestamptz_round_trip_preserves_value() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION tstest (id INTEGER PRIMARY KEY, ts TIMESTAMPTZ)")
        .await
        .expect("create");
    // Insert a fixed timestamptz with explicit offset
    srv.exec("INSERT INTO tstest (id, ts) VALUES (1, '2024-06-15 12:00:00+05:30')")
        .await
        .expect("insert");
    let rows = srv
        .query_text("SELECT ts FROM tstest WHERE id = 1")
        .await
        .expect("select");
    assert!(!rows.is_empty(), "timestamptz select returned no rows");
    // Value must contain some timestamp data — not empty, not NULL
    assert!(
        !rows[0].is_empty(),
        "timestamptz round-trip returned empty value"
    );
    // Must contain date component
    assert!(
        rows[0].contains("2024"),
        "timestamptz must preserve year, got: {}",
        rows[0]
    );
}
