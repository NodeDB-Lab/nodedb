//! Integration coverage for UPDATE statements whose RHS is a non-literal
//! expression (column arithmetic, scalar functions, `NOW()`, ...).
//!
//! These must be evaluated against the current row by the executor — not
//! serialized as `format!("{expr:?}")` and written back as a string. That
//! failure mode errors loudly on strict collections (re-encoder rejects
//! the debug string) and silently corrupts schemaless collections.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_column_increment_strict() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION counters TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, n INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO counters (id, n) VALUES ('a', 1)")
        .await
        .unwrap();

    server
        .exec("UPDATE counters SET n = n + 1 WHERE id = 'a'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM counters WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"n\":2"),
        "expected n=2, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_column_decrement_strict() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION inventory TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, stock INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO inventory (id, stock) VALUES ('item1', 10)")
        .await
        .unwrap();

    server
        .exec("UPDATE inventory SET stock = stock - 1 WHERE id = 'item1'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT stock FROM inventory WHERE id = 'item1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"stock\":9"),
        "expected stock=9, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_column_increment_schemaless() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION counters").await.unwrap();
    server
        .exec("INSERT INTO counters (id, n) VALUES ('a', 1)")
        .await
        .unwrap();

    server
        .exec("UPDATE counters SET n = n + 1 WHERE id = 'a'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM counters WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // Regression guard: the previous failure mode wrote a `format!("{expr:?}")`
    // debug string ("BinaryOp { left: Column { .. }, ... }") into the column
    // instead of evaluating the expression.
    assert!(
        !rows[0].contains("BinaryOp") && !rows[0].contains("Literal"),
        "schemaless UPDATE stored stringified AST instead of evaluating: {:?}",
        rows[0]
    );
    assert!(
        rows[0].contains("\"n\":2"),
        "expected n=2, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_now_function_rhs_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION rows TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY, \
                updated_at TIMESTAMP)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO rows (id, updated_at) VALUES ('r1', '2020-01-01T00:00:00Z')")
        .await
        .unwrap();

    server
        .exec("UPDATE rows SET updated_at = NOW() WHERE id = 'r1'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT updated_at FROM rows WHERE id = 'r1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        !rows[0].contains("Function") && !rows[0].contains("Identifier"),
        "updated_at stored stringified AST: {:?}",
        rows[0]
    );
    assert_ne!(rows[0], "2020-01-01T00:00:00Z", "NOW() was not evaluated");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_upper_function_rhs_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION users TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY, \
                name STRING NOT NULL)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO users (id, name) VALUES ('u1', 'alice')")
        .await
        .unwrap();

    server
        .exec("UPDATE users SET name = UPPER(name) WHERE id = 'u1'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT name FROM users WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"name\":\"ALICE\""),
        "expected name=ALICE, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_string_concat_rhs_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION users TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY, \
                first STRING NOT NULL, \
                last STRING NOT NULL, \
                full STRING)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO users (id, first, last) VALUES ('u1', 'Ada', 'Lovelace')")
        .await
        .unwrap();

    server
        .exec("UPDATE users SET full = first || ' ' || last WHERE id = 'u1'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT full FROM users WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"full\":\"Ada Lovelace\""),
        "expected full='Ada Lovelace', got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_case_expression_rhs_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION players TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY, \
                score INT NOT NULL, \
                tier STRING)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO players (id, score) VALUES ('p1', 95)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO players (id, score) VALUES ('p2', 60)")
        .await
        .unwrap();

    server
        .exec("UPDATE players SET tier = CASE WHEN score > 90 THEN 'gold' ELSE 'silver' END")
        .await
        .unwrap();

    let gold = server
        .query_text("SELECT id FROM players WHERE tier = 'gold'")
        .await
        .unwrap();
    assert_eq!(gold.len(), 1);
    assert!(gold[0].contains("p1"));

    let silver = server
        .query_text("SELECT id FROM players WHERE tier = 'silver'")
        .await
        .unwrap();
    assert_eq!(silver.len(), 1);
    assert!(silver[0].contains("p2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_do_update_with_column_arithmetic() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION counters TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY, \
                n INT NOT NULL)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO counters (id, n) VALUES ('a', 5)")
        .await
        .unwrap();

    // ON CONFLICT DO UPDATE with column arithmetic on the existing row.
    // Hits the same expression-evaluator path as plain UPDATE.
    server
        .exec(
            "INSERT INTO counters (id, n) VALUES ('a', 999) \
             ON CONFLICT (id) DO UPDATE SET n = n + 10",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM counters WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"n\":15"),
        "expected n=15 (5+10 via UPSERT DO UPDATE), got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_scalar_function_rhs_strict() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION scores TYPE DOCUMENT STRICT (\
                id STRING PRIMARY KEY, \
                confidence DOUBLE)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO scores (id, confidence) VALUES ('s1', 0.8)")
        .await
        .unwrap();

    server
        .exec("UPDATE scores SET confidence = LEAST(confidence + 0.05, 1.0) WHERE id = 's1'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT confidence FROM scores WHERE id = 's1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"confidence\":0.85"),
        "expected confidence=0.85, got {:?}",
        rows[0]
    );
}
