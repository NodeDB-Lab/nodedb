//! Integration coverage for RLS policy predicate parsing.
//!
//! The USING clause predicate must preserve balanced parentheses faithfully.
//! `trim_matches('(' | ')')` strips all leading/trailing parens — not just
//! one matched pair — corrupting nested boolean expressions.

mod common;

use common::pgwire_harness::TestServer;

/// Double-wrapped balanced parens `((x > 0) AND (y = 1))` must be stored
/// correctly and enforced at query time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rls_policy_nested_parens_preserved() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION rls_items TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                x INT, \
                y INT)",
        )
        .await
        .unwrap();

    // Create a policy with nested parens in USING predicate.
    let result = server
        .exec(
            "CREATE RLS POLICY pos_check ON rls_items FOR READ \
             USING ((x > 0) AND (y = 1))",
        )
        .await;
    assert!(
        result.is_ok(),
        "CREATE RLS POLICY with nested parens should succeed: {:?}",
        result
    );

    // Insert rows: one matching, one not.
    server
        .exec("INSERT INTO rls_items (id, x, y) VALUES ('ok', 5, 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rls_items (id, x, y) VALUES ('bad', -1, 1)")
        .await
        .unwrap();

    // Create a non-superuser to test RLS enforcement.
    server
        .exec("CREATE USER rls_reader WITH PASSWORD 'pass' ROLE readonly")
        .await
        .unwrap();

    let (reader, _handle) = server.connect_as("rls_reader", "pass").await.unwrap();
    let result = reader.simple_query("SELECT id FROM rls_items").await;
    match result {
        Ok(msgs) => {
            let mut ids: Vec<String> = Vec::new();
            for msg in &msgs {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    ids.push(row.get(0).unwrap_or("").to_string());
                }
            }
            // The policy should allow `ok` (x=5 > 0, y=1) and reject `bad` (x=-1).
            assert!(
                ids.iter().any(|id| id.contains("ok")),
                "row with x=5,y=1 should be visible under RLS: {ids:?}"
            );
            assert!(
                !ids.iter().any(|id| id.contains("bad")),
                "row with x=-1 should be filtered by RLS: {ids:?}"
            );
        }
        Err(e) => {
            // Extract detailed error if available.
            let detail = if let Some(db_err) = e.as_db_error() {
                format!(
                    "{}: {} (SQLSTATE {})",
                    db_err.severity(),
                    db_err.message(),
                    db_err.code().code()
                )
            } else {
                format!("{e:?}")
            };
            panic!("RLS query should not error — predicate may be corrupted: {detail}");
        }
    }
}

/// Triple-nested parens should not be over-stripped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rls_policy_triple_nested_parens() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION rls_deep TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                a INT, \
                b INT, \
                c INT)",
        )
        .await
        .unwrap();

    let result = server
        .exec(
            "CREATE RLS POLICY deep_check ON rls_deep FOR READ \
             USING (((a > 0) AND (b > 0)) OR (c = 99))",
        )
        .await;
    assert!(
        result.is_ok(),
        "deeply nested RLS predicate should parse without corruption: {:?}",
        result
    );
}
