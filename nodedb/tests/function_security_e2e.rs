//! End-to-end tests for function security: CREATE/DROP lifecycle,
//! DML rejection in function bodies, procedural UDF compilation.

mod common;

use common::pgwire_harness::TestServer;

/// CREATE FUNCTION succeeds and DROP FUNCTION removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_drop_function() {
    let server = TestServer::start().await;

    // Create a simple expression UDF.
    let result = server
        .exec("CREATE FUNCTION double_it(x INT) RETURNS INT AS SELECT x * 2")
        .await;
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    // DROP FUNCTION succeeds (proves it was stored in catalog).
    let drop_result = server.exec("DROP FUNCTION double_it").await;
    assert!(
        drop_result.is_ok(),
        "DROP FUNCTION failed: {:?}",
        drop_result
    );

    // DROP again should fail (already dropped).
    server
        .expect_error("DROP FUNCTION double_it", "does not exist")
        .await;
}

/// CREATE OR REPLACE FUNCTION works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_or_replace_function() {
    let server = TestServer::start().await;

    server
        .exec("CREATE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1")
        .await
        .unwrap();

    // Replace with new body.
    let result = server
        .exec("CREATE OR REPLACE FUNCTION f(x INT) RETURNS INT AS SELECT x + 2")
        .await;
    assert!(result.is_ok(), "CREATE OR REPLACE failed: {:?}", result);

    server.exec("DROP FUNCTION f").await.unwrap();
}

/// DML in function body is rejected at CREATE time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reject_dml_in_function_body() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "CREATE FUNCTION bad_func(x INT) RETURNS INT AS \
             BEGIN INSERT INTO t (id) VALUES (x); RETURN x; END",
            "DML",
        )
        .await;
}

/// Procedural UDF with IF/ELSE compiles successfully.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_procedural_udf() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "CREATE FUNCTION classify(score INT) RETURNS TEXT AS \
             BEGIN \
               IF score > 90 THEN RETURN 'excellent'; \
               ELSIF score > 70 THEN RETURN 'good'; \
               ELSE RETURN 'needs improvement'; \
               END IF; \
             END",
        )
        .await;
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    server.exec("DROP FUNCTION classify").await.unwrap();
}
