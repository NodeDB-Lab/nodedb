//! End-to-end tests for stored procedure execution: CREATE/DROP/CALL lifecycle,
//! exception handling, fuel metering via live pgwire server.

mod common;

use common::pgwire_harness::TestServer;

/// CREATE PROCEDURE and CALL succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_call_procedure() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "CREATE PROCEDURE noop() AS \
             BEGIN \
               DECLARE x INT := 0; \
             END",
        )
        .await;
    assert!(result.is_ok(), "CREATE PROCEDURE failed: {:?}", result);

    // CALL the procedure.
    let call_result = server.exec("CALL noop()").await;
    assert!(call_result.is_ok(), "CALL failed: {:?}", call_result);

    // DROP PROCEDURE removes it.
    server.exec("DROP PROCEDURE noop").await.unwrap();

    // DROP again should fail.
    server
        .expect_error("DROP PROCEDURE noop", "does not exist")
        .await;
}

/// Procedure with RAISE EXCEPTION aborts execution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_raise_exception() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE PROCEDURE fail_on_purpose() AS \
             BEGIN \
               RAISE EXCEPTION 'intentional failure'; \
             END",
        )
        .await
        .unwrap();

    server
        .expect_error("CALL fail_on_purpose()", "intentional failure")
        .await;

    server.exec("DROP PROCEDURE fail_on_purpose").await.unwrap();
}

/// Procedure with exception handler catches errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_exception_handler() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE PROCEDURE safe_proc() AS \
             BEGIN \
               RAISE EXCEPTION 'inner error'; \
             EXCEPTION \
               WHEN OTHERS THEN \
                 DECLARE x INT := 0; \
             END",
        )
        .await
        .unwrap();

    // Should NOT raise — exception handler catches it.
    let result = server.exec("CALL safe_proc()").await;
    assert!(
        result.is_ok(),
        "exception handler did not catch: {:?}",
        result
    );

    server.exec("DROP PROCEDURE safe_proc").await.unwrap();
}

/// Procedure with WITH (MAX_ITERATIONS) fuel limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_fuel_metering() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE PROCEDURE bounded_loop() WITH (MAX_ITERATIONS = 10) AS \
             BEGIN \
               LOOP \
                 BREAK; \
               END LOOP; \
             END",
        )
        .await
        .unwrap();

    let result = server.exec("CALL bounded_loop()").await;
    assert!(result.is_ok(), "fuel-limited proc failed: {:?}", result);

    server.exec("DROP PROCEDURE bounded_loop").await.unwrap();
}

/// CREATE OR REPLACE PROCEDURE works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_or_replace_procedure() {
    let server = TestServer::start().await;

    server
        .exec("CREATE PROCEDURE p() AS BEGIN DECLARE x INT := 0; END")
        .await
        .unwrap();

    let result = server
        .exec("CREATE OR REPLACE PROCEDURE p() AS BEGIN DECLARE x INT := 0; END")
        .await;
    assert!(result.is_ok(), "CREATE OR REPLACE failed: {:?}", result);

    server.exec("DROP PROCEDURE p").await.unwrap();
}

/// Procedure with SAVEPOINT syntax parses and executes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_savepoint_syntax() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "CREATE PROCEDURE sp_test() AS \
             BEGIN \
               SAVEPOINT sp1; \
               ROLLBACK TO sp1; \
               COMMIT; \
             END",
        )
        .await;
    assert!(
        result.is_ok(),
        "procedure with SAVEPOINT failed to create: {:?}",
        result
    );

    let call = server.exec("CALL sp_test()").await;
    assert!(call.is_ok(), "CALL sp_test failed: {:?}", call);

    server.exec("DROP PROCEDURE sp_test").await.unwrap();
}
