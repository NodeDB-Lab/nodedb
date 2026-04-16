//! Integration coverage for procedure/trigger body cache correctness.
//!
//! The ProcedureBlockCache keys on a 64-bit hash of the body SQL without
//! storing the source for equality verification. A hash collision returns
//! the wrong compiled block. This test verifies that two different trigger
//! bodies always execute their own logic, not each other's.

mod common;

use std::time::Duration;

use common::pgwire_harness::TestServer;

/// Two triggers with different bodies on different collections must each
/// execute their own body, never the other's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_trigger_bodies_execute_independently() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION cache_a TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                val TEXT DEFAULT 'none')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION cache_b TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                val TEXT DEFAULT 'none')",
        )
        .await
        .unwrap();
    // Audit log to capture trigger side effects.
    server.exec("CREATE COLLECTION trigger_log").await.unwrap();

    // Trigger A: logs 'trigger_a_fired'.
    server
        .exec(
            "CREATE TRIGGER trg_a AFTER INSERT ON cache_a FOR EACH ROW \
             BEGIN \
               INSERT INTO trigger_log { id: 'log_a', source: 'trigger_a_fired' }; \
             END",
        )
        .await
        .unwrap();

    // Trigger B: logs 'trigger_b_fired' — different body.
    server
        .exec(
            "CREATE TRIGGER trg_b AFTER INSERT ON cache_b FOR EACH ROW \
             BEGIN \
               INSERT INTO trigger_log { id: 'log_b', source: 'trigger_b_fired' }; \
             END",
        )
        .await
        .unwrap();

    // Fire trigger A.
    server
        .exec("INSERT INTO cache_a (id) VALUES ('a1')")
        .await
        .unwrap();
    // Wait for async AFTER trigger dispatch.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Fire trigger B.
    server
        .exec("INSERT INTO cache_b (id) VALUES ('b1')")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify trigger A logged 'trigger_a_fired', not 'trigger_b_fired'.
    let log_a = server
        .query_text("SELECT * FROM trigger_log WHERE id = 'log_a'")
        .await
        .unwrap();
    assert_eq!(log_a.len(), 1, "trigger A should have logged: {log_a:?}");
    assert!(
        log_a[0].contains("trigger_a_fired"),
        "trigger A should execute its own body, not B's: {:?}",
        log_a[0]
    );
    // Regression guard: if cache collision occurred, A's log entry would
    // contain B's message.
    assert!(
        !log_a[0].contains("trigger_b_fired"),
        "trigger A must not execute trigger B's body (cache collision): {:?}",
        log_a[0]
    );

    // Verify trigger B logged its own message.
    let log_b = server
        .query_text("SELECT * FROM trigger_log WHERE id = 'log_b'")
        .await
        .unwrap();
    assert_eq!(log_b.len(), 1, "trigger B should have logged: {log_b:?}");
    assert!(
        log_b[0].contains("trigger_b_fired"),
        "trigger B should execute its own body: {:?}",
        log_b[0]
    );
}
