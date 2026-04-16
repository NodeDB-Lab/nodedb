//! Integration coverage for trigger execution fuel / budget limits.
//!
//! Trigger bodies run user-supplied code. An infinite loop must not pin a
//! Control Plane worker for the full 3600-second deadline. The execution
//! budget must cap iterations and wall-clock time to a reasonable bound.

mod common;

use std::time::{Duration, Instant};

use common::pgwire_harness::TestServer;

/// An infinite-loop trigger body must be terminated by the execution budget
/// within a reasonable time (< 30 seconds), not the 1-hour wall clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn infinite_loop_trigger_terminates_within_budget() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION fuel_test TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                v INT)",
        )
        .await
        .unwrap();

    // Create a trigger with an infinite loop body.
    server
        .exec(
            "CREATE TRIGGER fuel_loop AFTER INSERT ON fuel_test FOR EACH ROW \
             BEGIN LOOP END LOOP; END",
        )
        .await
        .unwrap();

    let start = Instant::now();
    let result = server
        .exec("INSERT INTO fuel_test (id, v) VALUES ('a', 1)")
        .await;
    let elapsed = start.elapsed();

    // The trigger should either:
    // (a) error because the budget was exhausted, or
    // (b) succeed but complete within a sane time bound.
    // It must NOT hang for 3600 seconds.
    assert!(
        elapsed < Duration::from_secs(30),
        "infinite-loop trigger took {elapsed:?} — budget should cap execution well under 1 hour"
    );

    if let Err(msg) = result {
        assert!(
            msg.to_lowercase().contains("fuel")
                || msg.to_lowercase().contains("budget")
                || msg.to_lowercase().contains("timeout")
                || msg.to_lowercase().contains("iteration")
                || msg.to_lowercase().contains("limit"),
            "error should mention budget/fuel/timeout: {msg}"
        );
    }
}

/// A trigger with a bounded but expensive loop should succeed if within budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_loop_trigger_completes_normally() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION fuel_ok TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                counter INT DEFAULT 0)",
        )
        .await
        .unwrap();

    // A trigger that loops 10 times — well within any reasonable budget.
    server
        .exec(
            "CREATE TRIGGER fuel_bounded AFTER INSERT ON fuel_ok FOR EACH ROW \
             BEGIN \
               DECLARE i INT := 0; \
               WHILE i < 10 LOOP \
                 i := i + 1; \
               END LOOP; \
             END",
        )
        .await
        .unwrap();

    let result = server.exec("INSERT INTO fuel_ok (id) VALUES ('b1')").await;
    assert!(
        result.is_ok(),
        "bounded loop trigger should complete normally: {:?}",
        result
    );
}
