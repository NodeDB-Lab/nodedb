//! Integration tests for PREPARE / EXECUTE / DEALLOCATE (SQL-level).

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepare_execute_deallocate_lifecycle() {
    let server = TestServer::start().await;

    // PREPARE + DEALLOCATE lifecycle (no EXECUTE — avoids DataFusion plan converter limitations).
    server.exec("PREPARE q AS SELECT 1").await.unwrap();

    // DEALLOCATE removes the prepared statement.
    server.exec("DEALLOCATE q").await.unwrap();

    // EXECUTE after DEALLOCATE should fail.
    server.expect_error("EXECUTE q", "does not exist").await;

    // DEALLOCATE ALL removes all prepared statements.
    server.exec("PREPARE q1 AS SELECT 1").await.unwrap();
    server.exec("PREPARE q2 AS SELECT 2").await.unwrap();
    server.exec("DEALLOCATE ALL").await.unwrap();
    server.expect_error("EXECUTE q1", "does not exist").await;
}
