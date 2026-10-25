// SPDX-License-Identifier: BUSL-1.1

//! `FOR VALID_TIME FROM lo TO hi` on a table scan.
//!
//! A scan carries one valid-time instant. A range is refused with a typed
//! error, never widened to every valid time.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_valid_time_range_on_a_scan_is_refused() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION vtr_docs WITH (engine='document_schemaless', bitemporal=true)",
        "INSERT INTO vtr_docs (id, n) VALUES ('a', 1)",
    ] {
        server.exec(sql).await.expect(sql);
    }
    server
        .expect_error(
            "SELECT * FROM vtr_docs FOR VALID_TIME FROM 1 TO 5",
            "FOR VALID_TIME FROM 1 TO 5 is not supported",
        )
        .await;
    // The instant form still answers.
    let rows = server
        .query_text("SELECT id FROM vtr_docs FOR VALID_TIME CONTAINS 4102444800000")
        .await
        .expect("valid-time instant read");
    assert!(rows.iter().any(|row| row.contains('a')), "{rows:?}");
}
