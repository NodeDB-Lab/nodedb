// SPDX-License-Identifier: BUSL-1.1

//! A `LABEL` list needs a comma between its items on every graph walk.

use crate::harness::TestServer;

async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    for (dst, label) in [("p", "a"), ("q", "b"), ("r", "c")] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{collection}' FROM 'x' TO '{dst}' TYPE '{label}'"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_label_list_with_commas_keeps_both_labels() {
    let server = TestServer::start().await;
    seed(&server, "lls_ok").await;
    let rows = server
        .query_text("GRAPH NEIGHBORS IN 'lls_ok' OF 'x' LABEL 'a', 'b' DIRECTION out")
        .await
        .unwrap();
    let blob = rows.join("");
    assert!(blob.contains("\"p\"") && blob.contains("\"q\"") && !blob.contains("\"r\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_label_list_without_a_comma_is_refused_on_the_wire() {
    let server = TestServer::start().await;
    seed(&server, "lls_bad").await;
    for sql in [
        "GRAPH NEIGHBORS IN 'lls_bad' OF 'x' LABEL 'a' 'b' DIRECTION out",
        "GRAPH TRAVERSE IN 'lls_bad' FROM 'x' DEPTH 1 LABEL 'a' 'b'",
    ] {
        let result = server.query_text(sql).await;
        assert!(
            result.is_err(),
            "`{sql}` must be refused, got rows {result:?}"
        );
    }
}
