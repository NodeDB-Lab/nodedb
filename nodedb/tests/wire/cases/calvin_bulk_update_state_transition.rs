// SPDX-License-Identifier: BUSL-1.1

//! A bulk update the collection's transition rules forbid is refused while
//! Calvin stages it, not at COMMIT.
//!
//! A collection an edge was written into takes the dependent recon route for
//! its predicate writes, and that route recomputes each row against the body
//! the current store holds. Another session may have moved that body since
//! the statement planned, so the pair Calvin stages is not always the pair
//! the statement judged. The rules run again on the staged pair.
//!
//! A refusal at COMMIT halts the scheduler, so this case asserts an error
//! answer and then reads the row to prove the server still serves.

use crate::harness::TestServer;

/// A collection carrying the implicit-edge flag, one row `r` whose `status`
/// is `active`, and a rule that admits only `draft -> active`. The reverse is
/// therefore forbidden.
async fn fixture(collection: &str) -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, status TEXT)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM 'a' TO 'b' TYPE 'LINK'"
        ))
        .await
        .unwrap_or_else(|e| panic!("mark {collection} edge-bearing: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, status) VALUES ('r', 'active')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
    server
        .exec(&format!(
            "ALTER COLLECTION {collection} ADD CONSTRAINT forward_only \
             ON COLUMN status TRANSITIONS ('draft' -> 'active')"
        ))
        .await
        .unwrap_or_else(|e| panic!("declare the rule on {collection}: {e}"));
    server
}

/// The `status` of row `r`.
async fn status_of(server: &TestServer, collection: &str) -> String {
    let values = server
        .query_text(&format!("SELECT status FROM {collection} WHERE id = 'r'"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}.r: {e}"));
    let [value] = values.as_slice() else {
        panic!("{collection}.r must be one row, read {values:?}");
    };
    value.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forbidden_transition_is_refused_and_the_row_is_unchanged() {
    let server = fixture("cbt_refused").await;

    let (status, body) = server
        .http_query("UPDATE cbt_refused SET status = 'draft' WHERE status = 'active'")
        .await;

    assert!(
        !(200..300).contains(&status),
        "the rule forbids active -> draft, answered {status}: {body}"
    );
    assert_eq!(
        status_of(&server, "cbt_refused").await,
        "active",
        "a refused transition leaves the row: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_serves_a_read_after_a_refused_transition() {
    let server = fixture("cbt_serves").await;

    let (status, body) = server
        .http_query("UPDATE cbt_serves SET status = 'draft' WHERE status = 'active'")
        .await;
    assert!(
        !(200..300).contains(&status),
        "the rule forbids active -> draft, answered {status}: {body}"
    );

    // A scheduler halted by a refused redo install stops answering; a refusal
    // taken while staging leaves the node serving.
    assert_eq!(
        status_of(&server, "cbt_serves").await,
        "active",
        "the node must still answer a read: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admitted_transition_applies() {
    let server = fixture("cbt_admitted").await;
    server
        .exec("INSERT INTO cbt_admitted (id, status) VALUES ('d', 'draft')")
        .await
        .unwrap();
    server
        .exec("UPDATE cbt_admitted SET status = 'active' WHERE status = 'draft'")
        .await
        .unwrap_or_else(|e| panic!("draft -> active is admitted: {e}"));

    let values = server
        .query_text("SELECT status FROM cbt_admitted WHERE id = 'd'")
        .await
        .unwrap();
    assert_eq!(values, ["active"], "the admitted transition must apply");
}
