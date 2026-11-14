// SPDX-License-Identifier: BUSL-1.1

//! A request that inserts a new key and then rewrites it.
//!
//! The request plans as one task set. The later write names a key no insert has
//! bound yet, so it reads no stored row and settles nothing. The insert and the
//! rewrite both apply, and the balance falls behind the source sum. The request
//! is refused whole.

use nodedb::control::planner::calvin::settling_writes::SAME_ROW_WRITES_REFUSAL;
use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

const INSERT_THEN_UPDATE: &str = "INSERT INTO ptv_post (id, account_id, amount) \
     VALUES ('u1', 'acc', '1'); UPDATE ptv_post SET amount = '2' WHERE id = 'u1'";

const INSERT_THEN_DELETE: &str = "INSERT INTO ptv_post (id, account_id, amount) \
     VALUES ('u1', 'acc', '1'); DELETE FROM ptv_post WHERE id = 'u1'";

/// The sum of the `amount` column of the postings of `acc`.
async fn posted_sum(server: &TestServer) -> i64 {
    let rows = server
        .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc'")
        .await
        .unwrap_or_else(|e| panic!("read the postings: {e}"));
    rows.iter()
        .map(|amount| {
            amount
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("amount {amount:?}: {e}"))
        })
        .sum()
}

async fn refused_over_http(sql: &str) {
    let server = sum_fixture().await;
    let (status, body) = server.http_query(sql).await;

    assert_eq!(status, 501, "a refused request is a 501: {body}");
    assert!(
        body.contains(SAME_ROW_WRITES_REFUSAL),
        "refusal text: {body}"
    );
    assert_eq!(posted_sum(&server).await, 15);
    assert_eq!(balance(&server, "acc").await, "15");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_then_an_update_of_the_same_new_key_is_refused_over_http() {
    refused_over_http(INSERT_THEN_UPDATE).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_then_a_delete_of_the_same_new_key_is_refused_over_http() {
    refused_over_http(INSERT_THEN_DELETE).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_then_an_update_of_the_same_new_key_is_refused_over_native_sql() {
    let server = sum_fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;

    let response = send_sql(&mut stream, 2, INSERT_THEN_UPDATE).await;

    let error = response.error.expect("the refusal");
    assert_eq!(error.code, "0A000", "feature not supported");
    assert!(
        error.message.contains(SAME_ROW_WRITES_REFUSAL),
        "refusal text: {}",
        error.message
    );
    assert_eq!(posted_sum(&server).await, 15);
    assert_eq!(balance(&server, "acc").await, "15");
}

/// An update of a key that no write of the request inserts rewrites no row
/// and owes nothing, so the request applies and the balance equals the source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_beside_an_update_of_another_key_applies() {
    let server = sum_fixture().await;
    let (status, body) = server
        .http_query(
            "INSERT INTO ptv_post (id, account_id, amount) VALUES ('u1', 'acc', '1'); \
             UPDATE ptv_post SET amount = '2' WHERE id = 'u2'",
        )
        .await;

    assert_eq!(status, 200, "the request applies: {body}");
    assert_eq!(posted_sum(&server).await, 16);
    assert_eq!(balance(&server, "acc").await, "16");
}
