// SPDX-License-Identifier: BUSL-1.1

//! Several writes of one materialized-sum source in one request.
//!
//! The request plans as one task set, and each write settles its balance delta
//! before any write stages, so a later write cannot see an earlier one. A
//! request whose writes settle one balance from the same rows is refused whole.

use nodedb::control::planner::calvin::settling_writes::{
    SAME_ROW_WRITES_REFUSAL, SEVERAL_WRITES_REFUSAL,
};
use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::protocol::opcodes::ResponseStatus;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

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

/// `sql` is valid syntax the engine does not support: HTTP renders that `0A000`
/// as status 501. It answers `refusal` and changes nothing.
async fn refused_whole(server: &TestServer, sql: &str, refusal: &str) {
    let (status, body) = server.http_query(sql).await;

    assert_eq!(status, 501, "a refused request is a 501: {body}");
    assert!(body.contains(refusal), "refusal text: {body}");
    assert_eq!(posted_sum(server).await, 15);
    assert_eq!(balance(server, "acc").await, "15");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_settling_updates_in_one_request_are_refused_and_change_nothing() {
    let server = sum_fixture().await;
    refused_whole(
        &server,
        "UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'; \
         UPDATE ptv_post SET amount = '50' WHERE id = 'e0'",
        SEVERAL_WRITES_REFUSAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_predicate_updates_of_one_balance_are_refused_and_change_nothing() {
    let server = sum_fixture().await;
    refused_whole(
        &server,
        "UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'; \
         UPDATE ptv_post SET amount = '50' WHERE account_id = 'acc'",
        SEVERAL_WRITES_REFUSAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_beside_a_write_of_one_balance_is_refused_and_changes_nothing() {
    let server = sum_fixture().await;
    refused_whole(
        &server,
        "TRUNCATE ptv_post; UPDATE ptv_post SET amount = '50' WHERE id = 'e0'",
        SEVERAL_WRITES_REFUSAL,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_point_writes_of_one_row_are_refused_and_change_nothing() {
    let server = sum_fixture().await;
    refused_whole(
        &server,
        "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'; \
         UPDATE ptv_post SET amount = '2' WHERE id = 'e0'",
        SAME_ROW_WRITES_REFUSAL,
    )
    .await;
}

/// One `INSERT ... ON CONFLICT DO UPDATE` that names row `e0` twice. It lowers to
/// one `BatchUpsert` page, and each entry settles from the same stored image.
const REPEATED_ROW_UPSERT: &str = "INSERT INTO ptv_post (id, account_id, amount) \
     VALUES ('e0', 'acc', '1'), ('e0', 'acc', '2') \
     ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_upsert_naming_a_row_twice_is_refused_over_http() {
    let server = sum_fixture().await;
    refused_whole(&server, REPEATED_ROW_UPSERT, SAME_ROW_WRITES_REFUSAL).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_upsert_naming_a_row_twice_is_refused_over_pgwire() {
    let server = sum_fixture().await;

    let error = server
        .client
        .simple_query(REPEATED_ROW_UPSERT)
        .await
        .expect_err("the statement must be refused");

    assert_eq!(
        error.code().map(|code| code.code()),
        Some("0A000"),
        "feature not supported: {error}"
    );
    assert_eq!(posted_sum(&server).await, 15);
    assert_eq!(balance(&server, "acc").await, "15");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_upsert_naming_a_row_twice_is_refused_over_native_sql() {
    let server = sum_fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;

    let response = send_sql(&mut stream, 2, REPEATED_ROW_UPSERT).await;

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

/// An upsert of two different rows settles each from its own image.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_upsert_of_two_rows_keeps_the_balance_equal_to_the_source_sum() {
    let server = sum_fixture().await;

    server
        .exec(
            "INSERT INTO ptv_post (id, account_id, amount) \
             VALUES ('e0', 'acc', '1'), ('e1', 'acc', '2') \
             ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount",
        )
        .await
        .expect("the upsert of two rows");

    assert_eq!(posted_sum(&server).await, 3);
    assert_eq!(balance(&server, "acc").await, "3");
}

/// Each write folds its own row's image, so the pair stays exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_point_writes_of_different_rows_both_apply() {
    let server = sum_fixture().await;

    server
        .http_exec(
            "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'; \
             UPDATE ptv_post SET amount = '2' WHERE id = 'e1'",
        )
        .await;

    assert_eq!(posted_sum(&server).await, 3);
    assert_eq!(balance(&server, "acc").await, "3");
}

/// A native SQL request plans as one task set inside a block too, so its writes
/// settle before either stages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_predicate_updates_in_one_native_request_inside_a_block_are_refused() {
    let server = sum_fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;

    let begin = send_sql(&mut stream, 2, "BEGIN").await;
    assert_ne!(begin.status, ResponseStatus::Error, "BEGIN");

    let response = send_sql(
        &mut stream,
        3,
        "UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'; \
         UPDATE ptv_post SET amount = '50' WHERE account_id = 'acc'",
    )
    .await;
    assert_eq!(response.status, ResponseStatus::Error);
    let error = response.error.expect("the refusal");
    assert_eq!(error.code, "0A000", "feature not supported");
    assert!(
        error.message.contains(SEVERAL_WRITES_REFUSAL),
        "refusal text: {}",
        error.message
    );

    let rollback = send_sql(&mut stream, 4, "ROLLBACK").await;
    assert_ne!(rollback.status, ResponseStatus::Error, "ROLLBACK");
    assert_eq!(posted_sum(&server).await, 15);
    assert_eq!(balance(&server, "acc").await, "15");
}
