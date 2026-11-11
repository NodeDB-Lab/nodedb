// SPDX-License-Identifier: BUSL-1.1

//! A predicate `UPDATE` on the source of a cross-shard materialized sum
//! meters the rows its Calvin plan tag answered.
//!
//! The statement runs as one Calvin batch: the source write and the balance
//! task on the target's vShard. The scope below covers the source only, so its
//! quota counts the source write and not the balance task. The write charges
//! the `document_scan` cost (5 tokens) once per row. pgwire and HTTP meter
//! through different routes, so each has its own test.

use super::materialized_sum_cross_shard_expanders::{POSTINGS, sum_fixture_on};
use crate::harness::TestServer;

const SCOPE: &str = "calvin:meter";
/// The cost table entry every document plan charges per row.
const ROW_COST: u64 = 5;
/// Data Plane cores the server runs.
const CORES: usize = 2;

/// The numeric user id the harness client authenticates as.
async fn harness_user_id(server: &TestServer) -> String {
    let rows = server.query_rows("SHOW USERS").await.expect("SHOW USERS");
    let row = rows
        .iter()
        .find(|row| row.iter().any(|field| field == "nodedb"))
        .unwrap_or_else(|| panic!("the harness user must be listed: {rows:?}"));
    row.iter()
        .find(|field| field.parse::<u64>().is_ok())
        .unwrap_or_else(|| panic!("SHOW USERS row must carry a numeric id: {row:?}"))
        .clone()
}

/// Start a metered sum fixture whose quota scope covers writes on the source.
/// Returns the server and the grantee id.
async fn metered_fixture() -> (TestServer, String) {
    let server = sum_fixture_on(TestServer::start_multicores_metered(CORES).await).await;
    let user_id = harness_user_id(&server).await;
    for sql in [
        format!("DEFINE SCOPE '{SCOPE}' AS WRITE ON {POSTINGS}"),
        format!("GRANT SCOPE '{SCOPE}' TO USER '{user_id}'"),
        format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1000000 TOKENS PER 3600 SECONDS ENFORCEMENT SOFT"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    }
    (server, user_id)
}

/// Tokens the scope has charged `user_id`.
async fn used_tokens(server: &TestServer, user_id: &str) -> u64 {
    let rows = server
        .query_rows(&format!("SHOW QUOTA FOR AUTH USER '{user_id}'"))
        .await
        .expect("SHOW QUOTA");
    let row = rows
        .iter()
        .find(|row| row.first().map(String::as_str) == Some(SCOPE))
        .unwrap_or_else(|| panic!("the quota row of {SCOPE}: {rows:?}"));
    row[2]
        .parse()
        .unwrap_or_else(|_| panic!("used_tokens is a number: {row:?}"))
}

/// The tokens one statement charges the scope.
async fn charged_by(server: &TestServer, user_id: &str, sql: &str) -> u64 {
    let before = used_tokens(server, user_id).await;
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    used_tokens(server, user_id).await - before
}

/// A write that touches two rows charges the row cost twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_row_calvin_update_charges_each_row() {
    let (server, user_id) = metered_fixture().await;
    let charged = charged_by(
        &server,
        &user_id,
        &format!("UPDATE {POSTINGS} SET amount = '1' WHERE account_id = 'acc'"),
    )
    .await;
    assert_eq!(charged, 2 * ROW_COST, "e0 and e1 are two rows");
}

/// A write that touches one row charges the row cost once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_row_calvin_update_charges_one_row() {
    let (server, user_id) = metered_fixture().await;
    let charged = charged_by(
        &server,
        &user_id,
        &format!("UPDATE {POSTINGS} SET amount = '1' WHERE id = 'e0'"),
    )
    .await;
    assert_eq!(charged, ROW_COST, "e0 is one row");
}

/// The HTTP route meters the rows a two-row Calvin update answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_row_calvin_update_over_http_charges_each_row() {
    let (server, user_id) = metered_fixture().await;
    let before = used_tokens(&server, &user_id).await;
    server
        .http_exec(&format!(
            "UPDATE {POSTINGS} SET amount = '1' WHERE account_id = 'acc'"
        ))
        .await;
    let charged = used_tokens(&server, &user_id).await - before;
    assert_eq!(charged, 2 * ROW_COST, "e0 and e1 are two rows");
}
