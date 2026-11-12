// SPDX-License-Identifier: BUSL-1.1

//! The pgwire route of a `RETURNING` write on one vShard, on the simple and
//! the extended query protocol: a list that can fail commits through Calvin
//! and keeps the row-level-security write policy, a list that cannot fail
//! keeps the direct path, and both answer the same column names and types.

use crate::harness::TestServer;

use super::returning_vote_row_level_security::{PASSWORD, create_user, policed};
use super::returning_vote_single_shard::{create, rows_of};
use super::returning_vote_single_shard_engines::admitted_txns;
use super::returning_vote_single_shard_pgwire::fails_on_both;

/// An insert of another user's row with a list that can fail is refused with
/// `42501` on both protocols, and the row is absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_insert_fails_with_insufficient_privilege() {
    let server = TestServer::start().await;
    let user = "rvp_rls_user";
    create_user(&server, user).await;
    policed(&server, "rvp_rls", user).await;
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as the policed user");

    fails_on_both(
        &client,
        "INSERT INTO rvp_rls (id, owner, n) VALUES ('{k}', 'alice', 5) RETURNING id, n * 2",
        ["x", "y"],
        "42501",
    )
    .await;
    drop(client);
    handle.abort();

    assert_eq!(
        rows_of(&server, "SELECT id FROM rvp_rls").await,
        ["r_mine", "r_theirs"],
        "the refused rows are absent"
    );
}

/// `RETURNING id` on a point insert cannot fail, so no Calvin transaction
/// commits it on either protocol. A computed item can fail, so its insert
/// commits through Calvin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_that_cannot_fail_keeps_the_direct_path() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvp_direct (id TEXT PRIMARY KEY, n INT)",
    )
    .await;

    let before = admitted_txns(&server).await;
    server
        .client
        .simple_query("INSERT INTO rvp_direct (id, n) VALUES ('a', 1) RETURNING id")
        .await
        .expect("the simple insert answers");
    server
        .client
        .query(
            "INSERT INTO rvp_direct (id, n) VALUES ('b', 1) RETURNING id",
            &[],
        )
        .await
        .expect("the extended insert answers");
    assert_eq!(
        admitted_txns(&server).await,
        before,
        "no Calvin transaction committed a list that cannot fail"
    );

    server
        .client
        .query(
            "INSERT INTO rvp_direct (id, n) VALUES ('c', 1) RETURNING id, n * 2",
            &[],
        )
        .await
        .expect("the extended insert answers");
    let after_extended = admitted_txns(&server).await;
    assert!(
        after_extended > before,
        "an extended list that can fail commits through Calvin"
    );
    server
        .client
        .simple_query("INSERT INTO rvp_direct (id, n) VALUES ('d', 1) RETURNING id, n * 2")
        .await
        .expect("the simple insert answers");
    assert!(
        admitted_txns(&server).await > after_extended,
        "a simple list that can fail commits through Calvin"
    );
    assert_eq!(
        rows_of(&server, "SELECT id FROM rvp_direct").await,
        ["a", "b", "c", "d"]
    );
}

/// The names and types of the columns an extended-protocol insert answers.
async fn described(server: &TestServer, sql: &str) -> Vec<(String, String)> {
    let rows = server
        .client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let [row] = rows.as_slice() else {
        panic!("{sql}: one row, got {}", rows.len());
    };
    row.columns()
        .iter()
        .map(|column| (column.name().to_owned(), column.type_().name().to_owned()))
        .collect()
}

/// A list that can fail commits through Calvin. Its columns the direct
/// path's list also names answer the same names, types and values as there:
/// the Calvin list adds one item that can fail, and the shared columns are
/// compared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_on_calvin_answers_as_the_direct_path() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvp_parity (id TEXT PRIMARY KEY, v INT, n INT) \
         WITH (engine='document_strict')",
    )
    .await;
    let direct = "RETURNING id AS k, v";
    let calvin = "RETURNING id AS k, v, 1 / (n + 1) AS f";

    let direct_columns = described(
        &server,
        &format!("INSERT INTO rvp_parity (id, v, n) VALUES ('p0', 7, 0) {direct}"),
    )
    .await;
    let calvin_columns = described(
        &server,
        &format!("INSERT INTO rvp_parity (id, v, n) VALUES ('p1', 7, 0) {calvin}"),
    )
    .await;
    assert_eq!(
        calvin_columns.get(..direct_columns.len()),
        Some(direct_columns.as_slice()),
        "the shared columns answer the same names and types"
    );
    assert_eq!(
        calvin_columns
            .get(direct_columns.len())
            .map(|(name, _)| name.as_str()),
        Some("f")
    );

    for (id, list) in [("p2", direct), ("p3", calvin)] {
        let rows = server
            .query_named_rows(&format!(
                "INSERT INTO rvp_parity (id, v, n) VALUES ('{id}', 7, 0) {list}"
            ))
            .await
            .unwrap_or_else(|e| panic!("{list}: {e}"));
        let [row] = rows.as_slice() else {
            panic!("{list}: one row, got {rows:?}");
        };
        assert_eq!(row.get("k").map(String::as_str), Some(id), "{list}");
        assert_eq!(row.get("v").map(String::as_str), Some("7"), "{list}");
    }
}

/// `v * 1` on an `INT` column is `integer`, as in PostgreSQL, so a Calvin
/// list that computes it answers the same name, type and value as the
/// direct path's bare `v`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_integer_answers_as_its_column() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvp_width (id TEXT PRIMARY KEY, v INT) \
         WITH (engine='document_strict')",
    )
    .await;
    let direct = "RETURNING id AS k, v";
    let calvin = "RETURNING id AS k, v * 1 AS v";

    let direct_columns = described(
        &server,
        &format!("INSERT INTO rvp_width (id, v) VALUES ('w0', 7) {direct}"),
    )
    .await;
    let calvin_columns = described(
        &server,
        &format!("INSERT INTO rvp_width (id, v) VALUES ('w1', 7) {calvin}"),
    )
    .await;
    assert_eq!(
        calvin_columns, direct_columns,
        "the computed column answers the column's name and type"
    );
    assert_eq!(
        direct_columns.get(1).map(|(_, ty)| ty.as_str()),
        Some("int4")
    );

    for (id, list) in [("w2", direct), ("w3", calvin)] {
        let rows = server
            .query_named_rows(&format!(
                "INSERT INTO rvp_width (id, v) VALUES ('{id}', 7) {list}"
            ))
            .await
            .unwrap_or_else(|e| panic!("{list}: {e}"));
        let [row] = rows.as_slice() else {
            panic!("{list}: one row, got {rows:?}");
        };
        assert_eq!(row.get("v").map(String::as_str), Some("7"), "{list}");
    }
}
