// SPDX-License-Identifier: BUSL-1.1

//! The sequence values a refused Calvin `RETURNING` list drew stand, as in
//! PostgreSQL: the statement fails, and the session's `currval` is the last
//! value the list drew.
//!
//! A write of a cross-shard sum source commits through Calvin on several
//! vShards, so its participant evaluates the list before it votes. One
//! request's writes on two vShards commit as one Calvin txn, so each
//! participant evaluates its own list, in statement order when a later list
//! reads a sequence an earlier one changes.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::Value;
use nodedb_types::id::DatabaseId;
use nodedb_types::protocol::NativeResponse;

use super::materialized_sum_cross_shard_expanders::sum_fixture;
use crate::harness::TestServer;

const ONE: &str = "crs_one";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name on another vShard than `ONE`.
fn two() -> String {
    (0..)
        .map(|n| format!("crs_two_{n}"))
        .find(|name| vshard_of(name) != vshard_of(ONE))
        .expect("a name on another vShard")
}

/// The participant draws `nextval('s')`, then the division fails. The
/// divisor reads a row column, so only the participant's evaluation of the
/// written row can raise it. The session keeps the drawn value as its
/// `currval`, and the next `nextval` continues after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_returning_list_keeps_the_values_it_drew() {
    let server = sum_fixture().await;
    server
        .exec("ALTER TABLE ptv_post ADD COLUMN n INT NOT NULL DEFAULT 0")
        .await
        .expect("add column n");
    server.exec("CREATE SEQUENCE s").await.expect("create s");

    server
        .expect_error(
            "UPDATE ptv_post SET amount = '1', n = 5 WHERE id = 'e0' \
             RETURNING nextval('s'), 1 / (n - n)",
            "SQLSTATE 22012",
        )
        .await;

    assert_eq!(
        server
            .query_text("SELECT currval('s')")
            .await
            .expect("currval after the refusal"),
        ["1"],
        "the refused list drew 1"
    );
    assert_eq!(
        server
            .query_text("SELECT nextval('s')")
            .await
            .expect("nextval after the refusal"),
        ["2"]
    );
    assert_eq!(
        server
            .query_text_joined("SELECT amount, n FROM ptv_post WHERE id = 'e0'")
            .await
            .expect("read e0"),
        ["10\t0"],
        "the refusal stored nothing"
    );
}

/// Two writes of one request on two participants each draw `nextval('s')`
/// and then divide by zero. The second list follows the first one's draw,
/// so it never evaluates: the first refusal ends the txn, as the first
/// failed statement ends a PostgreSQL request. The session keeps the one
/// value the first list drew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_refusing_statement_ends_the_request_with_its_draws() {
    let server = TestServer::start_multicores(2).await;
    let two = two();
    for collection in [ONE, two.as_str()] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .expect("create the collection");
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, n) VALUES ('r1', 0)"
            ))
            .await
            .expect("seed the collection");
    }
    server.exec("CREATE SEQUENCE s").await.expect("create s");

    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let sql = format!(
        "UPDATE {ONE} SET n = 5 WHERE id = 'r1' RETURNING nextval('s'), 1 / (n - n); \
         UPDATE {two} SET n = 5 WHERE id = 'r1' RETURNING nextval('s'), 1 / (n - n)"
    );
    let response = send_sql(&mut stream, 2, &sql).await;
    let error = response
        .error
        .unwrap_or_else(|| panic!("both lists divide by zero: {sql}"));
    assert_eq!(error.code, "22012", "{}", error.message);

    let currval = only_value(send_sql(&mut stream, 3, "SELECT currval('s')").await);
    assert_eq!(currval, Value::Integer(1), "the first list drew 1");
    let next = only_value(send_sql(&mut stream, 4, "SELECT nextval('s')").await);
    assert_eq!(next, Value::Integer(2), "the second list drew nothing");
    for collection in [ONE, two.as_str()] {
        assert_eq!(
            server
                .query_text(&format!("SELECT n FROM {collection} WHERE id = 'r1'"))
                .await
                .expect("read n"),
            ["0"],
            "{collection} stored nothing"
        );
    }
}

/// The one cell `response` answers.
fn only_value(response: NativeResponse) -> Value {
    assert!(response.error.is_none(), "{:?}", response.error);
    match response.rows.as_deref() {
        Some([row]) if row.len() == 1 => row[0].clone(),
        rows => panic!("one cell, got {rows:?}"),
    }
}
