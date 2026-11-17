// SPDX-License-Identifier: BUSL-1.1

//! The object-literal `INSERT INTO c { … }` and `UPSERT INTO c { … }` forms
//! take the route every other write takes: an autocommit write whose
//! `RETURNING` list can fail commits through Calvin, so an error in the list
//! fails the statement and stores nothing. A list that cannot fail keeps the
//! direct path.
//!
//! Each divisor reads a row column, so only the evaluation of the written
//! row can raise the error.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::Value;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use super::returning_vote_row_level_security::{create_user, policed};
use super::returning_vote_single_shard::{create, native, native_error, only_value, rows_of};
use super::returning_vote_single_shard_engines::admitted_txns;
use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_on_an_object_literal_stores_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvo_ins (id TEXT PRIMARY KEY, n INT)",
    )
    .await;

    let mut stream = native(&server).await;
    for (seq, sql) in [
        (
            2,
            "INSERT INTO rvo_ins { id: 'a', n: 5 } RETURNING 1 / (n - n)",
        ),
        (
            3,
            "INSERT INTO rvo_ins [{ id: 'b', n: 5 }] RETURNING 1 / (n - n)",
        ),
        (
            4,
            "UPSERT INTO rvo_ins { id: 'c', n: 5 } RETURNING 1 / (n - n)",
        ),
    ] {
        let (code, message) = native_error(&mut stream, seq, sql).await;
        assert_eq!(code, "22012", "{sql}: {message}");
    }

    assert!(
        rows_of(&server, "SELECT id FROM rvo_ins").await.is_empty(),
        "the refusals stored nothing"
    );
}

/// The list draws `nextval('s')`, then the division fails. The drawn value
/// stands: the session's `currval` is 1 and the next `nextval` is 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_object_literal_list_keeps_the_values_it_drew() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvo_seq (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    create(&server, "CREATE SEQUENCE rvo_s").await;

    let mut stream = native(&server).await;
    let (code, message) = native_error(
        &mut stream,
        2,
        "INSERT INTO rvo_seq { id: 'a', n: 5 } RETURNING nextval('rvo_s'), 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    let currval = only_value(send_sql(&mut stream, 3, "SELECT currval('rvo_s')").await);
    assert_eq!(currval, Value::Integer(1), "the refused list drew 1");
    let next = only_value(send_sql(&mut stream, 4, "SELECT nextval('rvo_s')").await);
    assert_eq!(next, Value::Integer(2), "the drawn value stands");

    assert!(rows_of(&server, "SELECT id FROM rvo_seq").await.is_empty());
}

/// Three rows pass `max_returning_rows = 2`. The insert fails with `54000`
/// and stores no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_object_literal_list_past_the_row_limit_stores_no_row() {
    let server = TestServer::start_multicores_with_max_returning_rows(2, 2).await;
    create(
        &server,
        "CREATE COLLECTION rvo_lim (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await;

    let mut stream = native(&server).await;
    let (code, message) = native_error(
        &mut stream,
        2,
        "INSERT INTO rvo_lim [{ id: 'r1', n: 0 }, { id: 'r2', n: 0 }, { id: 'r3', n: 0 }] \
         RETURNING id",
    )
    .await;
    assert_eq!(code, "54000", "{message}");
    assert!(message.contains("exceeds limit 2"), "{message}");

    assert!(
        rows_of(&server, "SELECT id FROM rvo_lim").await.is_empty(),
        "no row stored"
    );
}

/// `RETURNING id` on a point write cannot fail, so no Calvin transaction
/// commits it. A computed item can fail, so its write commits through
/// Calvin and answers its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_that_cannot_fail_keeps_the_direct_path() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvo_direct (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    let mut stream = native(&server).await;

    let before = admitted_txns(&server).await;
    for (seq, id, sql) in [
        (
            2,
            "a",
            "INSERT INTO rvo_direct { id: 'a', n: 1 } RETURNING id",
        ),
        (
            3,
            "b",
            "UPSERT INTO rvo_direct { id: 'b', n: 1 } RETURNING id",
        ),
    ] {
        let answered = only_value(send_sql(&mut stream, seq, sql).await);
        assert_eq!(answered, Value::String(id.into()), "{sql}");
    }
    assert_eq!(
        admitted_txns(&server).await,
        before,
        "no Calvin transaction committed a list that cannot fail"
    );

    let response = send_sql(
        &mut stream,
        4,
        "INSERT INTO rvo_direct { id: 'c', n: 1 } RETURNING id, n * 2",
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(
        response.rows.as_deref(),
        Some([vec![Value::String("c".into()), Value::Integer(2)]].as_slice()),
        "the Calvin route answers the written row"
    );
    assert!(
        admitted_txns(&server).await > before,
        "a list that can fail commits through Calvin"
    );

    assert_eq!(
        rows_of(&server, "SELECT id FROM rvo_direct").await,
        ["a", "b", "c"]
    );
}

/// An object literal of a row another user owns fails with `42501` and
/// stores nothing. The same write of an owned row commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_object_literal_fails_with_insufficient_privilege() {
    let server = TestServer::start().await;
    let user = "rvo_rls_user";
    create_user(&server, user).await;
    policed(&server, "rvo_rls", user).await;
    let mut stream = open_trust_session(server.native_port, user).await;

    let (code, message) = native_error(
        &mut stream,
        2,
        "INSERT INTO rvo_rls { id: 'x', owner: 'alice', n: 5 } RETURNING id, n * 2",
    )
    .await;
    assert_eq!(code, "42501", "{message}");

    let response = send_sql(
        &mut stream,
        3,
        &format!("INSERT INTO rvo_rls {{ id: 'y', owner: '{user}', n: 5 }} RETURNING id, n * 2"),
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);

    assert_eq!(
        rows_of(&server, "SELECT id FROM rvo_rls").await,
        ["r_mine", "r_theirs", "y"],
        "the refused row is absent"
    );
}

/// A multi-row object literal into the source of a cross-shard sum ships a
/// balance to another vShard, so it commits as one Calvin transaction over
/// two vShards. Its list can fail, and it answers every written row, in
/// row order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_object_literal_across_vshards_answers_every_row() {
    let server = sum_fixture().await;
    create(
        &server,
        "ALTER TABLE ptv_post ADD COLUMN n INT NOT NULL DEFAULT 0",
    )
    .await;
    let mut stream = native(&server).await;

    let before = admitted_txns(&server).await;
    let response = send_sql(
        &mut stream,
        2,
        "INSERT INTO ptv_post [\
         { id: 'e2', account_id: 'acc', amount: '3', n: 1 }, \
         { id: 'e3', account_id: 'acc2', amount: '4', n: 4 }\
         ] RETURNING id, n * 2 AS q",
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(
        response.rows.as_deref(),
        Some(
            [
                vec![Value::String("e2".into()), Value::Integer(2)],
                vec![Value::String("e3".into()), Value::Integer(8)],
            ]
            .as_slice()
        ),
        "every row answers, in row order"
    );
    assert!(
        admitted_txns(&server).await > before,
        "the write commits through Calvin"
    );
    assert_eq!(balance(&server, "acc").await, "18");
    assert_eq!(balance(&server, "acc2").await, "4");
}
