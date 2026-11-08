// SPDX-License-Identifier: BUSL-1.1

//! `TRUNCATE ... RESTART IDENTITY` of the source of a cross-shard materialized
//! sum, over pgwire, HTTP and native SQL.
//!
//! The sequence `ptv_post_n_seq` matches the implicit pattern
//! `{collection}_*_seq`, so a TRUNCATE of `ptv_post` with the flag must restart
//! it. Row R0 checks that premise on a plain collection first.
//!
//! A TRUNCATE that settles a balance commits as a Calvin transaction. A
//! TRUNCATE of an empty source on HTTP commits as a lone write. Both must
//! restart the sequence once they commit.

use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request_within};
use nodedb_types::protocol::{OpCode, TextFields};
use tokio::net::TcpStream;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

/// The sequence the restart tests read.
const SEQUENCE: &str = "ptv_post_n_seq";
/// How long a statement waits for its answer.
const ANSWER_BOUND: Duration = Duration::from_secs(60);

/// A protocol that sends one statement to the server.
#[derive(Clone, Copy, Debug)]
enum Route {
    Pgwire,
    Http,
    NativeSql,
}

/// An open session on one [`Route`], as the superuser.
enum Session<'a> {
    Pgwire(tokio_postgres::Client),
    Http(&'a TestServer),
    NativeSql { stream: TcpStream, seq: u64 },
}

impl<'a> Session<'a> {
    async fn open(route: Route, server: &'a TestServer) -> Self {
        match route {
            Route::Pgwire => {
                let (client, _connection) = server
                    .connect_as("nodedb", "nodedb")
                    .await
                    .unwrap_or_else(|e| panic!("connect over pgwire: {e}"));
                Session::Pgwire(client)
            }
            Route::Http => Session::Http(server),
            // Seq 1 is the handshake.
            Route::NativeSql => Session::NativeSql {
                stream: open_trust_session(server.native_port, "nodedb").await,
                seq: 1,
            },
        }
    }

    /// Run `sql` and wait at most [`ANSWER_BOUND`] for its answer. An error
    /// carries the SQLSTATE, or the status on HTTP.
    async fn run(&mut self, sql: &str) -> Result<(), String> {
        let answer =
            async move {
                match self {
                    Session::Pgwire(client) => {
                        client.simple_query(sql).await.map(|_| ()).map_err(|error| {
                            match error.code() {
                                Some(code) => code.code().to_string(),
                                None => format!("{error:?}"),
                            }
                        })
                    }
                    Session::Http(server) => {
                        let (status, _body) = server.http_query(sql).await;
                        if (200..300).contains(&status) {
                            Ok(())
                        } else {
                            Err(status.to_string())
                        }
                    }
                    Session::NativeSql { stream, seq } => {
                        *seq += 1;
                        let fields = TextFields {
                            sql: Some(sql.to_string()),
                            ..Default::default()
                        };
                        let response =
                            send_request_within(stream, *seq, OpCode::Sql, fields, None).await;
                        match response.error {
                            Some(error) => Err(error.code),
                            None => Ok(()),
                        }
                    }
                }
            };
        tokio::time::timeout(ANSWER_BOUND, answer)
            .await
            .unwrap_or_else(|_| panic!("{sql} did not answer"))
    }
}

/// Run `sql` on the superuser's pgwire client. A statement that does not
/// return within 30 s fails the test.
async fn exec_bounded(server: &TestServer, sql: &str) {
    tokio::time::timeout(Duration::from_secs(30), server.exec(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql} did not return"))
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// The sum of the `acc` postings, read from the source rows.
async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc'")
        .await
        .expect("read the source postings")
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

/// The next value of `sequence`.
async fn nextval(server: &TestServer, sequence: &str) -> String {
    let rows = server
        .query_text(&format!("SELECT nextval('{sequence}')"))
        .await
        .unwrap_or_else(|e| panic!("read nextval of {sequence}: {e}"));
    match rows.as_slice() {
        [value] => value.clone(),
        other => panic!("one value for {sequence}, got {other:?}"),
    }
}

/// Create `sequence` and advance it twice.
async fn advance_twice(server: &TestServer, sequence: &str) {
    exec_bounded(server, &format!("CREATE SEQUENCE {sequence}")).await;
    nextval(server, sequence).await;
    nextval(server, sequence).await;
}

/// Rejects a plain-collection restart that leaves the sequence past 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_identity_truncate_restarts_the_collection_sequence() {
    let server = TestServer::start().await;
    exec_bounded(
        &server,
        "CREATE COLLECTION r0 (id TEXT PRIMARY KEY) WITH (engine='document_strict')",
    )
    .await;
    advance_twice(&server, "r0_n_seq").await;
    exec_bounded(&server, "TRUNCATE r0 RESTART IDENTITY").await;
    assert_eq!(nextval(&server, "r0_n_seq").await, "1");
}

/// Run `TRUNCATE ptv_post RESTART IDENTITY` on `route` over an empty source.
async fn zero_row_restart(route: Route) {
    let server = sum_fixture().await;
    advance_twice(&server, SEQUENCE).await;
    exec_bounded(&server, "DELETE FROM ptv_post WHERE account_id = 'acc'").await;
    let answer = Session::open(route, &server)
        .await
        .run("TRUNCATE ptv_post RESTART IDENTITY")
        .await;
    assert!(
        answer.is_ok(),
        "{route:?}: the TRUNCATE must commit, got {answer:?}"
    );
    assert_eq!(
        nextval(&server, SEQUENCE).await,
        "1",
        "{route:?}: the sequence restarts"
    );
}

/// Run `TRUNCATE ptv_post RESTART IDENTITY` on `route` over the seed rows.
async fn nonzero_restart(route: Route) {
    let server = sum_fixture().await;
    advance_twice(&server, SEQUENCE).await;
    let answer = Session::open(route, &server)
        .await
        .run("TRUNCATE ptv_post RESTART IDENTITY")
        .await;
    assert!(
        answer.is_ok(),
        "{route:?}: the TRUNCATE must commit, got {answer:?}"
    );
    assert_eq!(balance(&server, "acc").await, "0");
    assert_eq!(source_sum(&server).await, 0);
    assert_eq!(
        nextval(&server, SEQUENCE).await,
        "1",
        "{route:?}: the sequence restarts"
    );
}

/// Rejects a pgwire zero-row restart that leaves the next value past 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_zero_row_restart_identity_restarts_the_sequence() {
    zero_row_restart(Route::Pgwire).await;
}

/// Rejects an HTTP zero-row restart that leaves the next value past 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_zero_row_restart_identity_restarts_the_sequence() {
    zero_row_restart(Route::Http).await;
}

/// Rejects a native SQL zero-row restart that leaves the next value past 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_zero_row_restart_identity_restarts_the_sequence() {
    zero_row_restart(Route::NativeSql).await;
}

/// Rejects a pgwire restart over rows that leaves the next value at 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_restart_identity_over_rows_restarts_the_sequence() {
    nonzero_restart(Route::Pgwire).await;
}

/// Rejects an HTTP restart over rows that leaves the next value at 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_restart_identity_over_rows_restarts_the_sequence() {
    nonzero_restart(Route::Http).await;
}

/// Rejects a native SQL restart over rows that leaves the next value at 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_restart_identity_over_rows_restarts_the_sequence() {
    nonzero_restart(Route::NativeSql).await;
}
