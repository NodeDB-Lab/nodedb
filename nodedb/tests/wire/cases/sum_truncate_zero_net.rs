// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of the source of a cross-shard materialized sum when the
//! settlement ships no balance delta: the rows net to zero, or the source is
//! empty.
//!
//! Such a TRUNCATE still holds the read version of the source its settlement
//! observed. Only a Calvin transaction checks that read. So the TRUNCATE must
//! take Calvin on one vShard, and a source change between settlement and the
//! TRUNCATE must end it in commit, 40001 or 409. The balance must equal the
//! source sum either way.
//!
//! On pgwire the `stmt::before_route` gate parks the TRUNCATE after
//! settlement. HTTP and native never reach that gate, so there the
//! TRUNCATE parks at `recon::before_submit`, which every route passes before
//! it submits its Calvin transaction. The mover is a keyed or `INSERT ..
//! SELECT` write, which passes neither gate, so no mover parks. The
//! `recon::before_submit` gate stays open in the route probe. A pass through
//! it writes the `.parked` marker.

#![cfg(feature = "failpoints")]

use std::path::Path;
use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request_within};
use nodedb_types::id::DatabaseId;
use nodedb_types::protocol::{OpCode, TextFields};
use tokio::net::TcpStream;

use super::sum_cell_support::{
    SERIALIZATION_FAILURE, arm_gate, balance, clear_parked_marker, feed, parked_fixture_at,
    source_sum, sum_fixture, wait_parked_at,
};
use crate::harness::TestServer;

/// The gate a statement parks at after settlement and before its route.
const ROUTE_GATE: &str = "stmt::before_route";
/// The gate a recon write passes before it submits its Calvin transaction.
const SUBMIT_GATE: &str = "recon::before_submit";
/// The HTTP status of a serialization conflict.
const HTTP_CONFLICT: &str = "409";
/// How long a statement waits for its answer, parked or not.
const ANSWER_BOUND: Duration = Duration::from_secs(60);
/// The mover: `e0` takes the fed amount, so the sum of `acc` moves to it.
const MOVE_FROM_FEED: &str = "UPDATE ptv_post SET amount = f.amount \
                                         FROM ptv_feed f WHERE ptv_post.id = f.id";
/// The mover for an empty source: it copies the feed into the source.
const COPY_FROM_FEED: &str = "INSERT INTO ptv_post (id, account_id, amount) \
                              SELECT id, account_id, amount FROM ptv_feed";
/// Both seed rows net to zero.
const ZERO_NET: [&str; 2] = [
    "UPDATE ptv_post SET amount = '0' WHERE id = 'e0'",
    "UPDATE ptv_post SET amount = '0' WHERE id = 'e1'",
];

/// A protocol that sends one request to the server.
#[derive(Clone, Copy, Debug)]
pub(super) enum Route {
    Pgwire,
    Http,
    NativeSql,
}

impl Route {
    /// The code a serialization conflict answers with on this route.
    fn conflict(self) -> &'static str {
        match self {
            Route::Http => HTTP_CONFLICT,
            Route::Pgwire | Route::NativeSql => SERIALIZATION_FAILURE,
        }
    }
}

/// An open session on one [`Route`], as the superuser.
pub(super) enum Session<'a> {
    Pgwire(tokio_postgres::Client),
    Http(&'a TestServer),
    NativeSql { stream: TcpStream, seq: u64 },
}

impl<'a> Session<'a> {
    pub(super) async fn open(route: Route, server: &'a TestServer) -> Self {
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
    /// carries the SQLSTATE, or the status on HTTP. A commit carries the
    /// HTTP body, and an empty text on the other routes.
    pub(super) async fn run(&mut self, sql: &str) -> Result<String, String> {
        let answer = async move {
            match self {
                Session::Pgwire(client) => match client.simple_query(sql).await {
                    Ok(_) => Ok(String::new()),
                    Err(error) => Err(match error.code() {
                        Some(code) => code.code().to_string(),
                        None => format!("{error:?}"),
                    }),
                },
                Session::Http(server) => {
                    let (status, body) = server.http_query(sql).await;
                    if (200..300).contains(&status) {
                        Ok(body)
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
                        None => Ok(String::new()),
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
pub(super) async fn exec_bounded(server: &TestServer, sql: &str) {
    tokio::time::timeout(Duration::from_secs(30), server.exec(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql} did not return"))
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

fn vshard(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name that starts with `prefix` and homes on a vShard `accept`
/// takes.
pub(super) fn collection_where(prefix: &str, accept: impl Fn(u32) -> bool) -> String {
    (0..4096)
        .map(|index| format!("{prefix}_{index}"))
        .find(|name| accept(vshard(name)))
        .unwrap_or_else(|| panic!("no {prefix} name homes on an accepted vShard"))
}

/// A collection name that homes on the vShard of `base`.
pub(super) fn collection_beside(prefix: &str, base: &str) -> String {
    collection_where(prefix, |home| home == vshard(base))
}

/// Create a plain collection with an `id` key.
pub(super) async fn create_plain(server: &TestServer, name: &str) {
    exec_bounded(
        server,
        &format!("CREATE COLLECTION {name} (id TEXT PRIMARY KEY) WITH (engine='document_strict')"),
    )
    .await;
}

/// Park `sql` on `route` at its gate, run the mover, then release the
/// statement. `setup` runs first. `fed` is the feed row `(id, amount)` for
/// `acc`. Assert that the statement commits or aborts with a serialization
/// conflict, and that the balance equals the source sum.
async fn race(route: Route, sql: &str, setup: &[String], fed: (&str, &str), mover: &str) {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-route");
    let gate = match route {
        Route::Pgwire => ROUTE_GATE,
        Route::Http | Route::NativeSql => SUBMIT_GATE,
    };
    let server = parked_fixture_at(gate, &release).await;
    for statement in setup {
        exec_bounded(&server, statement).await;
    }
    feed(&server, fed.0, "acc", fed.1).await;
    let mut session = Session::open(route, &server).await;
    arm_gate(&release);

    let release_after_mover = async {
        wait_parked_at(gate, &release, sql).await;
        exec_bounded(&server, mover).await;
        std::fs::write(&release, b"release").expect("release the parked statement");
    };
    let (answer, ()) = tokio::join!(session.run(sql), release_after_mover);

    if let Err(code) = &answer {
        assert_eq!(
            code,
            route.conflict(),
            "{route:?}: {sql} must commit or abort with a serialization conflict"
        );
    }
    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server, "acc").await,
        sum.to_string(),
        "{route:?}: the balance must equal the source sum after {sql}"
    );
}

/// The setup statements that zero both seed rows.
fn zero_net_setup() -> Vec<String> {
    ZERO_NET.iter().map(|sql| (*sql).to_string()).collect()
}

/// The setup statement that empties the source of `acc`.
fn empty_setup() -> Vec<String> {
    vec!["DELETE FROM ptv_post WHERE account_id = 'acc'".to_string()]
}

/// Race a zero-net TRUNCATE. The mover sets `e0` to 20.
async fn zero_net_race(route: Route) {
    race(
        route,
        "TRUNCATE ptv_post",
        &zero_net_setup(),
        ("e0", "20"),
        MOVE_FROM_FEED,
    )
    .await;
}

/// Race a zero-row TRUNCATE. The mover copies `e2` with 7 into the source.
async fn zero_row_race(route: Route) {
    race(
        route,
        "TRUNCATE ptv_post",
        &empty_setup(),
        ("e2", "7"),
        COPY_FROM_FEED,
    )
    .await;
}

/// Rejects a pgwire zero-net TRUNCATE commit at balance 20 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_zero_net_truncate_matches_the_sum_after_a_concurrent_move() {
    zero_net_race(Route::Pgwire).await;
}

/// Rejects an HTTP zero-net TRUNCATE commit at balance 20 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_zero_net_truncate_matches_the_sum_after_a_concurrent_move() {
    zero_net_race(Route::Http).await;
}

/// Rejects a native SQL zero-net TRUNCATE commit at balance 20 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_zero_net_truncate_matches_the_sum_after_a_concurrent_move() {
    zero_net_race(Route::NativeSql).await;
}

/// Rejects a pgwire zero-row TRUNCATE commit at balance 7 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_zero_row_truncate_matches_the_sum_after_a_concurrent_move() {
    zero_row_race(Route::Pgwire).await;
}

/// Rejects an HTTP zero-row TRUNCATE commit at balance 7 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_zero_row_truncate_matches_the_sum_after_a_concurrent_move() {
    zero_row_race(Route::Http).await;
}

/// Rejects a native SQL zero-row TRUNCATE commit at balance 7 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_zero_row_truncate_matches_the_sum_after_a_concurrent_move() {
    zero_row_race(Route::NativeSql).await;
}

/// Whether a write reached the gate armed on `release`.
fn reached(release: &Path) -> bool {
    let mut marker = release.as_os_str().to_owned();
    marker.push(".parked");
    Path::new(&marker).exists()
}

/// Run a zero-net TRUNCATE with the submit gate open and assert that it
/// passes the gate and empties the source.
async fn zero_net_takes_calvin(route: Route) {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-probe");
    let server = parked_fixture_at(SUBMIT_GATE, &release).await;
    for statement in zero_net_setup() {
        exec_bounded(&server, &statement).await;
    }
    clear_parked_marker(&release);
    let answer = Session::open(route, &server)
        .await
        .run("TRUNCATE ptv_post")
        .await;
    assert!(
        answer.is_ok(),
        "{route:?}: the TRUNCATE must commit, got {answer:?}"
    );
    assert!(
        reached(&release),
        "{route:?}: a TRUNCATE that holds settlement reads must take Calvin"
    );
    assert_eq!(balance(&server, "acc").await, "0");
    assert_eq!(source_sum(&server).await, 0);
}

/// Rejects a pgwire zero-net TRUNCATE that leaves no `.parked` marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_zero_net_truncate_takes_calvin() {
    zero_net_takes_calvin(Route::Pgwire).await;
}

/// Rejects an HTTP zero-net TRUNCATE that leaves no `.parked` marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_zero_net_truncate_takes_calvin() {
    zero_net_takes_calvin(Route::Http).await;
}

/// Rejects a native SQL zero-net TRUNCATE that leaves no `.parked` marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_zero_net_truncate_takes_calvin() {
    zero_net_takes_calvin(Route::NativeSql).await;
}

/// The request `TRUNCATE ptv_post; INSERT INTO <near> ...`, with `near` homed
/// on the vShard of the source.
fn truncate_then_insert(near: &str) -> String {
    format!("TRUNCATE ptv_post; INSERT INTO {near} (id) VALUES ('n1')")
}

/// Race a TRUNCATE with a write on the same vShard in one request.
async fn truncate_beside_a_write_race(route: Route) {
    let near = collection_beside("ptv_near", "ptv_post");
    let mut setup = zero_net_setup();
    setup.push(format!(
        "CREATE COLLECTION {near} (id TEXT PRIMARY KEY) WITH (engine='document_strict')"
    ));
    let sql = truncate_then_insert(&near);
    race(route, &sql, &setup, ("e0", "20"), MOVE_FROM_FEED).await;
}

/// Rejects an HTTP request commit at balance 20 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_truncate_beside_a_write_matches_the_sum_after_a_concurrent_move() {
    truncate_beside_a_write_race(Route::Http).await;
}

/// Rejects a native SQL request commit at balance 20 against a source sum of 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_truncate_beside_a_write_matches_the_sum_after_a_concurrent_move() {
    truncate_beside_a_write_race(Route::NativeSql).await;
}

/// Run the request with no mover and assert that the write beside the
/// TRUNCATE lands.
async fn truncate_beside_a_write(route: Route) {
    let server = sum_fixture().await;
    let near = collection_beside("ptv_near", "ptv_post");
    create_plain(&server, &near).await;
    let answer = Session::open(route, &server)
        .await
        .run(&truncate_then_insert(&near))
        .await;
    assert!(
        answer.is_ok(),
        "{route:?}: the request must commit, got {answer:?}"
    );
    let rows = server
        .query_text(&format!("SELECT id FROM {near}"))
        .await
        .expect("read the near collection");
    assert_eq!(
        rows,
        vec!["n1".to_string()],
        "{route:?}: the write beside the TRUNCATE lands"
    );
    assert_eq!(source_sum(&server).await, 0);
}

/// Rejects an HTTP request that drops the write beside the TRUNCATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_truncate_beside_a_write_lands_both() {
    truncate_beside_a_write(Route::Http).await;
}

/// Rejects a native SQL request that drops the write beside the TRUNCATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_truncate_beside_a_write_lands_both() {
    truncate_beside_a_write(Route::NativeSql).await;
}

/// Rejects a COMMIT that applies a stale zero delta of a staged TRUNCATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_zero_net_truncate_aborts_at_commit_when_the_source_moved() {
    let server = sum_fixture().await;
    for statement in zero_net_setup() {
        exec_bounded(&server, &statement).await;
    }
    feed(&server, "e0", "acc", "20").await;
    let (client, _connection) = server
        .connect_as("nodedb", "nodedb")
        .await
        .unwrap_or_else(|e| panic!("connect over pgwire: {e}"));
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("TRUNCATE ptv_post")
        .await
        .expect("stage the TRUNCATE");
    exec_bounded(&server, MOVE_FROM_FEED).await;
    let commit = client.simple_query("COMMIT").await;
    let code = match commit {
        Ok(_) => String::new(),
        Err(error) => error
            .code()
            .map(|code| code.code().to_string())
            .unwrap_or_default(),
    };
    assert_eq!(
        code, SERIALIZATION_FAILURE,
        "the COMMIT must abort with a serialization conflict"
    );
    let sum = source_sum(&server).await;
    assert_eq!(balance(&server, "acc").await, sum.to_string());
}
