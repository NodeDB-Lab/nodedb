// SPDX-License-Identifier: BUSL-1.1

//! A point write of the source of a cross-shard materialized sum whose row has
//! nothing to settle when the write plans: its key is unbound, or its key is
//! bound and its row is deleted, or its row holds no join value.
//!
//! While the write is parked, a mover inserts the key, re-inserts the row or
//! sets its join value. The write must commit or abort with a serialization
//! conflict: SQLSTATE 40001 on pgwire and native, 409 on HTTP. The balance
//! must equal the source sum either way.
//!
//! Each mover runs in an implicit transaction, which returns before the
//! `stmt::before_route` gate. No read runs while a write is parked there.
//!
//! Only the pgwire statement path calls `stmt::before_route`
//! (`pgwire/handler/routing/execute.rs`). HTTP, NDJSON, WebSocket and native
//! never reach it, and no other gate sits between their settlement and their
//! commit. A race on those routes runs the write, then the mover, and checks
//! the balance: it cannot park the write.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_test_support::native_harness::{open_trust_session, send_request_within};
use nodedb_types::Value;
use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};
use tokio::net::TcpStream;
use tokio_postgres::{Client, Statement};

use super::sum_absent_row_http::{WsStream, http_end, ndjson_end, ws_connect, ws_end};
use super::sum_cell_support::{
    SERIALIZATION_FAILURE, arm_gate, balance, parked_fixture_at, source_sum, sum_fixture,
    wait_parked_at,
};
use crate::harness::TestServer;

/// The gate a statement parks at after settlement and before its route.
pub(super) const ROUTE_GATE: &str = "stmt::before_route";
/// The gate a Calvin transaction passes once its collections are stamped.
const STAMP_GATE: &str = "calvin::after_stamp::ptv_post";
/// How long a parked request waits for its answer.
pub(super) const PARKED_BOUND: Duration = Duration::from_secs(60);

/// One race: the setup run while the gate is open, the parked point write of
/// `ptv_post`, and the mover that changes the written row while it is parked.
#[derive(Clone, Copy, Debug)]
pub(super) struct Race {
    /// Statements run before the gate closes.
    pub setup: &'static [&'static str],
    /// The row the write names.
    pub key: &'static str,
    /// Whether the write deletes the row. Else it sets `amount` to 7.
    pub delete: bool,
    /// The statement that changes the row while the write is parked.
    pub mover: &'static str,
}

const FEED_E9: &str = "INSERT INTO ptv_feed (id, account_id, amount) VALUES ('e9', 'acc', '3')";
const COPY_E9: &str = "INSERT INTO ptv_post (id, account_id, amount) \
                       SELECT id, account_id, amount FROM ptv_feed WHERE id = 'e9'";
const DROP_E1: &str = "DELETE FROM ptv_post WHERE id = 'e1'";
const FEED_E1: &str = "INSERT INTO ptv_feed (id, account_id, amount) VALUES ('e1', 'acc', '5')";
const COPY_E1: &str = "INSERT INTO ptv_post (id, account_id, amount) \
                       SELECT id, account_id, amount FROM ptv_feed WHERE id = 'e1'";
const JOINLESS_E5: &str = "INSERT INTO ptv_post (id, amount) VALUES ('e5', '3')";
const FEED_E5: &str = "INSERT INTO ptv_feed (id, account_id, amount) VALUES ('e5', 'acc', '3')";
const JOIN_FROM_FEED: &str = "UPDATE ptv_post SET account_id = f.account_id \
                              FROM ptv_feed f WHERE ptv_post.id = f.id";

/// An update of `e9`, a key no row ever bound. The mover inserts `e9` with 3.
pub(super) const UNBOUND_UPDATE: Race = Race {
    setup: &[FEED_E9],
    key: "e9",
    delete: false,
    mover: COPY_E9,
};
/// A delete of the unbound `e9`. The mover inserts `e9` with 3.
pub(super) const UNBOUND_DELETE: Race = Race {
    delete: true,
    ..UNBOUND_UPDATE
};
/// An update of `e1`, whose row is deleted and whose key stays bound. The
/// mover inserts `e1` again with 5.
pub(super) const ABSENT_UPDATE: Race = Race {
    setup: &[DROP_E1, FEED_E1],
    key: "e1",
    delete: false,
    mover: COPY_E1,
};
/// A delete of the deleted `e1`. The mover inserts `e1` again with 5.
pub(super) const ABSENT_DELETE: Race = Race {
    delete: true,
    ..ABSENT_UPDATE
};
/// An update of `e5`, stored with amount 3 and no `account_id`. The mover sets
/// its `account_id` to `acc`.
pub(super) const JOINLESS_UPDATE: Race = Race {
    setup: &[JOINLESS_E5, FEED_E5],
    key: "e5",
    delete: false,
    mover: JOIN_FROM_FEED,
};

/// How a request ended on the wire.
#[derive(Debug)]
pub(super) enum End {
    /// No error.
    Answered,
    /// A serialization conflict: SQLSTATE 40001, or HTTP 409.
    Conflict,
    /// Any other error, with its text.
    Failed(String),
}

/// The route a write takes.
#[derive(Clone, Copy, Debug)]
pub(super) enum Route {
    /// pgwire simple query.
    Simple,
    /// pgwire extended query, the key bound as `$1`.
    Extended,
    /// The native SQL opcode.
    NativeSql,
    /// A native direct op: `DocumentUpdate` or `PointDelete`.
    NativeOp,
    /// HTTP `/v1/query`.
    Http,
    /// NDJSON `/v1/query/stream`.
    Ndjson,
    /// WebSocket RPC `/v1/ws`.
    Ws,
}

/// The task that drives a pgwire connection.
type Connection = tokio::task::JoinHandle<()>;

/// A connection opened before the gate closes.
enum Writer {
    Pg(Client, Connection),
    Prepared(Client, Statement, Connection),
    Native(TcpStream),
    Http(u16),
    Ws(Box<WsStream>),
}

/// The SQL of `race`'s write.
pub(super) fn write_sql(race: Race) -> String {
    if race.delete {
        format!("DELETE FROM ptv_post WHERE id = '{}'", race.key)
    } else {
        format!("UPDATE ptv_post SET amount = '7' WHERE id = '{}'", race.key)
    }
}

/// The native direct op of `race`'s write: `DocumentUpdate` or `PointDelete`.
pub(super) fn native_write(race: Race) -> (OpCode, TextFields) {
    let mut fields = TextFields {
        collection: Some("ptv_post".to_string()),
        document_id: Some(race.key.to_string()),
        ..Default::default()
    };
    if race.delete {
        return (OpCode::PointDelete, fields);
    }
    let amount = nodedb_types::value_to_msgpack(&Value::String("7".to_string()))
        .expect("encode the new amount");
    fields.updates = Some(vec![("amount".to_string(), amount)]);
    (OpCode::DocumentUpdate, fields)
}

/// The native SQL opcode's fields for `sql`.
pub(super) fn sql_fields(sql: &str) -> TextFields {
    TextFields {
        sql: Some(sql.to_string()),
        ..Default::default()
    }
}

/// How a pgwire request ended.
pub(super) fn pg_end(result: Result<(), tokio_postgres::Error>) -> End {
    match result {
        Ok(()) => End::Answered,
        Err(e) if e.code().map(|code| code.code()) == Some(SERIALIZATION_FAILURE) => End::Conflict,
        Err(e) => End::Failed(format!("{e:?}")),
    }
}

/// How a native request ended.
pub(super) fn native_end(response: &NativeResponse) -> End {
    match &response.error {
        None => End::Answered,
        Some(error) if error.code == SERIALIZATION_FAILURE => End::Conflict,
        Some(error) => End::Failed(format!("{}: {}", error.code, error.message)),
    }
}

/// Open the connection `route` writes `race` on. An extended write prepares
/// here, before any gate closes.
async fn open(server: &TestServer, route: Route, race: Race) -> Writer {
    match route {
        Route::Simple | Route::Extended => {
            let (client, handle) = server
                .connect_as("nodedb", "nodedb")
                .await
                .expect("connect the parked writer");
            if matches!(route, Route::Simple) {
                return Writer::Pg(client, handle);
            }
            let sql = if race.delete {
                "DELETE FROM ptv_post WHERE id = $1"
            } else {
                "UPDATE ptv_post SET amount = '7' WHERE id = $1"
            };
            let statement = client.prepare(sql).await.expect("prepare the write");
            Writer::Prepared(client, statement, handle)
        }
        Route::NativeSql | Route::NativeOp => {
            Writer::Native(open_trust_session(server.native_port, "nodedb").await)
        }
        Route::Http | Route::Ndjson => Writer::Http(server.http_port),
        Route::Ws => Writer::Ws(Box::new(ws_connect(server.http_port).await)),
    }
}

/// Send `race`'s write on `writer` and report how it ended.
async fn write(writer: Writer, route: Route, race: Race) -> End {
    match writer {
        Writer::Pg(client, _connection) => {
            pg_end(client.simple_query(&write_sql(race)).await.map(drop))
        }
        Writer::Prepared(client, statement, _connection) => {
            pg_end(client.execute(&statement, &[&race.key]).await.map(drop))
        }
        Writer::Native(mut stream) => {
            let (op, fields) = match route {
                Route::NativeOp => native_write(race),
                _ => (OpCode::Sql, sql_fields(&write_sql(race))),
            };
            let response =
                send_request_within(&mut stream, 2, op, fields, Some(PARKED_BOUND)).await;
            native_end(&response)
        }
        Writer::Http(port) if matches!(route, Route::Ndjson) => {
            ndjson_end(port, &write_sql(race)).await
        }
        Writer::Http(port) => http_end(port, &write_sql(race)).await,
        Writer::Ws(mut ws) => ws_end(&mut ws, &write_sql(race)).await,
    }
}

/// Run `sql` on the fixture's own connection. A statement that parks fails
/// the test.
pub(super) async fn exec(server: &TestServer, sql: &str) {
    tokio::time::timeout(Duration::from_secs(30), server.exec(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql} did not return while the write was parked"))
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// Assert that `end` is no error or a serialization conflict, and that the
/// balance of `acc` equals the source sum. `what` names the write.
pub(super) async fn assert_settled(server: &TestServer, end: &End, what: &str) {
    if let End::Failed(text) = end {
        panic!("{what} must commit or abort with a serialization conflict, got {text}");
    }
    let sum = source_sum(server).await;
    assert_eq!(
        balance(server, "acc").await,
        sum.to_string(),
        "{what} ended {end:?}: the balance must equal the source sum"
    );
}

/// Run `race`'s write on `route`, then the mover, and assert that it settled.
/// For a route with no gate between settlement and commit.
async fn race_unparked(route: Route, race: Race) {
    let server = sum_fixture().await;
    for sql in race.setup {
        exec(&server, sql).await;
    }
    let end = write(open(&server, route, race).await, route, race).await;
    exec(&server, race.mover).await;
    assert_settled(&server, &end, &format!("{route:?} {race:?}")).await;
}

/// Park `race`'s write on `route` at `gate`, run the mover, release the write,
/// and assert that it settled.
pub(super) async fn race_parked_at(gate: &str, route: Route, race: Race) {
    if gate == ROUTE_GATE && !matches!(route, Route::Simple | Route::Extended) {
        return race_unparked(route, race).await;
    }
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release");
    let server = parked_fixture_at(gate, &release).await;
    for sql in race.setup {
        exec(&server, sql).await;
    }
    let writer = open(&server, route, race).await;
    arm_gate(&release);
    let parked = tokio::spawn(write(writer, route, race));
    wait_parked_at(gate, &release, "absent-row write").await;
    exec(&server, race.mover).await;
    std::fs::write(&release, b"release").expect("release the parked write");
    let end = parked.await.expect("the parked write task");
    assert_settled(&server, &end, &format!("{route:?} {race:?}")).await;
}

/// Run the unbound and the absent-row update on `route` with no mover. Each
/// must commit.
pub(super) async fn uncontended(route: Route) {
    let server = sum_fixture().await;
    for race in [UNBOUND_UPDATE, ABSENT_UPDATE] {
        for sql in race.setup {
            exec(&server, sql).await;
        }
        let end = write(open(&server, route, race).await, route, race).await;
        assert!(
            matches!(end, End::Answered),
            "{route:?} {race:?} must commit, got {end:?}"
        );
    }
    assert_settled(&server, &End::Answered, &format!("{route:?}")).await;
}

/// Run the unbound update on `route` with no mover, and wait for the marker
/// of the Calvin stamp gate, which only a Calvin transaction passes.
pub(super) async fn probe_calvin_route(route: Route) {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release");
    let server = parked_fixture_at(STAMP_GATE, &release).await;
    for sql in UNBOUND_UPDATE.setup {
        exec(&server, sql).await;
    }
    let writer = open(&server, route, UNBOUND_UPDATE).await;
    arm_gate(&release);
    std::fs::write(&release, b"open").expect("open the gate again");
    let end = write(writer, route, UNBOUND_UPDATE).await;
    assert!(
        matches!(end, End::Answered),
        "{route:?} must commit, got {end:?}"
    );
    wait_parked_at(STAMP_GATE, &release, "unbound update").await;
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Simple, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Simple, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Simple, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Simple, ABSENT_DELETE).await;
}

/// A join-less update that commits after its join value lands ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_update_of_a_joinless_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Simple, JOINLESS_UPDATE).await;
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_extended_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Extended, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_extended_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Extended, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_extended_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Extended, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_extended_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Extended, ABSENT_DELETE).await;
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeSql, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeSql, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeSql, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeSql, ABSENT_DELETE).await;
}

/// A join-less update that commits after its join value lands ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_update_of_a_joinless_row_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeSql, JOINLESS_UPDATE).await;
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeOp, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeOp, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeOp, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeOp, ABSENT_DELETE).await;
}

/// A join-less update that commits after its join value lands ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_update_of_a_joinless_row_settles() {
    race_parked_at(ROUTE_GATE, Route::NativeOp, JOINLESS_UPDATE).await;
}

/// An unbound or absent-row update that answers 40001 with no mover aborts a write nothing raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_uncontended_absent_row_updates_commit() {
    uncontended(Route::Simple).await;
}

/// An unbound or absent-row update that answers 40001 with no mover aborts a write nothing raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_extended_uncontended_absent_row_updates_commit() {
    uncontended(Route::Extended).await;
}

/// An unbound or absent-row update that answers 40001 with no mover aborts a write nothing raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_uncontended_absent_row_updates_commit() {
    uncontended(Route::NativeSql).await;
}

/// An unbound or absent-row update that answers 40001 with no mover aborts a write nothing raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_uncontended_absent_row_updates_commit() {
    uncontended(Route::NativeOp).await;
}

/// An unbound update that skips Calvin carries no read, so no insert of its key aborts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_unbound_update_takes_calvin() {
    probe_calvin_route(Route::Simple).await;
}

/// An unbound update that skips Calvin carries no read, so no insert of its key aborts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_unbound_update_takes_calvin() {
    probe_calvin_route(Route::NativeSql).await;
}

/// An unbound update that skips Calvin carries no read, so no insert of its key aborts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_op_unbound_update_takes_calvin() {
    probe_calvin_route(Route::NativeOp).await;
}
