// SPDX-License-Identifier: BUSL-1.1

//! Native direct-op writes under a row-predicate write policy, on the source
//! of a materialized sum.
//!
//! The policy makes each document write governed. A governed write must
//! settle its balance before it resolves, as on the SQL path. A write that
//! skips settlement carries no sum target. On a cross-shard pair the Data
//! Plane then drops the delta and the balance stays short. On a co-resident
//! pair the write aborts.
//!
//! The superuser bypasses row-level security, so every write under test runs
//! as [`WRITER`]. Each test checks the source sum before the balance, so a
//! refused write cannot pass as a settled one.

use std::time::Duration;

use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb_test_support::native_harness::{
    RESPONSE_BOUND, open_trust_session, send_request_within, send_sql,
};
use nodedb_types::Value;
use nodedb_types::id::DatabaseId;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};
use tokio::net::TcpStream;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

#[cfg(feature = "failpoints")]
#[path = "sum_native_governed_direct_op_race.rs"]
mod race;

/// The sum of the `acc` postings, read from the source rows.
async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text(&format!(
            "SELECT amount FROM {POSTINGS} WHERE account_id = 'acc'"
        ))
        .await
        .expect("read the source postings")
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

/// The user every write under test runs as. `readwrite` lets it write the
/// source and the target, so a refusal is the policy's.
const WRITER: &str = "governed_writer";
const PASSWORD: &str = "governed-sum-secret";
/// The amount the write policy refuses. Every other amount passes.
const REFUSED_AMOUNT: &str = "99";
/// The SQLSTATE of a row-level-security refusal.
const INSUFFICIENT_PRIVILEGE: &str = "42501";
/// The cross-shard sum's source collection.
const POSTINGS: &str = "ptv_post";
/// A target and source on one vShard: the balance rides the source write.
const CO_RESIDENT: (&str, &str) = ("uo_accounts", "uo_entries");

/// Create [`WRITER`] and a row-predicate write policy on `source`.
async fn govern(server: &TestServer, source: &str) {
    for sql in [
        format!("CREATE USER {WRITER} PASSWORD '{PASSWORD}'"),
        format!("GRANT ROLE readwrite TO {WRITER}"),
        format!(
            "CREATE RLS POLICY {source}_w ON {source} FOR WRITE \
             USING (amount != '{REFUSED_AMOUNT}')"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    }
}

/// The cross-shard sum fixture, governed. `acc` holds 15: `e0` 10, `e1` 5.
async fn governed_fixture() -> TestServer {
    let server = sum_fixture().await;
    govern(&server, POSTINGS).await;
    server
}

/// A native session of [`WRITER`] with a running sequence number. Seq 1 is
/// the handshake.
struct Writer {
    stream: TcpStream,
    seq: u64,
}

impl Writer {
    async fn open(server: &TestServer) -> Self {
        let stream = open_trust_session(server.native_port, WRITER).await;
        Self { stream, seq: 1 }
    }

    async fn sql(&mut self, sql: &str) -> NativeResponse {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await
    }

    /// Send `op` and wait at most `bound` for its answer.
    async fn op_within(
        &mut self,
        op: OpCode,
        fields: TextFields,
        bound: Duration,
    ) -> NativeResponse {
        self.seq += 1;
        send_request_within(&mut self.stream, self.seq, op, fields, Some(bound)).await
    }

    async fn op(&mut self, op: OpCode, fields: TextFields) -> NativeResponse {
        self.op_within(op, fields, RESPONSE_BOUND).await
    }
}

/// How a response ended, for an assert message.
fn describe(response: &NativeResponse) -> String {
    match &response.error {
        Some(error) => format!("{:?} {}: {}", response.status, error.code, error.message),
        None => format!("{:?}", response.status),
    }
}

/// A msgpack text value.
fn text(value: &str) -> Vec<u8> {
    nodedb_types::value_to_msgpack(&Value::String(value.to_string())).expect("encode a text value")
}

/// A filter that matches rows whose `field` equals `value`.
fn eq_filter(field: &str, value: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: field.to_string(),
        op: FilterOp::Eq,
        value: Value::String(value.to_string()),
        clauses: Vec::new(),
        expr: None,
    }])
    .expect("encode the filter")
}

/// `DocumentUpdate`: set `amount` of row `id` in `collection`.
fn update_fields(collection: &str, id: &str, amount: &str) -> TextFields {
    TextFields {
        collection: Some(collection.to_string()),
        document_id: Some(id.to_string()),
        updates: Some(vec![("amount".to_string(), text(amount))]),
        ..Default::default()
    }
}

/// `DocumentBulkUpdate`: set `amount` of every row whose `field` is `value`.
fn bulk_update_fields(field: &str, value: &str, amount: &str) -> TextFields {
    TextFields {
        collection: Some(POSTINGS.to_string()),
        filters: Some(eq_filter(field, value)),
        updates: Some(vec![("amount".to_string(), text(amount))]),
        ..Default::default()
    }
}

/// Assert that `response` is no error. `what` names the request.
fn assert_applied(response: &NativeResponse, what: &str) {
    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "{what} must apply: {}",
        describe(response)
    );
}

/// Assert that `response` succeeded and the write settled: the postings of
/// `acc` sum to `expected`, and so does its balance. `what` names the write.
async fn assert_settled(server: &TestServer, response: &NativeResponse, expected: i64, what: &str) {
    assert_applied(response, what);
    assert_eq!(
        source_sum(server).await,
        expected,
        "{what}: the source rows take the write"
    );
    assert_eq!(
        balance(server, "acc").await,
        expected.to_string(),
        "{what}: the balance must equal the source sum"
    );
}

/// Run `op` as [`WRITER`] on the governed cross-shard fixture and assert
/// that it settled to `expected`.
async fn governed_op_settles(op: OpCode, fields: TextFields, expected: i64, what: &str) {
    let server = governed_fixture().await;
    let mut writer = Writer::open(&server).await;
    let response = writer.op(op, fields).await;
    assert_settled(&server, &response, expected, what).await;
}

/// The premise of every test below: the SQL opcode settles the same governed
/// update through the statement path. A failure here means Calvin does not
/// apply a governed write, not that the direct op skips settlement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_sql_update_of_a_sum_source_moves_the_balance() {
    let server = governed_fixture().await;
    let mut writer = Writer::open(&server).await;
    let response = writer
        .sql("UPDATE ptv_post SET amount = '1' WHERE id = 'e0'")
        .await;
    assert_settled(&server, &response, 6, "the SQL update").await;
}

/// `e0` goes from 10 to 1. Settlement ships -9 to `acc`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_point_update_of_a_sum_source_moves_the_balance() {
    governed_op_settles(
        OpCode::DocumentUpdate,
        update_fields(POSTINGS, "e0", "1"),
        6,
        "the point update",
    )
    .await;
}

/// Another writer moved `e0` from 10 to 20, so `acc` holds 25. The governed
/// write reads that image: `e0` goes from 20 to 1 and settlement ships -19.
/// The native route has no gate between settlement and commit, so this cell
/// asserts the outcome of the order, not a write parked between the two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_point_update_after_a_moved_source_row_moves_the_balance() {
    let server = governed_fixture().await;
    server
        .exec("UPDATE ptv_post SET amount = '20' WHERE id = 'e0'")
        .await
        .expect("move the source row");
    assert_eq!(
        balance(&server, "acc").await,
        "25",
        "the first change settles its own delta"
    );
    let mut writer = Writer::open(&server).await;
    let response = writer
        .op(OpCode::DocumentUpdate, update_fields(POSTINGS, "e0", "1"))
        .await;
    assert_settled(&server, &response, 6, "the point update").await;
}

/// `e1` leaves. Settlement ships -5 to `acc`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_point_delete_of_a_sum_source_moves_the_balance() {
    governed_op_settles(
        OpCode::PointDelete,
        TextFields {
            collection: Some(POSTINGS.to_string()),
            document_id: Some("e1".to_string()),
            ..Default::default()
        },
        10,
        "the point delete",
    )
    .await;
}

/// The upsert merges into the stored `e0`: 10 becomes 4. Settlement ships -6.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_upsert_of_a_sum_source_moves_the_balance() {
    let body = nodedb_types::json_to_msgpack(&serde_json::json!({
        "id": "e0",
        "account_id": "acc",
        "amount": "4",
    }))
    .expect("encode the upsert body");
    governed_op_settles(
        OpCode::DocumentUpsert,
        TextFields {
            collection: Some(POSTINGS.to_string()),
            document_id: Some("e0".to_string()),
            data: Some(body),
            ..Default::default()
        },
        9,
        "the upsert",
    )
    .await;
}

/// Both postings of `acc` go to 1. Settlement ships -13.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_bulk_update_of_a_sum_source_moves_the_balance() {
    governed_op_settles(
        OpCode::DocumentBulkUpdate,
        bulk_update_fields("account_id", "acc", "1"),
        2,
        "the bulk update",
    )
    .await;
}

/// The predicate matches `e1` only. Settlement ships -5.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_bulk_delete_of_a_sum_source_moves_the_balance() {
    governed_op_settles(
        OpCode::DocumentBulkDelete,
        TextFields {
            collection: Some(POSTINGS.to_string()),
            filters: Some(eq_filter("id", "e1")),
            ..Default::default()
        },
        10,
        "the bulk delete",
    )
    .await;
}

/// The policy still decides a write that settles first. A refused write
/// changes neither the row nor the balance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_write_the_policy_refuses_changes_neither_row_nor_balance() {
    let server = governed_fixture().await;
    let mut writer = Writer::open(&server).await;
    let response = writer
        .op(
            OpCode::DocumentUpdate,
            update_fields(POSTINGS, "e0", REFUSED_AMOUNT),
        )
        .await;
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some(INSUFFICIENT_PRIVILEGE),
        "the policy must refuse the write: {}",
        describe(&response)
    );
    assert_eq!(source_sum(&server).await, 15, "the refused row stays");
    assert_eq!(balance(&server, "acc").await, "15", "the balance stays");
}

/// Inside a block the write and its balance stage together, so `ROLLBACK`
/// undoes both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_point_update_in_a_block_rolls_back_with_its_balance() {
    let server = governed_fixture().await;
    let mut writer = Writer::open(&server).await;
    assert_applied(&writer.sql("BEGIN").await, "BEGIN");
    let update = writer
        .op(OpCode::DocumentUpdate, update_fields(POSTINGS, "e0", "1"))
        .await;
    assert_applied(&update, "the update inside the block");
    assert_applied(&writer.sql("ROLLBACK").await, "ROLLBACK");
    assert_eq!(source_sum(&server).await, 15, "ROLLBACK restores e0 to 10");
    assert_eq!(
        balance(&server, "acc").await,
        "15",
        "ROLLBACK keeps the balance"
    );
}

fn vshard(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// The premise of the co-resident test: its pair shares one vShard.
#[test]
fn the_co_resident_pair_shares_a_vshard() {
    assert_eq!(vshard(CO_RESIDENT.0), vshard(CO_RESIDENT.1));
}

/// The one text value `sql` reads.
async fn one_text(server: &TestServer, sql: &str) -> String {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    match rows.as_slice() {
        [value] => value.clone(),
        other => panic!("one row from {sql}, got {other:?}"),
    }
}

/// A co-resident target folds its balance with the source write, from the
/// target the write resolved. A governed write must resolve it first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_governed_point_update_of_a_co_resident_sum_source_moves_the_balance() {
    let (accounts, entries) = CO_RESIDENT;
    let server = TestServer::start().await;
    for sql in [
        format!(
            "CREATE COLLECTION {accounts} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "ALTER COLLECTION {accounts} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {entries} \
             ON {entries}.account_id = {accounts}.id VALUE {entries}.amount"
        ),
        format!("INSERT INTO {accounts} (id, owner, balance) VALUES ('acc', 'alice', '0')"),
        format!("INSERT INTO {entries} (id, account_id, amount) VALUES ('e0', 'acc', '10')"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    }
    govern(&server, entries).await;
    let balance_sql = format!("SELECT balance FROM {accounts} WHERE id = 'acc'");
    assert_eq!(
        one_text(&server, &balance_sql).await,
        "10",
        "the seed lands"
    );

    let mut writer = Writer::open(&server).await;
    let response = writer
        .op(OpCode::DocumentUpdate, update_fields(entries, "e0", "1"))
        .await;
    assert_applied(&response, "the co-resident update");
    assert_eq!(
        one_text(
            &server,
            &format!("SELECT amount FROM {entries} WHERE id = 'e0'")
        )
        .await,
        "1",
        "the source row takes the write"
    );
    assert_eq!(
        one_text(&server, &balance_sql).await,
        "1",
        "the balance must equal the source sum"
    );
}
