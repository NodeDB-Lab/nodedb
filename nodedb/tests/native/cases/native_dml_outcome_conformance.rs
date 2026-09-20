// SPDX-License-Identifier: BUSL-1.1

//! `rows_affected` and `command` on the native protocol are the statement's
//! folded DML outcome — the same fold pgwire renders as its command tag.
//!
//! A count-bearing task contributes the count and verb its Data Plane
//! payload reports. An opaque task (a buffered write, index maintenance, a
//! computed-value payload) contributes nothing. Neither field is ever
//! synthesised from the number of dispatched tasks, and a verb with no count
//! (`TRUNCATE`) reports the verb alone.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode, TextFields};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Open a native session and complete the handshake.
async fn native_session(server: &NativeTestServer) -> TcpStream {
    let (stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

/// Run `sql` and check it succeeded.
async fn ok(stream: &mut TcpStream, seq: u64, sql: &str) -> NativeResponse {
    let resp = send_sql(stream, seq, sql).await;
    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "statement must succeed: {sql}: {resp:?}"
    );
    resp
}

/// The `(rows_affected, command)` pair a response reports.
fn outcome(resp: &NativeResponse) -> (Option<u64>, Option<&str>) {
    (resp.rows_affected, resp.command.as_deref())
}

/// A multi-row `INSERT` plans one task per row; the wire answers with ONE
/// summed count under the `INSERT` verb, never the last task's `1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_multi_row_insert_reports_one_summed_count_and_insert_command() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_multi (id STRING NOT NULL PRIMARY KEY, n INT) \
         WITH (engine='document_strict')",
    )
    .await;
    let insert = ok(
        &mut stream,
        2,
        "INSERT INTO ndo_multi (id, n) VALUES ('a', 1), ('b', 2), ('c', 3)",
    )
    .await;
    assert_eq!(
        outcome(&insert),
        (Some(3), Some("INSERT")),
        "three rows fold into one INSERT 3: {insert:?}"
    );
}

/// A KV predicate `DELETE` inside a transaction is staged at statement time:
/// it reports the rows it matched, the transaction's own reads hide them,
/// and COMMIT applies them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_kv_predicate_delete_in_transaction_reports_real_count_and_commits() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_buf (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;
    ok(
        &mut stream,
        2,
        "INSERT INTO ndo_buf (key, n) VALUES ('a', 1), ('b', 5)",
    )
    .await;

    ok(&mut stream, 3, "BEGIN").await;
    // A KV predicate delete stages its matched rows at statement time, so
    // the statement reports the rows it removed and COMMIT applies them.
    let staged = ok(&mut stream, 4, "DELETE FROM ndo_buf WHERE n > 2").await;
    assert_eq!(
        outcome(&staged),
        (Some(1), Some("DELETE")),
        "a staged predicate delete reports the rows it matched: {staged:?}"
    );
    let in_txn = ok(&mut stream, 5, "SELECT key FROM ndo_buf ORDER BY key").await;
    assert_eq!(
        in_txn.rows,
        Some(vec![vec![Value::String("a".into())]]),
        "the transaction's own read hides the staged delete: {in_txn:?}"
    );
    ok(&mut stream, 6, "COMMIT").await;

    let remaining = ok(&mut stream, 7, "SELECT key FROM ndo_buf ORDER BY key").await;
    assert_eq!(
        remaining.rows,
        Some(vec![vec![Value::String("a".into())]]),
        "COMMIT applies the staged delete: {remaining:?}"
    );
}

/// A staged predicate `UPDATE` reports the rows it matched in BASE ∪
/// OVERLAY. Matching nothing is `UPDATE 0`, never the dispatcher's `1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_staged_update_matching_no_rows_reports_zero() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_upd0 (id STRING NOT NULL PRIMARY KEY, n INT) \
         WITH (engine='document_strict')",
    )
    .await;
    ok(
        &mut stream,
        2,
        "INSERT INTO ndo_upd0 (id, n) VALUES ('a', 1)",
    )
    .await;

    ok(&mut stream, 3, "BEGIN").await;
    let update = ok(&mut stream, 4, "UPDATE ndo_upd0 SET n = 9 WHERE n = 12345").await;
    assert_eq!(
        outcome(&update),
        (Some(0), Some("UPDATE")),
        "a staged UPDATE matching no rows reports UPDATE 0: {update:?}"
    );
    ok(&mut stream, 5, "ROLLBACK").await;
}

/// `SELECT KV_INCR(...)` inside a transaction stages a computed VALUE. The
/// value rides in the row; the statement carries no count and no verb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_kv_incr_in_transaction_returns_the_value_not_a_row_count() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_incr (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;
    ok(
        &mut stream,
        2,
        "INSERT INTO ndo_incr (key, n) VALUES ('ctr', 5)",
    )
    .await;

    ok(&mut stream, 3, "BEGIN").await;
    let incr = ok(&mut stream, 4, "SELECT KV_INCR('ndo_incr', 'ctr', 3)").await;
    assert_eq!(
        outcome(&incr),
        (None, None),
        "a computed-value statement reports no count and no verb: {incr:?}"
    );
    let rows = incr
        .rows
        .expect("KV_INCR answers with its computed value row");
    let cell = rows
        .first()
        .and_then(|row| row.first())
        .expect("one value cell");
    let text = match cell {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    assert!(
        text.contains('8'),
        "KV_INCR must return the computed value 8, got {text}"
    );
    ok(&mut stream, 5, "ROLLBACK").await;
}

/// A raw `OpCode::PointPut` on a KV collection is the SQL `UPSERT` shape. The
/// direct-op path reads the count the handler reported and names the verb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_direct_op_kv_put_reports_real_count_and_upsert_command() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_put (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;

    let value =
        nodedb_types::json_to_msgpack(&serde_json::json!({ "n": 7 })).expect("encode KV value");
    let put = send_request(
        &mut stream,
        2,
        OpCode::PointPut,
        TextFields {
            collection: Some("ndo_put".to_string()),
            document_id: Some("k1".to_string()),
            data: Some(value),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        put.status,
        ResponseStatus::Ok,
        "direct-op put must succeed: {put:?}"
    );
    assert_eq!(
        outcome(&put),
        (Some(1), Some("UPSERT")),
        "a direct-op KV put reports the handler's count under UPSERT: {put:?}"
    );
}

/// `TRUNCATE` is the one verb with no count: the statement names the verb
/// and leaves `rows_affected` unset, matching pgwire's bare `TRUNCATE` tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_truncate_reports_truncate_command_without_count() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_trunc (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless')",
    )
    .await;
    ok(
        &mut stream,
        2,
        "INSERT INTO ndo_trunc (id, v) VALUES ('a', 'x'), ('b', 'y')",
    )
    .await;

    let truncate = ok(&mut stream, 3, "TRUNCATE TABLE ndo_trunc").await;
    assert_eq!(
        outcome(&truncate),
        (None, Some("TRUNCATE")),
        "TRUNCATE names its verb and carries no count: {truncate:?}"
    );
}

/// `INSERT ... ON CONFLICT DO UPDATE` reports the verb the handler resolved
/// to: `INSERT` on first write, `UPDATE` when the key already existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_insert_on_conflict_update_reports_update_verb_on_conflict() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_conflict (k TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;

    let sql = "INSERT INTO ndo_conflict (k, n) VALUES ('a', 1) \
               ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.n";
    let first = ok(&mut stream, 2, sql).await;
    assert_eq!(
        outcome(&first),
        (Some(1), Some("INSERT")),
        "first write inserts: {first:?}"
    );
    let second = ok(&mut stream, 3, sql).await;
    assert_eq!(
        outcome(&second),
        (Some(1), Some("UPDATE")),
        "the conflicting write updates: {second:?}"
    );
}

/// An opaque execution (vector index config, no row payload) reports no
/// count and no verb — never a `1` for the one task that ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_execution_statement_reports_no_synthesized_count() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    ok(
        &mut stream,
        1,
        "CREATE COLLECTION ndo_exec WITH (engine='document_schemaless')",
    )
    .await;

    let set_params = send_request(
        &mut stream,
        2,
        OpCode::VectorSetParams,
        TextFields {
            collection: Some("ndo_exec".to_string()),
            field_name: Some("emb".to_string()),
            vector_dim: Some(4),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        set_params.status,
        ResponseStatus::Ok,
        "vector params op must succeed: {set_params:?}"
    );
    assert_eq!(
        outcome(&set_params),
        (None, None),
        "an opaque execution reports neither a count nor a verb: {set_params:?}"
    );
}
