// SPDX-License-Identifier: BUSL-1.1

//! An autocommit native KV atomic opcode is WAL-durable, and replay
//! reproduces it.
//!
//! `KvIncr`, `KvIncrFloat`, `KvCas` and `KvGetSet` reach the gateway as direct
//! ops. On a node with no Raft proposer the gateway applies them on its own
//! cores, and it must give each one the durable route: the write funnel
//! appends a WAL record for it. The harness restores a core from WAL replay
//! alone, so the value read after the restart is the value replay computed.
//! The native protocol has no transfer opcode, so `TRANSFER` is covered by
//! the SQL cases alone.

use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use tokio::net::TcpStream;

/// The MessagePack encoding of the short string `s`.
fn fixstr(s: &str) -> Vec<u8> {
    assert!(s.len() < 32, "a fixstr holds at most 31 bytes: {s}");
    let mut encoded = vec![0xa0 | s.len() as u8];
    encoded.extend_from_slice(s.as_bytes());
    encoded
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Whether the WAL holds a record of the KV op `op` on `collection`.
fn wal_holds(server: &TestServer, op: &str, collection: &str) -> bool {
    server.shared.wal.sync().expect("sync the WAL");
    let records = server.shared.wal.replay().expect("read the WAL");
    let (op, collection) = (fixstr(op), fixstr(collection));
    records
        .iter()
        .any(|record| contains(&record.payload, &op) && contains(&record.payload, &collection))
}

async fn native_session(server: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", server.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

fn ok(resp: NativeResponse, what: &str) -> NativeResponse {
    assert_ne!(resp.status, ResponseStatus::Error, "{what}: {resp:?}");
    resp
}

fn fields(collection: &str, key: &str) -> TextFields {
    TextFields {
        collection: Some(collection.to_string()),
        key: Some(key.to_string()),
        ..Default::default()
    }
}

/// The first column of every row `sql` returns over pgwire.
async fn column(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The JSON document a single-row KV function returns over pgwire.
async fn json(server: &TestServer, sql: &str) -> serde_json::Value {
    let rows = column(server, sql).await;
    assert_eq!(rows.len(), 1, "{sql} returns one row: {rows:?}");
    serde_json::from_str(&rows[0]).unwrap_or_else(|e| panic!("{sql} returns JSON: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_kv_atomic_opcodes_replay_to_the_values_they_wrote() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    for (seq, sql) in [
        "CREATE COLLECTION nat_ctr (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
        "CREATE COLLECTION nat_score (key TEXT PRIMARY KEY, score FLOAT) WITH (engine='kv')",
        "CREATE COLLECTION nat_swap (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')",
    ]
    .into_iter()
    .enumerate()
    {
        ok(send_sql(&mut stream, seq as u64 + 1, sql).await, sql);
    }

    let incr = TextFields {
        incr_delta: Some(5),
        ..fields("nat_ctr", "k")
    };
    ok(
        send_request(&mut stream, 10, OpCode::KvIncr, incr.clone()).await,
        "KvIncr",
    );
    ok(
        send_request(&mut stream, 11, OpCode::KvIncr, incr).await,
        "KvIncr",
    );
    for (seq, delta) in [(12, "0.1"), (13, "0.2")] {
        let incr_float = TextFields {
            incr_float_delta: Some(delta.to_string()),
            ..fields("nat_score", "s")
        };
        ok(
            send_request(&mut stream, seq, OpCode::KvIncrFloat, incr_float).await,
            "KvIncrFloat",
        );
    }
    let cas = TextFields {
        expected: Some(Vec::new()),
        new_value: Some(b"idle".to_vec()),
        ..fields("nat_swap", "state")
    };
    ok(
        send_request(&mut stream, 14, OpCode::KvCas, cas).await,
        "KvCas",
    );
    let getset = TextFields {
        new_value: Some(b"first-token".to_vec()),
        ..fields("nat_swap", "tok")
    };
    ok(
        send_request(&mut stream, 15, OpCode::KvGetSet, getset).await,
        "KvGetSet",
    );

    assert!(wal_holds(&server, "kv_incr", "nat_ctr"));
    assert!(wal_holds(&server, "kv_incr_float", "nat_score"));
    assert!(wal_holds(&server, "kv_cas", "nat_swap"));
    assert!(wal_holds(&server, "kv_getset", "nat_swap"));
    let counter = column(&server, "SELECT n FROM nat_ctr WHERE key = 'k'").await;
    assert_eq!(counter, vec!["10".to_string()]);
    let score = column(&server, "SELECT score FROM nat_score WHERE key = 's'").await;
    drop(stream);

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        column(&server, "SELECT n FROM nat_ctr WHERE key = 'k'").await,
        counter,
        "replay must rebuild the counter both KvIncr ops moved"
    );
    assert_eq!(
        column(&server, "SELECT score FROM nat_score WHERE key = 's'").await,
        score,
        "replay must add the same decimal digits both KvIncrFloat ops added"
    );
    let cas = json(
        &server,
        "SELECT KV_CAS('nat_swap', 'state', 'idle', 'ended')",
    )
    .await;
    assert_eq!(
        cas["success"], true,
        "replay must restore the value KvCas set: {cas}"
    );
    let getset = json(
        &server,
        "SELECT KV_GETSET('nat_swap', 'tok', 'second-token')",
    )
    .await;
    let old = getset["old_value"]
        .as_str()
        .unwrap_or_else(|| panic!("replay must restore the KvGetSet value: {getset}"));
    let old = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, old)
        .expect("old_value is base64");
    assert_eq!(old, b"first-token");
}
