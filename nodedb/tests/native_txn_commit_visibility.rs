// SPDX-License-Identifier: BUSL-1.1

//! Issue #193: a row committed inside an explicit native-protocol transaction
//! (OpCode::Begin → SQL INSERT → OpCode::Commit) must be visible to every
//! read path — PK point lookups and filtered aggregates, not just full scans —
//! on the writing connection and on fresh connections.
//!
//! Pre-fix, `NativeTxnDp::dispatch_no_wal` routed the commit's `MetaOp`
//! tasks through the gateway without `task.vshard_id`; the gateway's
//! `primary_vshard` fallback sent them to vShard 0, so the commit batch was
//! durably applied on the wrong core. The bug needs (a) the gateway wired,
//! as production boot does, and (b) more than one Data Plane core, so that
//! vShard 0 and the collection's owning vShard live on different cores.

mod common;

use std::time::Duration;

use common::native_harness::{do_handshake, read_frame, write_frame};
use common::pgwire_harness::TestServer;

use nodedb_types::id::VShardId;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeRequest, NativeResponse, OpCode, RequestFields};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Send one request and read frames until the response for `seq` completes
/// (non-`Partial`), accumulating streamed rows. Unlike the harness
/// `send_request`, this cannot desync on multi-frame responses.
async fn request_drain(
    stream: &mut TcpStream,
    seq: u64,
    op: OpCode,
    fields: TextFields,
) -> NativeResponse {
    let req = NativeRequest {
        op,
        seq,
        fields: RequestFields::Text(fields),
    };
    let json = sonic_rs::to_vec(&req).expect("json encode");
    write_frame(stream, &json).await;

    let mut acc_rows: Vec<Vec<Value>> = Vec::new();
    loop {
        let payload = tokio::time::timeout(Duration::from_secs(5), read_frame(stream))
            .await
            .expect("timeout waiting for response")
            .expect("response frame");
        let mut resp: NativeResponse =
            sonic_rs::from_slice(&payload).expect("json decode NativeResponse");
        assert_eq!(resp.seq, seq, "response seq mismatch: {resp:?}");
        if let Some(rows) = resp.rows.take() {
            acc_rows.extend(rows);
        }
        if resp.status != ResponseStatus::Partial {
            resp.rows = Some(acc_rows);
            return resp;
        }
    }
}

async fn sql_drain(stream: &mut TcpStream, seq: u64, sql: &str) -> NativeResponse {
    request_drain(
        stream,
        seq,
        OpCode::Sql,
        TextFields {
            sql: Some(sql.into()),
            ..Default::default()
        },
    )
    .await
}

const NUM_CORES: usize = 4;

/// Pick a collection name whose owning vShard maps to a core other than
/// core 0 (round-robin: core = vshard % NUM_CORES), so a commit misrouted
/// to vShard 0 lands on a different core than the one reads consult.
fn collection_on_nonzero_core() -> String {
    for i in 0..64u32 {
        let name = format!("native_txn_vis_{i}");
        let vshard =
            VShardId::from_collection_in_database(nodedb::types::DatabaseId::DEFAULT, &name)
                .as_u32();
        if vshard as usize % NUM_CORES != 0 {
            return name;
        }
    }
    unreachable!("no candidate collection hashed off core 0");
}

async fn native_session(srv: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

fn first_cell(resp: &nodedb_types::protocol::NativeResponse) -> Option<Value> {
    resp.rows
        .as_ref()
        .and_then(|rows| rows.first())
        .and_then(|r| r.first())
        .cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_committed_txn_row_visible_to_point_and_filtered_reads() {
    let server = TestServer::start_multicores(NUM_CORES).await;

    // Wire the gateway exactly as production boot does (state_wiring.rs).
    // The bug only manifests on the gateway branch of
    // `NativeTxnDp::dispatch_no_wal`; without this, tests exercise the
    // non-gateway fallback, which routes commits correctly.
    {
        use std::sync::Arc;
        let gateway = Arc::new(nodedb::control::gateway::Gateway::new(Arc::clone(
            &server.shared,
        )));
        let invalidator = Arc::new(nodedb::control::gateway::PlanCacheInvalidator::new(
            &gateway.plan_cache,
        ));
        let _ = server.shared.gateway.set(gateway);
        let _ = server.shared.gateway_invalidator.set(invalidator);
    }

    let coll = collection_on_nonzero_core();

    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, name STRING) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();

    let mut stream = native_session(&server).await;

    // Explicit transaction via protocol opcodes, as in the issue repro.
    let begin = request_drain(&mut stream, 1, OpCode::Begin, TextFields::default()).await;
    assert_eq!(begin.status, ResponseStatus::Ok, "BEGIN op: {begin:?}");

    let insert = sql_drain(
        &mut stream,
        2,
        &format!("INSERT INTO {coll} (id, name) VALUES ('a1', 'alpha')"),
    )
    .await;
    assert_eq!(insert.status, ResponseStatus::Ok, "in-tx INSERT: {insert:?}");

    let commit = request_drain(&mut stream, 3, OpCode::Commit, TextFields::default()).await;
    assert_eq!(commit.status, ResponseStatus::Ok, "COMMIT op: {commit:?}");

    // PK point lookup on the writing connection.
    let point = sql_drain(&mut stream, 4, &format!("SELECT id FROM {coll} WHERE id = 'a1'")).await;
    assert_ne!(point.status, ResponseStatus::Error, "point lookup: {point:?}");
    assert_eq!(
        first_cell(&point),
        Some(Value::String("a1".into())),
        "PK point lookup must see the committed row: {point:?}"
    );

    // Filtered aggregate.
    let count = sql_drain(
        &mut stream,
        5,
        &format!("SELECT count(*) FROM {coll} WHERE name = 'alpha'"),
    )
    .await;
    assert_ne!(count.status, ResponseStatus::Error, "filtered count: {count:?}");
    assert_eq!(
        first_cell(&count),
        Some(Value::Integer(1)),
        "filtered count(*) must see the committed row: {count:?}"
    );

    // Full scan — control: the row is durably stored. Streams (Partial
    // frames), so it runs last on this connection.
    let scan = sql_drain(&mut stream, 6, &format!("SELECT id FROM {coll}")).await;
    assert_ne!(scan.status, ResponseStatus::Error, "scan: {scan:?}");
    assert_eq!(
        first_cell(&scan),
        Some(Value::String("a1".into())),
        "full scan must see the committed row: {scan:?}"
    );

    // Fresh connection — the miss must not persist across sessions.
    let mut fresh = native_session(&server).await;
    let point2 = sql_drain(&mut fresh, 1, &format!("SELECT id FROM {coll} WHERE id = 'a1'")).await;
    assert_ne!(
        point2.status,
        ResponseStatus::Error,
        "fresh point lookup: {point2:?}"
    );
    assert_eq!(
        first_cell(&point2),
        Some(Value::String("a1".into())),
        "PK point lookup on a fresh connection must see the committed row: {point2:?}"
    );
}
