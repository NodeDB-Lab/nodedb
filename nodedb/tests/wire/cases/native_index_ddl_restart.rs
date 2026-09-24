// SPDX-License-Identifier: BUSL-1.1

//! Indexes made by the native `KvRegisterIndex` and `DocumentRegister`
//! opcodes are catalog indexes: they survive a restart, SQL lists them, and
//! SQL `DROP INDEX` removes them.

use nodedb_test_support::native_harness::{do_handshake, send_request};
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{NativeResponse, OpCode};
use tokio::net::TcpStream;

use crate::harness::TestServer;

async fn native_session(server: &TestServer) -> TcpStream {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.native_port);
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

async fn run_op(stream: &mut TcpStream, seq: u64, op: OpCode, fields: TextFields) {
    let response: NativeResponse = send_request(stream, seq, op, fields).await;
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "{op:?} must succeed: {response:?}"
    );
}

async fn lists(server: &TestServer, index: &str) -> bool {
    server
        .query_text("SHOW INDEXES")
        .await
        .unwrap()
        .iter()
        .any(|name| name == index)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_registered_indexes_survive_a_restart() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION nat_rs_kv (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO nat_rs_kv (key, bucket) VALUES ('s1', 'A')")
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    run_op(
        &mut stream,
        1,
        OpCode::KvRegisterIndex,
        TextFields {
            collection: Some("nat_rs_kv".to_string()),
            field: Some("bucket".to_string()),
            ..TextFields::default()
        },
    )
    .await;
    run_op(
        &mut stream,
        2,
        OpCode::DocumentRegister,
        TextFields {
            collection: Some("nat_rs_doc".to_string()),
            index_paths: Some(vec!["region".to_string()]),
            ..TextFields::default()
        },
    )
    .await;
    drop(stream);
    assert!(lists(&server, "idx_nat_rs_kv_bucket").await);
    assert!(lists(&server, "idx_nat_rs_doc_region").await);

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    assert!(
        lists(&server, "idx_nat_rs_kv_bucket").await,
        "the KV index is in the catalog after a restart"
    );
    assert!(
        lists(&server, "idx_nat_rs_doc_region").await,
        "the registered document index is in the catalog after a restart"
    );
    assert_eq!(
        server
            .query_text("SELECT key FROM nat_rs_kv WHERE bucket = 'A'")
            .await
            .unwrap(),
        vec!["s1".to_string()]
    );

    server
        .exec("DROP INDEX idx_nat_rs_kv_bucket")
        .await
        .expect("SQL drops the index the opcode made");
    assert!(!lists(&server, "idx_nat_rs_kv_bucket").await);
}
