// SPDX-License-Identifier: BUSL-1.1

//! A KV counter fault over the native protocol keeps its collection.
//!
//! `KV_INCR` on a raw value that is not a decimal integer, or past the i64
//! range, is refused by the Data Plane with the collection it ran on. The
//! native frame carries the data-exception SQLSTATE pgwire sends, the same
//! message text, the numeric code, and structured details that name the
//! collection.

use nodedb_test_support::native_harness::{do_handshake, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::error::{ErrorCode, ErrorDetails, NodeDbError, sqlstate};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{ErrorPayload, HelloFrame};
use tokio::net::TcpStream;

const COLLECTION: &str = "native_counters";

async fn native_session(srv: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

async fn seeded_server() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {COLLECTION} (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('name', 'abc')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('max', '{}')",
            i64::MAX
        ))
        .await
        .unwrap();
    server
}

async fn refusal(stream: &mut TcpStream, seq: u64, sql: &str) -> ErrorPayload {
    let resp = send_sql(stream, seq, sql).await;
    assert_eq!(resp.status, ResponseStatus::Error, "{sql} must be refused");
    resp.error.expect("error payload expected")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_counter_parse_fault_names_the_collection() {
    let server = seeded_server().await;
    let mut stream = native_session(&server).await;

    let err = refusal(
        &mut stream,
        1,
        &format!("SELECT KV_INCR('{COLLECTION}', 'name', 1)"),
    )
    .await;
    assert_eq!(err.code, sqlstate::INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.ndb_code, ErrorCode::TYPE_MISMATCH.0);
    assert_eq!(
        err.message,
        format!("value is not an integer or out of range on {COLLECTION}")
    );
    let expected = ErrorDetails::TypeMismatch {
        collection: COLLECTION.into(),
    };
    assert_eq!(err.details.as_ref(), Some(&expected));

    let typed = NodeDbError::from_wire_with_details(
        ErrorCode(err.ndb_code),
        err.message.clone(),
        err.details.clone(),
    );
    assert_eq!(typed.details(), &expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_counter_overflow_names_the_collection() {
    let server = seeded_server().await;
    let mut stream = native_session(&server).await;

    let err = refusal(
        &mut stream,
        1,
        &format!("SELECT KV_INCR('{COLLECTION}', 'max', 1)"),
    )
    .await;
    assert_eq!(err.code, sqlstate::NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.ndb_code, ErrorCode::OVERFLOW.0);
    assert_eq!(
        err.message,
        format!("increment or decrement would overflow on {COLLECTION}")
    );
    assert_eq!(
        err.details,
        Some(ErrorDetails::Overflow {
            collection: COLLECTION.into(),
        })
    );
}
