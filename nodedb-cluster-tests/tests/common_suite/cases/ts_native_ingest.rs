// SPDX-License-Identifier: BUSL-1.1

//! Raw ILP ingest into a timeseries collection over the native protocol.
//!
//! A timeseries collection takes no undeclared column over SQL: the planner
//! closes its schema to the catalog's columns. A fresh column, and so a type
//! conflict on one, comes only from a raw ILP line. The native
//! `TimeseriesIngest` opcode carries raw ILP lines on a session, inside or
//! outside a transaction. It changes the live schema only. The ILP listener
//! also projects each new column into the catalog, and that catalog change
//! drains every descriptor lease an open transaction holds on the collection.

use std::time::Duration;

use nodedb_test_support::native_harness::{do_handshake, response_bound, send_request_within};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use tokio::net::TcpStream;

use crate::common::cluster_harness::TestClusterNode;

/// How long a retried ingest waits for the collection's group to serve it.
const ACCEPT_WAIT: Duration = Duration::from_secs(20);

/// A native protocol session on `node`, as the harness superuser.
pub(super) async fn native_session(node: &TestClusterNode) -> TcpStream {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), node.native_port);
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .unwrap_or_else(|e| panic!("native handshake with node {}: {e:?}", node.node_id));
    stream
}

/// Ingest the raw ILP `lines` into `collection` through the native
/// `TimeseriesIngest` opcode, waiting at most [`response_bound`] for the
/// response.
pub(super) async fn ingest_native(
    stream: &mut TcpStream,
    seq: u64,
    collection: &str,
    lines: &str,
) -> NativeResponse {
    ingest_native_within(stream, seq, collection, lines, Some(response_bound())).await
}

/// [`ingest_native`] waiting at most `bound` for the response, or without
/// limit when `bound` is `None`. A test that parks the apply on purpose
/// bounds the wait itself.
pub(super) async fn ingest_native_within(
    stream: &mut TcpStream,
    seq: u64,
    collection: &str,
    lines: &str,
    bound: Option<Duration>,
) -> NativeResponse {
    send_request_within(
        stream,
        seq,
        OpCode::TimeseriesIngest,
        TextFields {
            collection: Some(collection.to_string()),
            payload: Some(lines.as_bytes().to_vec()),
            format: Some("ilp".to_string()),
            ..Default::default()
        },
        bound,
    )
    .await
}

/// Ingest `lines` through a fresh session on `node`, retrying while the
/// collection's group elects or mounts. Panics once [`ACCEPT_WAIT`] passes.
pub(super) async fn ingest_until_accepted(
    node: &TestClusterNode,
    collection: &str,
    lines: &str,
) -> NativeResponse {
    let deadline = tokio::time::Instant::now() + ACCEPT_WAIT;
    let mut seq = 0;
    loop {
        let mut stream = native_session(node).await;
        seq += 1;
        let response = ingest_native(&mut stream, seq, collection, lines).await;
        if response.status != ResponseStatus::Error {
            return response;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {} never accepted `{lines}`: {response:?}",
            node.node_id
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Panic unless `response` succeeded.
pub(super) fn assert_native_ok(response: &NativeResponse, what: &str) {
    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "{what} must succeed: {response:?}"
    );
}

/// The warnings of `response` that report rejected lines of `collection`.
pub(super) fn rejection_warnings(response: &NativeResponse, collection: &str) -> Vec<String> {
    response
        .warnings
        .iter()
        .filter(|warning| warning.contains(collection) && warning.contains("rejected"))
        .cloned()
        .collect()
}
