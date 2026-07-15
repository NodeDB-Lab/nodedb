// SPDX-License-Identifier: BUSL-1.1

//! Dispatcher correctness for administrative `SHOW` commands over the
//! native (MessagePack/JSON) protocol.
//!
//! The native SQL dispatcher intercepts every statement starting with
//! `SHOW ` and, unless the command is on a hard-coded allowlist, routes it
//! to the session-parameter fallback. That fallback returns a single row
//! with one column named `setting` and an empty string as its value —
//! making an unrouted administrative command look like a successful but
//! empty result instead of reaching its real handler.
//!
//! Mirrors `pgwire_show_dispatch.rs` for the native protocol entry point.

mod common;

use common::native_harness::{NativeTestServer, do_handshake, send_sql};

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;

/// Returns true if the response is the session-parameter fallback: exactly
/// one column named `setting`, one row, and an empty value.
fn is_session_param_fallback(response: &nodedb_types::protocol::NativeResponse) -> bool {
    let columns = match &response.columns {
        Some(c) => c,
        None => return false,
    };
    let rows = match &response.rows {
        Some(r) => r,
        None => return false,
    };
    if columns.as_slice() != ["setting"] || rows.len() != 1 || rows[0].len() != 1 {
        return false;
    }
    matches!(&rows[0][0], nodedb_types::value::Value::String(s) if s.is_empty())
}

#[tokio::test]
async fn native_set_to_after_unicode_key_preserves_original_offsets() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let set = send_sql(&mut stream, 1, "SET custom.ﬀﬀ TO enabled").await;
    assert_eq!(set.status, ResponseStatus::Ok, "Unicode SET must succeed");
    let show = send_sql(&mut stream, 2, "SHOW custom.ﬀﬀ").await;
    server.shutdown().await;

    assert_eq!(show.status, ResponseStatus::Ok, "Unicode SHOW must succeed");
    assert!(
        matches!(
            show.rows.as_deref(),
            Some([row]) if matches!(row.as_slice(), [nodedb_types::value::Value::String(value)] if value == "enabled")
        ),
        "SHOW must return the value stored by SET, got {show:?}"
    );
}

#[tokio::test]
async fn native_show_stats_not_session_fallback() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let resp = send_sql(&mut stream, 1, "SHOW STATS").await;
    server.shutdown().await;

    assert_eq!(resp.status, ResponseStatus::Ok, "SHOW STATS must not error");
    assert!(
        !is_session_param_fallback(&resp),
        "SHOW STATS must not be routed to the session-parameter fallback, got {resp:?}"
    );
}

#[tokio::test]
async fn native_show_metrics_not_session_fallback() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let resp = send_sql(&mut stream, 1, "SHOW METRICS").await;
    server.shutdown().await;

    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "SHOW METRICS must not error"
    );
    assert!(
        !is_session_param_fallback(&resp),
        "SHOW METRICS must not be routed to the session-parameter fallback, got {resp:?}"
    );
}

#[tokio::test]
async fn native_show_memory_not_session_fallback() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let resp = send_sql(&mut stream, 1, "SHOW MEMORY").await;
    server.shutdown().await;

    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "SHOW MEMORY must not error"
    );
    assert!(
        !is_session_param_fallback(&resp),
        "SHOW MEMORY must not be routed to the session-parameter fallback, got {resp:?}"
    );
}

#[tokio::test]
async fn native_show_roles_not_session_fallback() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let resp = send_sql(&mut stream, 1, "SHOW ROLES").await;
    server.shutdown().await;

    assert_eq!(resp.status, ResponseStatus::Ok, "SHOW ROLES must not error");
    assert!(
        !is_session_param_fallback(&resp),
        "SHOW ROLES must not be routed to the session-parameter fallback, got {resp:?}"
    );
}

/// Positive control: a genuine session parameter must still reach the
/// session-variable handler after the DDL/admin router reorder.
#[tokio::test]
async fn native_show_session_param_still_works() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let resp = send_sql(&mut stream, 1, "SHOW timezone").await;
    server.shutdown().await;

    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "SHOW timezone must not error"
    );
    assert_eq!(
        resp.columns.as_deref(),
        Some(["setting".to_string()].as_slice()),
        "SHOW timezone must still return the session-variable `setting` column, got {resp:?}"
    );
}
