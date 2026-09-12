// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for WebSocket RPC endpoint.
//!
//! Endpoint covered:
//! - GET /v1/ws  — WebSocket upgrade
//!
//! Contracts asserted:
//! - Under Trust mode: upgrade succeeds
//! - After upgrade: `query` method with valid SQL returns a JSON response with `result` field
//! - `ping` method returns `"pong"` result
//! - Under Password mode: upgrade is refused before any WS state is created (401)
//! - Non-upgrade GET does not hang (axum rejects it)

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::change_stream::{ChangeEvent, ChangeOperation};
use nodedb::control::security::catalog::DatabaseDescriptor;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, Lsn, TenantId};
use nodedb::wal::WalManager;
use nodedb_types::RowIdentity;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, http};

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const WS_READ_TIMEOUT: Duration = Duration::from_millis(500);

struct TestServer {
    local_addr: std::net::SocketAddr,
    shared: Arc<SharedState>,
    _server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn start_http(auth_mode: AuthMode) -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("ws.wal")).expect("open wal"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();
    if auth_mode == AuthMode::Trust {
        shared
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let local_addr = listener.local_addr().expect("local addr");

    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            auth_mode,
            None,
            bus,
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(40)).await;

    TestServer {
        local_addr,
        shared,
        _server: handle,
        _dir: dir,
    }
}

async fn connect_ws(srv: &TestServer) -> WsStream {
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect")
        .0
}

async fn connect_ws_in_database(srv: &TestServer, database: &str) -> WsStream {
    let mut request = format!("ws://{}/v1/ws", srv.local_addr)
        .into_client_request()
        .expect("WebSocket request");
    request.headers_mut().insert(
        http::HeaderName::from_static("x-nodedb-database"),
        http::HeaderValue::from_str(database).expect("database header"),
    );
    tokio_tungstenite::connect_async(request)
        .await
        .expect("database-scoped WS connect")
        .0
}

async fn next_ws_json(ws: &mut WsStream, context: &str) -> serde_json::Value {
    let message = tokio::time::timeout(WS_READ_TIMEOUT, ws.next())
        .await
        .unwrap_or_else(|_| panic!("timeout waiting for {context}"))
        .unwrap_or_else(|| panic!("WS stream ended while waiting for {context}"))
        .unwrap_or_else(|error| panic!("WS error while waiting for {context}: {error}"));
    let Message::Text(text) = message else {
        panic!("expected Text frame while waiting for {context}, got {message:?}");
    };
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("invalid JSON while waiting for {context}: {error}"))
}

async fn read_auth_exchange(
    ws: &mut WsStream,
    auth_id: u64,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let mut notifications = Vec::new();
    for _ in 0..3 {
        let message = next_ws_json(ws, "auth exchange").await;
        if message["id"] == auth_id {
            return (message, notifications);
        }
        assert_eq!(
            message["method"], "change",
            "auth exchange may contain only change notifications before its response: {message}"
        );
        notifications.push(message);
    }
    panic!("auth response {auth_id} was not received within the bounded auth exchange");
}

async fn send_auth(ws: &mut WsStream, id: u64, session_id: &str, cursor: Option<&str>) {
    let mut params = serde_json::json!({"session_id": session_id});
    if let Some(cursor) = cursor {
        params["cursor"] = serde_json::Value::String(cursor.to_owned());
    }
    ws.send(Message::Text(
        serde_json::json!({"id": id, "method": "auth", "params": params})
            .to_string()
            .into(),
    ))
    .await
    .expect("send auth");
}

async fn assert_no_ws_message(ws: &mut WsStream, context: &str) {
    match tokio::time::timeout(Duration::from_millis(150), ws.next()).await {
        Err(_) => {}
        Ok(Some(Ok(message))) => panic!("unexpected WS frame after {context}: {message:?}"),
        Ok(Some(Err(error))) => panic!("WS error after {context}: {error}"),
        Ok(None) => panic!("WS stream ended unexpectedly after {context}"),
    }
}

fn publish_change(srv: &TestServer, lsn: u64, document_id: &str) {
    srv.shared.change_stream.publish(ChangeEvent {
        lsn: Lsn::new(lsn),
        tenant_id: TenantId::new(1),
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key(document_id),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1_000,
        after: None,
    });
}

fn assert_error_contains(response: &serde_json::Value, expected: &str) {
    let error = response["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected error response, got: {response}"));
    assert!(
        error.contains(expected),
        "expected error containing {expected:?}, got {error:?}"
    );
}

// ─── Upgrade rejected under Password mode ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upgrade_refused_without_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_err(),
        "WS upgrade must be refused under Password mode with no bearer token"
    );
}

// ─── Upgrade succeeds under Trust mode ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upgrade_succeeds_under_trust_mode() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_ok(),
        "WS upgrade must succeed under Trust mode; error: {:?}",
        result.unwrap_err()
    );
}

// ─── ping → pong ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_ping_returns_pong_result() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    let req = serde_json::json!({
        "id": 1,
        "method": "ping"
    })
    .to_string();
    ws.send(Message::Text(req.into())).await.expect("send ping");

    let msg = tokio::time::timeout(Duration::from_millis(500), ws.next())
        .await
        .expect("timeout waiting for pong")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text, got {other:?}"),
    };

    let value: serde_json::Value =
        serde_json::from_str(&text).expect("pong response must be valid JSON");
    assert_eq!(value["id"], 1, "response id must match request id");
    assert_eq!(
        value["result"], "pong",
        "ping response must have result='pong'"
    );
}

// ─── query method returns result field ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_query_method_returns_result_field() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    let req = serde_json::json!({
        "id": 42,
        "method": "query",
        "params": {"sql": "SHOW USERS"}
    })
    .to_string();
    ws.send(Message::Text(req.into()))
        .await
        .expect("send query");

    let msg = tokio::time::timeout(Duration::from_millis(1000), ws.next())
        .await
        .expect("timeout waiting for query response")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text frame, got {other:?}"),
    };

    let value: serde_json::Value =
        serde_json::from_str(&text).expect("query response must be valid JSON");
    assert_eq!(value["id"], 42, "response id must match request id");
    // Either `result` (success) or `error` (failure) must be present — never neither.
    assert!(
        value.get("result").is_some() || value.get("error").is_some(),
        "WS query response must have 'result' or 'error' field; got: {value}"
    );
}

// ─── unknown method returns error field ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_unknown_method_returns_error() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    let req = serde_json::json!({
        "id": 99,
        "method": "nonexistent_method_xyz"
    })
    .to_string();
    ws.send(Message::Text(req.into()))
        .await
        .expect("send unknown method");

    let msg = tokio::time::timeout(Duration::from_millis(500), ws.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text frame, got {other:?}"),
    };

    let value: serde_json::Value = serde_json::from_str(&text).expect("must be valid JSON");
    assert_eq!(value["id"], 99, "response id must match request id");
    assert!(
        value.get("error").is_some(),
        "unknown method must produce an error response; got: {value}"
    );
}

// ─── malformed JSON frame ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_malformed_json_returns_error_not_crash() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    ws.send(Message::Text("{not valid json".into()))
        .await
        .expect("send malformed");

    let msg = tokio::time::timeout(Duration::from_millis(500), ws.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text, got {other:?}"),
    };
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("error response must be valid JSON");
    assert!(
        value.get("error").is_some(),
        "malformed JSON frame must return an error response, not crash; got: {value}"
    );
}

// ─── Resume cursor regressions ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_replay_isolates_events_by_selected_database() {
    let srv = start_http(AuthMode::Trust).await;
    let second_database = DatabaseId::new(43);
    let mut descriptor = DatabaseDescriptor::default_db();
    descriptor.id = second_database;
    descriptor.name = "ws_second_database".into();
    srv.shared
        .credentials
        .catalog()
        .put_database(&descriptor)
        .expect("add second WS database to catalog");

    for (database_id, lsn, document_id) in [
        (DatabaseId::DEFAULT, Lsn::new(110), "default-database-order"),
        (second_database, Lsn::new(111), "second-database-order"),
    ] {
        srv.shared.change_stream.publish_in_database(
            database_id,
            ChangeEvent {
                lsn,
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: RowIdentity::from_user_key(document_id),
                operation: ChangeOperation::Insert,
                timestamp_ms: 1_000,
                after: None,
            },
        );
    }

    let mut ws = connect_ws_in_database(&srv, "ws_second_database").await;
    send_auth(&mut ws, 1, "database-scoped-resume", None).await;
    let (response, notifications) = read_auth_exchange(&mut ws, 1).await;

    assert_eq!(response["result"]["replayed"], 1);
    assert_eq!(notifications.len(), 1, "only the selected database replays");
    assert_eq!(
        notifications[0]["params"]["document_id"],
        "second-database-order"
    );
    assert_eq!(
        notifications[0]["params"]["database_id"],
        second_database.as_u64(),
        "the replay notification must carry the selected database identity"
    );
}

/// A LIVE SELECT flooded past its buffers must never silently skip events: the
/// client either receives every change in order, or is told to reset.
///
/// This deliberately does NOT assert that a reset always happens. Whether the
/// broadcast channel actually overflows depends on how much the WS socket and
/// the forwarder absorb before backpressure reaches it, and socket buffer sizes
/// are a property of the machine, not of this code — on an idle host the
/// subscriber keeps up and delivering all 4608 events is the correct outcome.
/// Asserting the reset unconditionally made this fail on fast machines while
/// the server was behaving perfectly. The invariant that always holds, and the
/// one a client depends on, is the absence of a silent gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_live_lag_never_silently_skips_events() {
    let srv = start_http(AuthMode::Trust).await;
    let mut ws = connect_ws(&srv).await;
    ws.send(Message::Text(
        serde_json::json!({
            "id": 1,
            "method": "live",
            "params": {"sql": "LIVE SELECT * FROM orders"}
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("open LIVE SELECT subscription");
    let ack = next_ws_json(&mut ws, "LIVE SELECT acknowledgement").await;
    let subscription_id = ack["result"]["subscription_id"]
        .as_u64()
        .expect("LIVE SELECT acknowledgement must include a subscription id");
    tokio::task::yield_now().await;

    // Do not read while overflowing both the 256-message WS forwarder and
    // the 4096-entry broadcast channel. Large, bounded notifications ensure
    // the socket sender applies backpressure before the broadcast receiver
    // observes its lag.
    let document_suffix = "x".repeat(2_048);
    const PUBLISHED: u64 = 4_096 + 512;
    for sequence in 0..PUBLISHED {
        srv.shared.change_stream.publish(ChangeEvent {
            lsn: Lsn::new(10_000 + sequence),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(format!(
                "lagged-order-{sequence}-{document_suffix}"
            )),
            operation: ChangeOperation::Insert,
            timestamp_ms: 10_000,
            after: None,
        });
        if sequence % 32 == 31 {
            tokio::task::yield_now().await;
        }
    }

    // Drain until either a reset arrives or every published event has been
    // seen. Bounded by total elapsed time, not by a per-read timeout or a
    // message budget: how much arrives before backpressure reaches the
    // broadcast channel varies with machine load, and a stall mid-stream is
    // only a failure if the whole drain overruns.
    let mut delivered: Vec<u64> = Vec::with_capacity(PUBLISHED as usize);
    let mut reset: Option<serde_json::Value> = None;
    tokio::time::timeout(Duration::from_secs(30), async {
        while reset.is_none() && (delivered.len() as u64) < PUBLISHED {
            let Some(message) = ws.next().await else {
                panic!(
                    "WS stream ended after {} notifications with neither a reset nor the \
                     full event set",
                    delivered.len()
                );
            };
            let message = match message {
                Ok(Message::Text(text)) => serde_json::from_str::<serde_json::Value>(&text)
                    .unwrap_or_else(|e| panic!("invalid JSON in live notification: {e}")),
                Ok(other) => panic!("expected Text frame in live stream, got {other:?}"),
                Err(e) => panic!("WS error after {} notifications: {e}", delivered.len()),
            };
            if message["method"] == "reset_required" {
                reset = Some(message);
                continue;
            }
            let document_id = message["params"]["document_id"]
                .as_str()
                .unwrap_or_else(|| panic!("live notification without document_id: {message}"));
            let sequence: u64 = document_id
                .trim_start_matches("lagged-order-")
                .split('-')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| panic!("unexpected document_id shape: {document_id}"));
            delivered.push(sequence);
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "LIVE SELECT stalled: {} of {PUBLISHED} events delivered, no reset",
            delivered.len()
        )
    });

    match reset {
        // Events were dropped, and the client was told so — the whole point of
        // the reset. Which events were lost is not asserted: that is exactly
        // the information the reset exists to say is unavailable.
        Some(reset) => {
            assert_eq!(reset["params"]["subscription_id"], subscription_id);
            assert_eq!(reset["params"]["reason"], "change stream lagged");
        }
        // No reset, so the stream claims to be complete — hold it to that.
        // A gap here is the silent-loss bug: the client would have no way to
        // know it missed a change.
        None => {
            let expected: Vec<u64> = (0..PUBLISHED).collect();
            assert_eq!(
                delivered, expected,
                "LIVE SELECT delivered no reset_required, so every published event must have \
                 arrived exactly once and in publication order"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_replays_out_of_order_lsns_in_publication_order_with_opaque_cursors() {
    let srv = start_http(AuthMode::Trust).await;
    // The stream sequence is publication-ordered, not WAL-LSN-ordered.
    publish_change(&srv, 102, "published-first-lsn-102");
    publish_change(&srv, 101, "delayed-lower-lsn-101");

    let mut ws = connect_ws(&srv).await;
    send_auth(&mut ws, 1, "resume-publication-order", None).await;
    let (response, notifications) = read_auth_exchange(&mut ws, 1).await;

    assert_eq!(response["result"]["session_id"], "resume-publication-order");
    assert_eq!(response["result"]["replayed"], 2);
    let snapshot_cursor = response["result"]["snapshot_cursor"]
        .as_str()
        .expect("auth response must include snapshot_cursor");
    assert!(
        snapshot_cursor.starts_with("v1:"),
        "snapshot_cursor must be an opaque versioned cursor: {response}"
    );
    assert_eq!(notifications.len(), 2, "both pre-auth events must replay");
    assert_eq!(notifications[0]["params"]["wal_lsn"], 102);
    assert_eq!(notifications[1]["params"]["wal_lsn"], 101);
    assert_eq!(
        notifications[0]["params"]["document_id"],
        "published-first-lsn-102"
    );
    assert_eq!(
        notifications[1]["params"]["document_id"],
        "delayed-lower-lsn-101"
    );
    let first_cursor = notifications[0]["params"]["cursor"]
        .as_str()
        .expect("first replay must include an opaque cursor");
    let second_cursor = notifications[1]["params"]["cursor"]
        .as_str()
        .expect("second replay must include an opaque cursor");
    assert!(first_cursor.starts_with("v1:"));
    assert!(second_cursor.starts_with("v1:"));
    assert_ne!(
        first_cursor, second_cursor,
        "each publication must expose a distinct opaque cursor"
    );
    assert_no_ws_message(&mut ws, "the complete initial replay").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_cursor_controls_reconnect_progress_and_handoffs_to_live_events() {
    let srv = start_http(AuthMode::Trust).await;
    publish_change(&srv, 102, "first-replayed-event");
    publish_change(&srv, 101, "second-replayed-event");
    let session_id = "client-controlled-resume-session";

    let mut first_connection = connect_ws(&srv).await;
    send_auth(&mut first_connection, 1, session_id, None).await;
    let (_first_response, first_notifications) = read_auth_exchange(&mut first_connection, 1).await;
    assert_eq!(first_notifications.len(), 2);
    let first_cursor = first_notifications[0]["params"]["cursor"]
        .as_str()
        .expect("first delivered event must include cursor")
        .to_owned();
    drop(first_connection);

    let mut resumed_connection = connect_ws(&srv).await;
    send_auth(&mut resumed_connection, 2, session_id, Some(&first_cursor)).await;
    let (response, notifications) = read_auth_exchange(&mut resumed_connection, 2).await;
    assert_eq!(response["result"]["session_id"], session_id);
    assert_eq!(response["result"]["replayed"], 1);
    let snapshot_cursor = response["result"]["snapshot_cursor"]
        .as_str()
        .expect("reconnect auth response must include snapshot_cursor")
        .to_owned();
    assert_eq!(
        notifications.len(),
        1,
        "the supplied client cursor must suppress only the first publication"
    );
    assert_eq!(notifications[0]["params"]["wal_lsn"], 101);
    assert_eq!(
        notifications[0]["params"]["document_id"],
        "second-replayed-event"
    );
    assert_no_ws_message(&mut resumed_connection, "cursor-limited reconnect replay").await;

    // Publishing after the auth snapshot must flow through the live handoff.
    publish_change(&srv, 103, "published-after-auth-snapshot");
    let live_notification =
        next_ws_json(&mut resumed_connection, "post-snapshot live change").await;
    assert_eq!(live_notification["method"], "change");
    assert_eq!(live_notification["params"]["wal_lsn"], 103);
    assert_eq!(
        live_notification["params"]["document_id"],
        "published-after-auth-snapshot"
    );
    let live_cursor = live_notification["params"]["cursor"]
        .as_str()
        .expect("post-snapshot change must include cursor");
    assert!(live_cursor.starts_with("v1:"));
    assert_ne!(
        live_cursor, snapshot_cursor,
        "a post-snapshot publication must receive a later opaque cursor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_rejects_legacy_and_malformed_cursors_and_second_auth() {
    let srv = start_http(AuthMode::Trust).await;
    let mut ws = connect_ws(&srv).await;

    ws.send(Message::Text(
        serde_json::json!({
            "id": 1,
            "method": "auth",
            "params": {"session_id": "cursor-validation", "last_lsn": 101}
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send legacy last_lsn auth");
    assert_error_contains(
        &next_ws_json(&mut ws, "legacy last_lsn rejection").await,
        "last_lsn is no longer supported",
    );

    send_auth(&mut ws, 2, "cursor-validation", Some("not-a-valid-cursor")).await;
    assert_error_contains(
        &next_ws_json(&mut ws, "malformed cursor rejection").await,
        "cursor must be a valid opaque change cursor",
    );

    send_auth(&mut ws, 3, "cursor-validation", None).await;
    let (accepted, notifications) = read_auth_exchange(&mut ws, 3).await;
    assert!(
        notifications.is_empty(),
        "empty stream must not replay changes"
    );
    assert!(
        accepted.get("result").is_some(),
        "first valid auth must succeed"
    );

    send_auth(&mut ws, 4, "cursor-validation", None).await;
    assert_error_contains(
        &next_ws_json(&mut ws, "second auth rejection").await,
        "resume auth is permitted only once per connection",
    );
}
