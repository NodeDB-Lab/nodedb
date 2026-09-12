// SPDX-License-Identifier: BUSL-1.1

//! Authorization parity for SQL executed through WebSocket RPC.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb::config::auth::AuthMode;
use nodedb::control::change_stream::{ChangeEvent, ChangeOperation};
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, Lsn, TenantId};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::RowIdentity;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, http};

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct AuthenticatedWsEndpoint {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
}

async fn start_authenticated_ws(shared: Arc<SharedState>) -> AuthenticatedWsEndpoint {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind authenticated WebSocket listener");
    let local_addr = listener.local_addr().expect("authenticated WS address");
    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared,
            AuthMode::Password,
            None,
            bus,
        )
        .await
        .expect("authenticated WebSocket server");
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    AuthenticatedWsEndpoint {
        local_addr,
        _server: handle,
    }
}

fn create_ws_api_key(shared: &SharedState, username: &str) -> String {
    create_ws_api_key_with_roles(
        shared,
        username,
        TenantId::new(1),
        vec![Role::Custom(format!("{username}_role"))],
    )
}

fn create_ws_api_key_for_tenant(
    shared: &SharedState,
    username: &str,
    tenant_id: TenantId,
) -> String {
    create_ws_api_key_with_roles(shared, username, tenant_id, vec![Role::ReadOnly])
}

fn create_ws_api_key_with_roles(
    shared: &SharedState,
    username: &str,
    tenant_id: TenantId,
    roles: Vec<Role>,
) -> String {
    let user_id = shared
        .credentials
        .create_service_account(username, tenant_id, roles, vec![DatabaseId::DEFAULT])
        .expect("create WebSocket service account");
    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id,
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![DatabaseId::DEFAULT],
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create WebSocket API key")
}

async fn connect_authenticated_ws(endpoint: &AuthenticatedWsEndpoint, token: &str) -> WsStream {
    let mut request = format!("ws://{}/v1/ws", endpoint.local_addr)
        .into_client_request()
        .expect("WebSocket request");
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
    );
    tokio_tungstenite::connect_async(request)
        .await
        .expect("authenticated WebSocket connect")
        .0
}

async fn read_auth_exchange(
    ws: &mut WsStream,
    auth_id: u64,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let mut notifications = Vec::new();
    for _ in 0..3 {
        let message = tokio::time::timeout(Duration::from_millis(500), ws.next())
            .await
            .expect("bounded wait for auth exchange")
            .expect("WebSocket stream ended during auth exchange")
            .expect("WebSocket error during auth exchange");
        let Message::Text(text) = message else {
            panic!("expected WebSocket text frame during auth exchange, got {message:?}");
        };
        let value: serde_json::Value = sonic_rs::from_str(&text).expect("valid auth exchange JSON");
        if value["id"] == auth_id {
            return (value, notifications);
        }
        assert_eq!(
            value["method"], "change",
            "only change notifications may precede an auth response: {value}"
        );
        notifications.push(value);
    }
    panic!("auth response was not received within the bounded exchange");
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

async fn ws_request(
    endpoint: &AuthenticatedWsEndpoint,
    token: &str,
    method: &str,
    sql: &str,
) -> serde_json::Value {
    let mut request = format!("ws://{}/v1/ws", endpoint.local_addr)
        .into_client_request()
        .expect("WebSocket request");
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("authenticated WebSocket connect");
    ws.send(Message::Text(
        serde_json::json!({
            "id": 77,
            "method": method,
            "params": {"sql": sql}
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send WebSocket query");
    let message = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("timeout waiting for WS query response")
        .expect("WebSocket stream ended")
        .expect("WebSocket response error");
    let Message::Text(text) = message else {
        panic!("expected WebSocket text response, got {message:?}");
    };
    sonic_rs::from_str(&text).expect("valid WebSocket JSON response")
}

async fn ws_query(endpoint: &AuthenticatedWsEndpoint, token: &str, sql: &str) -> serde_json::Value {
    ws_request(endpoint, token, "query", sql).await
}

fn assert_permission_denied(response: &serde_json::Value, context: &str) {
    let error = response
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        error.to_ascii_lowercase().contains("permission denied"),
        "{context} must return an authorization denial: {response}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_resume_session_id_is_not_shared_across_authenticated_identities() {
    let srv = TestServer::start().await;
    let tenant_a = TenantId::new(101);
    let tenant_b = TenantId::new(202);
    let token_a = create_ws_api_key_for_tenant(&srv.shared, "ws_resume_tenant_a", tenant_a);
    let token_b = create_ws_api_key_for_tenant(&srv.shared, "ws_resume_tenant_b", tenant_b);
    let endpoint = start_authenticated_ws(Arc::clone(&srv.shared)).await;
    let session_id = "arbitrary-client-session-id";

    for (lsn, tenant_id, document_id) in [
        (Lsn::new(901), tenant_a, "tenant-a-order"),
        (Lsn::new(902), tenant_b, "tenant-b-order"),
    ] {
        srv.shared.change_stream.publish(ChangeEvent {
            lsn,
            tenant_id,
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document_id),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1_000,
            after: None,
        });
    }

    let mut first_a = connect_authenticated_ws(&endpoint, &token_a).await;
    send_auth(&mut first_a, 1, session_id, None).await;
    let (_first_a_response, first_a_notifications) = read_auth_exchange(&mut first_a, 1).await;
    assert_eq!(first_a_notifications.len(), 1);
    assert_eq!(
        first_a_notifications[0]["params"]["document_id"],
        "tenant-a-order"
    );
    let a_cursor = first_a_notifications[0]["params"]["cursor"]
        .as_str()
        .expect("tenant A replay cursor")
        .to_owned();
    drop(first_a);

    let mut resumed_a = connect_authenticated_ws(&endpoint, &token_a).await;
    send_auth(&mut resumed_a, 2, session_id, Some(&a_cursor)).await;
    let (resumed_a_response, resumed_a_notifications) = read_auth_exchange(&mut resumed_a, 2).await;
    assert_eq!(resumed_a_response["result"]["replayed"], 0);
    assert!(resumed_a_notifications.is_empty());
    drop(resumed_a);

    let mut first_b = connect_authenticated_ws(&endpoint, &token_b).await;
    send_auth(&mut first_b, 3, session_id, None).await;
    let (first_b_response, first_b_notifications) = read_auth_exchange(&mut first_b, 3).await;
    assert_eq!(first_b_response["result"]["replayed"], 1);
    assert_eq!(
        first_b_notifications
            .iter()
            .map(|notification| notification["params"]["document_id"]
                .as_str()
                .expect("document id"))
            .collect::<Vec<_>>(),
        vec!["tenant-b-order"],
        "a shared arbitrary session_id must neither reveal tenant A data nor fast-forward tenant B"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_upgrade_rejects_database_outside_api_key_scope() {
    let srv = TestServer::start().await;
    srv.exec("CREATE DATABASE ws_private_database")
        .await
        .expect("create private WebSocket database");
    let token = create_ws_api_key(&srv.shared, "ws_database_reader");
    let endpoint = start_authenticated_ws(Arc::clone(&srv.shared)).await;
    let mut request = format!("ws://{}/v1/ws", endpoint.local_addr)
        .into_client_request()
        .expect("WebSocket request");
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
    );
    request.headers_mut().insert(
        http::HeaderName::from_static("x-nodedb-database"),
        http::HeaderValue::from_static("ws_private_database"),
    );

    let error = tokio_tungstenite::connect_async(request)
        .await
        .expect_err("upgrade must reject a database outside API-key scope");
    let status = match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status(),
        other => panic!("expected HTTP upgrade rejection, got {other}"),
    };
    assert_eq!(status, http::StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_query_rejects_system_catalog_for_non_superuser() {
    let srv = TestServer::start().await;
    let token = create_ws_api_key(&srv.shared, "ws_catalog_reader");
    let endpoint = start_authenticated_ws(Arc::clone(&srv.shared)).await;

    let response = ws_query(&endpoint, &token, "SELECT * FROM _system.audit_log").await;

    assert_permission_denied(
        &response,
        "WebSocket system-catalog access for a non-superuser",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_query_rejects_write_without_permission_or_mutation() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ws_denied_writes")
        .await
        .expect("create denied WebSocket write collection");
    let token = create_ws_api_key(&srv.shared, "ws_ungranted_writer");
    let endpoint = start_authenticated_ws(Arc::clone(&srv.shared)).await;

    let response = ws_query(
        &endpoint,
        &token,
        "INSERT INTO ws_denied_writes { id: 'forbidden', value: 17 }",
    )
    .await;
    let rows = srv
        .query_text("SELECT id FROM ws_denied_writes")
        .await
        .expect("query denied WebSocket write collection");

    assert!(
        rows.is_empty(),
        "an unauthorized WebSocket write must not mutate the collection: {rows:?}"
    );
    assert_permission_denied(&response, "WebSocket write without a PermissionStore grant");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_live_rejects_collection_before_subscription_open() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ws_private_live")
        .await
        .expect("create private live collection");
    let token = create_ws_api_key(&srv.shared, "ws_ungranted_live_reader");
    let endpoint = start_authenticated_ws(Arc::clone(&srv.shared)).await;

    let response = ws_request(
        &endpoint,
        &token,
        "live",
        "LIVE SELECT * FROM ws_private_live",
    )
    .await;

    assert_permission_denied(&response, "WebSocket live subscription without read grant");
    assert_eq!(
        srv.shared.change_stream.subscriber_count(),
        0,
        "denial must occur before opening a live subscription"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_query_rejects_collection_without_permission() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ws_private_rows")
        .await
        .expect("create private WebSocket collection");
    srv.exec("INSERT INTO ws_private_rows { id: 'hidden', value: 13 }")
        .await
        .expect("seed private WebSocket collection");
    let token = create_ws_api_key(&srv.shared, "ws_ungranted_reader");
    let endpoint = start_authenticated_ws(Arc::clone(&srv.shared)).await;

    let response = ws_query(&endpoint, &token, "SELECT * FROM ws_private_rows").await;

    assert_permission_denied(
        &response,
        "WebSocket collection read without a PermissionStore grant",
    );
}
