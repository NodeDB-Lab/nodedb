// SPDX-License-Identifier: BUSL-1.1

//! Authorization parity for SQL executed through WebSocket RPC.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::pgwire_harness::TestServer;
use futures::{SinkExt, StreamExt};
use nodedb::config::auth::AuthMode;
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, http};

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
    let user_id = shared
        .credentials
        .create_service_account(
            username,
            TenantId::new(1),
            vec![Role::Custom(format!("{username}_role"))],
            vec![DatabaseId::DEFAULT],
        )
        .expect("create WebSocket service account");
    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![DatabaseId::DEFAULT],
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create WebSocket API key")
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
