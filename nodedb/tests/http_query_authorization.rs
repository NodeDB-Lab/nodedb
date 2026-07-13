// SPDX-License-Identifier: BUSL-1.1

//! Authorization parity for materialized HTTP SQL queries.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::pgwire_harness::TestServer;
use nodedb::config::auth::AuthMode;
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb::control::security::identity::{Permission, Role};
use nodedb::control::security::permission::collection_target;
use nodedb::control::server::session_auth::verify_api_key_identity;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};

struct AuthenticatedHttpEndpoint {
    local_addr: std::net::SocketAddr,
    _server: tokio::task::JoinHandle<()>,
}

async fn start_authenticated_http(shared: Arc<SharedState>) -> AuthenticatedHttpEndpoint {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind authenticated HTTP listener");
    let local_addr = listener.local_addr().expect("authenticated HTTP address");
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
        .expect("authenticated HTTP server");
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    AuthenticatedHttpEndpoint {
        local_addr,
        _server: handle,
    }
}

fn create_api_key(shared: &SharedState, username: &str, roles: Vec<Role>) -> String {
    let user_id = shared
        .credentials
        .create_service_account(username, TenantId::new(1), roles, vec![DatabaseId::DEFAULT])
        .expect("create database-scoped service account");
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
        .expect("create API key")
}

async fn post_query(http: &AuthenticatedHttpEndpoint, token: &str, sql: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{}/v1/query", http.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"sql": sql}))
        .send()
        .await
        .expect("POST authenticated query")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_rejects_database_outside_api_key_scope() {
    let srv = TestServer::start().await;
    srv.exec("CREATE DATABASE private_http_db")
        .await
        .expect("create inaccessible database");
    let token = create_api_key(&srv.shared, "http_db_reader", vec![Role::ReadOnly]);
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/query", http.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .header("X-NodeDB-Database", "private_http_db")
        .json(&serde_json::json!({"sql": "SELECT 1"}))
        .send()
        .await
        .expect("POST cross-database query");

    let status = response.status();
    let body = response
        .text()
        .await
        .expect("read cross-database authorization response");
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "HTTP queries must enforce the API key's database scope before planning or execution; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_rejects_cross_database_write_without_mutating_target() {
    let srv = TestServer::start().await;
    srv.exec("CREATE DATABASE private_write_db")
        .await
        .expect("create inaccessible write database");
    srv.exec("USE DATABASE private_write_db")
        .await
        .expect("switch to write database as superuser");
    srv.exec("CREATE COLLECTION private_write_rows")
        .await
        .expect("create private write collection");
    srv.exec("USE DATABASE default")
        .await
        .expect("return to default database");

    let token = create_api_key(&srv.shared, "http_db_writer", vec![Role::ReadWrite]);
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/query", http.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .header("X-NodeDB-Database", "private_write_db")
        .json(&serde_json::json!({
            "sql": "INSERT INTO private_write_rows { id: 'forbidden', value: 5 }"
        }))
        .send()
        .await
        .expect("POST cross-database write");

    srv.exec("USE DATABASE private_write_db")
        .await
        .expect("inspect write database");
    let rows = srv
        .query_text("SELECT id FROM private_write_rows")
        .await
        .expect("query private write rows");
    assert!(
        rows.is_empty(),
        "a cross-database write must not mutate the target: {rows:?}"
    );
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("read cross-database write authorization response");
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "cross-database HTTP writes must be rejected; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_rejects_schemaless_write_without_permission_or_mutation() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION denied_schemaless_writes")
        .await
        .expect("create denied schemaless collection");

    let token = create_api_key(
        &srv.shared,
        "http_ungranted_schemaless_writer",
        vec![Role::Custom("http_ungranted_schemaless_writer_role".into())],
    );
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(
        &http,
        &token,
        "INSERT INTO denied_schemaless_writes { id: 'forbidden', value: 19 }",
    )
    .await;

    let rows = srv
        .query_text("SELECT id FROM denied_schemaless_writes")
        .await
        .expect("inspect denied schemaless collection");
    assert!(
        rows.is_empty(),
        "an unauthorized DDL-routed write must not mutate the collection: {rows:?}"
    );

    let status = response.status();
    let body = response
        .text()
        .await
        .expect("read schemaless write authorization response");
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "DDL-routed schemaless writes require authorization before dispatch; response: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_authorizes_schemaless_write_before_firing_triggers() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION trigger_guarded_writes")
        .await
        .expect("create trigger-guarded collection");
    srv.exec(
        "CREATE TRIGGER authorization_order BEFORE INSERT ON trigger_guarded_writes \
         FOR EACH ROW BEGIN RAISE EXCEPTION 'trigger fired before authorization'; END",
    )
    .await
    .expect("create rejecting trigger");

    let token = create_api_key(
        &srv.shared,
        "http_trigger_bypass_writer",
        vec![Role::Custom("http_trigger_bypass_role".into())],
    );
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(
        &http,
        &token,
        "INSERT INTO trigger_guarded_writes { id: 'forbidden' }",
    )
    .await;

    let status = response.status();
    let body = response.text().await.expect("read authorization response");
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN, "response: {body}");
    assert!(
        body.contains("permission denied") && !body.contains("trigger fired before authorization"),
        "authorization must reject before trigger execution: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_rejects_system_catalog_for_non_superuser() {
    let srv = TestServer::start().await;
    let token = create_api_key(&srv.shared, "http_catalog_reader", vec![Role::ReadOnly]);
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;

    let response = post_query(&http, &token, "SELECT * FROM _system.audit_log").await;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "system catalog access must require a superuser on every SQL transport"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_honors_explicit_collection_grant_for_custom_role() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION granted_rows")
        .await
        .expect("create granted collection");
    srv.exec("INSERT INTO granted_rows { id: 'visible', value: 9 }")
        .await
        .expect("seed granted collection");

    let username = "http_explicit_reader";
    let token = create_api_key(
        &srv.shared,
        username,
        vec![Role::Custom("http_explicit_role".into())],
    );
    srv.shared
        .permissions
        .grant(
            &collection_target(TenantId::new(1), "granted_rows"),
            &format!("user:{username}"),
            Permission::Read,
            "nodedb",
            Some(srv.shared.credentials.catalog()),
        )
        .expect("grant collection read");
    let identity = verify_api_key_identity(&srv.shared, &token, "test", "http")
        .expect("resolve granted API-key identity");
    assert_eq!(identity.username, username, "API-key identity username");
    assert_eq!(
        identity.tenant_id,
        TenantId::new(1),
        "API-key identity tenant"
    );
    assert!(
        identity.can_access_database(DatabaseId::DEFAULT),
        "API-key identity must access the selected default database"
    );
    assert!(
        srv.shared.permissions.check(
            &identity,
            Permission::Read,
            "granted_rows",
            &srv.shared.roles,
            &NoopAuditEmitter,
        ),
        "PermissionStore must accept the explicit collection READ grant"
    );

    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(&http, &token, "SELECT * FROM granted_rows").await;
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("read HTTP authorization response");

    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "HTTP authorization must honor PermissionStore grants before built-in role fallback; response: {body}"
    );
}
