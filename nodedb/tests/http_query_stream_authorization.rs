// SPDX-License-Identifier: BUSL-1.1

//! Authorization parity for lazy NDJSON HTTP SQL queries.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::pgwire_harness::TestServer;
use nodedb::config::auth::AuthMode;
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::{Permission, Role};
use nodedb::control::security::permission::collection_target;
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

async fn post_query(
    http: &AuthenticatedHttpEndpoint,
    token: &str,
    path: &str,
    sql: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{}{}", http.local_addr, path))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"sql": sql}))
        .send()
        .await
        .expect("POST authenticated streaming query")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_stream_rejects_database_outside_api_key_scope_before_lazy_execution() {
    let srv = TestServer::start().await;
    srv.exec("CREATE DATABASE private_stream_db")
        .await
        .expect("create inaccessible database");
    srv.exec("USE DATABASE private_stream_db")
        .await
        .expect("switch to inaccessible database as superuser");
    srv.exec("CREATE COLLECTION private_rows")
        .await
        .expect("create private collection");
    srv.exec("INSERT INTO private_rows { id: 'secret', value: 7 }")
        .await
        .expect("seed private collection");
    srv.exec("USE DATABASE default")
        .await
        .expect("return to default database");

    let token = create_api_key(&srv.shared, "http_stream_reader", vec![Role::ReadOnly]);
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(
        &http,
        &token,
        "/v1/query/stream?database=private_stream_db",
        "SELECT * FROM private_rows",
    )
    .await;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "NDJSON authorization must reject the database before opening a lazy result stream"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_stream_rejects_system_catalog_for_non_superuser() {
    let srv = TestServer::start().await;
    let token = create_api_key(
        &srv.shared,
        "http_stream_catalog_reader",
        vec![Role::ReadOnly],
    );
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;

    let response = post_query(
        &http,
        &token,
        "/v1/query/stream",
        "SELECT * FROM _system.audit_log",
    )
    .await;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "system catalog access must be denied before NDJSON materialization or streaming"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_stream_rejects_join_when_only_one_collection_is_granted() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION granted_join_rows")
        .await
        .expect("create granted join collection");
    srv.exec("CREATE COLLECTION denied_join_rows")
        .await
        .expect("create denied join collection");
    srv.exec("INSERT INTO granted_join_rows { id: 'shared', value: 1 }")
        .await
        .expect("seed granted join collection");
    srv.exec("INSERT INTO denied_join_rows { id: 'shared', secret: 2 }")
        .await
        .expect("seed denied join collection");

    let username = "http_partial_join_reader";
    let token = create_api_key(
        &srv.shared,
        username,
        vec![Role::Custom("http_partial_join_role".into())],
    );
    srv.shared
        .permissions
        .grant(
            &collection_target(TenantId::new(1), "granted_join_rows"),
            &format!("user:{username}"),
            Permission::Read,
            "nodedb",
            Some(srv.shared.credentials.catalog()),
        )
        .expect("grant only the left join collection");
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(
        &http,
        &token,
        "/v1/query/stream",
        "SELECT g.id FROM granted_join_rows g JOIN denied_join_rows d ON g.id = d.id",
    )
    .await;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "every collection referenced by a multi-resource plan must be authorized"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_stream_rejects_collection_without_permission() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION denied_stream_rows")
        .await
        .expect("create denied collection");
    srv.exec("INSERT INTO denied_stream_rows { id: 'hidden', value: 11 }")
        .await
        .expect("seed denied collection");

    let token = create_api_key(
        &srv.shared,
        "http_ungranted_reader",
        vec![Role::Custom("http_ungranted_role".into())],
    );
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(
        &http,
        &token,
        "/v1/query/stream",
        "SELECT * FROM denied_stream_rows",
    )
    .await;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "an unauthorized lazy scan must fail before response headers are committed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_stream_rejects_write_without_permission_or_mutation() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION denied_stream_writes")
        .await
        .expect("create denied write collection");
    let token = create_api_key(
        &srv.shared,
        "http_ungranted_writer",
        vec![Role::Custom("http_ungranted_writer_role".into())],
    );
    let http = start_authenticated_http(Arc::clone(&srv.shared)).await;
    let response = post_query(
        &http,
        &token,
        "/v1/query/stream",
        "INSERT INTO denied_stream_writes { id: 'forbidden', value: 19 }",
    )
    .await;
    let rows = srv
        .query_text("SELECT id FROM denied_stream_writes")
        .await
        .expect("query denied write collection");

    assert!(
        rows.is_empty(),
        "an unauthorized NDJSON write must not mutate the collection: {rows:?}"
    );
    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "NDJSON writes require an explicit write permission before dispatch"
    );
}
