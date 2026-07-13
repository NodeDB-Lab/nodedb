// SPDX-License-Identifier: BUSL-1.1

//! Authorization parity for native-protocol SQL execution.
//!
//! Native materialized and lazy SQL paths must apply the same collection-
//! permission gates as pgwire before dispatch.

mod common;

use common::native_harness::{NativeTestServer, do_handshake, send_api_key_auth, send_sql};
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;

fn create_api_key(shared: &SharedState, username: &str, roles: Vec<Role>) -> String {
    let user_id = if username == "nodedb" {
        shared
            .credentials
            .get_user(username)
            .expect("harness superuser")
            .user_id
    } else {
        shared
            .credentials
            .create_service_account(username, TenantId::new(1), roles, vec![DatabaseId::DEFAULT])
            .expect("create native service account")
    };
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
        .expect("create native API key")
}

async fn authenticated_stream(server: &NativeTestServer, token: String) -> tokio::net::TcpStream {
    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_api_key_auth(&mut stream, 1, token).await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native API key auth");
    stream
}

async fn seed_private_collection(server: &NativeTestServer, collection: &str) {
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(server, admin_token).await;
    let create = send_sql(&mut admin, 2, &format!("CREATE COLLECTION {collection}")).await;
    assert_eq!(
        create.status,
        ResponseStatus::Ok,
        "create private collection"
    );
    let insert = send_sql(
        &mut admin,
        3,
        &format!("INSERT INTO {collection} {{ id: 'hidden', value: 17 }}"),
    )
    .await;
    assert_eq!(insert.status, ResponseStatus::Ok, "seed private collection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_materialized_sql_rejects_collection_without_permission() {
    let server = NativeTestServer::start_authenticated().await;
    seed_private_collection(&server, "native_materialized_private").await;
    let token = create_api_key(
        &server.shared,
        "native_materialized_reader",
        vec![Role::Custom("native_materialized_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(
        &mut stream,
        2,
        "SELECT * FROM native_materialized_private ORDER BY id",
    )
    .await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native materialized SQL must enforce PermissionStore before dispatch"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native materialized denial must report insufficient privilege: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_materialized_sql_rejects_write_without_permission_or_mutation() {
    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;
    let create = send_sql(&mut admin, 2, "CREATE COLLECTION native_denied_writes").await;
    assert_eq!(create.status, ResponseStatus::Ok, "create write target");

    let token = create_api_key(
        &server.shared,
        "native_ungranted_writer",
        vec![Role::Custom("native_ungranted_writer_role".into())],
    );
    let mut restricted = authenticated_stream(&server, token).await;
    let response = send_sql(
        &mut restricted,
        2,
        "INSERT INTO native_denied_writes { id: 'forbidden', value: 23 }",
    )
    .await;
    let observed = send_sql(
        &mut admin,
        3,
        "SELECT id FROM native_denied_writes ORDER BY id",
    )
    .await;
    drop(restricted);
    drop(admin);
    server.shutdown().await;

    assert!(
        observed.rows.as_ref().is_none_or(Vec::is_empty)
            && observed.rows_affected.unwrap_or_default() == 0,
        "an unauthorized native write must not mutate the collection: {observed:?}"
    );
    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native writes require explicit PermissionStore authorization before dispatch"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native write denial must report insufficient privilege: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_explain_rejects_collection_before_plan_metadata() {
    let server = NativeTestServer::start_authenticated().await;
    seed_private_collection(&server, "native_explain_private").await;
    let token = create_api_key(
        &server.shared,
        "native_explain_reader",
        vec![Role::Custom("native_explain_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(
        &mut stream,
        2,
        "EXPLAIN SELECT * FROM native_explain_private",
    )
    .await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(response.status, ResponseStatus::Error);
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native EXPLAIN must authorize before exposing plan metadata: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_lazy_sql_rejects_collection_without_permission_before_stream_open() {
    let server = NativeTestServer::start_authenticated().await;
    seed_private_collection(&server, "native_lazy_private").await;
    let token = create_api_key(
        &server.shared,
        "native_lazy_reader",
        vec![Role::Custom("native_lazy_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(&mut stream, 2, "SELECT * FROM native_lazy_private").await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native lazy SQL must reject access before opening the result stream"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native lazy denial must report insufficient privilege: {response:?}"
    );
}
