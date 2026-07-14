// SPDX-License-Identifier: BUSL-1.1

//! End-to-end TCP roundtrip: real pgwire connection executes DDL and
//! observes both state mutation and SHOW SESSION results.

mod common;

use std::sync::Arc;

use common::{pgwire_auth_helpers::make_state, pgwire_harness::TestServer};
use nodedb::control::security::identity::Role;
use nodedb::types::TenantId;
use tokio_postgres::SimpleQueryMessage;

async fn connect_empty_store_trust(
    server: &TestServer,
    username: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(server.pg_port)
        .user(username)
        .dbname("default");
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("trust mode must accept a client-selected identity");
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, connection_handle)
}

#[tokio::test]
async fn pgwire_ddl_roundtrip() {
    let state = make_state();

    let pg_listener =
        nodedb::control::server::pgwire::listener::PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let port = pg_listener.local_addr().port();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&state.shutdown));
    let shared_pg = Arc::clone(&state);
    let test_startup_gate = Arc::clone(&state.startup);
    let bus_pg = shutdown_bus.clone();
    let listener_handle = tokio::spawn(async move {
        pg_listener
            .run(
                shared_pg,
                nodedb::config::auth::AuthMode::Trust,
                None,
                Arc::new(tokio::sync::Semaphore::new(128)),
                test_startup_gate,
                bus_pg,
            )
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let conn_str = format!("host=127.0.0.1 port={port} user=nodedb dbname=nodedb");
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .simple_query("CREATE USER wire_test WITH PASSWORD 'pass'")
        .await
        .unwrap();

    let msgs = client.simple_query("SHOW SESSION").await.unwrap();
    let username = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(username, Some("nodedb".to_string()));

    assert!(state.credentials.get_user("wire_test").is_some());

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    let _shutdown = shutdown_bus.initiate();
    listener_handle
        .await
        .expect("pgwire listener task must not panic")
        .expect("pgwire listener must shut down cleanly");
}

#[tokio::test]
async fn trust_session_identity_is_not_persisted_across_password_restart() {
    let server = TestServer::start_empty_store_trust().await;
    let username = "ephemeral_wire_identity";

    let (client, connection_handle) = connect_empty_store_trust(&server, username).await;

    let messages = client
        .simple_query("SHOW SESSION")
        .await
        .expect("trusted connection must retain the client-selected identity");
    let session_username = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    });
    assert_eq!(session_username, Some(username.to_owned()));

    // Parse and Execute must use the same startup-bound resolver as the
    // simple-query path; this identity has no credential-store record.
    let statement = client
        .prepare("SELECT 1 AS prepared_identity")
        .await
        .expect("prepared Parse must resolve the ephemeral trust identity");
    let rows = client
        .query(&statement, &[])
        .await
        .expect("prepared Execute must resolve the ephemeral trust identity");
    assert_eq!(rows.len(), 1);

    let credentials = &server.shared.credentials;
    assert!(
        credentials.get_user(username).is_none(),
        "a trust-mode client-selected identity must not become a credential"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    let (server, data_dir) = server.take_dir();
    server.graceful_shutdown().await;

    let (reopened, _data_dir) = TestServer::open_on_path_empty_store_password(data_dir).await;
    assert!(
        reopened.shared.credentials.get_user(username).is_none(),
        "the trust-mode session identity must not be persisted across restart"
    );

    let mut password_config = tokio_postgres::Config::new();
    password_config
        .host("127.0.0.1")
        .port(reopened.pg_port)
        .user(username)
        .password("")
        .dbname("default");
    let password_login_rejected = password_config
        .connect(tokio_postgres::NoTls)
        .await
        .is_err();

    reopened.graceful_shutdown().await;

    assert!(
        password_login_rejected,
        "a client-selected trust identity must not authenticate in password mode"
    );
}

#[tokio::test]
async fn trust_identity_survives_discard_all_without_credential_persistence() {
    let server = TestServer::start_empty_store_trust().await;
    let username = "discard_all_ephemeral_wire_identity";
    let (client, connection_handle) = connect_empty_store_trust(&server, username).await;

    client
        .simple_query("SET nodedb.consistency = eventual")
        .await
        .expect("SET must establish mutable session state before DISCARD ALL");
    client
        .simple_query("SET TENANT = 99")
        .await
        .expect("SET TENANT must establish a temporary tenant overlay");
    client
        .simple_query("DISCARD ALL")
        .await
        .expect("DISCARD ALL must retain the authenticated trust identity");
    let messages = client
        .simple_query("SHOW SESSION")
        .await
        .expect("trusted connection must remain authenticated after DISCARD ALL");
    let session_username = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    });

    assert_eq!(session_username, Some(username.to_owned()));

    let tenant_messages = client
        .simple_query("SHOW TENANT")
        .await
        .expect("DISCARD ALL must clear the tenant overlay");
    let effective_tenant = tenant_messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    });
    assert_eq!(effective_tenant, Some("1".to_owned()));

    let consistency_messages = client
        .simple_query("SHOW nodedb.consistency")
        .await
        .expect("DISCARD ALL must reset session parameters");
    let consistency = consistency_messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        });
    assert_eq!(consistency, Some("strong".to_owned()));
    assert!(
        server.shared.credentials.get_user(username).is_none(),
        "DISCARD ALL must not persist an ephemeral trust identity"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_known_user_role_downgrade_takes_effect() {
    let server = TestServer::start().await;
    let username = "known_trust_role_downgrade";
    server
        .shared
        .credentials
        .create_user(
            username,
            "unused-in-trust-mode",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .expect("create known Trust user");

    let (client, connection_handle) = server
        .connect_as(username, "ignored")
        .await
        .expect("known Trust user must authenticate");
    client
        .simple_query("SHOW SESSION")
        .await
        .expect("known Trust user must issue an initial query");

    server
        .shared
        .credentials
        .update_roles(username, vec![Role::ReadOnly])
        .expect("downgrade known Trust user");
    let role_downgrade = client
        .simple_query("CREATE USER stale_trust_role_probe WITH PASSWORD 'x'")
        .await;
    assert!(
        role_downgrade.is_err(),
        "a known Trust connection must not retain a stale superuser identity after role removal"
    );
    assert!(
        server
            .shared
            .credentials
            .get_user("stale_trust_role_probe")
            .is_none(),
        "stale Trust roles must not authorize DDL"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_known_user_drop_fails_closed() {
    let server = TestServer::start().await;
    let username = "known_trust_drop";
    server
        .shared
        .credentials
        .create_user(
            username,
            "unused-in-trust-mode",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .expect("create known Trust user");

    let (client, connection_handle) = server
        .connect_as(username, "ignored")
        .await
        .expect("known Trust user must authenticate");
    client
        .simple_query("SHOW SESSION")
        .await
        .expect("known Trust user must issue an initial query");

    server
        .shared
        .credentials
        .drop_user(username)
        .expect("drop known Trust user");
    assert!(
        client.simple_query("SHOW SESSION").await.is_err(),
        "a dropped known Trust user must fail closed on its next request"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}
