// SPDX-License-Identifier: BUSL-1.1

//! Session kills against real pgwire, WebSocket and SSE connections, and the
//! admin DDL that issues them.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::identity::{AuthenticatedIdentity, Role};
use nodedb::control::security::sessions::{
    AUTH_METHOD_OIDC, KillReason, SessionBindRequest, SessionParams, bind_session,
};
use nodedb::control::server::session_auth::trust_identity;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb_test_support::pgwire_harness::TestServer;

const CLOSE_BOUND: Duration = Duration::from_secs(10);

/// Wait for the connection task of a client the server closed.
async fn assert_connection_closed(connection: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(CLOSE_BOUND, connection)
        .await
        .expect("the kill closes the connection")
        .expect("the connection task ends cleanly");
}

/// The id of `user`'s only session in `SHOW SESSIONS`.
fn session_id_of(server: &TestServer, user: &str) -> String {
    let mut sessions = server.shared.session_registry.list_all();
    sessions.retain(|session| session.db_user == user);
    assert_eq!(sessions.len(), 1, "{user} holds one session");
    sessions.remove(0).session_id
}

async fn connected_user(
    server: &TestServer,
    user: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD 'pw'"))
        .await
        .expect("create user");
    server.connect_as(user, "pw").await.expect("connect")
}

#[tokio::test]
async fn dropping_a_user_closes_its_quiet_pgwire_connection() {
    let server = TestServer::start().await;
    let (_client, connection) = connected_user(&server, "pg_victim").await;

    server.exec("DROP USER pg_victim").await.expect("drop user");

    assert_connection_closed(connection).await;
}

/// The issuer's own statement finishes before its session closes, and its result
/// reaches it. `MOVE TENANT` issued from inside the moved tenant takes this path.
#[tokio::test]
async fn a_session_that_kills_itself_receives_the_result() {
    let server = TestServer::start().await;
    let own_session = server
        .shared
        .session_registry
        .list_all()
        .into_iter()
        .find(|session| session.protocol == "pgwire")
        .expect("the harness client holds a session")
        .session_id;

    server
        .exec(&format!("KILL SESSION '{own_session}'"))
        .await
        .expect("the statement that ends the session still returns its result");

    let refused = tokio::time::timeout(CLOSE_BOUND, async {
        while server.exec("SELECT 1").await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(refused.is_ok(), "the session ends after its result");
}

/// `MOVE TENANT` requires a superuser, and a superuser can belong to the moved
/// tenant. The issuer's own session ends at the cutover, after its statement
/// returns its result.
#[tokio::test]
async fn move_tenant_from_inside_the_moved_tenant_returns_then_closes() {
    let server = TestServer::start().await;
    let collection = "CREATE COLLECTION self_orders \
                      (order_id STRING PRIMARY KEY, amount STRING) WITH (engine='kv')";
    for statement in [
        "CREATE DATABASE self_src",
        "USE DATABASE self_src",
        "CREATE TENANT self_acme ID 31",
        collection,
        "INSERT INTO self_orders (order_id, amount) VALUES ('o1', '100')",
        "USE DATABASE default",
        "CREATE DATABASE self_tgt",
        "USE DATABASE self_tgt",
        collection,
        "USE DATABASE default",
    ] {
        server.exec(statement).await.expect(statement);
    }
    server
        .shared
        .credentials
        .create_user("self_mover", "pw", TenantId::new(31), vec![Role::Superuser])
        .expect("create the issuer inside the tenant");
    let (client, connection) = server
        .connect_as("self_mover", "pw")
        .await
        .expect("connect");

    let moved = client
        .simple_query("MOVE TENANT self_acme FROM self_src TO self_tgt")
        .await;

    assert!(moved.is_ok(), "the issuer receives the result: {moved:?}");
    assert_connection_closed(connection).await;
}

/// A password session names its user by the startup username. A new user under
/// that name is a different principal, and the old socket cannot act as it.
#[tokio::test]
async fn a_recreated_name_does_not_inherit_a_password_session() {
    let server = TestServer::start_password().await;
    let (client, connection) = connected_user(&server, "pw_victim").await;

    server.exec("DROP USER pw_victim").await.expect("drop user");
    server
        .exec("CREATE USER pw_victim PASSWORD 'pw'")
        .await
        .expect("recreate user");

    assert_connection_closed(connection).await;
    assert!(client.simple_query("SELECT 1").await.is_err());
}

#[tokio::test]
async fn show_sessions_lists_a_real_session_and_kill_session_ends_it() {
    let server = TestServer::start().await;
    let (_client, connection) = connected_user(&server, "listed").await;
    let session_id = session_id_of(&server, "listed");

    let listed = server
        .query_text("SHOW SESSIONS")
        .await
        .expect("show sessions");
    assert!(listed.contains(&session_id), "{listed:?}");

    server
        .exec(&format!("KILL SESSION '{session_id}'"))
        .await
        .expect("kill session");

    assert_connection_closed(connection).await;
}

#[tokio::test]
async fn kill_user_sessions_ends_every_session_of_the_user() {
    let server = TestServer::start().await;
    let (_first, first) = connected_user(&server, "many").await;
    let (_second, second) = server.connect_as("many", "pw").await.expect("connect");
    let user_id = server
        .shared
        .credentials
        .get_user("many")
        .expect("user")
        .user_id;

    server
        .exec(&format!("KILL USER SESSIONS '{user_id}'"))
        .await
        .expect("kill user sessions");

    assert_connection_closed(first).await;
    assert_connection_closed(second).await;
}

/// A stored user and an OIDC principal can hold the same number. The statement
/// names one of them, and the other stays connected.
#[tokio::test]
async fn kill_user_sessions_names_one_principal_for_a_shared_number() {
    let server = TestServer::start().await;
    let (_client, stored) = connected_user(&server, "numbered").await;
    let user_id = server
        .shared
        .credentials
        .get_user("numbered")
        .expect("user")
        .user_id;
    let oidc = server
        .shared
        .session_registry
        .register(
            "oidc-same-number",
            &SessionParams {
                user_id,
                username: "external_user".to_string(),
                db_user: "external_user".to_string(),
                peer_addr: "10.0.0.9:1".to_string(),
                protocol: "native".to_string(),
                auth_method: AUTH_METHOD_OIDC.to_string(),
                tenant_id: 1,
                credential_version: 0,
                current_database: None,
                token_expiry_ms: None,
                api_key_id: None,
            },
        )
        .expect("register");

    server
        .exec(&format!("KILL USER SESSIONS '{user_id}'"))
        .await
        .expect("kill stored user");
    assert_connection_closed(stored).await;
    assert_eq!(*oidc.borrow(), KillReason::Alive);

    server
        .exec(&format!("KILL USER SESSIONS '{user_id}' OIDC"))
        .await
        .expect("kill oidc user");
    assert_eq!(*oidc.borrow(), KillReason::AdminKill);

    let refused = server
        .exec(&format!("KILL USER SESSIONS '{user_id}' ALL"))
        .await;
    assert!(refused.is_err(), "an unknown qualifier is refused");
}

/// The blacklist names a principal by the id its auth context carries.
#[tokio::test]
async fn blacklist_with_kill_sessions_ends_the_named_users_sessions() {
    let server = TestServer::start().await;
    let (_client, connection) = connected_user(&server, "banned").await;
    let user_id = server
        .shared
        .credentials
        .get_user("banned")
        .expect("user")
        .user_id;

    server
        .exec(&format!(
            "BLACKLIST AUTH USER '{user_id}' REASON 'abuse' WITH KILL SESSIONS"
        ))
        .await
        .expect("blacklist");

    assert_connection_closed(connection).await;
}

#[tokio::test]
async fn a_websocket_session_ends_on_a_kill() {
    let server = TestServer::start().await;
    let url = format!("ws://127.0.0.1:{}/v1/ws", server.http_port);
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("upgrade");
    let session_id = server
        .shared
        .session_registry
        .list_all()
        .into_iter()
        .find(|session| session.protocol == "websocket")
        .expect("the upgrade registers a session")
        .session_id;

    server
        .shared
        .session_registry
        .kill_session_by_id(&session_id, KillReason::AdminKill);

    let drained = tokio::time::timeout(CLOSE_BOUND, async {
        while let Some(Ok(_)) = socket.next().await {}
    })
    .await;
    assert!(drained.is_ok(), "the kill closes the WebSocket");
}

#[tokio::test]
async fn an_sse_stream_ends_on_a_kill_and_leaves_no_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION sse_docs (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')")
        .await
        .expect("create collection");
    let url = format!("http://127.0.0.1:{}/v1/cdc/sse_docs", server.http_port);
    let mut response = reqwest::get(&url).await.expect("open stream");
    assert!(response.status().is_success());
    let session_id = server
        .shared
        .session_registry
        .list_all()
        .into_iter()
        .find(|session| session.protocol == "sse")
        .expect("the stream registers a session")
        .session_id;

    server
        .shared
        .session_registry
        .kill_session_by_id(&session_id, KillReason::AdminKill);

    tokio::time::timeout(CLOSE_BOUND, async {
        while let Ok(Some(_)) = response.chunk().await {}
    })
    .await
    .expect("the kill ends the stream");
    let row_gone = tokio::time::timeout(CLOSE_BOUND, async {
        while server
            .shared
            .session_registry
            .list_all()
            .iter()
            .any(|session| session.session_id == session_id)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(row_gone.is_ok(), "the ended stream leaves no row");
}

/// The cap reaches the registry through the `SharedState::open` configuration.
#[test]
fn max_active_sessions_refuses_a_bind_over_the_cap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = Arc::new(
        nodedb::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
    );
    let auth_config = nodedb::config::auth::AuthConfig::default();
    let (dispatcher, _sides) = Dispatcher::new(1, 64);
    let state = SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: Arc::new(nodedb::control::metrics::SystemMetrics::new()),
        },
        wal,
        &dir.path().join("system.redb"),
        nodedb::control::state::OpenConfig {
            max_active_sessions: 1,
            ..nodedb::control::state::OpenConfig::strict(&auth_config)
        },
        nodedb_types::config::TuningConfig::default(),
        false,
        nodedb::data::executor::core_loop::test_governor(),
    )
    .expect("open");
    state
        .credentials
        .create_user("capped", "pw", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create user");
    let identity: AuthenticatedIdentity = trust_identity(&state, "capped").expect("identity");
    let request = || SessionBindRequest::new(&identity, "test", "127.0.0.1:1");

    let first = bind_session(&state, request()).expect("the first session fits");
    assert!(matches!(
        bind_session(&state, request()),
        Err(nodedb::Error::SessionCapExceeded { cap: 1 })
    ));

    drop(first);
    bind_session(&state, request()).expect("a closed session frees its slot");
}
