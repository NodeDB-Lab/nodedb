// SPDX-License-Identifier: BUSL-1.1

//! Native sessions against the session registry.
//!
//! The native identity lives as long as the connection, so the kill signal ends
//! a quiet session and the refresh at the request boundary carries a credential
//! change. The admin connection is the trust superuser. The victim authenticates
//! with its own Auth frame.

use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use nodedb::control::security::identity::Role;
use nodedb::types::TenantId;
use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode};

const CLOSE_BOUND: Duration = Duration::from_secs(10);

async fn authenticated_as(server: &NativeTestServer, username: &str) -> TcpStream {
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let response = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust {
                username: username.into(),
            }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(response.status, ResponseStatus::Ok, "{:?}", response.error);
    stream
}

fn create_user(server: &NativeTestServer, username: &str, role: Role) {
    server
        .shared
        .credentials
        .create_user(username, "pw", TenantId::new(1), vec![role])
        .expect("create user");
}

async fn assert_closed_by_server(stream: &mut TcpStream) {
    let mut rest = Vec::new();
    tokio::time::timeout(CLOSE_BOUND, stream.read_to_end(&mut rest))
        .await
        .expect("the kill closes a quiet session")
        .expect("the close is clean");
    assert!(!rest.is_empty(), "the server reports why before it closes");
}

#[tokio::test]
async fn dropping_a_user_closes_its_quiet_native_session() {
    let server = NativeTestServer::start().await;
    create_user(&server, "native_victim", Role::ReadOnly);
    let mut stream = authenticated_as(&server, "native_victim").await;
    assert_eq!(server.shared.session_registry.count(None), 1);

    server
        .shared
        .credentials
        .drop_user("native_victim")
        .expect("drop user");

    assert_closed_by_server(&mut stream).await;
}

#[tokio::test]
async fn a_closed_session_leaves_no_registry_row() {
    let server = NativeTestServer::start().await;
    create_user(&server, "native_brief", Role::ReadOnly);
    let stream = authenticated_as(&server, "native_brief").await;
    drop(stream);

    let deadline = tokio::time::Instant::now() + CLOSE_BOUND;
    while server.shared.session_registry.count(None) != 0 {
        assert!(tokio::time::Instant::now() < deadline, "row stays");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A second Auth frame is refused, so it cannot replace the binding.
#[tokio::test]
async fn a_second_auth_frame_keeps_the_session_id() {
    let server = NativeTestServer::start().await;
    create_user(&server, "native_once", Role::ReadOnly);
    create_user(&server, "native_other", Role::ReadOnly);
    let mut stream = authenticated_as(&server, "native_once").await;
    let before = server.shared.session_registry.list_all();

    let refused = send_request(
        &mut stream,
        2,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust {
                username: "native_other".into(),
            }),
            ..Default::default()
        },
    )
    .await;

    assert_eq!(refused.status, ResponseStatus::Error);
    let after = server.shared.session_registry.list_all();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].session_id, before[0].session_id);
    assert_eq!(after[0].db_user, "native_once");
}

#[tokio::test]
async fn a_role_change_reaches_the_next_native_request() {
    let server = NativeTestServer::start().await;
    create_user(&server, "native_soft", Role::ReadWrite);
    let (mut admin, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let created = send_sql(
        &mut admin,
        1,
        "CREATE COLLECTION soft_docs (id TEXT PRIMARY KEY, content TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    assert_eq!(created.status, ResponseStatus::Ok, "{:?}", created.error);
    let mut stream = authenticated_as(&server, "native_soft").await;
    let write = "INSERT INTO soft_docs (id, content) VALUES ('a', 'x')";
    let allowed = send_sql(&mut stream, 2, write).await;
    assert_eq!(allowed.status, ResponseStatus::Ok, "{:?}", allowed.error);

    server
        .shared
        .credentials
        .update_roles("native_soft", vec![Role::ReadOnly])
        .expect("update roles");

    let refused = send_sql(
        &mut stream,
        3,
        "INSERT INTO soft_docs (id, content) VALUES ('b', 'y')",
    )
    .await;
    assert_eq!(refused.status, ResponseStatus::Error);
    // The session stays open: a role change is a soft revoke.
    let read = send_sql(&mut stream, 4, "SELECT id FROM soft_docs").await;
    assert_eq!(read.status, ResponseStatus::Ok, "{:?}", read.error);
}
