//! Wire-level verification that `SessionHandleStore::resolve()` binds to the
//! caller's `(tenant_id, peer IP)` fingerprint.
//!
//! Strategy: loopback provides every `127.0.0.0/8` address as a routable
//! source on Linux. A client TCP socket that `bind()`s to `127.1.0.2` before
//! `connect()`ing to `127.0.0.1:<server_port>` is seen by the server with
//! `peer_addr() == 127.1.0.2`. That lets us exercise the fingerprint check
//! without netns or a second NIC.
//!
//! We drive two SET LOCAL attempts against the same handle:
//!   1. from a client bound on the handle's creating IP — must be accepted
//!   2. from a client bound on a `/24`-distant IP — must be rejected and
//!      counted as a miss against the caller's tenant.
//!
//! Both assertions are wire-initiated (pgwire `SET LOCAL`) and observed
//! through side effects that are externally meaningful: the store's
//! per-tenant miss counter and the recorded `AuditEvent` stream.

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use nodedb::control::security::audit::AuditEvent;
use nodedb::control::security::auth_context::{AuthContext, generate_session_id};
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::control::security::session_handle::ClientFingerprint;
use nodedb::types::TenantId;

use common::pgwire_harness::TestServer;

fn nodedb_auth_ctx() -> AuthContext {
    // Matches the harness-provisioned `nodedb` superuser (tenant 1) so the
    // resolver can hand back a context the query path will accept.
    let identity = AuthenticatedIdentity {
        user_id: 1,
        username: "nodedb".into(),
        tenant_id: TenantId::new(1),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    };
    AuthContext::from_identity(&identity, generate_session_id())
}

async fn connect_from(
    local: IpAddr,
    server_port: u16,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let socket = match local {
        IpAddr::V4(_) => tokio::net::TcpSocket::new_v4().unwrap(),
        IpAddr::V6(_) => tokio::net::TcpSocket::new_v6().unwrap(),
    };
    socket.bind(SocketAddr::new(local, 0)).unwrap();
    let stream = socket
        .connect(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            server_port,
        ))
        .await
        .unwrap();

    let mut cfg = tokio_postgres::Config::new();
    cfg.user("nodedb").dbname("nodedb");
    let (client, conn) = cfg
        .connect_raw(stream, tokio_postgres::NoTls)
        .await
        .unwrap();
    let handle = tokio::spawn(async move {
        let _ = conn.await;
    });
    (client, handle)
}

#[tokio::test]
async fn set_local_auth_session_rejects_mismatched_fingerprint_origin() {
    let server = TestServer::start().await;

    // Install a handle pre-bound to a specific client fingerprint. In
    // production this capture happens inside `POST /api/auth/session`; here
    // we short-circuit the HTTP path because only the pgwire wire-level
    // resolver is under test.
    let bound_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 5));
    let captured_fp = ClientFingerprint::new(TenantId::new(1), bound_ip);
    let handle = server
        .shared
        .session_handles
        .create(nodedb_auth_ctx(), captured_fp);

    let tenant = TenantId::new(1);
    let baseline_miss = server.shared.session_handles.miss_total_for_tenant(tenant);
    let audit_events_before: Vec<AuditEvent> = {
        let log = server.shared.audit.lock().unwrap();
        log.all().iter().map(|e| e.event.clone()).collect()
    };

    // 1) Matching origin: same /24 as the captured IP, so Subnet (default)
    //    accepts. The SET is a no-op on miss anyway; here we assert there
    //    is NO miss recorded.
    {
        let (client, conn) = connect_from(bound_ip, server.pg_port).await;
        client
            .simple_query(&format!("SET LOCAL nodedb.auth_session = '{handle}'"))
            .await
            .unwrap();
        drop(client);
        // Wait for connection teardown — avoids the rate-limit state bleeding
        // across connections (keyed by pgwire peer addr).
        let _ = tokio::time::timeout(Duration::from_millis(200), conn).await;
    }
    let after_match = server.shared.session_handles.miss_total_for_tenant(tenant);
    assert_eq!(
        after_match, baseline_miss,
        "matching-fingerprint SET LOCAL must not increment miss counter"
    );

    // 2) Mismatched origin: `127.1.0.2` is a different /24 AND /16 from the
    //    captured `127.0.0.5`, so Subnet rejects. Miss counter must tick
    //    and a `SessionHandleFingerprintMismatch` audit event must be
    //    recorded.
    let attacker_ip = IpAddr::V4(Ipv4Addr::new(127, 1, 0, 2));
    {
        let (client, conn) = connect_from(attacker_ip, server.pg_port).await;
        client
            .simple_query(&format!("SET LOCAL nodedb.auth_session = '{handle}'"))
            .await
            .unwrap();
        drop(client);
        let _ = tokio::time::timeout(Duration::from_millis(200), conn).await;
    }
    let after_mismatch = server.shared.session_handles.miss_total_for_tenant(tenant);
    assert!(
        after_mismatch > after_match,
        "mismatched-fingerprint SET LOCAL must increment miss counter \
         (before={after_match}, after={after_mismatch})"
    );

    let audit_events_after: Vec<AuditEvent> = {
        let log = server.shared.audit.lock().unwrap();
        log.all().iter().map(|e| e.event.clone()).collect()
    };
    let new_events = &audit_events_after[audit_events_before.len()..];
    assert!(
        new_events
            .iter()
            .any(|e| matches!(e, AuditEvent::SessionHandleFingerprintMismatch)),
        "expected SessionHandleFingerprintMismatch in the audit log; got {new_events:?}"
    );
}
