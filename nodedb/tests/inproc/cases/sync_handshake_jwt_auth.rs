// SPDX-License-Identifier: BUSL-1.1

//! Bearer-token authentication on the sync WebSocket handshake.
//!
//! A sync client authenticates by putting a JWT in its `Handshake` frame. That
//! token must be verified by the same machinery every other bearer route uses:
//! the server-wide `[auth.jwt]` providers reached through `SharedState`, plus
//! the post-verification policy (claim remapping, blocked account status,
//! declared-scope enforcement).
//!
//! These tests pin both directions of that contract:
//!
//! 1. A token minted by a configured provider authenticates the session.
//! 2. A token the server cannot verify — no provider configured, wrong key,
//!    or refused by policy — never does, and never falls through to the
//!    trust identity the empty-token branch hands out.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb::config::auth::JwtAuthConfig;
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::server::sync::shape::handler::{ShapeSnapshotMsg, ShapeSubscribeMsg};
use nodedb_test_support::jwks_fixture::{JwksFixture, loopback_egress, now_secs};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{HandshakeAckMsg, HandshakeMsg, SyncFrame, SyncMessageType};

/// Tenant the test provider binds its identities to.
const TENANT: u64 = 1;
const ISSUER: &str = "https://sync-handshake-jwt-auth.example/";
const AUDIENCE: &str = "nodedb-sync";

/// How long a test waits for a server frame before concluding none is coming.
const FRAME_WAIT: Duration = Duration::from_secs(3);

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

// ── Fixture ────────────────────────────────────────────────────────────────

/// A JWKS fixture plus the server whose `[auth.jwt]` section points at it.
struct SyncAuthFixture {
    server: TestServer,
    jwks: JwksFixture,
}

impl SyncAuthFixture {
    /// Start a server whose only JWT provider is this fixture's endpoint.
    async fn start() -> Self {
        Self::start_with(|config| config).await
    }

    /// Start a server, letting the caller adjust the `[auth.jwt]` section
    /// built from the fixture's provider before the registry is initialized.
    async fn start_with(adjust: impl FnOnce(JwtAuthConfig) -> JwtAuthConfig) -> Self {
        let jwks = JwksFixture::spawn().await;
        let config = adjust(jwks.auth_config(ISSUER, AUDIENCE, TENANT));
        let registry = JwksRegistry::init(config, loopback_egress())
            .await
            .expect("test JWKS registry must initialize");
        let server = TestServer::start_with_jwks(std::sync::Arc::new(registry)).await;
        Self { server, jwks }
    }

    /// Mint a token this fixture's provider accepts, carrying `extra` claims
    /// on top of the standard route and time claims.
    fn mint(&self, subject: &str, extra: serde_json::Value) -> String {
        let now = now_secs();
        let mut claims = serde_json::json!({
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "roles": ["readwrite"],
            "iat": now,
            "exp": now + 600,
        });
        let (Some(claims_map), Some(extra_map)) = (claims.as_object_mut(), extra.as_object())
        else {
            panic!("claim sets must be JSON objects");
        };
        for (key, value) in extra_map {
            claims_map.insert(key.clone(), value.clone());
        }
        self.jwks.mint(&claims)
    }
}

// ── Sync WebSocket plumbing ────────────────────────────────────────────────

async fn start_listener(server: &TestServer) -> SocketAddr {
    let config = SyncListenerConfig {
        listen_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) = nodedb::control::shutdown::ShutdownBus::new(
        std::sync::Arc::new(nodedb::control::shutdown::ShutdownWatch::new()),
    );
    let state = start_sync_listener(
        config,
        Some(std::sync::Arc::clone(&server.shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    state.config.listen_addr
}

async fn connect(addr: SocketAddr) -> Ws {
    let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .expect("sync websocket connect");
    ws
}

async fn send<T: zerompk::ToMessagePack>(ws: &mut Ws, msg_type: SyncMessageType, body: &T) {
    let frame = SyncFrame::try_encode(msg_type, body).expect("frame encodes");
    ws.send(Message::Binary(frame.to_bytes().into()))
        .await
        .expect("frame send");
}

async fn next_frame(ws: &mut Ws) -> Option<SyncFrame> {
    let deadline = tokio::time::Instant::now() + FRAME_WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                if let Some(frame) = SyncFrame::from_bytes(&bytes) {
                    return Some(frame);
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

/// Present `token` in a handshake and return the ack the server answers with.
async fn handshake(ws: &mut Ws, token: &str) -> HandshakeAckMsg {
    let msg = HandshakeMsg {
        jwt_token: token.to_string(),
        vector_clock: HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "sync-jwt-auth-test".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: nodedb_types::sync::wire::SYNC_WIRE_VERSION,
    };
    send(ws, SyncMessageType::Handshake, &msg).await;
    let frame = next_frame(ws).await.expect("handshake must be answered");
    assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);
    frame.decode_body().expect("handshake ack decodes")
}

/// Subscribe to a document shape and return the snapshot, if one arrives.
///
/// A session with no identity is never answered at all, so a returned
/// snapshot is proof that the handshake bound a real identity to the session.
async fn subscribe_shape(ws: &mut Ws, collection: &str) -> Option<ShapeSnapshotMsg> {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: "jwt-auth-shape".into(),
            tenant_id: TENANT as u32,
            shape_type: ShapeType::Document {
                collection: collection.to_string(),
                predicate: Vec::new(),
            },
            description: "handshake identity probe".into(),
            field_filter: Vec::new(),
        },
    };
    send(ws, SyncMessageType::ShapeSubscribe, &msg).await;
    while let Some(frame) = next_frame(ws).await {
        if frame.msg_type == SyncMessageType::ShapeSnapshot {
            return Some(frame.decode_body().expect("shape snapshot decodes"));
        }
    }
    None
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// The regression this whole path exists for: a token signed by a configured
/// provider must authenticate the sync session. Before the handshake used the
/// shared bearer gate it consulted a standalone validator that production
/// never configured, so *every* token was refused and no sync client could
/// authenticate at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_token_authenticates_the_sync_session() {
    let fixture = SyncAuthFixture::start().await;
    fixture
        .server
        .exec("CREATE COLLECTION sync_jwt_docs")
        .await
        .expect("create probe collection");
    let addr = start_listener(&fixture.server).await;

    let mut ws = connect(addr).await;
    let ack = handshake(&mut ws, &fixture.mint("alice", serde_json::json!({}))).await;

    assert!(
        ack.success,
        "a token minted by the configured provider was refused: {:?}",
        ack.error
    );

    // The session carries an identity, not merely a success flag: an
    // identity-less session is never answered with a shape snapshot.
    let snapshot = subscribe_shape(&mut ws, "sync_jwt_docs").await;
    assert!(
        snapshot.is_some(),
        "the authenticated session was not answered with a shape snapshot"
    );
}

/// A deployment with no `[auth.jwt]` section cannot verify anything a client
/// presents. The token must be refused — not accepted, and not silently
/// downgraded to the configured trust identity that an *empty* token gets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_without_a_configured_provider_is_refused() {
    // Mint against a real provider, then present it to a server that has none.
    let jwks = JwksFixture::spawn().await;
    let now = now_secs();
    let token = jwks.mint(&serde_json::json!({
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "alice",
        "roles": ["readwrite"],
        "iat": now,
        "exp": now + 600,
    }));

    let server = TestServer::start().await;
    assert!(
        server.shared.jwks_registry.is_none(),
        "this fixture must have no JWT verifier for the refusal to be meaningful"
    );
    server
        .exec("CREATE COLLECTION sync_jwt_unverifiable_docs")
        .await
        .expect("create probe collection");
    let addr = start_listener(&server).await;

    let mut ws = connect(addr).await;
    let ack = handshake(&mut ws, &token).await;

    assert!(
        !ack.success,
        "an unverifiable bearer token authenticated the sync session"
    );
    assert!(
        subscribe_shape(&mut ws, "sync_jwt_unverifiable_docs")
            .await
            .is_none(),
        "the refused session was answered with a shape snapshot"
    );
}

/// A token whose account-status claim carries a blocked value is refused by
/// the JWT policy the JWKS registry applies after signature verification.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_with_blocked_status_claim_does_not_authenticate() {
    let fixture = SyncAuthFixture::start_with(|config| JwtAuthConfig {
        status_claim: Some("account_status".into()),
        blocked_statuses: vec!["suspended".into()],
        ..config
    })
    .await;
    let addr = start_listener(&fixture.server).await;

    let mut ws = connect(addr).await;
    let blocked = fixture.mint("alice", serde_json::json!({"account_status": "suspended"}));
    let ack = handshake(&mut ws, &blocked).await;
    assert!(
        !ack.success,
        "a token carrying a blocked account status authenticated the sync session"
    );

    // The same provider, key, and route with an allowed status: the refusal
    // above is the policy's doing, not a broken fixture.
    let mut ws = connect(addr).await;
    let allowed = fixture.mint("alice", serde_json::json!({"account_status": "active"}));
    let ack = handshake(&mut ws, &allowed).await;
    assert!(
        ack.success,
        "an allowed account status was refused: {:?}",
        ack.error
    );
}

/// The stateful half of the policy (`enforce_stateful_jwt_policy`) must run on
/// this path too: a token declaring a scope the server has never defined is
/// refused, exactly as on the HTTP bearer route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_declaring_an_undefined_scope_does_not_authenticate() {
    let fixture = SyncAuthFixture::start_with(|config| JwtAuthConfig {
        enforce_scopes: true,
        ..config
    })
    .await;
    let addr = start_listener(&fixture.server).await;

    let mut ws = connect(addr).await;
    let undeclared = fixture.mint(
        "alice",
        serde_json::json!({"permissions": ["scope_this_server_never_defined"]}),
    );
    let ack = handshake(&mut ws, &undeclared).await;
    assert!(
        !ack.success,
        "a token declaring an undefined scope authenticated the sync session"
    );

    let mut ws = connect(addr).await;
    let ack = handshake(&mut ws, &fixture.mint("alice", serde_json::json!({}))).await;
    assert!(
        ack.success,
        "a token declaring no scopes was refused: {:?}",
        ack.error
    );
}

/// A token whose signature does not verify against the provider's published
/// key is refused, and the failure is not disclosed to the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forged_signature_is_refused_without_disclosure() {
    let fixture = SyncAuthFixture::start().await;
    let addr = start_listener(&fixture.server).await;

    let token = fixture.mint("alice", serde_json::json!({}));
    let (signing_input, signature) = token
        .rsplit_once('.')
        .expect("a minted token has a signature");
    let forged = format!("{signing_input}.{}", "A".repeat(signature.len()));

    let mut ws = connect(addr).await;
    let ack = handshake(&mut ws, &forged).await;

    assert!(!ack.success, "a forged signature authenticated the session");
    let error = ack.error.unwrap_or_default().to_lowercase();
    for detail in ["signature", "issuer", "audience", "provider", "algorithm"] {
        assert!(
            !error.contains(detail),
            "the rejection disclosed {detail}: {error}"
        );
    }
}
