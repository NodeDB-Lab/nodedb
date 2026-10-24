// SPDX-License-Identifier: BUSL-1.1

//! Read authorization for sync shape snapshots.
//!
//! A shape subscription is a client-supplied `ShapeDefinition` that names a
//! collection; the server answers it with a snapshot of that collection's
//! documents. The snapshot is a read on behalf of the sync session, so it must
//! obey exactly the same rules every other read obeys:
//!
//! 1. An unauthenticated session gets no data.
//! 2. A session without a read grant on the named collection gets no data.
//! 3. Rows excluded by a row-level-security policy are never delivered.
//! 4. The same rules apply to `ResyncRequest`, which re-runs the same snapshot.
//!
//! The write side of sync already enforces this (`authorize_delta_write` →
//! `authorize_collection` → `authorize_sync_task`); these tests pin the read
//! side to the same contract.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::server::sync::shape::handler::{ShapeSnapshotMsg, ShapeSubscribeMsg};
use nodedb_test_support::jwks_fixture::{JwksFixture, loopback_egress, now_secs};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{
    HandshakeAckMsg, HandshakeMsg, ResyncReason, ResyncRequestMsg, SyncFrame, SyncMessageType,
};

/// Sync sessions on this harness authenticate into tenant 1, matching the
/// tenant the pgwire harness superuser writes into.
const TENANT: u64 = 1;

/// Issuer and audience of the test JWT provider.
const ISSUER: &str = "https://sync-shape-read-authorization.example/";
const AUDIENCE: &str = "nodedb-sync";

/// How long a test waits for a server frame before concluding none is coming.
const FRAME_WAIT: Duration = Duration::from_secs(3);

// ── JWT minting ────────────────────────────────────────────────────────────

/// A server whose `[auth.jwt]` provider accepts [`Jwt::mint`] output, so sync
/// sessions authenticate through the same bearer gate as every other route.
struct Jwt {
    server: TestServer,
    fixture: JwksFixture,
}

impl Jwt {
    async fn start_server() -> Self {
        let fixture = JwksFixture::spawn().await;
        let registry = JwksRegistry::init(
            fixture.auth_config(ISSUER, AUDIENCE, TENANT),
            loopback_egress(),
        )
        .await
        .expect("test JWKS registry must initialize");
        let server = TestServer::start_with_jwks(std::sync::Arc::new(registry)).await;
        Self { server, fixture }
    }

    /// Mint a token for `subject` carrying `roles`.
    fn mint_token(&self, subject: &str, roles: &[&str]) -> String {
        let now = now_secs();
        self.fixture.mint(&serde_json::json!({
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "roles": roles,
            "iat": now,
            "exp": now + 600,
        }))
    }
}

// ── Sync WebSocket plumbing ────────────────────────────────────────────────

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Start a sync listener bound to an ephemeral port, wired to `server`'s state
/// — including the JWKS registry that verifies [`Jwt::mint_token`] output.
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

/// Complete a handshake as the principal named by `token`.
async fn handshake(ws: &mut Ws, token: &str) {
    let msg = HandshakeMsg {
        jwt_token: token.to_string(),
        vector_clock: HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "shape-authz-test".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: nodedb_types::sync::wire::SYNC_WIRE_VERSION,
    };
    send(ws, SyncMessageType::Handshake, &msg).await;
    let frame = next_frame(ws).await.expect("handshake must be answered");
    assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);
    let ack: HandshakeAckMsg = frame.decode_body().expect("handshake ack decodes");
    assert!(
        ack.success,
        "handshake rejected for a valid token: {:?}",
        ack.error
    );
}

async fn send<T: zerompk::ToMessagePack>(ws: &mut Ws, msg_type: SyncMessageType, body: &T) {
    let frame = SyncFrame::try_encode(msg_type, body).expect("frame encodes");
    ws.send(Message::Binary(frame.to_bytes().into()))
        .await
        .expect("frame send");
}

/// Read the next binary sync frame, or `None` if the server sends nothing
/// within [`FRAME_WAIT`].
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

/// Send a `ShapeSubscribe` for `collection` and return the snapshot the server
/// answers with, if any. Schema-announce frames that may precede the snapshot
/// are skipped.
async fn subscribe_shape(
    ws: &mut Ws,
    shape_id: &str,
    collection: &str,
) -> Option<ShapeSnapshotMsg> {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: shape_id.to_string(),
            tenant_id: TENANT as u32,
            shape_type: ShapeType::Document {
                collection: collection.to_string(),
                predicate: Vec::new(),
            },
            description: "read authorization probe".into(),
            field_filter: Vec::new(),
        },
    };
    send(ws, SyncMessageType::ShapeSubscribe, &msg).await;
    await_snapshot(ws).await
}

async fn await_snapshot(ws: &mut Ws) -> Option<ShapeSnapshotMsg> {
    while let Some(frame) = next_frame(ws).await {
        if frame.msg_type == SyncMessageType::ShapeSnapshot {
            return Some(frame.decode_body().expect("shape snapshot decodes"));
        }
    }
    None
}

/// Documents delivered by a snapshot, or zero when the server refused it.
fn delivered_documents(snapshot: &Option<ShapeSnapshotMsg>) -> usize {
    snapshot.as_ref().map(|s| s.doc_count).unwrap_or(0)
}

// ── Fixtures ───────────────────────────────────────────────────────────────

/// Create `collection` and populate it with three rows owned by `owner`.
async fn seed_collection(server: &TestServer, collection: &str, owner: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for i in 1..=3 {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, secret) \
                 VALUES ('doc-{i}', '{owner}', 'classified-{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection} row {i}: {e}"));
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// A session that never completed a handshake has no identity, so it must not
/// receive collection data. The shape frames are processed before the session
/// loop's authentication gate, so this is the widest form of the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthenticated_shape_subscribe_delivers_no_documents() {
    let jwt = Jwt::start_server().await;
    seed_collection(&jwt.server, "shape_anon_docs", "alice").await;
    let addr = start_listener(&jwt.server).await;

    let mut ws = connect(addr).await;
    let snapshot = subscribe_shape(&mut ws, "anon-shape", "shape_anon_docs").await;

    // The refusal must be visible in the protocol: a session with no identity
    // gets no snapshot at all. Asserting only on `doc_count` would pass for the
    // wrong reason — an unauthenticated session falls back to tenant 0, whose
    // collections happen to be empty, so an answered-but-empty snapshot still
    // means the read ran without an identity behind it.
    assert!(
        snapshot.is_none(),
        "a session with no established identity was answered with a shape snapshot: {snapshot:?}"
    );
}

/// An authenticated principal with no grant on the named collection must not
/// be able to read it by naming it in a shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shape_subscribe_without_read_grant_delivers_no_documents() {
    let jwt = Jwt::start_server().await;
    seed_collection(&jwt.server, "shape_ungranted_docs", "alice").await;
    let addr = start_listener(&jwt.server).await;

    let mut ws = connect(addr).await;
    handshake(&mut ws, &jwt.mint_token("shape_intruder", &[])).await;
    let snapshot = subscribe_shape(&mut ws, "ungranted-shape", "shape_ungranted_docs").await;

    assert_eq!(
        delivered_documents(&snapshot),
        0,
        "a principal without a read grant received documents from the collection"
    );
}

/// A read-level RLS policy must apply to the snapshot. The rows here belong to
/// `alice`; the subscribing principal is `shape_reader`, so the policy excludes
/// every row — the snapshot is delivered, and it is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shape_subscribe_applies_row_level_security() {
    let jwt = Jwt::start_server().await;
    seed_collection(&jwt.server, "shape_rls_docs", "alice").await;
    jwt.server
        .exec(
            "CREATE RLS POLICY shape_owner_only ON shape_rls_docs FOR READ \
             USING (owner = $auth.id)",
        )
        .await
        .expect("create RLS policy");
    let addr = start_listener(&jwt.server).await;

    let mut ws = connect(addr).await;
    handshake(&mut ws, &jwt.mint_token("shape_reader", &["readwrite"])).await;
    let snapshot = subscribe_shape(&mut ws, "rls-shape", "shape_rls_docs").await;

    assert_eq!(
        delivered_documents(&snapshot),
        0,
        "the snapshot delivered rows the read policy excludes"
    );
}

/// `ResyncRequest` re-runs the same snapshot machinery, so it is a second door
/// to the same data and must be closed the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resync_without_read_grant_delivers_no_documents() {
    let jwt = Jwt::start_server().await;
    seed_collection(&jwt.server, "shape_resync_docs", "alice").await;
    let addr = start_listener(&jwt.server).await;

    let mut ws = connect(addr).await;
    handshake(&mut ws, &jwt.mint_token("resync_intruder", &[])).await;
    let _ = subscribe_shape(&mut ws, "resync-shape", "shape_resync_docs").await;

    send(
        &mut ws,
        SyncMessageType::ResyncRequest,
        &ResyncRequestMsg {
            reason: ResyncReason::CorruptedState,
            from_mutation_id: 0,
            collection: "shape_resync_docs".to_string(),
            shape_id: "resync-shape".to_string(),
        },
    )
    .await;
    let snapshot = await_snapshot(&mut ws).await;

    assert_eq!(
        delivered_documents(&snapshot),
        0,
        "resync delivered documents to a principal without a read grant"
    );
}
