// SPDX-License-Identifier: BUSL-1.1

//! Sync WebSocket plumbing for the row-push authorization cases.
//!
//! A session wakes on every outbound queue, so the collector only reads: it
//! sends nothing, as an idle client sends nothing.

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
use nodedb_types::filter::MetadataFilter;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{
    CollectionPurgedMsg, DefinitionSyncMsg, HandshakeAckMsg, HandshakeMsg, PresenceBroadcastMsg,
    PresenceLeaveMsg, RowOp, RowPushMsg, ShapeSnapshotRefusal, ShapeSnapshotRejectMsg, SyncFrame,
    SyncMessageType,
};

/// Sync sessions authenticate into tenant 1, the tenant the pgwire harness
/// superuser writes into.
pub const TENANT: u64 = 1;

const ISSUER: &str = "https://sync-row-push-authorization.example/";
/// Tokens of this issuer authenticate into [`OTHER_TENANT`].
const OTHER_ISSUER: &str = "https://sync-row-push-other-tenant.example/";
/// The tenant [`Harness::token_in_other_tenant`] tokens authenticate into.
pub const OTHER_TENANT: u64 = 10;
const AUDIENCE: &str = "nodedb-sync";

/// How long a subscription waits for its snapshot.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(3);
/// How long a collector waits for the push it expects.
pub const PUSH_WAIT: Duration = Duration::from_secs(10);
/// How long a collector keeps reading after the expected push arrived, so a
/// push that must not arrive has the chance to.
const SETTLE: Duration = Duration::from_millis(750);
/// How long one read waits before the collector checks its condition.
const POLL: Duration = Duration::from_millis(100);

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A server whose JWT provider accepts [`Harness::token`] output, plus a sync
/// listener wired to its state.
pub struct Harness {
    pub server: TestServer,
    fixture: JwksFixture,
    addr: SocketAddr,
}

impl Harness {
    pub async fn start() -> Self {
        let fixture = JwksFixture::spawn().await;
        let mut auth = fixture.auth_config(ISSUER, AUDIENCE, TENANT);
        let other = auth
            .providers
            .first()
            .cloned()
            .map(|mut provider| {
                provider.name = "other-tenant-provider".into();
                provider.issuer = OTHER_ISSUER.into();
                provider.tenant_id = OTHER_TENANT;
                provider
            })
            .expect("the fixture config names a provider");
        auth.providers.push(other);
        let registry = JwksRegistry::init(auth, loopback_egress())
            .await
            .expect("test JWKS registry must initialize");
        let server = TestServer::start_with_jwks(std::sync::Arc::new(registry)).await;
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
        Self {
            server,
            fixture,
            addr: state.config.listen_addr,
        }
    }

    /// Run `sql` as the harness superuser.
    pub async fn exec(&self, sql: &str) {
        self.server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// A token for `subject` carrying `roles`.
    pub fn token(&self, subject: &str, roles: &[&str]) -> String {
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

    /// A token for `subject` carrying `roles`, in [`OTHER_TENANT`].
    pub fn token_in_other_tenant(&self, subject: &str, roles: &[&str]) -> String {
        let now = now_secs();
        self.fixture.mint(&serde_json::json!({
            "iss": OTHER_ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "roles": roles,
            "iat": now,
            "exp": now + 600,
        }))
    }

    /// An unauthenticated sync socket.
    pub async fn connect(&self) -> Ws {
        let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr))
            .await
            .expect("sync websocket connect");
        ws
    }

    /// Run a handshake with `token` naming `held` collections and
    /// `last_server_watermark`. Returns the frames sent ahead of the ack and
    /// the ack.
    pub async fn handshake_holding(
        &self,
        ws: &mut Ws,
        token: String,
        held: &[&str],
        last_server_watermark: u64,
    ) -> (Vec<SyncFrame>, HandshakeAckMsg) {
        let msg = HandshakeMsg {
            jwt_token: token,
            vector_clock: held
                .iter()
                .map(|name| (name.to_string(), HashMap::new()))
                .collect(),
            subscribed_shapes: Vec::new(),
            client_version: "purge-catch-up-test".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: nodedb_types::sync::wire::SYNC_WIRE_VERSION,
            last_server_watermark,
        };
        send(ws, SyncMessageType::Handshake, &msg).await;
        let mut ahead = Vec::new();
        loop {
            let frame = next_frame(ws, SNAPSHOT_WAIT)
                .await
                .expect("handshake must be answered");
            if frame.msg_type == SyncMessageType::HandshakeAck {
                let ack: HandshakeAckMsg = frame.decode_body().expect("handshake ack decodes");
                return (ahead, ack);
            }
            ahead.push(frame);
        }
    }

    /// A sync session authenticated as `subject` with `roles`.
    pub async fn session(&self, subject: &str, roles: &[&str]) -> Ws {
        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr))
            .await
            .expect("sync websocket connect");
        self.handshake(&mut ws, subject, roles).await;
        ws
    }

    /// Run a handshake on `ws` as `subject` with `roles`. On an open session
    /// this replaces the session's identity.
    pub async fn handshake(&self, ws: &mut Ws, subject: &str, roles: &[&str]) {
        let msg = HandshakeMsg {
            jwt_token: self.token(subject, roles),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "row-push-authz-test".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: nodedb_types::sync::wire::SYNC_WIRE_VERSION,
            last_server_watermark: 0,
        };
        send(ws, SyncMessageType::Handshake, &msg).await;
        let ack = loop {
            let frame = next_frame(ws, SNAPSHOT_WAIT)
                .await
                .expect("handshake must be answered");
            if frame.msg_type == SyncMessageType::HandshakeAck {
                break frame;
            }
        };
        let ack: HandshakeAckMsg = ack.decode_body().expect("handshake ack decodes");
        assert!(ack.success, "handshake rejected: {:?}", ack.error);
    }
}

pub async fn send<T: zerompk::ToMessagePack>(ws: &mut Ws, msg_type: SyncMessageType, body: &T) {
    let frame = SyncFrame::try_encode(msg_type, body).expect("frame encodes");
    ws.send(Message::Binary(frame.to_bytes().into()))
        .await
        .expect("frame send");
}

async fn next_frame(ws: &mut Ws, wait: Duration) -> Option<SyncFrame> {
    let deadline = tokio::time::Instant::now() + wait;
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
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

/// The encoded predicate `field = value`.
pub fn eq_predicate(field: &str, value: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&MetadataFilter::Eq {
        field: field.into(),
        value: nodedb_types::Value::String(value.into()),
    })
    .expect("encode shape predicate")
}

/// Subscribe a document shape and return its snapshot, or `None` when the
/// server answers with none.
pub async fn subscribe(
    ws: &mut Ws,
    shape_id: &str,
    collection: &str,
    predicate: Vec<u8>,
) -> Option<ShapeSnapshotMsg> {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: shape_id.to_string(),
            tenant_id: TENANT as u32,
            shape_type: ShapeType::Document {
                collection: collection.to_string(),
                predicate,
            },
            description: "row push authorization probe".into(),
            field_filter: Vec::new(),
        },
    };
    send(ws, SyncMessageType::ShapeSubscribe, &msg).await;
    while let Some(frame) = next_frame(ws, SNAPSHOT_WAIT).await {
        if frame.msg_type == SyncMessageType::ShapeSnapshot {
            return Some(frame.decode_body().expect("shape snapshot decodes"));
        }
    }
    None
}

/// The pushes one session received.
#[derive(Default)]
pub struct Feed {
    pub rows: Vec<RowPushMsg>,
    pub purges: Vec<CollectionPurgedMsg>,
    pub definitions: Vec<DefinitionSyncMsg>,
    pub rejects: Vec<ShapeSnapshotRejectMsg>,
    pub presence_broadcasts: Vec<PresenceBroadcastMsg>,
    pub presence_leaves: Vec<PresenceLeaveMsg>,
}

impl Feed {
    pub fn has_row(&self, collection: &str, document_id: &str, op: RowOp) -> bool {
        self.rows
            .iter()
            .any(|r| r.collection == collection && r.document_id == document_id && r.op == op)
    }

    pub fn pushed_document(&self, document_id: &str) -> bool {
        self.rows.iter().any(|r| r.document_id == document_id)
    }

    pub fn pushed_collection(&self, collection: &str) -> bool {
        self.rows.iter().any(|r| r.collection == collection)
    }

    pub fn purged(&self, collection: &str) -> bool {
        self.purges.iter().any(|p| p.name == collection)
    }

    pub fn has_definition(&self, name: &str) -> bool {
        self.definitions.iter().any(|d| d.name == name)
    }

    pub fn rate_limited(&self) -> usize {
        self.rejects
            .iter()
            .filter(|r| r.refusal == ShapeSnapshotRefusal::RateLimited)
            .count()
    }

    pub fn left_channel(&self, channel: &str) -> bool {
        self.presence_leaves.iter().any(|l| l.channel == channel)
    }

    pub fn broadcast_on(&self, channel: &str) -> bool {
        self.presence_broadcasts
            .iter()
            .any(|b| b.channel == channel)
    }
}

/// Read the session's pushes until `done` holds, then for a settle window
/// more. Sends nothing. Returns at [`PUSH_WAIT`] when `done` never holds.
pub async fn collect(ws: &mut Ws, done: impl Fn(&Feed) -> bool) -> Feed {
    let mut feed = Feed::default();
    let deadline = tokio::time::Instant::now() + PUSH_WAIT;
    let mut stop_at = deadline;
    let mut settling = false;
    while tokio::time::Instant::now() < stop_at {
        while let Some(frame) = next_frame(ws, POLL).await {
            match frame.msg_type {
                SyncMessageType::RowPush => {
                    feed.rows
                        .push(frame.decode_body().expect("row push decodes"));
                }
                SyncMessageType::CollectionPurged => {
                    feed.purges
                        .push(frame.decode_body().expect("purge notice decodes"));
                }
                SyncMessageType::DefinitionSync => {
                    feed.definitions
                        .push(frame.decode_body().expect("definition frame decodes"));
                }
                SyncMessageType::ShapeSnapshotReject => {
                    feed.rejects
                        .push(frame.decode_body().expect("snapshot reject decodes"));
                }
                SyncMessageType::PresenceBroadcast => {
                    feed.presence_broadcasts
                        .push(frame.decode_body().expect("presence broadcast decodes"));
                }
                SyncMessageType::PresenceLeave => {
                    feed.presence_leaves
                        .push(frame.decode_body().expect("presence leave decodes"));
                }
                _ => {}
            }
        }
        if !settling && done(&feed) {
            settling = true;
            stop_at = tokio::time::Instant::now() + SETTLE;
        }
    }
    feed
}

/// Read the session's pushes for the settle window and return what arrived.
pub async fn collect_quiet(ws: &mut Ws) -> Feed {
    collect(ws, |_| true).await
}
