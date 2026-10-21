// SPDX-License-Identifier: BUSL-1.1

//! A role-guarded state transition binds a synced write the same way it binds
//! a SQL write. The session's authenticated roles decide it.
//!
//! The test drives the Lite sync wire end to end. A sync session pushes row
//! `a` in `draft` as a Loro delta. The collection then declares `status
//! TRANSITIONS ('draft' -> 'active' BY ROLE 'approver')`. Two sessions push the
//! same `draft -> active` update, each forked from the base. The session
//! without `approver` is refused and the row stays `draft`. The session with
//! `approver` is admitted and the row reads `active`.
//!
//! Every push is sent once. CREATE and ALTER install the collection's CRDT
//! constraint version before they return, so no push is fenced with `Gap`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb_test_support::jwks_fixture::{JwksFixture, loopback_egress, now_secs};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{
    AckStatus, CollectionSchemaSyncMsg, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, HandshakeAckMsg,
    HandshakeMsg, ShapeSubscribeMsg, SyncFrame, SyncMessageType,
};

/// Sync sessions authenticate into tenant 1, the tenant the pgwire harness
/// superuser writes into.
const TENANT: u64 = 1;
const ISSUER: &str = "https://sync-state-transition-by-role.example/";
const AUDIENCE: &str = "nodedb-sync";

/// How long the test waits for the server's verdict on one delta.
const FRAME_WAIT: Duration = Duration::from_secs(10);

const COLL: &str = "sync_role_flow";
const ROW: &str = "a";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The server's final verdict on one pushed delta.
#[derive(Debug, PartialEq)]
enum Verdict {
    Applied,
    Refused(String),
    NoAnswer,
    /// The server answered `Gap`: the apply was fenced and nothing applied.
    /// A collection whose constraint version is not installed on the
    /// applying replica answers this way.
    Gap,
}

struct Harness {
    server: TestServer,
    fixture: JwksFixture,
    addr: SocketAddr,
}

impl Harness {
    async fn start() -> Self {
        let fixture = JwksFixture::spawn().await;
        let registry = JwksRegistry::init(
            fixture.auth_config(ISSUER, AUDIENCE, TENANT),
            loopback_egress(),
        )
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
        let listener = start_sync_listener(
            config,
            Some(std::sync::Arc::clone(&server.shared)),
            shutdown_bus,
        )
        .await
        .expect("start sync listener");
        let addr = listener.config.listen_addr;
        Self {
            server,
            fixture,
            addr,
        }
    }

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

    /// Open a sync session authenticated as `subject` with `roles`.
    async fn session(&self, subject: &str, roles: &[&str]) -> Ws {
        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{}", self.addr))
            .await
            .expect("sync websocket connect");
        let msg = HandshakeMsg {
            jwt_token: self.mint_token(subject, roles),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "sync-role-test".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: nodedb_types::sync::wire::SYNC_WIRE_VERSION,
        };
        send(&mut ws, SyncMessageType::Handshake, &msg).await;
        let frame = next_frame(&mut ws)
            .await
            .expect("handshake must be answered");
        assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);
        let ack: HandshakeAckMsg = frame.decode_body().expect("handshake ack decodes");
        assert!(ack.success, "handshake rejected: {:?}", ack.error);
        ws
    }

    /// The row's current `status`, read from the document the synced apply
    /// materializes. `crdt_state` returns the Loro snapshot, which keeps
    /// every past value in its history, so it cannot tell the current status.
    async fn status(&self) -> String {
        let rows = self
            .server
            .query_text(&format!("SELECT status FROM {COLL} WHERE id = '{ROW}'"))
            .await
            .expect("read status");
        assert_eq!(rows.len(), 1, "row {ROW} must exist: {rows:?}");
        rows[0].clone()
    }
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
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

/// The incarnation of [`COLL`], as a Lite client learns it: subscribe to the
/// collection and read the `creation_hlc` of the `CollectionSchema` announce
/// that precedes the snapshot.
async fn incarnation(ws: &mut Ws) -> nodedb_types::Hlc {
    let shape = ShapeDefinition {
        shape_id: format!("{COLL}_incarnation"),
        tenant_id: TENANT as u32,
        shape_type: ShapeType::Document {
            collection: COLL.into(),
            predicate: Vec::new(),
        },
        description: String::new(),
        field_filter: Vec::new(),
    };
    send(
        ws,
        SyncMessageType::ShapeSubscribe,
        &ShapeSubscribeMsg { shape },
    )
    .await;
    while let Some(frame) = next_frame(ws).await {
        if frame.msg_type == SyncMessageType::CollectionSchema {
            let announce: CollectionSchemaSyncMsg = frame.decode_body().expect("announce decodes");
            if announce.descriptor.name == COLL {
                return announce.creation_hlc;
            }
        }
    }
    panic!("no CollectionSchema announce for {COLL}");
}

/// Push the updates `doc` holds past `base` once, as a Lite client does, and
/// wait for the server's answer to it. The delta states `incarnation`, the
/// collection's incarnation the session learned from its announce.
async fn push(
    ws: &mut Ws,
    incarnation: nodedb_types::Hlc,
    doc: &loro::LoroDoc,
    base: &loro::VersionVector,
    mutation_id: u64,
) -> Verdict {
    let delta = doc
        .export(loro::ExportMode::updates(base))
        .expect("export loro updates");
    let msg = DeltaPushMsg {
        collection: COLL.into(),
        document_id: ROW.into(),
        delta,
        peer_id: doc.peer_id(),
        mutation_id,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
        device_id: 0,
        delta_signature: [0; 32],
        collection_incarnation: incarnation,
    };
    send(ws, SyncMessageType::DeltaPush, &msg).await;
    while let Some(frame) = next_frame(ws).await {
        match frame.msg_type {
            SyncMessageType::DeltaReject => {
                let reject: DeltaRejectMsg = frame.decode_body().expect("reject decodes");
                if reject.mutation_id == mutation_id {
                    return Verdict::Refused(reject.reason);
                }
            }
            SyncMessageType::DeltaAck => {
                let ack: DeltaAckMsg = frame.decode_body().expect("ack decodes");
                if ack.mutation_id != mutation_id {
                    continue;
                }
                match ack.status {
                    AckStatus::Applied => return Verdict::Applied,
                    // Provisional: the durable apply has not answered yet.
                    AckStatus::Accepted => continue,
                    AckStatus::Gap { .. } => return Verdict::Gap,
                    other => return Verdict::Refused(format!("{other:?}")),
                }
            }
            _ => continue,
        }
    }
    Verdict::NoAnswer
}

/// A document holding row `a` in `draft`, and the empty version it was
/// written from.
fn draft_base() -> (loro::LoroDoc, loro::VersionVector) {
    let doc = loro::LoroDoc::new();
    let empty = doc.oplog_vv();
    let row = doc
        .get_map(COLL)
        .insert_container(ROW, loro::LoroMap::new())
        .expect("row container");
    row.insert("id", ROW).expect("id field");
    row.insert("status", "draft").expect("status field");
    doc.commit();
    (doc, empty)
}

/// A fork of `base_doc` that moves the row to `next`.
fn status_fork(base_doc: &loro::LoroDoc, next: &str) -> loro::LoroDoc {
    let fork = base_doc.fork();
    // The row map the server's CRDT state writes: the existing container, or
    // a new one at the row key.
    let coll = fork.get_map(COLL);
    let row = match coll.get(ROW) {
        Some(loro::ValueOrContainer::Container(loro::Container::Map(m))) => m,
        _ => coll
            .insert_container(ROW, loro::LoroMap::new())
            .expect("row container"),
    };
    row.insert("status", next).expect("status field");
    fork.commit();
    fork
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synced_update_judges_the_session_role() {
    let h = Harness::start().await;
    h.server.exec("CREATE ROLE approver").await.unwrap();
    h.server
        .exec(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"
        ))
        .await
        .unwrap();

    let mut approver = h
        .session("sync_role_approver", &["readwrite", "approver"])
        .await;
    let mut clerk = h.session("sync_role_clerk", &["readwrite"]).await;
    let approver_incarnation = incarnation(&mut approver).await;
    let clerk_incarnation = incarnation(&mut clerk).await;

    // The base: row `a` in `draft`. An insert has no prior row, so no
    // transition is judged.
    let (doc, empty) = draft_base();
    assert_eq!(
        push(&mut approver, approver_incarnation, &doc, &empty, 1).await,
        Verdict::Applied
    );
    assert_eq!(h.status().await, "draft");

    h.server
        .exec(&format!(
            "ALTER COLLECTION {COLL} ADD CONSTRAINT approval \
             ON COLUMN status TRANSITIONS ('draft' -> 'active' BY ROLE 'approver')"
        ))
        .await
        .unwrap();
    let base = doc.oplog_vv();

    let refused = push(
        &mut clerk,
        clerk_incarnation,
        &status_fork(&doc, "active"),
        &base,
        2,
    )
    .await;
    assert!(
        matches!(refused, Verdict::Refused(_)),
        "a session without the role must be refused: {refused:?}"
    );
    assert_eq!(
        h.status().await,
        "draft",
        "a refused delta must leave the row in draft"
    );

    let admitted = push(
        &mut approver,
        approver_incarnation,
        &status_fork(&doc, "active"),
        &base,
        3,
    )
    .await;
    assert_eq!(
        admitted,
        Verdict::Applied,
        "a session with the role must be admitted"
    );
    assert_eq!(
        h.status().await,
        "active",
        "an admitted delta must store active"
    );
}

/// CREATE installs the collection's CRDT constraint version before it
/// returns. The `PRIMARY KEY` gives the collection constraint version 1, so
/// a delta pushed right after CREATE is fenced unless that version is live.
/// No reconcile pass runs in this harness, so only the DDL can install it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_pushed_right_after_create_applies() {
    let h = Harness::start().await;
    let mut session = h.session("sync_create_writer", &["readwrite"]).await;
    h.server
        .exec(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"
        ))
        .await
        .unwrap();

    let incarnation = incarnation(&mut session).await;
    let (doc, empty) = draft_base();
    assert_eq!(
        push(&mut session, incarnation, &doc, &empty, 1).await,
        Verdict::Applied,
        "the first delta after CREATE must apply, not answer Gap"
    );
    assert_eq!(h.status().await, "draft");
}

/// Every client the handshake admits states the collection's incarnation. A
/// delta that states none is refused, and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_that_states_no_incarnation_is_refused() {
    let h = Harness::start().await;
    let mut session = h.session("sync_zero_writer", &["readwrite"]).await;
    h.server
        .exec(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"
        ))
        .await
        .unwrap();

    let (doc, empty) = draft_base();
    let verdict = push(&mut session, nodedb_types::Hlc::ZERO, &doc, &empty, 1).await;
    assert!(
        matches!(&verdict, Verdict::Refused(reason) if reason.contains("states no incarnation")),
        "{verdict:?}"
    );
    let rows = h
        .server
        .query_text(&format!("SELECT status FROM {COLL} WHERE id = '{ROW}'"))
        .await
        .expect("read status");
    assert!(rows.is_empty(), "a refused delta writes nothing: {rows:?}");
}
