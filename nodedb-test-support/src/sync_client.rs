// SPDX-License-Identifier: BUSL-1.1

//! Minimal WebSocket sync-protocol test client.
//!
//! Test-only infra for driving the Lite↔Origin sync WebSocket end-to-end:
//! performs a Trust-mode handshake, then pushes CRDT deltas and reports
//! back whether the server acked or rejected each one. Each delta states its
//! collection's incarnation, learned from the server's `CollectionSchema`
//! announce as a Lite client learns it. Not a library-grade
//! client — errors are returned as `String` since this crate is test
//! infrastructure, not production code.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use nodedb_types::Hlc;
use nodedb_types::sync::wire::{
    CollectionDescriptor, CollectionSchemaSyncMsg, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg,
    HandshakeAckMsg, HandshakeMsg, SyncFrame, SyncMessageType,
};

/// Bound on how long a single frame receive may take before the test client
/// gives up and reports a timeout error.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Outcome of a single `push_delta` call.
#[derive(Debug)]
pub enum DeltaOutcome {
    Ack(DeltaAckMsg),
    Reject(DeltaRejectMsg),
}

/// A connected, handshake-completed sync WebSocket test client.
pub struct SyncTestClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    producer_id: u64,
    epoch: u64,
    next_seq: u64,
    /// The `creation_hlc` of every collection the server announced.
    incarnations: HashMap<String, Hlc>,
}

impl SyncTestClient {
    /// Connect to `addr` and perform the Trust-mode handshake as a non-Lite
    /// client: no `lite_id`, so the server assigns no durable producer id.
    pub async fn connect(addr: SocketAddr) -> Result<Self, String> {
        Self::connect_with_lite_id(addr, String::new(), 0).await
    }

    /// Connect as a Lite client with a durable identity.
    ///
    /// A `lite_id` plus a non-zero epoch is what makes the server register a
    /// producer for this connection. Anything that depends on distinguishing
    /// one client from another — peer-id ownership, epoch fencing — is inert
    /// without it, so tests for those must connect this way.
    pub async fn connect_as_lite(
        addr: SocketAddr,
        lite_id: &str,
        epoch: u64,
    ) -> Result<Self, String> {
        Self::connect_with_lite_id(addr, lite_id.to_string(), epoch).await
    }

    /// The durable producer id the server assigned to this connection, or `0`
    /// when it connected without a Lite identity.
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }

    async fn connect_with_lite_id(
        addr: SocketAddr,
        lite_id: String,
        epoch: u64,
    ) -> Result<Self, String> {
        let url = format!("ws://{addr}");
        let (mut ws, _resp) = connect_async(&url)
            .await
            .map_err(|e| format!("connect_async({url}) failed: {e}"))?;

        let handshake = HandshakeMsg {
            jwt_token: String::new(),
            vector_clock: HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: "test".to_string(),
            lite_id,
            epoch,
            wire_version: nodedb_types::sync::wire::SYNC_WIRE_VERSION,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::Handshake, &handshake)
            .ok_or_else(|| "failed to encode HandshakeMsg".to_string())?;
        ws.send(Message::Binary(frame.to_bytes().into()))
            .await
            .map_err(|e| format!("failed to send handshake frame: {e}"))?;

        let ack_frame = recv_frame(&mut ws, SyncMessageType::HandshakeAck).await?;
        let ack: HandshakeAckMsg = ack_frame
            .decode_body()
            .ok_or_else(|| "failed to decode HandshakeAckMsg body".to_string())?;
        if !ack.success {
            return Err(format!(
                "handshake rejected by server: {}",
                ack.error.unwrap_or_default()
            ));
        }

        Ok(Self {
            ws,
            producer_id: ack.producer_id,
            epoch: ack.accepted_epoch,
            next_seq: 1,
            incarnations: HashMap::new(),
        })
    }

    /// Push one delta for `(collection, document_id)` on the next stream
    /// sequence number and wait for the matching `DeltaAck` or `DeltaReject`
    /// response.
    pub async fn push_delta(
        &mut self,
        collection: &str,
        document_id: &str,
        peer_id: u64,
        mutation_id: u64,
        delta: Vec<u8>,
    ) -> Result<DeltaOutcome, String> {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.push_delta_at_seq(collection, document_id, peer_id, mutation_id, seq, delta)
            .await
    }

    /// Push one delta at an explicitly chosen stream sequence number.
    ///
    /// A real edge client stores the seq assigned at first send on its durable
    /// pending-delta record and reuses it verbatim on every re-send, so a
    /// refusal that Origin reports as retryable comes back at the *same* seq.
    /// [`Self::push_delta`] allocates a fresh seq per call and cannot express
    /// that; this entry point can, which is what makes the held
    /// high-water-mark observable from the client side.
    pub async fn push_delta_at_seq(
        &mut self,
        collection: &str,
        document_id: &str,
        peer_id: u64,
        mutation_id: u64,
        seq: u64,
        delta: Vec<u8>,
    ) -> Result<DeltaOutcome, String> {
        let collection_incarnation = self.incarnation(collection).await?;
        let push = DeltaPushMsg {
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            delta,
            peer_id,
            mutation_id,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: self.producer_id,
            epoch: self.epoch,
            seq,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::DeltaPush, &push)
            .ok_or_else(|| "failed to encode DeltaPushMsg".to_string())?;
        self.ws
            .send(Message::Binary(frame.to_bytes().into()))
            .await
            .map_err(|e| format!("failed to send DeltaPush frame: {e}"))?;

        loop {
            let frame = self.recv_next_frame().await?;
            match frame.msg_type {
                SyncMessageType::DeltaAck => {
                    let ack: DeltaAckMsg = frame
                        .decode_body()
                        .ok_or_else(|| "failed to decode DeltaAckMsg body".to_string())?;
                    return Ok(DeltaOutcome::Ack(ack));
                }
                SyncMessageType::DeltaReject => {
                    let reject: DeltaRejectMsg = frame
                        .decode_body()
                        .ok_or_else(|| "failed to decode DeltaRejectMsg body".to_string())?;
                    return Ok(DeltaOutcome::Reject(reject));
                }
                _ => continue,
            }
        }
    }

    /// Announce a collection descriptor to the server. Fire-and-forget:
    /// the CollectionSchema announce has no ack frame, so this only sends
    /// the frame and returns once it is flushed onto the socket.
    pub async fn push_collection_schema(
        &mut self,
        descriptor: CollectionDescriptor,
        creation_hlc: Hlc,
    ) -> Result<(), String> {
        let msg = CollectionSchemaSyncMsg {
            descriptor,
            creation_hlc,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::CollectionSchema, &msg)
            .ok_or_else(|| "failed to encode CollectionSchemaSyncMsg".to_string())?;
        self.ws
            .send(Message::Binary(frame.to_bytes().into()))
            .await
            .map_err(|e| format!("failed to send CollectionSchema frame: {e}"))?;
        Ok(())
    }

    /// Subscribe to a Document shape covering all documents in `collection`
    /// (empty predicate). Fire-and-forget: the server's response frames — a
    /// `CollectionSchema` announce followed by the `ShapeSnapshot` — are read
    /// by the caller via `recv_next_frame`.
    pub async fn subscribe_document_shape(
        &mut self,
        shape_id: &str,
        collection: &str,
        tenant_id: u32,
    ) -> Result<(), String> {
        use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
        use nodedb_types::sync::wire::ShapeSubscribeMsg;

        let shape = ShapeDefinition {
            shape_id: shape_id.to_string(),
            tenant_id,
            shape_type: ShapeType::Document {
                collection: collection.to_string(),
                predicate: Vec::new(),
            },
            description: String::new(),
            field_filter: Vec::new(),
        };
        let msg = ShapeSubscribeMsg { shape };
        let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeSubscribe, &msg)
            .ok_or_else(|| "failed to encode ShapeSubscribeMsg".to_string())?;
        self.ws
            .send(Message::Binary(frame.to_bytes().into()))
            .await
            .map_err(|e| format!("failed to send ShapeSubscribe frame: {e}"))?;
        Ok(())
    }

    /// Receive the next binary sync frame from the server, decoded as a
    /// `SyncFrame`. Non-binary frames are skipped. Bounded internally by
    /// `RECV_TIMEOUT`, so callers do not need to wrap this in a timeout to
    /// avoid hanging on a silent server.
    pub async fn recv_next_frame(&mut self) -> Result<SyncFrame, String> {
        let frame = recv_any_frame(&mut self.ws).await?;
        if frame.msg_type == SyncMessageType::CollectionSchema
            && let Some(announce) = frame.decode_body::<CollectionSchemaSyncMsg>()
        {
            self.incarnations
                .insert(announce.descriptor.name, announce.creation_hlc);
        }
        Ok(frame)
    }

    /// The incarnation of `collection` the server announced. When it has not
    /// announced it on this session yet, subscribe to the collection: the
    /// server sends the announce before the snapshot. The server binds the
    /// subscription to the session's tenant, not to the shape's.
    pub async fn incarnation(&mut self, collection: &str) -> Result<Hlc, String> {
        if let Some(hlc) = self.incarnations.get(collection) {
            return Ok(*hlc);
        }
        self.subscribe_document_shape(&format!("{collection}_incarnation"), collection, 0)
            .await?;
        loop {
            self.recv_next_frame().await.map_err(|e| {
                format!("no CollectionSchema announce for collection {collection}: {e}")
            })?;
            if let Some(hlc) = self.incarnations.get(collection) {
                return Ok(*hlc);
            }
        }
    }
}

/// Receive the next binary frame, decode it as a `SyncFrame`, and error out
/// if it isn't of the expected `msg_type`.
async fn recv_frame(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    expect: SyncMessageType,
) -> Result<SyncFrame, String> {
    let frame = recv_any_frame(ws).await?;
    if frame.msg_type != expect {
        return Err(format!(
            "expected frame type {expect:?}, got {:?}",
            frame.msg_type
        ));
    }
    Ok(frame)
}

/// Receive the next binary WebSocket message and decode it as a `SyncFrame`,
/// skipping any non-binary frames, bounded by `RECV_TIMEOUT`.
async fn recv_any_frame(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<SyncFrame, String> {
    tokio::time::timeout(RECV_TIMEOUT, async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(bytes))) => {
                    return SyncFrame::from_bytes(&bytes)
                        .ok_or_else(|| "failed to decode SyncFrame from bytes".to_string());
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(format!("WebSocket read error: {e}")),
                None => return Err("WebSocket stream closed unexpectedly".to_string()),
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for sync frame".to_string())?
}
