// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the wire-version handshake exchange.
//!
//! Tests cover:
//! - Unit-level negotiation (no I/O): identical/overlapping/disjoint ranges.
//! - Wire-type roundtrips: VersionHandshake, VersionHandshakeAck, capabilities.
//! - End-to-end transport: happy path RPC roundtrip after handshake.
//! - Agreed-version caching: `agreed_version_for(stable_id)` returns the real negotiated value.
//! - Incompatible-version rejection: client connecting to a raw server that sends
//!   a disjoint version range receives a transport error; no RPC is dispatched.
//! - JoinRequest payload validation: `handle_join_request` rejects a wire_version
//!   outside the supported range.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nodedb_cluster::{
    ClusterError, NexarTransport, RaftRpc, RaftRpcHandler, TransportCredentials, VersionHandshake,
    VersionHandshakeAck, VersionRange, WireVersion, local_version_range, negotiate,
};
use nodedb_raft::message::{AppendEntriesRequest, AppendEntriesResponse};

// ── helpers ──────────────────────────────────────────────────────────────────

struct EchoHandler;

impl RaftRpcHandler for EchoHandler {
    async fn on_timeout_now(&self, _req: nodedb_raft::TimeoutNowRequest) {}

    async fn handle_rpc(&self, rpc: RaftRpc) -> nodedb_cluster::Result<RaftRpc> {
        match rpc {
            RaftRpc::AppendEntriesRequest(req) => {
                Ok(RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    last_log_index: req.prev_log_index,
                    round: req.round,
                    needs_snapshot: false,
                }))
            }
            other => Err(ClusterError::Transport {
                detail: format!("unexpected rpc: {other:?}"),
            }),
        }
    }

    async fn handle_rpc_streaming(
        &self,
        _req: nodedb_cluster::rpc_codec::ExecuteRequest,
        _sink: impl nodedb_cluster::ChunkSink,
    ) -> Option<nodedb_cluster::rpc_codec::TypedClusterError> {
        Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
            code: 0,
            message: "streaming not supported by test EchoHandler".into(),
        })
    }

    async fn on_shuffle_request(&self, _req: nodedb_cluster::ShufflePushRequest) {}

    async fn on_shuffle_chunk(
        &self,
        _shuffle_id: u64,
        _part: u32,
        _side: u8,
        _payload: Vec<u8>,
    ) -> nodedb_cluster::Result<()> {
        Ok(())
    }

    async fn on_shuffle_end(
        &self,
        _shuffle_id: u64,
        _part: u32,
        _side: u8,
        _error: Option<nodedb_cluster::rpc_codec::TypedClusterError>,
    ) {
    }

    async fn on_shuffle_produce(
        &self,
        _req: nodedb_cluster::rpc_codec::ShuffleProduceRequest,
    ) -> nodedb_cluster::rpc_codec::ShuffleProduceResponse {
        nodedb_cluster::rpc_codec::ShuffleProduceResponse {
            error: None,
            read_versions: Vec::new(),
        }
    }

    async fn on_shuffle_consume(
        &self,
        _req: nodedb_cluster::rpc_codec::ShuffleConsumeRequest,
    ) -> nodedb_cluster::rpc_codec::ShuffleConsumeResponse {
        nodedb_cluster::rpc_codec::ShuffleConsumeResponse {
            rows: Vec::new(),
            error: None,
        }
    }

    async fn on_shuffle_aggregate(
        &self,
        _req: nodedb_cluster::rpc_codec::ShuffleAggregateConsumeRequest,
    ) -> nodedb_cluster::rpc_codec::ShuffleAggregateConsumeResponse {
        nodedb_cluster::rpc_codec::ShuffleAggregateConsumeResponse {
            rows: Vec::new(),
            error: None,
        }
    }

    async fn on_assign_surrogate(
        &self,
        _req: nodedb_cluster::rpc_codec::AssignSurrogateRequest,
    ) -> nodedb_cluster::rpc_codec::AssignSurrogateResponse {
        nodedb_cluster::rpc_codec::AssignSurrogateResponse {
            surrogate: 0,
            error: None,
            found: None,
        }
    }

    async fn on_submit_calvin_txn(
        &self,
        _req: nodedb_cluster::rpc_codec::SubmitCalvinTxnRequest,
    ) -> nodedb_cluster::rpc_codec::SubmitCalvinTxnResponse {
        nodedb_cluster::rpc_codec::SubmitCalvinTxnResponse {
            error: None,
            payload_bytes: None,
        }
    }

    async fn on_submit_calvin_inbox(
        &self,
        _req: nodedb_cluster::rpc_codec::SubmitCalvinInboxRequest,
    ) -> nodedb_cluster::rpc_codec::SubmitCalvinInboxResponse {
        nodedb_cluster::rpc_codec::SubmitCalvinInboxResponse {
            inbox_seq: 0,
            epoch: 0,
            position: 0,
            participants: 0,
            error: None,
        }
    }

    async fn on_reserve_read(
        &self,
        _req: nodedb_cluster::rpc_codec::ReserveReadRequest,
    ) -> nodedb_cluster::rpc_codec::ReserveReadResponse {
        nodedb_cluster::rpc_codec::ReserveReadResponse {
            owner_bytes: None,
            error: None,
        }
    }

    async fn on_release_reservation(
        &self,
        _req: nodedb_cluster::rpc_codec::ReleaseReservationRequest,
    ) -> nodedb_cluster::rpc_codec::ReleaseReservationResponse {
        nodedb_cluster::rpc_codec::ReleaseReservationResponse { error: None }
    }
}

/// Handler that records whether it was ever invoked.
struct SentinelHandler {
    invoked: Arc<AtomicBool>,
}

impl RaftRpcHandler for SentinelHandler {
    async fn on_timeout_now(&self, _req: nodedb_raft::TimeoutNowRequest) {}

    async fn handle_rpc(&self, _rpc: RaftRpc) -> nodedb_cluster::Result<RaftRpc> {
        self.invoked.store(true, Ordering::SeqCst);
        Err(ClusterError::Transport {
            detail: "sentinel: unexpected dispatch".into(),
        })
    }

    async fn handle_rpc_streaming(
        &self,
        _req: nodedb_cluster::rpc_codec::ExecuteRequest,
        _sink: impl nodedb_cluster::ChunkSink,
    ) -> Option<nodedb_cluster::rpc_codec::TypedClusterError> {
        self.invoked.store(true, Ordering::SeqCst);
        Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
            code: 0,
            message: "sentinel: unexpected streaming dispatch".into(),
        })
    }

    async fn on_shuffle_request(&self, _req: nodedb_cluster::ShufflePushRequest) {
        self.invoked.store(true, Ordering::SeqCst);
    }

    async fn on_shuffle_chunk(
        &self,
        _shuffle_id: u64,
        _part: u32,
        _side: u8,
        _payload: Vec<u8>,
    ) -> nodedb_cluster::Result<()> {
        self.invoked.store(true, Ordering::SeqCst);
        Err(ClusterError::Transport {
            detail: "sentinel: unexpected shuffle dispatch".into(),
        })
    }

    async fn on_shuffle_end(
        &self,
        _shuffle_id: u64,
        _part: u32,
        _side: u8,
        _error: Option<nodedb_cluster::rpc_codec::TypedClusterError>,
    ) {
        self.invoked.store(true, Ordering::SeqCst);
    }

    async fn on_shuffle_produce(
        &self,
        _req: nodedb_cluster::rpc_codec::ShuffleProduceRequest,
    ) -> nodedb_cluster::rpc_codec::ShuffleProduceResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::ShuffleProduceResponse {
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected produce dispatch".into(),
            }),
            read_versions: Vec::new(),
        }
    }

    async fn on_shuffle_consume(
        &self,
        _req: nodedb_cluster::rpc_codec::ShuffleConsumeRequest,
    ) -> nodedb_cluster::rpc_codec::ShuffleConsumeResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::ShuffleConsumeResponse {
            rows: Vec::new(),
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected consume dispatch".into(),
            }),
        }
    }

    async fn on_shuffle_aggregate(
        &self,
        _req: nodedb_cluster::rpc_codec::ShuffleAggregateConsumeRequest,
    ) -> nodedb_cluster::rpc_codec::ShuffleAggregateConsumeResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::ShuffleAggregateConsumeResponse {
            rows: Vec::new(),
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected aggregate dispatch".into(),
            }),
        }
    }

    async fn on_assign_surrogate(
        &self,
        _req: nodedb_cluster::rpc_codec::AssignSurrogateRequest,
    ) -> nodedb_cluster::rpc_codec::AssignSurrogateResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::AssignSurrogateResponse {
            surrogate: 0,
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected assign-surrogate dispatch".into(),
            }),
            found: None,
        }
    }

    async fn on_submit_calvin_txn(
        &self,
        _req: nodedb_cluster::rpc_codec::SubmitCalvinTxnRequest,
    ) -> nodedb_cluster::rpc_codec::SubmitCalvinTxnResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::SubmitCalvinTxnResponse {
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected submit-calvin-txn dispatch".into(),
            }),
            payload_bytes: None,
        }
    }

    async fn on_submit_calvin_inbox(
        &self,
        _req: nodedb_cluster::rpc_codec::SubmitCalvinInboxRequest,
    ) -> nodedb_cluster::rpc_codec::SubmitCalvinInboxResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::SubmitCalvinInboxResponse {
            inbox_seq: 0,
            epoch: 0,
            position: 0,
            participants: 0,
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected submit-calvin-inbox dispatch".into(),
            }),
        }
    }

    async fn on_reserve_read(
        &self,
        _req: nodedb_cluster::rpc_codec::ReserveReadRequest,
    ) -> nodedb_cluster::rpc_codec::ReserveReadResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::ReserveReadResponse {
            owner_bytes: None,
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected reserve-read dispatch".into(),
            }),
        }
    }

    async fn on_release_reservation(
        &self,
        _req: nodedb_cluster::rpc_codec::ReleaseReservationRequest,
    ) -> nodedb_cluster::rpc_codec::ReleaseReservationResponse {
        self.invoked.store(true, Ordering::SeqCst);
        nodedb_cluster::rpc_codec::ReleaseReservationResponse {
            error: Some(nodedb_cluster::rpc_codec::TypedClusterError::Internal {
                code: 0,
                message: "sentinel: unexpected release-reservation dispatch".into(),
            }),
        }
    }
}

fn sample_append(term: u64) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        group_id: 0,
        round: 1,
        replicated_floor: 0,
    }
}

async fn spawn_server(transport: Arc<NexarTransport>) -> tokio::sync::watch::Sender<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let srv = transport.clone();
    tokio::spawn(async move {
        let _ = srv.serve(Arc::new(EchoHandler), rx).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    tx
}

async fn spawn_sentinel_server(
    transport: Arc<NexarTransport>,
    invoked: Arc<AtomicBool>,
) -> tokio::sync::watch::Sender<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let srv = transport.clone();
    tokio::spawn(async move {
        let _ = srv.serve(Arc::new(SentinelHandler { invoked }), rx).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    tx
}

fn new_transport(node_id: u64) -> Arc<NexarTransport> {
    Arc::new(
        NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Insecure,
        )
        .unwrap(),
    )
}

fn insecure_pair() -> (Arc<NexarTransport>, Arc<NexarTransport>) {
    (new_transport(1), new_transport(2))
}

/// Write a length-prefixed message to a quinn SendStream using the same
/// framing as `handshake_io::write_framed` (4-byte big-endian length + payload).
async fn write_framed_raw(send: &mut quinn::SendStream, bytes: &[u8]) {
    let len = (bytes.len() as u32).to_be_bytes();
    send.write_all(&len).await.unwrap();
    send.write_all(bytes).await.unwrap();
}

// ── unit-level negotiation tests (no I/O) ────────────────────────────────────

/// `local_version_range()` must be consistent: min <= max.
#[test]
fn local_range_is_valid() {
    let r = local_version_range();
    assert!(
        r.min <= r.max,
        "local_version_range min ({}) must be <= max ({})",
        r.min,
        r.max
    );
}

/// Two identical ranges negotiate to the shared max.
#[test]
fn negotiate_identical_ranges_picks_max() {
    let r = local_version_range();
    let agreed = negotiate(r, r).expect("identical ranges must agree");
    assert_eq!(agreed, r.max);
}

/// In-range overlap: server [N-1, N], client [N, N+1] → agreed N.
#[test]
fn negotiate_in_range_overlap() {
    let n = 10u16;
    let server_range = VersionRange::new(WireVersion(n - 1), WireVersion(n));
    let client_range = VersionRange::new(WireVersion(n), WireVersion(n + 1));
    let agreed = negotiate(server_range, client_range).expect("overlapping ranges must agree");
    assert_eq!(agreed, WireVersion(n));
}

/// Disjoint ranges surface NegotiationFailed — no agreed version.
#[test]
fn negotiate_disjoint_ranges_fails() {
    let low = VersionRange::new(WireVersion(1), WireVersion(2));
    let high = VersionRange::new(WireVersion(10), WireVersion(20));
    let err = negotiate(low, high).unwrap_err();
    assert!(
        matches!(
            err,
            nodedb_cluster::WireVersionError::NegotiationFailed { .. }
        ),
        "expected NegotiationFailed, got: {err}"
    );
}

/// `VersionHandshake` with non-zero capabilities survives roundtrip.
#[test]
fn handshake_capabilities_roundtrip() {
    let caps: u64 = 0xDEAD_BEEF_0000_0001;
    let r = local_version_range();
    let hs = VersionHandshake {
        range: (r.min.0, r.max.0),
        capabilities: caps,
        build_id: "test-build".to_owned(),
    };
    let bytes = zerompk::to_msgpack_vec(&hs).unwrap();
    let decoded: VersionHandshake = zerompk::from_msgpack(&bytes).unwrap();
    assert_eq!(decoded.capabilities, caps);
    assert_eq!(decoded.to_range(), r);
}

/// `VersionHandshakeAck` with non-zero capabilities survives roundtrip.
#[test]
fn ack_capabilities_roundtrip() {
    let caps: u64 = 0x1234_5678_9ABC_DEF0;
    let ack = VersionHandshakeAck {
        agreed: 2,
        capabilities: caps,
        build_id: "test-build".to_owned(),
    };
    let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
    let decoded: VersionHandshakeAck = zerompk::from_msgpack(&bytes).unwrap();
    assert_eq!(decoded.agreed_version(), WireVersion(2));
    assert_eq!(decoded.capabilities, caps);
}

/// Capabilities with all bits set are accepted without codec failure.
#[test]
fn capabilities_non_zero_accepted_in_negotiation() {
    let r = local_version_range();
    let hs = VersionHandshake {
        range: (r.min.0, r.max.0),
        capabilities: u64::MAX,
        build_id: "test-build".to_owned(),
    };
    let bytes = zerompk::to_msgpack_vec(&hs).unwrap();
    let decoded: VersionHandshake = zerompk::from_msgpack(&bytes).unwrap();
    assert_eq!(decoded.capabilities, u64::MAX);
    assert_eq!(decoded.to_range(), r);
}

// ── end-to-end transport tests ────────────────────────────────────────────────

/// Happy path: insecure pair with compatible (identical) version ranges.
/// The handshake must complete and the RPC roundtrip must succeed.
#[tokio::test]
async fn handshake_roundtrip_happy_path() {
    let (server, client) = insecure_pair();
    let server_addr = server.local_addr();

    let _shutdown = spawn_server(server.clone()).await;

    client.register_peer(1, server_addr);
    let rpc = RaftRpc::AppendEntriesRequest(sample_append(42));
    let response = client
        .send_rpc(1, rpc)
        .await
        .expect("RPC should succeed after handshake");

    match response {
        RaftRpc::AppendEntriesResponse(resp) => {
            assert_eq!(resp.term, 42);
            assert!(resp.success);
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

/// After the first successful RPC, `agreed_version_for(stable_id)` must return
/// the negotiated version cached from the real handshake (not a stub/default).
#[tokio::test]
async fn handshake_succeeds_with_compatible_versions_and_caches_agreed() {
    let (server, client) = insecure_pair();
    let server_addr = server.local_addr();

    let _shutdown = spawn_server(server.clone()).await;

    client.register_peer(1, server_addr);
    client
        .send_rpc(1, RaftRpc::AppendEntriesRequest(sample_append(1)))
        .await
        .expect("first RPC must succeed");

    // Connection must be cached.
    let snapshot = client.peer_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot[0].connected, "connection must be cached after RPC");

    // Agreed version must be stored in the cache (populated by the real handshake).
    let stable_id = client
        .peer_connection_stable_id(1)
        .expect("connection must be cached");
    let agreed = client
        .agreed_version_for(stable_id)
        .expect("agreed_version_for must return Some after handshake — cache was not populated");

    // The agreed version must fall within the locally supported range.
    let local = local_version_range();
    assert!(
        local.contains(agreed),
        "agreed version {agreed} must be within local range {local:?}"
    );
}

/// A client connecting to a raw server that sends a disjoint VersionHandshake
/// must receive a `ClusterError::Transport` and not proceed to RPC dispatch.
///
/// The "fake server" uses `accept_raw()` so it can write an arbitrary
/// incompatible range on the first bidi stream. The real client's
/// `get_or_connect` path runs `perform_version_handshake_client` and must
/// reject the server's ack (or the connection close).
#[tokio::test]
async fn handshake_rejects_incompatible_versions() {
    // Fake server: accepts raw connections and sends an incompatible range.
    let fake_server = new_transport(99);
    let fake_server_addr = fake_server.local_addr();

    let fake_srv = fake_server.clone();
    tokio::spawn(async move {
        // Accept one connection, perform server-side protocol: read client's
        // VersionHandshake, then send back a VersionHandshakeAck with an
        // agreed version that is outside the client's supported range.
        if let Ok(conn) = fake_srv.accept_raw().await {
            if let Ok((mut send, mut recv)) = conn.accept_bi().await {
                // Read (and discard) the client's VersionHandshake.
                let mut len_buf = [0u8; 4];
                if recv.read_exact(&mut len_buf).await.is_ok() {
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut body = vec![0u8; len];
                    let _ = recv.read_exact(&mut body).await;
                }
                // Send a VersionHandshakeAck whose agreed version is u16::MAX —
                // guaranteed to be outside the client's local range [1, WIRE_VERSION].
                let ack = VersionHandshakeAck {
                    agreed: u16::MAX,
                    capabilities: 0,
                    build_id: "fake-server-build".to_owned(),
                };
                let payload = zerompk::to_msgpack_vec(&ack).unwrap();
                write_framed_raw(&mut send, &payload).await;
            }
            // Allow the client time to read the ack before we close.
            tokio::time::sleep(Duration::from_millis(50)).await;
            conn.close(quinn::VarInt::from_u32(0x01), b"disjoint range");
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Real client: attempts to connect and run the handshake.
    let client = new_transport(2);
    client.register_peer(99, fake_server_addr);

    let result = client
        .send_rpc(99, RaftRpc::AppendEntriesRequest(sample_append(1)))
        .await;

    assert!(
        result.is_err(),
        "send_rpc must fail when server sends incompatible version range"
    );
    match result.unwrap_err() {
        ClusterError::Transport { .. } => {}
        other => panic!("expected ClusterError::Transport, got: {other:?}"),
    }
}

/// `handle_join_request` must reject a `JoinRequest` whose `wire_version` is
/// outside the locally supported range, returning an informative error message.
#[test]
fn handle_join_request_rejects_incompatible_wire_version() {
    use nodedb_cluster::bootstrap::handle_join_request;
    use nodedb_cluster::rpc_codec::JoinRequest;
    use nodedb_cluster::topology::{ClusterTopology, NodeState};
    use nodedb_cluster::{NodeInfo, routing::RoutingTable};

    let mut topology = ClusterTopology::new();
    topology.add_node(NodeInfo::new(
        1,
        "10.0.0.1:9400".parse().unwrap(),
        NodeState::Active,
    ));
    let routing = RoutingTable::uniform(1, &[1], 1);

    // wire_version = 0 is outside the accepted cluster schema version.
    let req = JoinRequest {
        node_id: 2,
        listen_addr: "10.0.0.2:9400".into(),
        wire_version: 0,
        build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
        spiffe_id: None,
        spki_pin: None,
        swim_addr: None,
    };

    let resp = handle_join_request(&req, &mut topology, &routing, 42);

    assert!(
        !resp.success,
        "JoinRequest with wire_version=0 must be rejected"
    );
    assert!(
        resp.error.contains("wire_version"),
        "rejection error must mention wire_version: {}",
        resp.error
    );
    // The error must tell the operator what version mismatch occurred.
    assert!(
        resp.error.contains('0')
            || resp.error.contains("mismatch")
            || resp.error.contains("does not match"),
        "rejection error must describe the mismatch: {}",
        resp.error
    );
    // Topology must not be mutated.
    assert_eq!(topology.node_count(), 1);
}

/// When the client sends a disjoint VersionHandshake to a real server, the
/// SentinelHandler must NOT be invoked — no RPC is dispatched after a mismatch.
///
/// The raw client opens a direct QUIC connection to the server (bypassing
/// `NexarTransport::get_or_connect`) and manually sends a bad handshake on
/// the first bidi stream. The server's `handle_connection` rejects the stream
/// and the sentinel remains un-invoked.
#[tokio::test]
async fn mismatch_does_not_dispatch_rpc() {
    let invoked = Arc::new(AtomicBool::new(false));
    let server = new_transport(1);
    let server_addr = server.local_addr();
    let _shutdown = spawn_sentinel_server(server.clone(), invoked.clone()).await;

    // Raw client: connect and send a VersionHandshake with a disjoint range
    // directly (bypassing NexarTransport's normal handshake logic).
    let raw_client = new_transport(3);
    if let Ok(conn) = raw_client.connect_raw(server_addr).await {
        if let Ok((mut send, mut recv)) = conn.open_bi().await {
            // Send a VersionHandshake with range [u16::MAX-1, u16::MAX] —
            // guaranteed disjoint from [1, WIRE_VERSION].
            let hs = VersionHandshake {
                range: (u16::MAX - 1, u16::MAX),
                capabilities: 0,
                build_id: "raw-client-build".to_owned(),
            };
            let payload = zerompk::to_msgpack_vec(&hs).unwrap();
            write_framed_raw(&mut send, &payload).await;
            // Server rejects and closes the QUIC connection.
            let _ = recv.read_to_end(64).await;
        }
        // Give the server time to process the rejection and close.
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    assert!(
        !invoked.load(Ordering::SeqCst),
        "SentinelHandler must not be invoked when handshake version is incompatible"
    );
}

/// A version mismatch causes the server to close the QUIC connection with
/// application error code 0x01. Verified by observing that after sending a
/// disjoint VersionHandshake, the connection's `close_reason()` is `Some`.
///
/// This exercises the same code path that emits `tracing::warn!` with
/// peer_addr, local_min, local_max, error fields.
#[tokio::test]
async fn mismatch_closes_quic_connection_with_app_error() {
    let server = new_transport(1);
    let server_addr = server.local_addr();
    let _shutdown = spawn_server(server.clone()).await;

    let raw_client = new_transport(4);
    if let Ok(conn) = raw_client.connect_raw(server_addr).await {
        if let Ok((mut send, mut recv)) = conn.open_bi().await {
            // Send VersionHandshake with disjoint range.
            let hs = VersionHandshake {
                range: (u16::MAX - 1, u16::MAX),
                capabilities: 0,
                build_id: "raw-client-build".to_owned(),
            };
            let payload = zerompk::to_msgpack_vec(&hs).unwrap();
            write_framed_raw(&mut send, &payload).await;
            // Wait for server to close the connection.
            let _ = recv.read_to_end(64).await;
        }
        // Wait for server to detect mismatch and close connection.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The server closes the connection on mismatch (application error 0x01).
        assert!(
            conn.close_reason().is_some(),
            "server must close the QUIC connection after a version mismatch"
        );
    }
}
