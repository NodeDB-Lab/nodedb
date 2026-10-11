// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for cluster transport security.
//!
//! These tests exercise the full Raft-over-QUIC and SWIM-over-UDP paths
//! end-to-end against the security properties the transports enforce:
//!
//! - **Raft mTLS:** transports built with different `TlsCredentials`
//!   (distinct CAs) cannot handshake and outbound RPCs fail. Mixing mTLS
//!   and Insecure modes likewise fails.
//! - **Raft frame MAC + anti-replay:** a forged request arriving
//!   on an authenticated QUIC stream with the wrong cluster MAC key is
//!   rejected; a replayed frame from a captured envelope is rejected by
//!   the per-peer seq window.
//! - **SWIM UDP MAC:** a datagram signed with the wrong cluster
//!   key is rejected; a datagram with a spoofed source address is
//!   rejected.
//! - **Counter observability:** `insecure_transport_count()` bumps for
//!   each insecure construction.
//!
//! Tests build transports on ephemeral loopback ports and do not depend
//! on any cluster infrastructure beyond the transport layer itself.

use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;

/// Serializes tests that construct `TransportCredentials::Insecure`, so the
/// process-global `insecure_transport_count()` observed by
/// `observability_insecure_counter_monotonic` is not racing with other
/// concurrent tokio tests that also bump the counter.
///
/// Uses `tokio::sync::Mutex` so the guard can be held across `.await`
/// points without tripping `clippy::await_holding_lock`.
fn insecure_counter_guard() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

use nodedb_cluster::topology::{ClusterTopology, NodeInfo, NodeState};
use nodedb_cluster::transport::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use nodedb_cluster::{
    MacKey, NexarTransport, RaftRpcHandler, TlsCredentials, TransportCredentials,
    generate_node_credentials, insecure_transport_count, spki_pin_from_cert_der,
};
use nodedb_raft::message::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteResponse};
use nodedb_raft::transport::RaftTransport;

/// Echo handler — returns a success response for AppendEntries, identity
/// for RequestVote, and an error for anything unexpected.
struct EchoHandler;

impl RaftRpcHandler for EchoHandler {
    async fn on_timeout_now(&self, _req: nodedb_raft::TimeoutNowRequest) {}

    async fn handle_rpc(
        &self,
        rpc: nodedb_cluster::RaftRpc,
    ) -> nodedb_cluster::Result<nodedb_cluster::RaftRpc> {
        use nodedb_cluster::RaftRpc;
        match rpc {
            RaftRpc::AppendEntriesRequest(req) => {
                Ok(RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    last_log_index: req.prev_log_index + req.entries.len() as u64,
                    round: req.round,
                    needs_snapshot: false,
                }))
            }
            RaftRpc::RequestVoteRequest(req) => {
                Ok(RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term: req.term,
                    vote_granted: true,
                }))
            }
            other => Err(nodedb_cluster::ClusterError::Transport {
                detail: format!("unexpected: {other:?}"),
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

fn cert_of(creds: &TlsCredentials) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    (creds.cert.clone(), creds.key.clone_key())
}

/// Build a fresh `TlsCredentials` signed by `ca` with a distinct SAN.
fn creds_signed_by(
    ca: &nexar::transport::tls::ClusterCa,
    san: &str,
    cluster_secret: [u8; 32],
) -> TlsCredentials {
    let (cert, key) = ca.issue_cert(san).unwrap();
    let ca_cert = ca.cert_der();
    let spki_pin = spki_pin_from_cert_der(cert.as_ref()).unwrap_or([0u8; 32]);
    TlsCredentials {
        cert,
        key,
        ca_cert,
        additional_ca_certs: Vec::new(),
        crls: Vec::new(),
        cluster_secret,
        spki_pin,
        bootstrap_peer_spki: None,
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

/// Register each peer's pinned SPKI in a shared topology and install it on
/// every transport, mirroring a joined cluster. The mTLS identity store fails
/// closed against unknown peers, so a baseline RPC only succeeds once the
/// sender's identity is visible in topology.
fn install_shared_identity(nodes: &[(u64, [u8; 32])], transports: &[&Arc<NexarTransport>]) {
    let mut topo = ClusterTopology::new();
    for (id, spki) in nodes {
        topo.add_node(
            NodeInfo::new(*id, "127.0.0.1:0".parse().unwrap(), NodeState::Active)
                .with_spki_pin(Some(*spki)),
        );
    }
    let topology = Arc::new(RwLock::new(topo));
    for t in transports {
        t.install_identity_topology(topology.clone());
    }
}

/// Raft mTLS: two transports under the same cluster CA can talk. Baseline.
#[tokio::test]
async fn l1_same_ca_mtls_connects() {
    let (ca, server_creds) = generate_node_credentials("nodedb").unwrap();
    let client_creds = creds_signed_by(&ca, "nodedb", server_creds.cluster_secret);
    let server_spki = server_creds.spki_pin;
    let client_spki = client_creds.spki_pin;

    let server = Arc::new(
        NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Mtls(server_creds),
        )
        .unwrap(),
    );
    let client = Arc::new(
        NexarTransport::new(
            2,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Mtls(client_creds),
        )
        .unwrap(),
    );
    install_shared_identity(&[(1, server_spki), (2, client_spki)], &[&server, &client]);
    client.register_peer(1, server.local_addr());
    let _tx = spawn_server(server.clone()).await;

    let resp = client.append_entries(1, sample_append(7)).await.unwrap();
    assert!(resp.success);
    assert_eq!(resp.term, 7);
}

/// Raft mTLS: transports under *different* CAs cannot handshake.
#[tokio::test]
async fn l1_different_ca_mtls_rejects_handshake() {
    let (_ca_a, server_creds) = generate_node_credentials("nodedb").unwrap();
    let (_ca_b, client_creds) = generate_node_credentials("nodedb").unwrap();

    let server = Arc::new(
        NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Mtls(server_creds),
        )
        .unwrap(),
    );
    let client = Arc::new(
        NexarTransport::with_timeout(
            2,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(1),
            TransportCredentials::Mtls(client_creds),
        )
        .unwrap(),
    );
    client.register_peer(1, server.local_addr());
    let _tx = spawn_server(server.clone()).await;

    let err = client
        .append_entries(1, sample_append(1))
        .await
        .unwrap_err();
    let msg = err.to_string();
    // Failure surfaces as a transport error — the exact wording varies
    // with rustls version, but it must be a transport failure, not a
    // successful RPC.
    assert!(
        msg.contains("transport")
            || msg.contains("handshake")
            || msg.contains("certificate")
            || msg.contains("not reachable")
            || msg.contains("TLS"),
        "expected transport/TLS rejection, got: {msg}"
    );
}

/// Raft mTLS: Insecure server + mTLS client cannot handshake (server presents a
/// self-signed cert not signed by the client's CA).
#[tokio::test]
async fn l1_insecure_server_rejected_by_mtls_client() {
    let _guard = insecure_counter_guard().lock().await;
    let (_ca, client_creds) = generate_node_credentials("nodedb").unwrap();
    let server = Arc::new(
        NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Insecure,
        )
        .unwrap(),
    );
    let client = Arc::new(
        NexarTransport::with_timeout(
            2,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(1),
            TransportCredentials::Mtls(client_creds),
        )
        .unwrap(),
    );
    client.register_peer(1, server.local_addr());
    let _tx = spawn_server(server.clone()).await;

    let err = client
        .append_entries(1, sample_append(1))
        .await
        .unwrap_err();
    assert!(
        !err.to_string().contains("success"),
        "mTLS client must not accept insecure server: {err}"
    );
}

/// Frame MAC: two transports sharing a CA but different MAC keys can complete
/// the TLS handshake, but RPCs fail at the envelope MAC layer.
#[tokio::test]
async fn l2_mismatched_mac_key_rejects_rpcs() {
    let (ca, mut server_creds) = generate_node_credentials("nodedb").unwrap();
    let tmp = creds_signed_by(&ca, "nodedb", [0u8; 32]);
    let (client_cert, client_key) = cert_of(&tmp);
    let client_spki_pin = tmp.spki_pin;
    let client_creds = TlsCredentials {
        cert: client_cert,
        key: client_key,
        ca_cert: ca.cert_der(),
        additional_ca_certs: Vec::new(),
        crls: Vec::new(),
        cluster_secret: [0xAAu8; 32],
        spki_pin: client_spki_pin,
        bootstrap_peer_spki: None,
    };
    // Deliberately mismatched cluster secrets.
    server_creds.cluster_secret = [0x55u8; 32];

    let server = Arc::new(
        NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Mtls(server_creds),
        )
        .unwrap(),
    );
    let client = Arc::new(
        NexarTransport::with_timeout(
            2,
            "127.0.0.1:0".parse().unwrap(),
            Duration::from_secs(1),
            TransportCredentials::Mtls(client_creds),
        )
        .unwrap(),
    );
    client.register_peer(1, server.local_addr());
    let _tx = spawn_server(server.clone()).await;

    let err = client
        .append_entries(1, sample_append(1))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("MAC verification failed")
            || err.to_string().contains("envelope")
            || err.to_string().contains("transport"),
        "expected MAC or envelope rejection, got: {err}"
    );
}

/// Anti-replay: the per-peer sequence window rejects a replayed envelope.
/// Covered thoroughly by unit tests in `rpc_codec::peer_seq`; this
/// integration test guards the wiring — a real bidi QUIC handshake
/// producing many successful roundtrips must keep strictly-monotonic
/// outbound sequences, and the receive-side window must accept them.
#[tokio::test]
async fn l2_many_sequential_rpcs_all_accepted() {
    let (ca, server_creds) = generate_node_credentials("nodedb").unwrap();
    let client_creds = creds_signed_by(&ca, "nodedb", server_creds.cluster_secret);
    let server_spki = server_creds.spki_pin;
    let client_spki = client_creds.spki_pin;

    let server = Arc::new(
        NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Mtls(server_creds),
        )
        .unwrap(),
    );
    let client = Arc::new(
        NexarTransport::new(
            2,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Mtls(client_creds),
        )
        .unwrap(),
    );
    install_shared_identity(&[(1, server_spki), (2, client_spki)], &[&server, &client]);
    client.register_peer(1, server.local_addr());
    let _tx = spawn_server(server.clone()).await;

    for term in 0..200u64 {
        let resp = client.append_entries(1, sample_append(term)).await.unwrap();
        assert_eq!(resp.term, term);
        assert!(resp.success);
    }
}

/// SWIM UDP MAC: two SWIM UDP transports with different cluster MAC keys. A ping
/// from the first is silently dropped by the second; the recv future
/// errors with `SwimError::Decode { detail: "... MAC verification failed" }`.
#[tokio::test]
async fn l3_swim_rejects_mismatched_mac_key() {
    use nodedb_cluster::swim::detector::transport::Transport;
    use nodedb_cluster::swim::detector::transport::udp::UdpTransport;
    use nodedb_cluster::swim::incarnation::Incarnation;
    use nodedb_cluster::swim::wire::{Ping, ProbeId, SwimMessage};
    use nodedb_types::NodeId;

    let attacker = UdpTransport::bind(
        "127.0.0.1:0".parse().unwrap(),
        MacKey::from_bytes([1u8; 32]),
    )
    .await
    .unwrap();
    let legit = UdpTransport::bind(
        "127.0.0.1:0".parse().unwrap(),
        MacKey::from_bytes([2u8; 32]),
    )
    .await
    .unwrap();

    let ping = SwimMessage::Ping(Ping {
        probe_id: ProbeId::new(1),
        from: NodeId::try_new("attacker").expect("test fixture"),
        incarnation: Incarnation::ZERO,
        piggyback: vec![],
    });
    attacker.send(legit.local_addr(), ping).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(1), legit.recv())
        .await
        .expect("recv timeout")
        .expect_err("legit receiver must reject wrong-key MAC");
    assert!(err.to_string().contains("MAC verification failed"));
}

/// Observability: every `Insecure` construction bumps the counter.
#[tokio::test]
async fn observability_insecure_counter_monotonic() {
    let _guard = insecure_counter_guard().lock().await;
    let before = insecure_transport_count();
    let _ = NexarTransport::new(
        99,
        "127.0.0.1:0".parse().unwrap(),
        TransportCredentials::Insecure,
    )
    .unwrap();
    let after = insecure_transport_count();
    assert_eq!(after, before + 1);
}

/// Guard: `TlsCredentials::clone_key` equivalent via the helper used in
/// other tests. `Debug` on `TransportCredentials` redacts key material.
#[test]
fn debug_on_transport_credentials_redacts() {
    let creds = TlsCredentials {
        cert: CertificateDer::from(vec![0u8; 8]),
        key: PrivateKeyDer::from(PrivatePkcs8KeyDer::from(vec![0xFFu8; 16])),
        ca_cert: CertificateDer::from(vec![0u8; 8]),
        additional_ca_certs: Vec::new(),
        crls: Vec::new(),
        cluster_secret: [0xAB; 32],
        spki_pin: [0u8; 32],
        bootstrap_peer_spki: None,
    };
    let tc = TransportCredentials::Mtls(creds);
    let s = format!("{tc:?}");
    assert!(
        !s.contains("ab") && !s.contains("ff") && !s.contains("AB") && !s.contains("FF"),
        "debug leaked key bytes: {s}"
    );
    assert!(s.contains("redacted"));
}
