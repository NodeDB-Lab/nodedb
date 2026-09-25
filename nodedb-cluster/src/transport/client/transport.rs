// SPDX-License-Identifier: BUSL-1.1

//! [`NexarTransport`] struct, constructors, and basic accessors.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;
use tracing::info;

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, RetryPolicy};
use crate::error::{ClusterError, Result};
use crate::transport::auth_context::AuthContext;
use crate::transport::config;
use crate::transport::credentials::{self, TransportCredentials};
use crate::transport::peer_identity_store::{NoopIdentityStore, PeerIdentityStore};
use crate::transport::topology_identity_store::TopologyIdentityStore;

/// QUIC-based Raft transport with retry and circuit breaker.
///
/// Implements [`RaftTransport`] for outbound RPCs and provides [`serve`]
/// for inbound RPC handling. Thread-safe — wrap in `Arc` for shared use.
///
/// Resilience features:
/// - **Retry**: Transient transport failures are retried with exponential backoff.
/// - **Circuit breaker**: Peers with consecutive failures are fast-failed until cooldown.
/// - **Connection eviction**: Stale connections are evicted on failure and re-established on retry.
///
/// [`RaftTransport`]: nodedb_raft::transport::RaftTransport
/// [`serve`]: Self::serve
pub struct NexarTransport {
    pub(super) node_id: u64,
    pub(super) listener: nexar::TransportListener,
    pub(super) client_config: quinn::ClientConfig,
    /// Cached connections to peers. Stale connections are replaced on next use.
    pub(super) peers: RwLock<HashMap<u64, quinn::Connection>>,
    /// Known peer addresses for connection establishment.
    pub(super) peer_addrs: RwLock<HashMap<u64, SocketAddr>>,
    pub(super) rpc_timeout: Duration,
    pub(super) circuit_breaker: Arc<CircuitBreaker>,
    pub(super) retry_policy: RetryPolicy,
    /// MAC key + per-peer sequence trackers. Shared with every spawned
    /// per-connection / per-stream task via `Arc::clone`.
    pub(super) auth: Arc<AuthContext>,
    /// Shared by TLS verification and the post-envelope node-id check.
    pub(super) identity_store: Arc<dyn PeerIdentityStore>,
    /// SPKI pin for this node's own TLS leaf certificate.  `None` when
    /// running in insecure transport mode.  Transmitted in `JoinRequest`
    /// so remote peers can pin our identity.
    local_spki_pin: Option<[u8; 32]>,
    /// Token-bound issuer identity for initial address-only join RPCs.
    pub(super) bootstrap_peer_spki: Option<[u8; 32]>,
    /// Agreed wire version per connection, keyed on `conn.stable_id()`.
    /// Populated by `perform_version_handshake_client` after the
    /// per-connection handshake completes; evicted on connection drop.
    ///
    /// Cluster-plane only: this cache is consulted by send-path callers
    /// inside the cluster transport and never crosses the SPSC bridge
    /// into the Data Plane.
    pub(super) agreed_versions: RwLock<HashMap<usize, crate::wire_version::WireVersion>>,
    /// Peers this node refuses to send to. See [`super::sever`].
    pub(super) severed: RwLock<std::collections::HashSet<u64>>,
}

fn default_identity_store(creds: &TransportCredentials) -> Arc<dyn PeerIdentityStore> {
    match creds {
        TransportCredentials::Mtls(_) => Arc::new(TopologyIdentityStore::new()),
        TransportCredentials::Insecure => Arc::new(NoopIdentityStore),
    }
}

impl NexarTransport {
    /// Create a new transport bound to the given address.
    ///
    /// `creds` selects channel-level authentication — see
    /// [`TransportCredentials`]. Uses `ClusterTransportTuning::default()`
    /// for all QUIC and RPC settings. Prefer [`NexarTransport::with_tuning`]
    /// in production to read values from the server's `TuningConfig`.
    ///
    /// Uses [`NoopIdentityStore`] for TLS-layer SPKI pinning, which accepts
    /// all CA-signed certs in the bootstrap window. For production clusters
    /// with an active topology, use [`NexarTransport::with_tuning_and_identity`].
    pub fn new(node_id: u64, listen_addr: SocketAddr, creds: TransportCredentials) -> Result<Self> {
        Self::with_timeout(node_id, listen_addr, config::DEFAULT_RPC_TIMEOUT, creds)
    }

    /// Create a new transport with a custom RPC timeout.
    ///
    /// `creds` selects channel-level authentication. Uses
    /// `ClusterTransportTuning::default()` for all QUIC settings.
    /// Uses [`NoopIdentityStore`] for TLS-layer SPKI pinning.
    pub fn with_timeout(
        node_id: u64,
        listen_addr: SocketAddr,
        rpc_timeout: Duration,
        creds: TransportCredentials,
    ) -> Result<Self> {
        let defaults = ClusterTransportTuning::default();
        let identity_store = default_identity_store(&creds);
        Self::build(
            node_id,
            listen_addr,
            rpc_timeout,
            &defaults,
            creds,
            identity_store,
        )
    }

    /// Create a new transport using values from `ClusterTransportTuning`.
    ///
    /// All QUIC parameters (streams, windows, keep-alive, idle timeout) and
    /// the RPC timeout are read from `tuning`. `creds` selects channel-level
    /// authentication. Use this in production so that operators can override
    /// defaults via the `[tuning.cluster_transport]` section of `config.toml`.
    ///
    /// Uses [`NoopIdentityStore`] for TLS-layer SPKI pinning. For a fully
    /// pinned production cluster, use [`NexarTransport::with_tuning_and_identity`].
    pub fn with_tuning(
        node_id: u64,
        listen_addr: SocketAddr,
        tuning: &ClusterTransportTuning,
        creds: TransportCredentials,
    ) -> Result<Self> {
        let rpc_timeout = Duration::from_secs(tuning.rpc_timeout_secs);
        let identity_store = default_identity_store(&creds);
        Self::build(
            node_id,
            listen_addr,
            rpc_timeout,
            tuning,
            creds,
            identity_store,
        )
    }

    /// Create a new transport using tuning and an explicit identity store for
    /// TLS-layer SPKI/SPIFFE pinning.
    ///
    /// Pass the cluster topology (wrapped as `Arc<dyn PeerIdentityStore>`) to
    /// enable per-node cert pinning at the TLS handshake layer. The pinning is
    /// additive: the WebPki chain verifier (CA trust, expiry, CRL) still runs
    /// first, and the SPKI check fires only when a topology entry exists for
    /// the connecting peer's cert.
    pub fn with_tuning_and_identity(
        node_id: u64,
        listen_addr: SocketAddr,
        tuning: &ClusterTransportTuning,
        creds: TransportCredentials,
        identity_store: Arc<dyn PeerIdentityStore>,
    ) -> Result<Self> {
        let rpc_timeout = Duration::from_secs(tuning.rpc_timeout_secs);
        Self::build(
            node_id,
            listen_addr,
            rpc_timeout,
            tuning,
            creds,
            identity_store,
        )
    }

    /// Internal assembly shared by every constructor.
    fn build(
        node_id: u64,
        listen_addr: SocketAddr,
        rpc_timeout: Duration,
        tuning: &ClusterTransportTuning,
        creds: TransportCredentials,
        identity_store: Arc<dyn PeerIdentityStore>,
    ) -> Result<Self> {
        if creds.is_insecure() && !credentials::insecure_transport_bind_allowed(listen_addr) {
            return Err(ClusterError::Config {
                detail: format!(
                    "insecure cluster transport requires a loopback or private bind address; \
                     {listen_addr} is unspecified or publicly routable"
                ),
            });
        }

        let (server_config, client_config) = match &creds {
            TransportCredentials::Mtls(tls) => (
                config::make_raft_server_config_mtls(tls, tuning, Arc::clone(&identity_store))?,
                config::make_raft_client_config_mtls(tls, tuning, Arc::clone(&identity_store))?,
            ),
            TransportCredentials::Insecure => {
                credentials::announce_insecure_transport(node_id);
                (
                    config::make_raft_server_config(tuning)?,
                    config::make_raft_client_config(tuning)?,
                )
            }
        };

        let (local_spki_pin, bootstrap_peer_spki) = match &creds {
            TransportCredentials::Mtls(tls) => (Some(tls.spki_pin), tls.bootstrap_peer_spki),
            TransportCredentials::Insecure => (None, None),
        };

        let auth = Arc::new(AuthContext::from_credentials(node_id, &creds));

        let listener = nexar::TransportListener::bind_with_config(listen_addr, server_config)
            .map_err(|e| ClusterError::Transport {
                detail: format!("bind {listen_addr}: {e}"),
            })?;

        info!(
            node_id,
            addr = %listener.local_addr(),
            rpc_timeout_ms = rpc_timeout.as_millis() as u64,
            mtls = !creds.is_insecure(),
            "raft transport bound"
        );

        Ok(Self {
            node_id,
            listener,
            client_config,
            peers: RwLock::new(HashMap::new()),
            peer_addrs: RwLock::new(HashMap::new()),
            rpc_timeout,
            circuit_breaker: Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
            retry_policy: RetryPolicy::default(),
            auth,
            identity_store,
            local_spki_pin,
            bootstrap_peer_spki,
            agreed_versions: RwLock::new(HashMap::new()),
            severed: RwLock::new(std::collections::HashSet::new()),
        })
    }

    /// Accessor used by the serve / send paths when spawning per-connection
    /// tasks that need the shared auth state.
    pub(super) fn auth(&self) -> &Arc<AuthContext> {
        &self.auth
    }

    /// Access the circuit breaker (for observability / testing and
    /// for subsystems that need to share the same breaker instance).
    pub fn circuit_breaker(&self) -> &Arc<CircuitBreaker> {
        &self.circuit_breaker
    }

    /// The local address this transport is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }

    /// Permit a freshly issued CA-signed leaf until its bounded enrollment
    /// deadline. Returns false when the bounded preauthorization set is full.
    pub fn preauthorize_peer_identity(&self, spki: [u8; 32], ttl: Duration) -> bool {
        let Some(expires_at) = std::time::Instant::now().checked_add(ttl) else {
            return false;
        };
        self.identity_store.preauthorize(spki, expires_at)
    }

    /// Close an enrollment exception after an issuance-state failure.
    pub fn revoke_peer_preauthorization(&self, spki: &[u8; 32], ttl: Duration) {
        if let Some(expires_at) = std::time::Instant::now().checked_add(ttl) {
            self.identity_store
                .revoke_preauthorization(spki, expires_at);
        }
    }

    /// Attach the authoritative shared topology to the transport identity
    /// verifier before the inbound RPC server starts.
    pub fn install_identity_topology(
        &self,
        topology: Arc<RwLock<crate::topology::ClusterTopology>>,
    ) {
        self.identity_store.install_topology(topology);
    }

    /// This node's ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// This node's cluster-epoch state.
    ///
    /// The transport owns it because it is the thing that stamps and reads the
    /// field. The raft loop takes a handle to the same state so that applying a
    /// committed bump and stamping the new generation outbound are the same
    /// node's fact rather than two counters that drift.
    pub fn cluster_epoch(&self) -> std::sync::Arc<crate::cluster_epoch::ClusterEpochState> {
        std::sync::Arc::clone(&self.auth.epoch)
    }

    /// The cluster MAC key carried by this transport. SWIM subsystem
    /// uses it to authenticate UDP datagrams on the same key material.
    pub fn mac_key(&self) -> crate::rpc_codec::MacKey {
        self.auth.mac_key.clone()
    }

    /// SHA-256 SPKI pin for this node's own TLS leaf certificate.
    /// `None` in insecure transport mode.
    pub fn local_spki_pin(&self) -> Option<[u8; 32]> {
        self.local_spki_pin
    }

    /// Return the negotiated [`WireVersion`] for the connection identified by
    /// `stable_id`. Returns `None` if the handshake has not yet completed for
    /// this connection (e.g. first RPC not yet sent, or connection was evicted).
    pub fn agreed_version_for(&self, stable_id: usize) -> Option<crate::wire_version::WireVersion> {
        let versions = self
            .agreed_versions
            .read()
            .unwrap_or_else(|p| p.into_inner());
        versions.get(&stable_id).copied()
    }

    /// Store the agreed wire version for a connection (called after the
    /// per-connection handshake completes on the client side).
    pub(super) fn store_agreed_version(
        &self,
        stable_id: usize,
        version: crate::wire_version::WireVersion,
    ) {
        let mut versions = self
            .agreed_versions
            .write()
            .unwrap_or_else(|p| p.into_inner());
        versions.insert(stable_id, version);
    }

    /// Remove the cached agreed version for an evicted connection.
    pub(super) fn evict_agreed_version(&self, stable_id: usize) {
        let mut versions = self
            .agreed_versions
            .write()
            .unwrap_or_else(|p| p.into_inner());
        versions.remove(&stable_id);
    }

    /// Accept a raw incoming QUIC connection without the wire-version handshake
    /// or any RPC dispatch. Intended for tests that need to inspect or send
    /// deliberately malformed handshake frames server-side.
    #[doc(hidden)]
    pub async fn accept_raw(&self) -> crate::error::Result<quinn::Connection> {
        self.listener
            .accept()
            .await
            .map_err(|e| crate::error::ClusterError::Transport {
                detail: format!("accept_raw: {e}"),
            })
    }

    /// Establish a raw QUIC connection to `addr` without registering it as a
    /// named peer and without running the wire-version handshake. Intended for
    /// tests that need to send deliberately malformed handshake frames.
    #[doc(hidden)]
    pub async fn connect_raw(
        &self,
        addr: std::net::SocketAddr,
    ) -> crate::error::Result<quinn::Connection> {
        self.listener
            .endpoint()
            .connect_with(
                self.client_config.clone(),
                addr,
                crate::transport::config::SNI_HOSTNAME,
            )
            .map_err(|e| crate::error::ClusterError::Transport {
                detail: format!("connect_raw to {addr}: {e}"),
            })?
            .await
            .map_err(|e| crate::error::ClusterError::Transport {
                detail: format!("connect_raw handshake with {addr}: {e}"),
            })
    }

    /// Snapshot of every peer the transport has addresses cached for,
    /// with a per-peer `connected` flag for whether a nexar client is
    /// currently held in the pool. Sorted by peer id so
    /// `/cluster/debug/transport` output is deterministic.
    pub fn peer_snapshot(&self) -> Vec<TransportPeerSnapshot> {
        let addrs = self.peer_addrs.read().unwrap_or_else(|p| p.into_inner());
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<TransportPeerSnapshot> = addrs
            .iter()
            .map(|(id, addr)| TransportPeerSnapshot {
                peer_id: *id,
                addr: addr.to_string(),
                connected: peers.contains_key(id),
            })
            .collect();
        out.sort_by_key(|p| p.peer_id);
        out
    }
}

/// Per-peer view emitted by [`NexarTransport::peer_snapshot`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct TransportPeerSnapshot {
    pub peer_id: u64,
    pub addr: String,
    /// True when a nexar client is currently held in the connection
    /// cache. False means either we've never connected or the client
    /// was evicted.
    pub connected: bool,
}

/// Unit tests for [`NexarTransport`]: end-to-end RPC roundtrips, concurrent
/// fan-out, connection reuse, and unreachable-peer errors.
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use nodedb_raft::message::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, LogEntry, RequestVoteRequest, RequestVoteResponse,
        TimeoutNowRequest,
    };
    use nodedb_raft::transport::RaftTransport;

    use crate::error::{ClusterError, Result};
    use crate::rpc_codec::RaftRpc;
    use crate::transport::credentials::TransportCredentials;
    use crate::transport::rpc_handler::RaftRpcHandler;

    use super::NexarTransport;

    /// Mock handler that returns fixed responses for testing.
    struct EchoHandler;

    impl RaftRpcHandler for EchoHandler {
        async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
            match rpc {
                RaftRpc::AppendEntriesRequest(req) => {
                    Ok(RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                        term: req.term,
                        success: true,
                        last_log_index: req.prev_log_index + req.entries.len() as u64,
                    }))
                }
                RaftRpc::RequestVoteRequest(req) => {
                    Ok(RaftRpc::RequestVoteResponse(RequestVoteResponse {
                        term: req.term,
                        vote_granted: true,
                    }))
                }
                RaftRpc::InstallSnapshotRequest(req) => {
                    Ok(RaftRpc::InstallSnapshotResponse(InstallSnapshotResponse {
                        term: req.term,
                    }))
                }
                other => Err(ClusterError::Transport {
                    detail: format!("unexpected request: {other:?}"),
                }),
            }
        }

        async fn handle_rpc_streaming(
            &self,
            _req: crate::rpc_codec::ExecuteRequest,
            mut sink: impl crate::forward::ChunkSink,
        ) -> Option<crate::rpc_codec::TypedClusterError> {
            // Emit three deterministic chunks then clean EOF. Exercises the
            // multi-frame chunk/end envelope path end-to-end over loopback QUIC.
            for i in 0u64..3 {
                if let Err(_e) = sink.send_chunk(vec![i as u8; 4], i + 100, 0).await {
                    // Coordinator gone — stop producing, no terminal frame.
                    return None;
                }
            }
            None
        }

        async fn on_shuffle_request(&self, _req: crate::rpc_codec::ShufflePushRequest) {}

        async fn on_shuffle_chunk(
            &self,
            _shuffle_id: u64,
            _part: u32,
            _side: u8,
            _payload: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn on_shuffle_end(
            &self,
            _shuffle_id: u64,
            _part: u32,
            _side: u8,
            _error: Option<crate::rpc_codec::TypedClusterError>,
        ) {
        }

        async fn on_shuffle_produce(
            &self,
            _req: crate::rpc_codec::ShuffleProduceRequest,
        ) -> crate::rpc_codec::ShuffleProduceResponse {
            crate::rpc_codec::ShuffleProduceResponse {
                error: None,
                read_version_lsn: 0,
            }
        }

        async fn on_shuffle_consume(
            &self,
            _req: crate::rpc_codec::ShuffleConsumeRequest,
        ) -> crate::rpc_codec::ShuffleConsumeResponse {
            crate::rpc_codec::ShuffleConsumeResponse {
                rows: Vec::new(),
                error: None,
            }
        }

        async fn on_shuffle_aggregate(
            &self,
            _req: crate::rpc_codec::ShuffleAggregateConsumeRequest,
        ) -> crate::rpc_codec::ShuffleAggregateConsumeResponse {
            crate::rpc_codec::ShuffleAggregateConsumeResponse {
                rows: Vec::new(),
                error: None,
            }
        }

        async fn on_assign_surrogate(
            &self,
            _req: crate::rpc_codec::AssignSurrogateRequest,
        ) -> crate::rpc_codec::AssignSurrogateResponse {
            crate::rpc_codec::AssignSurrogateResponse {
                surrogate: 0,
                error: None,
                found: None,
            }
        }

        async fn on_submit_calvin_txn(
            &self,
            _req: crate::rpc_codec::SubmitCalvinTxnRequest,
        ) -> crate::rpc_codec::SubmitCalvinTxnResponse {
            crate::rpc_codec::SubmitCalvinTxnResponse {
                error: None,
                payload_bytes: None,
            }
        }

        async fn on_submit_calvin_inbox(
            &self,
            _req: crate::rpc_codec::SubmitCalvinInboxRequest,
        ) -> crate::rpc_codec::SubmitCalvinInboxResponse {
            crate::rpc_codec::SubmitCalvinInboxResponse {
                inbox_seq: 0,
                epoch: 0,
                position: 0,
                participants: 0,
                error: None,
            }
        }

        async fn on_reserve_read(
            &self,
            _req: crate::rpc_codec::ReserveReadRequest,
        ) -> crate::rpc_codec::ReserveReadResponse {
            crate::rpc_codec::ReserveReadResponse {
                owner_bytes: None,
                error: None,
            }
        }

        async fn on_release_reservation(
            &self,
            _req: crate::rpc_codec::ReleaseReservationRequest,
        ) -> crate::rpc_codec::ReleaseReservationResponse {
            crate::rpc_codec::ReleaseReservationResponse { error: None }
        }

        async fn on_timeout_now(&self, _req: TimeoutNowRequest) {}
    }

    #[test]
    fn insecure_transport_rejects_non_private_bind_before_observability_increment() {
        let before = crate::transport::credentials::insecure_transport_count();
        let error = match NexarTransport::new(
            1,
            "0.0.0.0:0".parse().unwrap(),
            TransportCredentials::Insecure,
        ) {
            Ok(_) => panic!("unspecified insecure bind must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(error, ClusterError::Config { .. }));
        assert_eq!(
            crate::transport::credentials::insecure_transport_count(),
            before
        );
    }

    fn make_transport(node_id: u64) -> NexarTransport {
        NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Insecure,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn append_entries_roundtrip() {
        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        let serve_task = tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = AppendEntriesRequest {
            term: 5,
            leader_id: 2,
            prev_log_index: 10,
            prev_log_term: 4,
            entries: vec![
                LogEntry {
                    term: 5,
                    index: 11,
                    data: b"cmd1".to_vec(),
                },
                LogEntry {
                    term: 5,
                    index: 12,
                    data: b"cmd2".to_vec(),
                },
            ],
            leader_commit: 10,
            group_id: 7,
        };

        let resp = client.append_entries(1, req).await.unwrap();
        assert_eq!(resp.term, 5);
        assert!(resp.success);
        assert_eq!(resp.last_log_index, 12);

        shutdown_tx.send(true).unwrap();
        serve_task.abort();
    }

    #[tokio::test]
    async fn execute_stream_roundtrip() {
        use futures::StreamExt;

        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        let serve_task = tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RaftRpc::ExecuteStreamRequest(crate::rpc_codec::ExecuteRequest {
            plan_bytes: b"plan".to_vec(),
            tenant_id: 1,
            database_id: 0,
            deadline_remaining_ms: 5000,
            trace_id: [0u8; 16],
            descriptor_versions: vec![],
            txn_id: None,
        });

        let stream = client.send_rpc_stream(1, req).await.unwrap();
        futures::pin_mut!(stream);

        let mut chunks: Vec<(Vec<u8>, u64)> = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.unwrap());
        }

        // EchoHandler emits three chunks then a clean EOF.
        assert_eq!(chunks.len(), 3, "expected three streamed chunks");
        for (i, (payload, lsn)) in chunks.iter().enumerate() {
            assert_eq!(payload, &vec![i as u8; 4]);
            assert_eq!(*lsn, i as u64 + 100);
        }

        shutdown_tx.send(true).unwrap();
        serve_task.abort();
    }

    #[tokio::test]
    async fn request_vote_roundtrip() {
        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RequestVoteRequest {
            term: 10,
            candidate_id: 2,
            last_log_index: 100,
            last_log_term: 9,
            group_id: 3,
        };

        let resp = client.request_vote(1, req).await.unwrap();
        assert_eq!(resp.term, 10);
        assert!(resp.vote_granted);
    }

    #[tokio::test]
    async fn install_snapshot_roundtrip() {
        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = InstallSnapshotRequest {
            term: 7,
            leader_id: 2,
            last_included_index: 500,
            last_included_term: 6,
            offset: 0,
            data: vec![0xAB; 4096],
            done: true,
            group_id: 0,
            total_size: 0,
        };

        let resp = client.install_snapshot(1, req).await.unwrap();
        assert_eq!(resp.term, 7);
    }

    #[tokio::test]
    async fn concurrent_rpcs() {
        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut handles = Vec::new();
        for i in 0..10u64 {
            let c = client.clone();
            handles.push(tokio::spawn(async move {
                let req = AppendEntriesRequest {
                    term: i,
                    leader_id: 2,
                    prev_log_index: i * 10,
                    prev_log_term: i.saturating_sub(1),
                    entries: vec![],
                    leader_commit: i * 10,
                    group_id: 0,
                };
                let resp = c.append_entries(1, req).await.unwrap();
                assert_eq!(resp.term, i);
                assert!(resp.success);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn connection_reuse() {
        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        for _ in 0..2 {
            let req = RequestVoteRequest {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
                group_id: 0,
            };
            client.request_vote(1, req).await.unwrap();
        }

        let peers = client.peers.read().unwrap();
        assert_eq!(peers.len(), 1);
    }

    #[tokio::test]
    async fn unregistered_peer_fails() {
        let client = make_transport(1);

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 0,
        };

        let err = client.append_entries(99, req).await.unwrap_err();
        assert!(
            err.to_string().contains("not reachable"),
            "expected unreachable error: {err}"
        );
    }

    #[tokio::test]
    async fn heartbeat_roundtrip() {
        let server = Arc::new(make_transport(1));
        let client = Arc::new(make_transport(2));

        client.register_peer(1, server.local_addr());

        let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let srv = server.clone();
        tokio::spawn(async move {
            srv.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 50,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 50,
            group_id: 0,
        };

        let resp = client.append_entries(1, req).await.unwrap();
        assert_eq!(resp.term, 3);
        assert!(resp.success);
        assert_eq!(resp.last_log_index, 50);
    }
}
