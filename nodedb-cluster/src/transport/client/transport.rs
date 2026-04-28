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
}

impl NexarTransport {
    /// Create a new transport bound to the given address.
    ///
    /// `creds` selects channel-level authentication — see
    /// [`TransportCredentials`]. Uses `ClusterTransportTuning::default()`
    /// for all QUIC and RPC settings. Prefer [`NexarTransport::with_tuning`]
    /// in production to read values from the server's `TuningConfig`.
    pub fn new(node_id: u64, listen_addr: SocketAddr, creds: TransportCredentials) -> Result<Self> {
        Self::with_timeout(node_id, listen_addr, config::DEFAULT_RPC_TIMEOUT, creds)
    }

    /// Create a new transport with a custom RPC timeout.
    ///
    /// `creds` selects channel-level authentication. Uses
    /// `ClusterTransportTuning::default()` for all QUIC settings.
    pub fn with_timeout(
        node_id: u64,
        listen_addr: SocketAddr,
        rpc_timeout: Duration,
        creds: TransportCredentials,
    ) -> Result<Self> {
        let defaults = ClusterTransportTuning::default();
        Self::build(node_id, listen_addr, rpc_timeout, &defaults, creds)
    }

    /// Create a new transport using values from `ClusterTransportTuning`.
    ///
    /// All QUIC parameters (streams, windows, keep-alive, idle timeout) and
    /// the RPC timeout are read from `tuning`. `creds` selects channel-level
    /// authentication. Use this in production so that operators can override
    /// defaults via the `[tuning.cluster_transport]` section of `config.toml`.
    pub fn with_tuning(
        node_id: u64,
        listen_addr: SocketAddr,
        tuning: &ClusterTransportTuning,
        creds: TransportCredentials,
    ) -> Result<Self> {
        let rpc_timeout = Duration::from_secs(tuning.rpc_timeout_secs);
        Self::build(node_id, listen_addr, rpc_timeout, tuning, creds)
    }

    /// Internal assembly shared by every constructor.
    fn build(
        node_id: u64,
        listen_addr: SocketAddr,
        rpc_timeout: Duration,
        tuning: &ClusterTransportTuning,
        creds: TransportCredentials,
    ) -> Result<Self> {
        let (server_config, client_config) = match &creds {
            TransportCredentials::Mtls(tls) => (
                config::make_raft_server_config_mtls(tls, tuning)?,
                config::make_raft_client_config_mtls(tls, tuning)?,
            ),
            TransportCredentials::Insecure => {
                credentials::announce_insecure_transport(node_id);
                (
                    config::make_raft_server_config(tuning)?,
                    config::make_raft_client_config(tuning)?,
                )
            }
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

    /// This node's ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The cluster MAC key carried by this transport. SWIM subsystem
    /// uses it to authenticate UDP datagrams on the same key material.
    pub fn mac_key(&self) -> crate::rpc_codec::MacKey {
        self.auth.mac_key.clone()
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
