// SPDX-License-Identifier: BUSL-1.1

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Distributed cluster configuration.
///
/// Example TOML:
/// ```toml
/// [cluster]
/// node_id = 1
/// listen = "0.0.0.0:9400"
/// seed_nodes = ["10.0.0.1:9400", "10.0.0.2:9400"]
/// num_groups = 4
/// replication_factor = 3
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// Unique node ID within the cluster. Must be unique and non-zero.
    pub node_id: u64,

    /// Address to bind the Raft RPC QUIC listener.
    pub listen: SocketAddr,

    /// Seed node addresses for cluster formation or joining.
    /// On first startup, the first reachable seed bootstraps the cluster.
    /// Subsequent nodes join by contacting any seed.
    pub seed_nodes: Vec<SocketAddr>,

    /// Number of Raft groups to create on bootstrap. Each group owns
    /// a subset of the 1024 vShards. Default: 4.
    #[serde(default = "default_num_groups")]
    pub num_groups: u64,

    /// Replication factor — number of replicas per Raft group.
    /// Default: 3. Single-node clusters use RF=1 automatically.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: usize,

    /// Operator escape hatch: force this node to bootstrap a new
    /// cluster even if it is not the lexicographically smallest
    /// seed. Set this **only** during disaster recovery when the
    /// designated bootstrapper has been permanently lost and a
    /// different seed must take over.
    ///
    /// Requires `listen` to be present in `seed_nodes` (enforced in
    /// [`Self::validate`]). Default: `false`.
    #[serde(default)]
    pub force_bootstrap: bool,

    /// Paths to TLS credentials for the cluster QUIC transport.
    /// When `None` the bootstrapping node auto-generates a cluster CA and
    /// its own cert on first boot (persisted under `data_dir/tls/`) and
    /// loads them on subsequent restarts. Joining nodes receive the CA
    /// out-of-band (see L.4). If `insecure_transport = true` is also set,
    /// this field is ignored.
    #[serde(default)]
    pub tls: Option<TlsPaths>,

    /// Maximum number of simultaneously active authenticated sessions cluster-wide.
    /// 0 = unlimited (default).  Over-cap new logins are rejected with a
    /// `SESSION_CAP_EXCEEDED` error; existing sessions are never evicted.
    #[serde(default)]
    pub max_active_sessions: usize,

    /// Maximum login attempts per unique source IP per minute.
    ///
    /// Pre-authentication token bucket. Attempts that exceed this limit are
    /// rejected before the SCRAM exchange or Argon2 verification begins.
    /// Default: 30. Set to 0 to disable.
    #[serde(default = "default_login_attempts_per_ip_per_min")]
    pub login_attempts_per_ip_per_min: u64,

    /// Maximum login attempts per username per minute.
    ///
    /// Pre-authentication token bucket. Attempts that exceed this limit are
    /// rejected before the SCRAM exchange or Argon2 verification begins.
    /// Default: 10. Set to 0 to disable.
    #[serde(default = "default_login_attempts_per_user_per_min")]
    pub login_attempts_per_user_per_min: u64,

    /// **SECURITY ESCAPE HATCH.** When `true` the Raft QUIC transport
    /// accepts any client certificate (including none) and the client
    /// skips server certificate verification. Any network peer reaching
    /// the QUIC port can forge Raft RPCs.
    ///
    /// Only enable on fully isolated private networks where every peer
    /// on the wire is already inside the trust boundary. Production
    /// deployments must leave this `false` and provide `tls` credentials
    /// (or let the bootstrapping node auto-generate them).
    ///
    /// Default: `false`.
    #[serde(default)]
    pub insecure_transport: bool,

    /// Auto-compaction threshold for every Raft group on this node:
    /// the number of applied entries retained past the snapshot index
    /// before the log is compacted and a fresh snapshot is taken.
    ///
    /// `None` (the default) disables auto-compaction, so a lagging
    /// peer is always caught up via `AppendEntries`. Setting a low
    /// value forces the leader's data-group log to compact past the
    /// start after only a handful of writes, which is what makes a
    /// freshly-joined learner unable to be caught up incrementally —
    /// the leader must send a real `InstallSnapshot` instead. Threaded
    /// to each group's `RaftConfig::log_compaction_threshold` via
    /// `nodedb_cluster::ClusterConfig`.
    #[serde(default)]
    pub log_compaction_threshold: Option<u64>,

    /// Total join attempts before the join loop gives up.
    ///
    /// Production leaves this at its default. The integration test harness
    /// lowers it so a join-retry path does not spend a minute sleeping.
    /// Default: 8.
    #[serde(default = "default_join_retry_max_attempts")]
    pub join_retry_max_attempts: u32,

    /// Cap on the per-attempt join backoff delay, in seconds.
    ///
    /// Production leaves this at its default. The integration test harness
    /// lowers it so a join-retry path does not spend a minute sleeping.
    /// Default: 32.
    #[serde(default = "default_join_retry_max_backoff_secs")]
    pub join_retry_max_backoff_secs: u64,
}

/// Paths to on-disk PEM-encoded TLS credentials.
///
/// All paths are read once at node startup. See `nodedb-cluster::TlsCredentials`
/// for the runtime representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsPaths {
    /// Node certificate (PEM).
    pub cert: PathBuf,
    /// Node private key (PEM). Must be `0600` or the node refuses to start.
    pub key: PathBuf,
    /// Cluster CA certificate (PEM).
    pub ca: PathBuf,
    /// Optional CRL (PEM).
    #[serde(default)]
    pub crl: Option<PathBuf>,
    /// Cluster-wide 32-byte HMAC key for the authenticated Raft frame
    /// envelope. Raw bytes, no PEM framing. Must be `0600`.
    ///
    /// When `None`, the resolver falls back to `data_dir/tls/cluster_secret.bin`
    /// (auto-generated on first bootstrap, delivered via the join RPC
    /// otherwise).
    #[serde(default)]
    pub cluster_secret: Option<PathBuf>,
}

fn default_num_groups() -> u64 {
    4
}

fn default_replication_factor() -> usize {
    3
}

fn default_login_attempts_per_ip_per_min() -> u64 {
    30
}

fn default_login_attempts_per_user_per_min() -> u64 {
    10
}

fn default_join_retry_max_attempts() -> u32 {
    8
}

fn default_join_retry_max_backoff_secs() -> u64 {
    32
}

impl ClusterSettings {
    /// Validate cluster configuration.
    pub fn validate(&self) -> crate::Result<()> {
        if self.node_id == 0 {
            return Err(crate::Error::Config {
                detail: "cluster.node_id must be non-zero".into(),
            });
        }
        if self.seed_nodes.is_empty() {
            return Err(crate::Error::Config {
                detail: "cluster.seed_nodes must contain at least one address".into(),
            });
        }
        if self.num_groups == 0 {
            return Err(crate::Error::Config {
                detail: "cluster.num_groups must be at least 1".into(),
            });
        }
        if self.replication_factor == 0 {
            return Err(crate::Error::Config {
                detail: "cluster.replication_factor must be at least 1".into(),
            });
        }
        if self.insecure_transport && !nodedb_cluster::insecure_transport_bind_allowed(self.listen)
        {
            return Err(crate::Error::Config {
                detail: format!(
                    "cluster.insecure_transport requires cluster.listen to use a loopback or \
                     private address; {} is unspecified or publicly routable",
                    self.listen
                ),
            });
        }
        if self.force_bootstrap && !self.seed_nodes.contains(&self.listen) {
            return Err(crate::Error::Config {
                detail: "cluster.force_bootstrap requires cluster.listen to be present in \
                     cluster.seed_nodes"
                    .into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insecure_settings(listen: &str) -> ClusterSettings {
        let listen = listen.parse().unwrap();
        ClusterSettings {
            node_id: 1,
            listen,
            seed_nodes: vec![listen],
            num_groups: 1,
            replication_factor: 1,
            force_bootstrap: false,
            tls: None,
            max_active_sessions: 0,
            login_attempts_per_ip_per_min: 30,
            login_attempts_per_user_per_min: 10,
            insecure_transport: true,
            log_compaction_threshold: None,
            join_retry_max_attempts: 8,
            join_retry_max_backoff_secs: 32,
        }
    }

    #[test]
    fn insecure_transport_rejects_public_and_unspecified_binds() {
        for listen in ["0.0.0.0:9400", "8.8.8.8:9400", "[::]:9400"] {
            assert!(insecure_settings(listen).validate().is_err(), "{listen}");
        }
    }

    #[test]
    fn insecure_transport_allows_private_binds() {
        for listen in ["127.0.0.1:9400", "10.0.0.1:9400", "[fd00::1]:9400"] {
            assert!(insecure_settings(listen).validate().is_ok(), "{listen}");
        }
    }
}
