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

impl ClusterSettings {
    /// Validate cluster configuration.
    pub fn validate(&self) -> crate::Result<()> {
        if self.node_id == 0 {
            return Err(crate::Error::Config {
                detail: "cluster.node_id must be non-zero".into(),
            });
        }
        // Transaction ids pack the node id into the high 16 bits
        // (`session::transaction`), so a node id that does not fit in 16 bits
        // would silently lose its high bits and mint colliding ids across
        // nodes. Reject it loudly rather than mask (see CLAUDE.md on silent
        // truncation at hard caps).
        if self.node_id >= 1 << 16 {
            return Err(crate::Error::Config {
                detail: format!(
                    "cluster.node_id must be < 65536 (got {}): it is packed into the \
                     high 16 bits of a transaction id, and a larger value would silently \
                     collide with another node's ids",
                    self.node_id
                ),
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
    use super::ClusterSettings;

    fn settings_with_node_id(node_id: u64) -> ClusterSettings {
        let toml = format!(
            r#"
            node_id = {node_id}
            listen = "127.0.0.1:7000"
            seed_nodes = ["127.0.0.1:7000"]
            cert = "/tmp/cert.pem"
            key = "/tmp/key.pem"
            ca = "/tmp/ca.pem"
            "#
        );
        toml::from_str(&toml).expect("valid cluster TOML")
    }

    #[test]
    fn node_id_fitting_in_16_bits_is_accepted() {
        assert!(settings_with_node_id(1).validate().is_ok());
        assert!(settings_with_node_id(65_535).validate().is_ok());
    }

    #[test]
    fn node_id_at_or_above_16_bits_is_rejected_not_truncated() {
        // 65536 would pack to 0 in the high 16 bits of a TxnId and collide
        // with node 1's ids — must be rejected, never silently masked.
        for bad in [65_536u64, 65_537, 1 << 20] {
            let err = settings_with_node_id(bad)
                .validate()
                .expect_err("node_id >= 65536 must be rejected");
            assert!(
                matches!(err, crate::Error::Config { .. }),
                "expected Config error for node_id {bad}, got {err:?}"
            );
        }
    }
}
