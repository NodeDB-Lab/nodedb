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
