// SPDX-License-Identifier: BUSL-1.1

//! Root configuration for the NodeDB server.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use nodedb_types::config::TuningConfig;
use serde::{Deserialize, Serialize};

use super::checkpoint::CheckpointSettings;
use super::cluster::ClusterSettings;
use super::cold_storage::ColdStorageSettings;
use super::observability::ObservabilityConfig;
use super::retention::RetentionSettings;
use super::scheduler::SchedulerConfig;
use super::section::ServerSection;
use super::snapshot_storage::{QuarantineStorageSettings, SnapshotStorageSettings};
use super::tls::{BackupEncryptionSettings, EncryptionSettings};
use crate::config::EngineConfig;

/// Root configuration for the NodeDB server.
///
/// On disk this is a TOML document with `[server]` for runtime fields and
/// independent subsystem tables (`[auth]`, `[tls]`, `[cluster]`, `[engines]`,
/// …) as siblings at the root. `deny_unknown_fields` rejects typos and
/// stray tables so misconfiguration surfaces at startup instead of being
/// silently ignored.
///
/// Example:
///
/// ```toml
/// [server]
/// host         = "0.0.0.0"
/// data_dir     = "/var/lib/nodedb"
/// memory_limit = "4GiB"
///
/// [server.ports]
/// pgwire = 6432
/// native = 6433
/// http   = 6480
/// sync   = 9090
///
/// [auth]
/// # ...
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Server-level runtime fields (bind address, ports, resource budgets,
    /// on-disk location, log format).
    #[serde(default)]
    pub server: ServerSection,

    /// Per-engine budget configuration. Lives under `[engines]`.
    #[serde(default)]
    pub engines: EngineConfig,

    /// Authentication and authorization configuration.
    #[serde(default)]
    pub auth: crate::config::AuthConfig,

    /// Encryption at rest configuration. If present, WAL payloads are encrypted.
    #[serde(default)]
    pub encryption: Option<EncryptionSettings>,

    /// Per-backup encryption configuration. If present, every backup envelope
    /// is encrypted with a per-backup DEK wrapped by this KEK. If absent, a
    /// warning is emitted once per process at the first backup operation.
    /// The key MUST differ from the WAL key; a matching path triggers a warning.
    #[serde(default)]
    pub backup_encryption: Option<BackupEncryptionSettings>,

    /// Checkpoint and WAL management settings.
    #[serde(default)]
    pub checkpoint: CheckpointSettings,

    /// Collection-lifecycle retention settings. Drives when the
    /// Event-Plane collection-GC sweeper hard-deletes a soft-deleted
    /// collection, and how often it evaluates candidates.
    #[serde(default)]
    pub retention: RetentionSettings,

    /// Cluster mode settings. When present, the node participates in a
    /// distributed cluster via Multi-Raft consensus over QUIC transport.
    /// When absent, runs in single-node mode (default).
    #[serde(default)]
    pub cluster: Option<ClusterSettings>,

    /// Cold storage (L2 tiering) configuration.
    /// When present, old L1 segments are promoted to S3-compatible cold storage.
    #[serde(default)]
    pub cold_storage: Option<ColdStorageSettings>,

    /// Snapshot storage configuration.
    /// Controls where warm-tier snapshots are persisted. When absent, defaults
    /// to local filesystem at `{data_dir}/snapshots`.
    #[serde(default)]
    pub snapshot_storage: Option<SnapshotStorageSettings>,

    /// Quarantine storage configuration.
    /// Controls where corrupt-segment archives are stored. When absent, defaults
    /// to local filesystem at `{data_dir}/quarantine`.
    #[serde(default)]
    pub quarantine_storage: Option<QuarantineStorageSettings>,

    /// Performance tuning knobs for engines, query execution, WAL, bridge,
    /// network, and cluster transport. All fields have sensible defaults;
    /// override selectively via the `[tuning]` TOML section.
    #[serde(default)]
    pub tuning: TuningConfig,

    /// Observability integrations: PromQL, OTLP receiver/export.
    /// All capabilities are always compiled in; toggled at runtime via this config.
    #[serde(default)]
    pub observability: ObservabilityConfig,

    /// Cron scheduler settings (timezone offset, future tuning knobs).
    #[serde(default)]
    pub scheduler: SchedulerConfig,
}

impl ServerConfig {
    /// Load configuration from a TOML file, falling back to defaults.
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| crate::Error::Config {
            detail: format!("failed to read config file {}: {e}", path.display()),
        })?;
        let parsed: Self = toml::from_str(&content).map_err(|e| crate::Error::Config {
            detail: format!("invalid TOML config: {e}"),
        })?;
        parsed.validate()?;
        Ok(parsed)
    }

    /// Validate cross-field invariants that serde cannot express. Called
    /// from [`Self::from_file`] so misconfiguration fails startup.
    pub fn validate(&self) -> crate::Result<()> {
        if let Some(ref jwt) = self.auth.jwt {
            jwt.validate()?;
        }
        super::domain::validate_domain(self)
    }

    /// Build a `SocketAddr` from the shared host and a port.
    pub fn addr(&self, port: u16) -> SocketAddr {
        SocketAddr::new(self.server.host, port)
    }

    /// Native protocol listen address.
    pub fn native_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.native)
    }

    /// pgwire listen address.
    pub fn pgwire_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.pgwire)
    }

    /// HTTP API listen address.
    pub fn http_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.http)
    }

    /// Sync WebSocket listen address (NodeDB-Lite clients).
    ///
    /// Does not follow a routable `server.host`: `bind_sync_listener` refuses
    /// a non-loopback bind, so deriving it from `0.0.0.0` could not boot. A
    /// loopback `server.host` passes through unchanged; an explicit
    /// `sync_host` passes through even when it will be refused.
    pub fn sync_addr(&self) -> SocketAddr {
        let host = self
            .server
            .sync_host
            .unwrap_or(if self.server.host.is_loopback() {
                self.server.host
            } else {
                IpAddr::V4(Ipv4Addr::LOCALHOST)
            });
        SocketAddr::new(host, self.server.ports.sync)
    }

    /// RESP listen address (None if disabled).
    pub fn resp_addr(&self) -> Option<SocketAddr> {
        self.server.ports.resp.map(|p| self.addr(p))
    }

    /// ILP listen address (None if disabled).
    pub fn ilp_addr(&self) -> Option<SocketAddr> {
        self.server.ports.ilp.map(|p| self.addr(p))
    }

    /// WAL directory within the data directory.
    pub fn wal_dir(&self) -> PathBuf {
        self.server.data_dir.join("wal")
    }

    /// Segments directory within the data directory.
    pub fn segments_dir(&self) -> PathBuf {
        self.server.data_dir.join("segments")
    }

    /// System catalog (auth, roles, tenants) redb file.
    pub fn catalog_path(&self) -> PathBuf {
        self.server.data_dir.join("system.redb")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::server::log_format::LogFormat;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn default_config_valid() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.server.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(cfg.server.ports.native, 6433);
        assert_eq!(cfg.server.ports.pgwire, 6432);
        assert_eq!(cfg.server.ports.http, 6480);
        assert_eq!(cfg.server.ports.sync, 9090);
        assert!(cfg.server.ports.resp.is_none());
        assert!(cfg.server.ports.ilp.is_none());
        assert!(cfg.server.data_plane_cores >= 1);
        assert_eq!(cfg.server.memory_limit, 1024 * 1024 * 1024);
    }

    #[test]
    fn config_roundtrip() {
        let cfg = ServerConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).expect("serialize");
        let _parsed: ServerConfig = toml::from_str(&toml_str).expect("deserialize");
    }

    #[test]
    fn log_format_default_is_text() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.server.log_format, LogFormat::Text);
    }

    fn config_toml_with_log_format(value: &str) -> String {
        format!("[server]\nlog_format = {value}\n")
    }

    #[test]
    fn log_format_toml_text_parses() {
        let raw = config_toml_with_log_format("\"text\"");
        let cfg: ServerConfig = toml::from_str(&raw).expect("deserialize");
        assert_eq!(cfg.server.log_format, LogFormat::Text);
    }

    #[test]
    fn log_format_toml_json_parses() {
        let raw = config_toml_with_log_format("\"json\"");
        let cfg: ServerConfig = toml::from_str(&raw).expect("deserialize");
        assert_eq!(cfg.server.log_format, LogFormat::Json);
    }

    #[test]
    fn log_format_toml_unknown_rejected() {
        let raw = config_toml_with_log_format("\"yaml\"");
        let result: Result<ServerConfig, _> = toml::from_str(&raw);
        assert!(result.is_err(), "unknown log_format value must be rejected");
    }

    /// The image ships `NODEDB_HOST=0.0.0.0`; sync took it and refused to
    /// bind, so `docker run` exited 1 before serving anything.
    #[test]
    fn routable_host_keeps_sync_on_loopback() {
        let mut cfg = ServerConfig::default();
        cfg.server.host = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

        assert!(
            cfg.sync_addr().ip().is_loopback(),
            "routable server.host must not move sync off loopback: {}",
            cfg.sync_addr()
        );
        // The others must still bind it, or port mapping breaks instead.
        assert_eq!(cfg.pgwire_addr().ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(cfg.http_addr().ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(cfg.native_addr().ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    /// A config that booted before keeps its exact address: a client reaching
    /// sync at `[::1]:9090` must not be moved to `127.0.0.1`.
    #[test]
    fn loopback_host_is_passed_through_unchanged() {
        for host in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
        ] {
            let mut cfg = ServerConfig::default();
            cfg.server.host = host;
            assert_eq!(
                cfg.sync_addr().ip(),
                host,
                "a loopback server.host must reach sync unchanged"
            );
        }
    }

    /// An explicit routable `sync_host` must fail at bind, not be rewritten
    /// to loopback and appear to work.
    #[test]
    fn explicit_sync_host_is_not_rewritten() {
        let mut cfg = ServerConfig::default();
        cfg.server.host = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        cfg.server.sync_host = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)));

        assert_eq!(cfg.sync_addr().ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)));
    }

    #[test]
    fn unknown_top_level_table_rejected() {
        // The misplaced `[server_typo]` table must surface, not be silently ignored.
        let raw = "[server]\n\n[server_typo]\nfoo = 1\n";
        let err = toml::from_str::<ServerConfig>(raw).unwrap_err().to_string();
        assert!(
            err.contains("unknown field") || err.contains("server_typo"),
            "unexpected error: {err}"
        );
    }

    fn write_temp_config(name: &str, contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, contents).expect("write temp config");
        path
    }

    /// The environment gate rejects a zero core count. A TOML file reaches the
    /// same field without passing that gate, so the bound holds here too.
    #[test]
    fn from_file_rejects_zero_data_plane_cores() {
        let path = write_temp_config(
            "nodedb-domain-zero-cores.toml",
            "[server]\ndata_plane_cores = 0\n",
        );
        let err = ServerConfig::from_file(&path).unwrap_err();
        std::fs::remove_file(&path).ok();
        let msg = err.to_string();
        assert!(msg.contains("server.data_plane_cores"), "{msg}");
        assert!(msg.contains("positive integer"), "{msg}");
    }

    #[test]
    fn from_file_rejects_scope_expiry_below_the_floor() {
        let path = write_temp_config(
            "nodedb-domain-expiry-floor.toml",
            "[tuning.maintenance]\nscope_expiry_interval_secs = 5\n",
        );
        let err = ServerConfig::from_file(&path).unwrap_err();
        std::fs::remove_file(&path).ok();
        let msg = err.to_string();
        assert!(
            msg.contains("tuning.maintenance.scope_expiry_interval_secs"),
            "{msg}"
        );
        assert!(msg.contains("at least 10 seconds"), "{msg}");
    }

    #[test]
    fn from_file_rejects_a_wal_write_buffer_under_the_floor() {
        let path = write_temp_config(
            "nodedb-domain-wal-buffer.toml",
            "[tuning.wal]\nwrite_buffer_size = 4096\n",
        );
        let err = ServerConfig::from_file(&path).unwrap_err();
        std::fs::remove_file(&path).ok();
        let msg = err.to_string();
        assert!(msg.contains("tuning.wal.write_buffer_size"), "{msg}");
    }

    /// Every shipped default satisfies every bound the gate enforces.
    #[test]
    fn the_compiled_defaults_are_in_domain() {
        ServerConfig::default().validate().expect("defaults valid");
    }
}
