mod checkpoint;
mod cluster;
mod cold_storage;
mod env;
mod tls;

pub use checkpoint::CheckpointSettings;
pub use cluster::ClusterSettings;
pub use cold_storage::ColdStorageSettings;
pub use env::{apply_env_overrides, parse_memory_size, parse_seed_nodes};
pub use tls::{EncryptionSettings, TlsSettings};

use std::net::SocketAddr;
use std::path::PathBuf;

use nodedb_types::config::TuningConfig;
use serde::{Deserialize, Serialize};

use super::EngineConfig;

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address to bind the native wire protocol listener.
    pub listen: SocketAddr,

    /// Address to bind the PostgreSQL wire protocol listener.
    /// Defaults to 127.0.0.1:5432.
    pub pg_listen: SocketAddr,

    /// Address to bind the HTTP API (health, metrics, REST).
    /// Defaults to 127.0.0.1:8080.
    pub http_listen: SocketAddr,

    /// Data directory for WAL, segments, and indexes.
    pub data_dir: PathBuf,

    /// Number of Data Plane cores. Defaults to available CPUs minus one
    /// (reserving one core for the Control Plane).
    pub data_plane_cores: usize,

    /// Global memory ceiling in bytes. The memory governor enforces this.
    pub memory_limit: usize,

    /// Maximum concurrent client connections across all listeners.
    /// Enforced at accept time via a shared semaphore — no permit means
    /// immediate TCP RST. Prevents connection floods from exhausting memory
    /// before per-tenant quotas kick in (those are checked post-authentication).
    /// 0 = unlimited (not recommended for production).
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Per-engine budget configuration.
    pub engines: EngineConfig,

    /// Authentication and authorization configuration.
    #[serde(default)]
    pub auth: super::AuthConfig,

    /// Client TLS configuration. If present, pgwire connections support SSL.
    #[serde(default)]
    pub tls: Option<TlsSettings>,

    /// Encryption at rest configuration. If present, WAL payloads are encrypted.
    #[serde(default)]
    pub encryption: Option<EncryptionSettings>,

    /// Log output format: "text" (default, human-readable) or "json" (structured).
    #[serde(default = "default_log_format")]
    pub log_format: String,

    /// Checkpoint and WAL management settings.
    #[serde(default)]
    pub checkpoint: CheckpointSettings,

    /// Address to bind the RESP (Redis-compatible) KV protocol listener.
    /// Disabled by default (None). Enable for Redis-compatible key-value access.
    /// Default port: 6381 (distinct from Redis's 6379).
    #[serde(default)]
    pub resp_listen: Option<SocketAddr>,

    /// Address to bind the ILP (InfluxDB Line Protocol) TCP listener.
    /// Disabled by default (None). Enable for timeseries ingest.
    /// Standard InfluxDB port: 8086.
    #[serde(default)]
    pub ilp_listen: Option<SocketAddr>,

    /// Cluster mode settings. When present, the node participates in a
    /// distributed cluster via Multi-Raft consensus over QUIC transport.
    /// When absent, runs in single-node mode (default).
    #[serde(default)]
    pub cluster: Option<ClusterSettings>,

    /// Cold storage (L2 tiering) configuration.
    /// When present, old L1 segments are promoted to S3-compatible cold storage.
    #[serde(default)]
    pub cold_storage: Option<ColdStorageSettings>,

    /// Performance tuning knobs for engines, query execution, WAL, bridge,
    /// network, and cluster transport. All fields have sensible defaults;
    /// override selectively via the `[tuning]` TOML section.
    #[serde(default)]
    pub tuning: TuningConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1);

        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 6433)),
            pg_listen: SocketAddr::from(([127, 0, 0, 1], 6432)),
            http_listen: SocketAddr::from(([127, 0, 0, 1], 6480)),
            data_dir: default_data_dir(),
            data_plane_cores: cores,
            max_connections: default_max_connections(),
            memory_limit: 1024 * 1024 * 1024, // 1 GiB default
            engines: EngineConfig::default(),
            auth: super::AuthConfig::default(),
            tls: None,
            encryption: None,
            log_format: "text".into(),
            checkpoint: CheckpointSettings::default(),
            resp_listen: None,
            ilp_listen: None,
            cluster: None,
            cold_storage: None,
            tuning: TuningConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Load configuration from a TOML file, falling back to defaults.
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| crate::Error::Config {
            detail: format!("failed to read config file {}: {e}", path.display()),
        })?;
        toml::from_str(&content).map_err(|e| crate::Error::Config {
            detail: format!("invalid TOML config: {e}"),
        })
    }

    /// WAL directory within the data directory.
    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    /// Segments directory within the data directory.
    pub fn segments_dir(&self) -> PathBuf {
        self.data_dir.join("segments")
    }

    /// System catalog (auth, roles, tenants) redb file.
    pub fn catalog_path(&self) -> PathBuf {
        self.data_dir.join("system.redb")
    }
}

fn default_max_connections() -> usize {
    4096
}

fn default_log_format() -> String {
    "text".into()
}

/// Default data directory following platform conventions.
///
/// - Linux: `$XDG_DATA_HOME/nodedb` or `~/.local/share/nodedb`
/// - macOS: `~/Library/Application Support/nodedb`
/// - Windows: `%LOCALAPPDATA%\nodedb\data`
///
/// Falls back to `./nodedb-data` if the home directory cannot be determined.
fn default_data_dir() -> PathBuf {
    if let Some(dir) = platform_data_dir() {
        dir.join("nodedb")
    } else {
        PathBuf::from("nodedb-data")
    }
}

fn platform_data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(xdg) = std::env::var("XDG_DATA_HOME")
            && !xdg.is_empty()
        {
            return Some(PathBuf::from(xdg));
        }
        home_dir().map(|h| h.join(".local").join("share"))
    }

    #[cfg(target_os = "macos")]
    {
        home_dir().map(|h| h.join("Library").join("Application Support"))
    }

    #[cfg(target_os = "windows")]
    {
        if let Ok(local) = std::env::var("LOCALAPPDATA")
            && !local.is_empty()
        {
            return Some(PathBuf::from(local));
        }
        home_dir().map(|h| h.join("AppData").join("Local"))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        home_dir().map(|h| h.join(".local").join("share"))
    }
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var("USERPROFILE").ok().map(PathBuf::from)
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME").ok().map(PathBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_valid() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.listen.port(), 6433);
        assert_eq!(cfg.pg_listen.port(), 6432);
        assert_eq!(cfg.http_listen.port(), 6480);
        assert!(cfg.data_plane_cores >= 1);
        assert_eq!(cfg.memory_limit, 1024 * 1024 * 1024);
    }

    #[test]
    fn config_roundtrip() {
        let cfg = ServerConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).expect("serialize");
        let _parsed: ServerConfig = toml::from_str(&toml_str).expect("deserialize");
    }
}
