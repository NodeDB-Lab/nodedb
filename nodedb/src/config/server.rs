use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
    pub engines: super::EngineConfig,

    /// Authentication and authorization configuration.
    #[serde(default)]
    pub auth: super::AuthConfig,

    /// Client TLS configuration. If present, pgwire connections support SSL.
    #[serde(default)]
    pub tls: Option<super::server::TlsSettings>,

    /// Encryption at rest configuration. If present, WAL payloads are encrypted.
    #[serde(default)]
    pub encryption: Option<super::server::EncryptionSettings>,

    /// Log output format: "text" (default, human-readable) or "json" (structured).
    #[serde(default = "default_log_format")]
    pub log_format: String,

    /// Checkpoint and WAL management settings.
    #[serde(default)]
    pub checkpoint: CheckpointSettings,
}

/// Checkpoint and WAL segment management configuration.
///
/// Controls how often engine state is flushed to disk and how the WAL
/// is segmented and truncated. All intervals are in seconds.
///
/// Example TOML:
/// ```toml
/// [checkpoint]
/// interval_secs = 300
/// core_timeout_secs = 30
/// wal_segment_target_mb = 64
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointSettings {
    /// How often the checkpoint manager runs (seconds). Each cycle:
    /// dispatches `Checkpoint` to all cores, collects watermark LSNs,
    /// writes a WAL checkpoint marker, and truncates old WAL segments.
    /// Default: 300 (5 minutes).
    #[serde(default = "default_checkpoint_interval")]
    pub interval_secs: u64,

    /// Maximum time to wait for each Data Plane core to complete its
    /// checkpoint flush (seconds). Cores that don't respond in time are
    /// skipped — the global checkpoint LSN uses only responding cores.
    /// Default: 30.
    #[serde(default = "default_core_timeout")]
    pub core_timeout_secs: u64,

    /// Target WAL segment file size in MiB. When the active segment
    /// exceeds this, the writer rolls to a new segment. Old segments
    /// are deleted after checkpoint confirmation.
    /// This is a soft limit — the current record is always completed
    /// before rolling.
    /// Default: 64 MiB.
    #[serde(default = "default_wal_segment_target_mb")]
    pub wal_segment_target_mb: u64,

    /// How often each Data Plane core runs automatic compaction (seconds).
    /// Compaction removes tombstoned vectors from HNSW indexes, compacts
    /// CSR write buffers, and sweeps dangling edges.
    /// Default: 600 (10 minutes).
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_secs: u64,

    /// Tombstone ratio threshold for automatic vector compaction (0.0–1.0).
    /// Collections with tombstone ratio below this are skipped during
    /// periodic compaction. On-demand compaction (`COMPACT`) ignores this.
    /// Default: 0.2 (20%).
    #[serde(default = "default_compaction_tombstone_threshold")]
    pub compaction_tombstone_threshold: f64,
}

impl Default for CheckpointSettings {
    fn default() -> Self {
        Self {
            interval_secs: default_checkpoint_interval(),
            core_timeout_secs: default_core_timeout(),
            wal_segment_target_mb: default_wal_segment_target_mb(),
            compaction_interval_secs: default_compaction_interval(),
            compaction_tombstone_threshold: default_compaction_tombstone_threshold(),
        }
    }
}

impl CheckpointSettings {
    /// Convert to the checkpoint manager config used by the Control Plane.
    pub fn to_manager_config(&self) -> crate::control::checkpoint_manager::CheckpointManagerConfig {
        crate::control::checkpoint_manager::CheckpointManagerConfig {
            interval: std::time::Duration::from_secs(self.interval_secs),
            core_timeout: std::time::Duration::from_secs(self.core_timeout_secs),
        }
    }

    /// WAL segment target size in bytes.
    pub fn wal_segment_target_bytes(&self) -> u64 {
        self.wal_segment_target_mb * 1024 * 1024
    }

    /// Compaction interval as `Duration`.
    pub fn compaction_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.compaction_interval_secs)
    }
}

fn default_checkpoint_interval() -> u64 {
    300
}

fn default_core_timeout() -> u64 {
    30
}

fn default_wal_segment_target_mb() -> u64 {
    64
}

fn default_compaction_interval() -> u64 {
    600
}

fn default_compaction_tombstone_threshold() -> f64 {
    0.2
}

fn default_max_connections() -> usize {
    4096
}

fn default_log_format() -> String {
    "text".into()
}

/// Encryption at rest settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionSettings {
    /// Path to the 32-byte AES-256-GCM key file.
    /// Generate with: `head -c 32 /dev/urandom > /etc/nodedb/keys/wal.key`
    pub key_path: PathBuf,
}

/// Client-facing TLS settings (distinct from inter-node mTLS in mtls.rs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Path to server certificate (PEM).
    pub cert_path: PathBuf,
    /// Path to server private key (PEM).
    pub key_path: PathBuf,
    /// Certificate hot-reload check interval (seconds). The server watches
    /// cert/key files for mtime changes and atomically swaps the TLS config.
    /// Default: 3600 (1 hour). Set to 0 to disable hot rotation.
    #[serde(default)]
    pub cert_reload_interval_secs: Option<u64>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1);

        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 5433)),
            pg_listen: SocketAddr::from(([127, 0, 0, 1], 5432)),
            http_listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
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

use super::EngineConfig;

/// Default data directory following platform conventions.
///
/// - Linux: `$XDG_DATA_HOME/nodedb` or `~/.local/share/nodedb`
/// - macOS: `~/Library/Application Support/nodedb`
/// - Windows: `%LOCALAPPDATA%\nodedb\data` (e.g. `C:\Users\<user>\AppData\Local\nodedb\data`)
///
/// Falls back to `./nodedb-data` if the home directory cannot be determined.
fn default_data_dir() -> PathBuf {
    if let Some(dir) = platform_data_dir() {
        dir.join("nodedb")
    } else {
        // Last resort — never pollute cwd with just "data".
        PathBuf::from("nodedb-data")
    }
}

fn platform_data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        // XDG Base Directory: $XDG_DATA_HOME or ~/.local/share
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
        // %LOCALAPPDATA% (e.g. C:\Users\<user>\AppData\Local)
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
        assert!(cfg.data_plane_cores >= 1);
        assert_eq!(cfg.memory_limit, 1024 * 1024 * 1024);
        assert_eq!(cfg.listen.port(), 5433);
        assert_eq!(cfg.pg_listen.port(), 5432);
    }

    #[test]
    fn wal_dir_derived() {
        let cfg = ServerConfig::default();
        assert!(
            cfg.wal_dir().ends_with("nodedb/wal") || cfg.wal_dir().ends_with("nodedb-data/wal")
        );
    }

    #[test]
    fn toml_roundtrip() {
        let cfg = ServerConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).unwrap();
        let parsed: ServerConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.listen, cfg.listen);
        assert_eq!(parsed.memory_limit, cfg.memory_limit);
    }
}
