// SPDX-License-Identifier: BUSL-1.1

//! `[server]` section: process/runtime configuration.
//!
//! Groups the fields that describe *how the server runs* — bind address,
//! ports, resource budgets, on-disk location, log format — and keeps them
//! distinct from the independent subsystem tables (`[auth]`, `[tls]`,
//! `[cluster]`, …) which sit as siblings at the root.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::env::parse_memory_size;
use super::log_format::LogFormat;
use super::paths::default_data_dir;
use super::ports::PortsConfig;
use super::tls::TlsSettings;

/// Server-level (process/runtime) configuration. Lives under the
/// `[server]` table in the TOML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    /// Bind address for the native, pgwire, HTTP, RESP and ILP listeners.
    /// Default: 127.0.0.1 (localhost only). Use 0.0.0.0 for all interfaces.
    /// Does not move the sync listener — see [`Self::sync_host`].
    #[serde(default = "default_host")]
    pub host: IpAddr,

    /// Bind address for the sync listener alone, which must be loopback:
    /// it serves plaintext `ws://` and terminates no TLS of its own.
    /// `None` follows [`Self::host`] when that is loopback, else IPv4
    /// loopback. A routable value here is refused at bind, not rewritten.
    #[serde(default)]
    pub sync_host: Option<IpAddr>,

    /// Per-protocol port numbers.
    #[serde(default)]
    pub ports: PortsConfig,

    /// Data directory for WAL, segments, and indexes.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Number of Data Plane cores. Defaults to available CPUs minus one
    /// (reserving one core for the Control Plane).
    #[serde(default = "default_data_plane_cores")]
    pub data_plane_cores: usize,

    /// Global memory ceiling. Accepts either a raw byte count
    /// (`memory_limit = 4294967296`) or a human-readable string
    /// (`memory_limit = "4GiB"`). The memory governor enforces this.
    #[serde(
        default = "default_memory_limit",
        deserialize_with = "deserialize_memory_limit"
    )]
    pub memory_limit: usize,

    /// Maximum concurrent client connections across all listeners.
    /// Enforced at accept time via a shared semaphore — no permit means
    /// immediate TCP RST. Prevents connection floods from exhausting memory
    /// before per-tenant quotas kick in (those are checked post-authentication).
    /// 0 = unlimited (not recommended for production).
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Log output format: `"text"` (default, human-readable) or `"json"` (structured).
    /// Unknown values are rejected at startup — there is no silent fallback.
    #[serde(default)]
    pub log_format: LogFormat,

    /// Boot-time raft metadata group readiness timeout in milliseconds.
    /// Default 30_000 (30s). Replay of a large raft log (long write bursts,
    /// big migrations) can exceed 30s — raise this via `raft_ready_timeout_ms`.
    /// Script/config set: `[server] raft_ready_timeout_ms = 300000`.
    #[serde(default)]
    pub raft_ready_timeout_ms: Option<u64>,

    /// Per-listener TLS configuration. Lives under `[server.tls]` because
    /// it directly modifies the listeners declared in `[server.ports]`
    /// (per-protocol toggles + cert paths). When absent, every listener
    /// runs in plaintext. When present, individual protocols can still
    /// opt out via the per-protocol bool fields on [`TlsSettings`].
    #[serde(default)]
    pub tls: Option<TlsSettings>,

    /// Enable the Calvin transaction stack on a standalone (non-cluster)
    /// server. Default `false`.
    ///
    /// When `true` and `[cluster]` is absent, the server synthesizes a
    /// one-node cluster (this node as its own sole seed, replication
    /// factor 1) and drives the same cluster startup a real deployment
    /// uses: the sequencer Raft group and per-vShard schedulers come up,
    /// its QUIC transport binds to a loopback port that never dials a
    /// peer, and a cross-core (cross-vShard) transaction traverses the
    /// deterministic Calvin path exactly as in cluster mode.
    ///
    /// On by default: a standalone node stands up the single-node Calvin
    /// stack so cross-core (cross-vShard) transactions commit atomically
    /// through the deterministic sequencer path instead of being rejected.
    /// Set to `false` to force the legacy single-node path (no Calvin stack;
    /// cross-shard interactive transactions are rejected) — e.g. for a
    /// minimal deployment that never issues cross-core transactions.
    #[serde(default = "default_single_node_calvin")]
    pub single_node_calvin: bool,

    /// Capture an out-of-process minidump when the server dies to a native
    /// fault (SIGSEGV, abort, stack overflow) rather than a Rust panic.
    /// Default `false`.
    ///
    /// Structured corruption, invariant, and panic reports are always recorded
    /// — they carry diagnostic fields that pass through a redactor. A minidump
    /// cannot: it is a verbatim copy of the whole address space, which for this
    /// process means decrypted rows, session credentials, and WAL keys still
    /// resident in memory. Writing that to disk is an operator's decision about
    /// their own data, not a default, so the handler is armed only on request.
    ///
    /// Reports are written under `<data_dir>/diagnostics`, owner-only.
    #[serde(default)]
    pub native_crash_dumps: bool,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            host: default_host(),
            sync_host: None,
            ports: PortsConfig::default(),
            data_dir: default_data_dir(),
            data_plane_cores: default_data_plane_cores(),
            memory_limit: default_memory_limit(),
            max_connections: default_max_connections(),
            log_format: LogFormat::Text,
            raft_ready_timeout_ms: None,
            tls: None,
            single_node_calvin: default_single_node_calvin(),
            native_crash_dumps: false,
        }
    }
}

/// Default for [`ServerSection::single_node_calvin`]: on, so a standalone
/// node supports cross-core transactions through the single-node Calvin path
/// out of the box.
fn default_single_node_calvin() -> bool {
    true
}

fn default_host() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

fn default_data_plane_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1))
        .unwrap_or(1)
}

fn default_memory_limit() -> usize {
    1024 * 1024 * 1024 // 1 GiB
}

fn default_max_connections() -> usize {
    4096
}

/// Deserializer for `memory_limit` that accepts either a raw byte count
/// (`memory_limit = 4294967296`) or a human-readable string
/// (`memory_limit = "4GiB"`). Suffixes: `K/KiB`, `M/MiB`, `G/GiB`, `T/TiB`,
/// case-insensitive. See [`parse_memory_size`].
fn deserialize_memory_limit<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct MemSizeVisitor;

    impl<'de> Visitor<'de> for MemSizeVisitor {
        type Value = usize;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a byte count integer or a string like \"4GiB\" / \"512MiB\"")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<usize, E> {
            usize::try_from(v).map_err(|_| de::Error::custom("memory_limit too large for usize"))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<usize, E> {
            if v < 0 {
                return Err(de::Error::custom("memory_limit must be non-negative"));
            }
            usize::try_from(v).map_err(|_| de::Error::custom("memory_limit too large for usize"))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<usize, E> {
            parse_memory_size(v).map_err(|e| de::Error::custom(format!("memory_limit: {e}")))
        }
    }

    deserializer.deserialize_any(MemSizeVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_values() {
        let s = ServerSection::default();
        assert_eq!(s.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(s.memory_limit, 1024 * 1024 * 1024);
        assert!(s.data_plane_cores >= 1);
        assert_eq!(s.max_connections, 4096);
        assert_eq!(s.log_format, LogFormat::Text);
    }

    #[test]
    fn memory_limit_string_form_parses() {
        let s: ServerSection = toml::from_str("memory_limit = \"4GiB\"\n").unwrap();
        assert_eq!(s.memory_limit, 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn memory_limit_int_form_parses() {
        let s: ServerSection = toml::from_str("memory_limit = 2147483648\n").unwrap();
        assert_eq!(s.memory_limit, 2147483648);
    }

    #[test]
    fn memory_limit_negative_rejected() {
        let result: Result<ServerSection, _> = toml::from_str("memory_limit = -1\n");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_field_rejected() {
        let result: Result<ServerSection, _> = toml::from_str("frobnicate = true\n");
        assert!(result.is_err());
    }
}
