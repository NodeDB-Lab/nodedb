//! Runtime configuration for NodeDB-Lite.
//!
//! `LiteConfig` controls memory budget allocation across the embedded engines.
//! It is designed for future TOML support via `serde`, but can be constructed
//! programmatically or loaded from environment variables via `LiteConfig::from_env()`.
//!
//! ## Environment variables
//!
//! | Variable                | Description                                  | Default |
//! |-------------------------|----------------------------------------------|---------|
//! | `NODEDB_LITE_MEMORY_MB` | Total memory budget in mebibytes             | 100     |

use nodedb_types::error::{NodeDbError, NodeDbResult};
use serde::{Deserialize, Serialize};

/// Per-engine budget percentages must leave at least some headroom.
///
/// The four engine percentages must not exceed 99 to preserve at least 1% headroom.
const MAX_TOTAL_ENGINE_PERCENT: usize = 99;

/// Runtime configuration for a NodeDB-Lite instance.
///
/// All percentage fields express a fraction of `memory_budget` allocated to
/// the corresponding engine. The remaining percentage is headroom (untracked).
///
/// # Example
/// ```
/// use nodedb_lite::config::LiteConfig;
///
/// let cfg = LiteConfig {
///     memory_budget: 256 * 1024 * 1024, // 256 MiB
///     ..LiteConfig::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteConfig {
    /// Total memory budget in bytes. Default: 100 MiB.
    pub memory_budget: usize,

    /// Percentage of `memory_budget` reserved for HNSW vector index. Default: 40.
    pub hnsw_percent: usize,

    /// Percentage of `memory_budget` reserved for CSR graph index. Default: 15.
    pub csr_percent: usize,

    /// Percentage of `memory_budget` reserved for Loro CRDT engine. Default: 15.
    pub loro_percent: usize,

    /// Percentage of `memory_budget` reserved for query scratch space. Default: 15.
    pub query_percent: usize,

    /// Enable CRDT sync for KV operations. Default: `true`.
    ///
    /// When `false`, KV operations go directly to redb (B-tree), bypassing
    /// Loro entirely. This gives SQLite-class performance for local-only use.
    /// Other engines (vector, graph, document) still use Loro for their storage.
    ///
    /// When `true`, KV writes also generate sync log entries (append-only)
    /// for replication to Origin via LWW merge.
    #[serde(default = "default_sync_enabled")]
    pub sync_enabled: bool,

    /// Argon2id memory cost in KiB. Default: 19 MiB (19_456 KiB).
    /// Corresponds to the OWASP recommended minimum for interactive login.
    #[serde(default = "default_argon2_m_cost")]
    pub argon2_m_cost: u32,

    /// Argon2id iteration count. Default: 2.
    #[serde(default = "default_argon2_t_cost")]
    pub argon2_t_cost: u32,

    /// Argon2id parallelism lanes. Default: 1.
    #[serde(default = "default_argon2_p_cost")]
    pub argon2_p_cost: u32,
}

fn default_sync_enabled() -> bool {
    true
}

fn default_argon2_m_cost() -> u32 {
    19_456
}

fn default_argon2_t_cost() -> u32 {
    2
}

fn default_argon2_p_cost() -> u32 {
    1
}

impl Default for LiteConfig {
    fn default() -> Self {
        Self {
            memory_budget: 100 * 1024 * 1024, // 100 MiB
            hnsw_percent: 40,
            csr_percent: 15,
            loro_percent: 15,
            query_percent: 15,
            sync_enabled: true,
            argon2_m_cost: default_argon2_m_cost(),
            argon2_t_cost: default_argon2_t_cost(),
            argon2_p_cost: default_argon2_p_cost(),
        }
    }
}

impl LiteConfig {
    /// Load configuration from environment variables, falling back to defaults
    /// for any variable that is absent or malformed.
    ///
    /// Handled variables:
    /// - `NODEDB_LITE_MEMORY_MB` — total memory budget in mebibytes (parsed as `usize`)
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(val) = std::env::var("NODEDB_LITE_MEMORY_MB") {
            match val.trim().parse::<usize>() {
                Ok(mb) => {
                    let bytes = mb.saturating_mul(1024 * 1024);
                    tracing::info!(
                        env_var = "NODEDB_LITE_MEMORY_MB",
                        value = mb,
                        bytes,
                        "environment variable override applied"
                    );
                    cfg.memory_budget = bytes;
                }
                Err(_) => {
                    tracing::warn!(
                        env_var = "NODEDB_LITE_MEMORY_MB",
                        value = %val,
                        "ignoring malformed environment variable (expected unsigned integer), \
                         using default 100 MiB"
                    );
                }
            }
        }

        cfg
    }

    /// Validate that percentage fields are coherent.
    ///
    /// Returns an error if:
    /// - Any individual percentage exceeds 100
    /// - The sum of all engine percentages exceeds `MAX_TOTAL_ENGINE_PERCENT`
    pub fn validate(&self) -> NodeDbResult<()> {
        for (name, pct) in [
            ("hnsw_percent", self.hnsw_percent),
            ("csr_percent", self.csr_percent),
            ("loro_percent", self.loro_percent),
            ("query_percent", self.query_percent),
        ] {
            if pct > 100 {
                return Err(NodeDbError::config(format!(
                    "{name} must be 0–100, got {pct}"
                )));
            }
        }

        let total = self
            .hnsw_percent
            .saturating_add(self.csr_percent)
            .saturating_add(self.loro_percent)
            .saturating_add(self.query_percent);

        if total > MAX_TOTAL_ENGINE_PERCENT {
            return Err(NodeDbError::config(format!(
                "sum of engine percentages is {total}%, must not exceed {MAX_TOTAL_ENGINE_PERCENT}% \
                 (at least 1% headroom required)"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = LiteConfig::default();
        assert_eq!(cfg.memory_budget, 100 * 1024 * 1024);
        assert_eq!(cfg.hnsw_percent, 40);
        assert_eq!(cfg.csr_percent, 15);
        assert_eq!(cfg.loro_percent, 15);
        assert_eq!(cfg.query_percent, 15);
        assert_eq!(cfg.argon2_m_cost, 19_456);
        assert_eq!(cfg.argon2_t_cost, 2);
        assert_eq!(cfg.argon2_p_cost, 1);
    }

    #[test]
    fn default_config_validates() {
        assert!(LiteConfig::default().validate().is_ok());
    }

    /// All `from_env` cases run sequentially in one test to avoid parallel
    /// env-var mutation across threads (no `serial_test` dependency needed).
    #[test]
    fn from_env_all_cases() {
        // Use a mutex so if other test files ever share this process they
        // cannot race on the env var.
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // SAFETY: we hold ENV_LOCK and are the only thread touching this var.

        // Case 1: var absent → default.
        unsafe { std::env::remove_var("NODEDB_LITE_MEMORY_MB") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            100 * 1024 * 1024,
            "absent var should give default 100 MiB"
        );

        // Case 2: valid integer → applied.
        unsafe { std::env::set_var("NODEDB_LITE_MEMORY_MB", "256") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            256 * 1024 * 1024,
            "256 MiB should be applied"
        );

        // Case 3: malformed → fallback to default.
        unsafe { std::env::set_var("NODEDB_LITE_MEMORY_MB", "not_a_number") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            100 * 1024 * 1024,
            "malformed var should fall back to default"
        );

        // Case 4: whitespace-padded integer → trimmed and applied.
        unsafe { std::env::set_var("NODEDB_LITE_MEMORY_MB", "  512  ") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            512 * 1024 * 1024,
            "padded value should be trimmed and applied"
        );

        // Cleanup.
        unsafe { std::env::remove_var("NODEDB_LITE_MEMORY_MB") };
    }

    #[test]
    fn validate_rejects_percent_over_100() {
        let cfg = LiteConfig {
            hnsw_percent: 101,
            ..LiteConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_sum_over_max() {
        let cfg = LiteConfig {
            hnsw_percent: 40,
            csr_percent: 25,
            loro_percent: 25,
            query_percent: 15,
            ..LiteConfig::default()
        };
        // Sum = 105 > 99.
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = LiteConfig::default();
        let json = sonic_rs::to_string(&cfg).unwrap();
        let parsed: LiteConfig = sonic_rs::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }
}
