// SPDX-License-Identifier: Apache-2.0

//! Startup tuning — boot-time bounds applied by the readiness gates.

use std::time::Duration;

use serde::{Deserialize, Serialize};

fn default_raft_ready_timeout_ms() -> u64 {
    // 5 minutes: a node with a large metadata apply backlog (tens of
    // thousands of entries from a burst of cross-shard writes) needs minutes
    // of replay before the metadata group applies its first entry. A tighter
    // bound turned a slow boot into a restart loop.
    300_000
}

fn default_data_group_recovery_timeout_ms() -> u64 {
    // 10 minutes: the value production carried before the bound became a
    // hard-coded constant.
    600_000
}

/// Boot-time bounds for the readiness gates in `bootstrap::cluster_ready`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupTuning {
    /// How long the metadata raft group may go without applying an entry
    /// before the readiness gate fails startup. Every applied-index advance
    /// resets the clock, so a large replay finishes; only a stuck group fails.
    /// Default: 300_000 (5 minutes).
    #[serde(default = "default_raft_ready_timeout_ms")]
    pub raft_ready_timeout_ms: u64,

    /// How long the locally hosted data raft groups have to replay their
    /// retained logs before startup fails. Default: 600_000 (10 minutes).
    #[serde(default = "default_data_group_recovery_timeout_ms")]
    pub data_group_recovery_timeout_ms: u64,
}

impl Default for StartupTuning {
    fn default() -> Self {
        Self {
            raft_ready_timeout_ms: default_raft_ready_timeout_ms(),
            data_group_recovery_timeout_ms: default_data_group_recovery_timeout_ms(),
        }
    }
}

impl StartupTuning {
    /// Metadata-group readiness stall bound as a `Duration`.
    pub fn raft_ready_timeout(&self) -> Duration {
        Duration::from_millis(self.raft_ready_timeout_ms)
    }

    /// Data-group recovery bound as a `Duration`.
    pub fn data_group_recovery_timeout(&self) -> Duration {
        Duration::from_millis(self.data_group_recovery_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_five_and_ten_minutes() {
        let t = StartupTuning::default();
        assert_eq!(t.raft_ready_timeout(), Duration::from_secs(300));
        assert_eq!(t.data_group_recovery_timeout(), Duration::from_secs(600));
    }

    #[test]
    fn both_bounds_take_overrides() {
        let parsed: StartupTuning =
            toml::from_str("raft_ready_timeout_ms = 1000\ndata_group_recovery_timeout_ms = 2000\n")
                .unwrap();
        assert_eq!(parsed.raft_ready_timeout(), Duration::from_millis(1000));
        assert_eq!(
            parsed.data_group_recovery_timeout(),
            Duration::from_millis(2000)
        );
    }
}
