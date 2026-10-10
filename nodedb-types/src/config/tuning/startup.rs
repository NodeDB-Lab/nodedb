// SPDX-License-Identifier: Apache-2.0

//! Startup tuning: boot-time bounds applied by the readiness gates.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default bound for the metadata-group readiness stall.
///
/// A node with a large metadata apply backlog (tens of thousands of entries
/// from a burst of cross-shard writes) needs minutes of replay before the
/// metadata group applies its first entry. A tighter bound turns a slow boot
/// into a restart loop.
pub const DEFAULT_RAFT_READY_TIMEOUT_MS: u64 = 300_000;

/// Default bound for the local data raft groups' replay.
///
/// Sized for a cold restart on a data dir that ran for weeks. The backlog of
/// committed entries takes minutes to apply, not seconds.
pub const DEFAULT_DATA_GROUP_RECOVERY_TIMEOUT_MS: u64 = 600_000;

/// Metadata-group readiness stall bound, in the type system.
///
/// Two adjacent `Duration` arguments transpose without a compile error. This
/// newtype pins the bound where it is chosen and handed to a gate; a helper
/// that takes plain `Duration` values converts at its own boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftReadyTimeout(pub Duration);

/// Local data-group replay bound, in the type system. See [`RaftReadyTimeout`].
///
/// A separate type, not a shared one: the two bounds measure different waits
/// and are passed by different call sites, so making them interchangeable buys
/// nothing and costs a class of silent mix-ups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataGroupRecoveryTimeout(pub Duration);

impl From<Duration> for RaftReadyTimeout {
    fn from(value: Duration) -> Self {
        Self(value)
    }
}

impl From<Duration> for DataGroupRecoveryTimeout {
    fn from(value: Duration) -> Self {
        Self(value)
    }
}

fn default_raft_ready_timeout_ms() -> u64 {
    DEFAULT_RAFT_READY_TIMEOUT_MS
}

fn default_data_group_recovery_timeout_ms() -> u64 {
    DEFAULT_DATA_GROUP_RECOVERY_TIMEOUT_MS
}

/// Boot-time bounds for the readiness gates in `bootstrap::cluster_ready`.
///
/// Both are range-checked where the config is loaded: below 1 ms the wait is
/// over before the group it waits for can answer, and above a day it stops
/// bounding anything while the instant it is added to is still finite.
///
/// A key this section does not define is refused. A misspelt bound, such as
/// one without the `_ms` suffix, otherwise leaves the default in force and
/// fails a long recovery without a message that names the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupTuning {
    /// How long the metadata raft group can go without applying an entry
    /// before the readiness gate fails startup. Every applied-index advance
    /// resets the clock, so a large replay finishes. Only a stuck group fails.
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
    /// Metadata-group readiness stall bound.
    pub fn raft_ready_timeout(&self) -> RaftReadyTimeout {
        RaftReadyTimeout(Duration::from_millis(self.raft_ready_timeout_ms))
    }

    /// Data-group recovery bound.
    pub fn data_group_recovery_timeout(&self) -> DataGroupRecoveryTimeout {
        DataGroupRecoveryTimeout(Duration::from_millis(self.data_group_recovery_timeout_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_backlogged_recovery_bounds() {
        let cfg = StartupTuning::default();
        assert_eq!(cfg.raft_ready_timeout_ms, 300_000);
        assert_eq!(cfg.data_group_recovery_timeout_ms, 600_000);
        assert_eq!(
            cfg.raft_ready_timeout(),
            RaftReadyTimeout(Duration::from_secs(300))
        );
        assert_eq!(
            cfg.data_group_recovery_timeout(),
            DataGroupRecoveryTimeout(Duration::from_secs(600))
        );
    }

    #[test]
    fn one_bound_set_leaves_the_other_at_its_default() {
        let cfg: StartupTuning =
            toml::from_str("raft_ready_timeout_ms = 1234").expect("deserialize");
        assert_eq!(cfg.raft_ready_timeout_ms, 1234);
        assert_eq!(cfg.data_group_recovery_timeout_ms, 600_000);
    }

    #[test]
    fn an_unknown_key_is_refused() {
        let error = toml::from_str::<StartupTuning>("data_group_recovery_timeout = 1800000")
            .expect_err("a key without the _ms suffix is not a bound");
        let message = error.to_string();
        assert!(message.contains("data_group_recovery_timeout"), "{message}");
        assert!(
            message.contains("data_group_recovery_timeout_ms"),
            "the message must list the keys that exist: {message}"
        );
    }
}
