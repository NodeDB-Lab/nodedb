// SPDX-License-Identifier: Apache-2.0

//! Mirror source tuning: how often a source cuts its logs for its mirrors, and
//! how much log one link's pin holds.

use std::time::Duration;

use serde::{Deserialize, Serialize};

fn default_cut_interval_ms() -> u64 {
    1_000
}

fn default_max_pinned_bytes_per_group() -> u64 {
    1024 * 1024 * 1024
}

fn default_max_pin_age_secs() -> u64 {
    24 * 60 * 60
}

fn default_sync_timeout_ms() -> u64 {
    10_000
}

/// Tuning of a source cluster's mirror links.
///
/// A pinned Raft log stays in memory, so the two bounds protect the source's
/// memory. A pin past either one expires, and its link bootstraps again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorTuning {
    /// Interval between cuts while any link holds a pin, in milliseconds.
    #[serde(default = "default_cut_interval_ms")]
    pub cut_interval_ms: u64,

    /// The log memory one replica of one group holds for pins, in bytes. It
    /// counts entry bytes plus the cost of each retained entry.
    #[serde(default = "default_max_pinned_bytes_per_group")]
    pub max_pinned_bytes_per_group: u64,

    /// How long a link can go without an ack before its pin expires, in
    /// seconds.
    #[serde(default = "default_max_pin_age_secs")]
    pub max_pin_age_secs: u64,

    /// How long a committed write waits for a sync mirror's confirmation before
    /// it answers `MIRROR_SYNC_TIMEOUT`, in milliseconds.
    #[serde(default = "default_sync_timeout_ms")]
    pub sync_timeout_ms: u64,
}

impl Default for MirrorTuning {
    fn default() -> Self {
        Self {
            cut_interval_ms: default_cut_interval_ms(),
            max_pinned_bytes_per_group: default_max_pinned_bytes_per_group(),
            max_pin_age_secs: default_max_pin_age_secs(),
            sync_timeout_ms: default_sync_timeout_ms(),
        }
    }
}

impl MirrorTuning {
    /// The interval between cuts. A zero reads as one millisecond.
    pub fn cut_interval(&self) -> Duration {
        Duration::from_millis(self.cut_interval_ms.max(1))
    }

    /// How long a committed write waits for a sync mirror.
    pub fn sync_timeout(&self) -> Duration {
        Duration::from_millis(self.sync_timeout_ms)
    }
}
