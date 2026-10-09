// SPDX-License-Identifier: Apache-2.0

//! Calvin scheduler tuning — channel sizes, deadlines, and the in-flight
//! bounds of each per-vShard scheduler.
//!
//! The epoch duration belongs to the sequencer, so it is not set here. The
//! scheduler reads it from the sequencer config it runs beside.

use serde::{Deserialize, Serialize};

fn default_channel_capacity() -> usize {
    512
}

fn default_txn_deadline_multiplier() -> u32 {
    3
}

fn default_dependent_read_passive_timeout_ms() -> u64 {
    // Three 20 ms sequencer epochs.
    60
}

fn default_verdict_stall_warn_ms() -> u64 {
    // 250 sequencer epochs of 20 ms.
    5_000
}

fn default_max_inflight_backlog() -> usize {
    // The per-core request queue capacity of the bridge dispatcher. Every
    // dispatch of one vShard goes to one Data Plane core.
    1_024
}

fn default_catch_up_window() -> u64 {
    // One channel of missed entries per drain.
    512
}

fn default_max_redo_entry_bytes() -> usize {
    // Well below the 64 MiB RPC frame limit, so one entry and the frame
    // around it always fit.
    8 * 1024 * 1024
}

fn default_max_open_redo_bytes() -> u64 {
    // Sixty-four streams of the default entry size.
    512 * 1024 * 1024
}

/// Tuning knobs for the Calvin scheduler that runs per hosted vShard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalvinTuning {
    /// Capacity of each scheduler's bounded input, completion, read-result,
    /// and verdict channels.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Transaction deadline, in sequencer epochs.
    #[serde(default = "default_txn_deadline_multiplier")]
    pub txn_deadline_multiplier: u32,

    /// Timeout in milliseconds for passive participant read results of a
    /// dependent-read transaction.
    #[serde(default = "default_dependent_read_passive_timeout_ms")]
    pub dependent_read_passive_timeout_ms: u64,

    /// Interval in milliseconds between stall warnings for a transaction
    /// parked on its global verdict. The transaction keeps waiting and never
    /// aborts. The idle sweep runs at a quarter of this interval.
    #[serde(default = "default_verdict_stall_warn_ms")]
    pub verdict_stall_warn_ms: u64,

    /// In-flight backlog at which a scheduler stops taking new sequenced
    /// input. The backlog counts pending, blocked, and dependent-barrier
    /// transactions.
    #[serde(default = "default_max_inflight_backlog")]
    pub max_inflight_backlog: usize,

    /// Most sequencer log entries one catch-up drain reads and replays.
    #[serde(default = "default_catch_up_window")]
    pub catch_up_window: u64,

    /// Largest encoded redo entry a proposer puts in a data-group log. A
    /// committed redo past it travels as chunk entries and a final entry.
    #[serde(default = "default_max_redo_entry_bytes")]
    pub max_redo_entry_bytes: usize,

    /// Most bytes of chunked redo streams a node holds open. A group leader
    /// refuses a new stream that would pass it on the leader's node. A node
    /// holds at most this much per data group it hosts.
    #[serde(default = "default_max_open_redo_bytes")]
    pub max_open_redo_bytes: u64,
}

impl Default for CalvinTuning {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
            txn_deadline_multiplier: default_txn_deadline_multiplier(),
            dependent_read_passive_timeout_ms: default_dependent_read_passive_timeout_ms(),
            verdict_stall_warn_ms: default_verdict_stall_warn_ms(),
            max_inflight_backlog: default_max_inflight_backlog(),
            catch_up_window: default_catch_up_window(),
            max_redo_entry_bytes: default_max_redo_entry_bytes(),
            max_open_redo_bytes: default_max_open_redo_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let t = CalvinTuning::default();
        assert_eq!(t.channel_capacity, 512);
        assert_eq!(t.txn_deadline_multiplier, 3);
        assert_eq!(t.dependent_read_passive_timeout_ms, 60);
        assert_eq!(t.verdict_stall_warn_ms, 5_000);
        assert_eq!(t.max_inflight_backlog, 1_024);
        assert_eq!(t.catch_up_window, 512);
        assert_eq!(t.max_redo_entry_bytes, 8 * 1024 * 1024);
        assert_eq!(t.max_open_redo_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn partial_override() {
        let toml_str = r#"
max_inflight_backlog = 256
verdict_stall_warn_ms = 10000
"#;
        let t: CalvinTuning = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(t.max_inflight_backlog, 256);
        assert_eq!(t.verdict_stall_warn_ms, 10_000);
        assert_eq!(t.channel_capacity, 512);
        assert_eq!(t.catch_up_window, 512);
    }

    #[test]
    fn empty_table_keeps_the_defaults() {
        let t: CalvinTuning = toml::from_str("").expect("deserialize");
        assert_eq!(t, CalvinTuning::default());
    }
}
