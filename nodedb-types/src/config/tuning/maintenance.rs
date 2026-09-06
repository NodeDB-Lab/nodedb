// SPDX-License-Identifier: Apache-2.0

//! Background maintenance tuning — auto-ANALYZE triggering.
//!
//! Covers the Control-Plane maintenance work that runs off the write path.
//! Per-database CPU budgets come from the quota record, not from here.

use serde::{Deserialize, Serialize};

fn default_auto_analyze_min_mutations() -> u64 {
    // A collection re-analyzes once mutations reach 10% of its last row
    // count. This floor keeps a small collection from re-scanning on a
    // handful of writes.
    1_000
}

fn default_clone_sweep_interval_ms() -> u64 {
    30_000
}

fn default_constraint_reconcile_interval_ms() -> u64 {
    1_000
}

fn default_scope_expiry_interval_secs() -> u64 {
    60
}

/// Tuning knobs for background maintenance triggered by user writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceTuning {
    /// Smallest mutation count that can trigger an automatic ANALYZE.
    ///
    /// The trigger fires at `max(last_row_count / 10, this)`. Lowering it
    /// refreshes a small collection's statistics sooner. Raising it trades
    /// planner accuracy for fewer background scans.
    #[serde(default = "default_auto_analyze_min_mutations")]
    pub auto_analyze_min_mutations: u64,

    /// Interval between clone materializer sweeps, in milliseconds.
    ///
    /// The sweep progresses cloned collections from Shadowed to Materialized
    /// without explicit DDL. Lowering it materializes clones sooner, at the
    /// cost of more scan passes.
    #[serde(default = "default_clone_sweep_interval_ms")]
    pub clone_sweep_interval_ms: u64,

    /// Interval between CRDT constraint reconcile passes, in milliseconds.
    ///
    /// Each pass re-derives every collection's constraint set from the
    /// catalog and replicates it to data-group replicas. Lowering it converges
    /// an altered collection sooner, at the cost of catalog reads and Raft
    /// proposals.
    #[serde(default = "default_constraint_reconcile_interval_ms")]
    pub constraint_reconcile_interval_ms: u64,

    /// Interval between scope grant expiry sweeps, in seconds.
    ///
    /// Each sweep executes the `ON EXPIRE` action of every expired grant.
    /// `ScopeGrant::is_effective` already enforces expiry on every read, so
    /// this loop only makes the outcome durable. 10 is the floor. Below it the
    /// sweep costs more than the resolution it buys.
    #[serde(default = "default_scope_expiry_interval_secs")]
    pub scope_expiry_interval_secs: u64,
}

impl Default for MaintenanceTuning {
    fn default() -> Self {
        Self {
            auto_analyze_min_mutations: default_auto_analyze_min_mutations(),
            clone_sweep_interval_ms: default_clone_sweep_interval_ms(),
            constraint_reconcile_interval_ms: default_constraint_reconcile_interval_ms(),
            scope_expiry_interval_secs: default_scope_expiry_interval_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_floor_is_one_thousand() {
        assert_eq!(
            MaintenanceTuning::default().auto_analyze_min_mutations,
            1000
        );
    }

    #[test]
    fn new_loop_interval_defaults() {
        let tuning = MaintenanceTuning::default();
        assert_eq!(tuning.clone_sweep_interval_ms, 30_000);
        assert_eq!(tuning.constraint_reconcile_interval_ms, 1_000);
        assert_eq!(tuning.scope_expiry_interval_secs, 60);
    }

    #[test]
    fn override_via_toml() {
        let parsed: MaintenanceTuning =
            toml::from_str("auto_analyze_min_mutations = 20").expect("deserialize");
        assert_eq!(parsed.auto_analyze_min_mutations, 20);
    }

    #[test]
    fn empty_table_keeps_the_default() {
        let parsed: MaintenanceTuning = toml::from_str("").expect("deserialize");
        assert_eq!(parsed.auto_analyze_min_mutations, 1000);
    }
}
