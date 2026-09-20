// SPDX-License-Identifier: Apache-2.0

//! Top-level `TuningConfig` aggregating every per-domain tuning struct.

use serde::{Deserialize, Serialize};

use super::bitemporal::BitemporalTuning;
use super::data_plane::{DataPlaneTuning, QueryTuning};
use super::engines::{GraphTuning, KvTuning, SparseTuning, TimeseriesToning, VectorTuning};
use super::maintenance::MaintenanceTuning;
use super::memory::MemoryTuning;
use super::network::{BridgeTuning, ClusterTransportTuning, NetworkTuning, WalTuning};
use super::scheduler::SchedulerTuning;
use super::shutdown::ShutdownTuning;
use super::startup::StartupTuning;

/// Top-level tuning configuration.
///
/// All fields have sensible defaults derived from the current hardcoded values.
/// Override via the `[tuning]` section in `config.toml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TuningConfig {
    #[serde(default)]
    pub data_plane: DataPlaneTuning,
    #[serde(default)]
    pub query: QueryTuning,
    #[serde(default)]
    pub vector: VectorTuning,
    #[serde(default)]
    pub sparse: SparseTuning,
    #[serde(default)]
    pub graph: GraphTuning,
    #[serde(default)]
    pub timeseries: TimeseriesToning,
    #[serde(default)]
    pub kv: KvTuning,
    #[serde(default)]
    pub bridge: BridgeTuning,
    #[serde(default)]
    pub network: NetworkTuning,
    #[serde(default)]
    pub wal: WalTuning,
    #[serde(default)]
    pub cluster_transport: ClusterTransportTuning,
    #[serde(default)]
    pub memory: MemoryTuning,
    #[serde(default)]
    pub scheduler: SchedulerTuning,
    #[serde(default)]
    pub shutdown: ShutdownTuning,
    #[serde(default)]
    pub startup: StartupTuning,
    #[serde(default)]
    pub bitemporal: BitemporalTuning,
    #[serde(default)]
    pub maintenance: MaintenanceTuning,
}

impl TuningConfig {
    /// Tick interval for the bitemporal audit-retention enforcement loop.
    /// Returns `None` when the operator wants the loop default; returns
    /// `Some(d)` when `[tuning.bitemporal] tick_interval_secs` is set.
    pub fn bitemporal_retention_tick(&self) -> Option<std::time::Duration> {
        Some(self.bitemporal.tick_interval())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_tuning_roundtrip() {
        let cfg = TuningConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).expect("serialize");
        let parsed: TuningConfig = toml::from_str(&toml_str).expect("deserialize");

        assert_eq!(parsed.data_plane.idle_poll_timeout_ms, 100);
        assert_eq!(parsed.startup.raft_ready_timeout_ms, 300_000);
        assert_eq!(parsed.startup.data_group_recovery_timeout_ms, 600_000);
        assert_eq!(parsed.query.sort_run_size, 100_000);
        assert_eq!(parsed.vector.flat_index_threshold, 10_000);
        assert_eq!(parsed.sparse.bm25_k1, 1.2);
        assert_eq!(parsed.graph.max_visited, 100_000);
        assert_eq!(parsed.graph.varlen_max_results, 100_000);
        assert_eq!(parsed.graph.varlen_max_frontier, 100_000);
        assert_eq!(parsed.timeseries.memtable_budget_bytes, 64 * 1024 * 1024);
        assert_eq!(
            parsed.timeseries.memtable_hard_limit_bytes,
            80 * 1024 * 1024
        );
        assert_eq!(parsed.timeseries.max_tag_cardinality, 100_000);
        assert_eq!(parsed.kv.default_capacity, 16_384);
        assert_eq!(parsed.kv.rehash_load_factor, 0.75);
        assert_eq!(parsed.kv.expiry_reap_budget, 1024);
        assert_eq!(parsed.bridge.slab_page_size, 64 * 1024);
        assert_eq!(parsed.network.default_deadline_secs, 30);
        assert_eq!(parsed.wal.write_buffer_size, 2 * 1024 * 1024);
        assert_eq!(parsed.cluster_transport.raft_tick_interval_ms, 10);
        // New ClusterTransportTuning fields.
        assert_eq!(
            parsed.cluster_transport.broadcast_threshold_bytes,
            8 * 1024 * 1024
        );
        assert_eq!(parsed.cluster_transport.ghost_sweep_interval_secs, 1800);
        assert_eq!(parsed.cluster_transport.health_ping_interval_secs, 5);
        assert_eq!(parsed.cluster_transport.health_failure_threshold, 3);
        // New QueryTuning fields.
        assert_eq!(parsed.query.doc_cache_entries, 4096);
        assert_eq!(parsed.query.columnar_flush_threshold, 65_536);
        assert_eq!(parsed.query.compaction_target_bytes, 256 * 1024 * 1024);
        assert_eq!(parsed.query.aggregate_chunk_size, 10_000);
        // New MemoryTuning fields.
        assert_eq!(parsed.memory.overflow_initial_bytes, 64 * 1024 * 1024);
        assert_eq!(parsed.memory.overflow_max_bytes, 1024 * 1024 * 1024);
        assert_eq!(parsed.memory.doc_cache_entries, 4096);
        assert_eq!(parsed.shutdown.deadline_ms, 900);
        assert_eq!(parsed.maintenance.auto_analyze_min_mutations, 1000);
    }

    #[test]
    fn maintenance_section_overrides_the_analyze_floor() {
        let toml_str = r#"
[maintenance]
auto_analyze_min_mutations = 20
"#;
        let cfg: TuningConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(cfg.maintenance.auto_analyze_min_mutations, 20);
        assert_eq!(cfg.query.sort_run_size, 100_000);
    }

    #[test]
    fn partial_override() {
        let toml_str = r#"
[query]
sort_run_size = 50000

[network]
default_deadline_secs = 60
"#;
        let cfg: TuningConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(cfg.query.sort_run_size, 50_000);
        assert_eq!(cfg.network.default_deadline_secs, 60);
        assert_eq!(cfg.query.aggregate_scan_cap, 10_000_000);
        assert_eq!(cfg.vector.seal_threshold, 65_536);
        // Unset fields retain defaults.
        assert_eq!(cfg.query.columnar_flush_threshold, 65_536);
        assert_eq!(cfg.query.compaction_target_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.query.aggregate_chunk_size, 10_000);
    }

    #[test]
    fn empty_toml_yields_defaults() {
        let cfg: TuningConfig = toml::from_str("").expect("deserialize");
        assert_eq!(cfg.data_plane.max_consecutive_panics, 3);
        assert_eq!(cfg.graph.max_depth, 10);
        // Memory tuning defaults.
        assert_eq!(cfg.memory.overflow_initial_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.memory.overflow_max_bytes, 1024 * 1024 * 1024);
        assert_eq!(cfg.memory.doc_cache_entries, 4096);
        // Cluster transport new defaults.
        assert_eq!(
            cfg.cluster_transport.broadcast_threshold_bytes,
            8 * 1024 * 1024
        );
        assert_eq!(cfg.cluster_transport.ghost_sweep_interval_secs, 1800);
        assert_eq!(cfg.cluster_transport.health_ping_interval_secs, 5);
        assert_eq!(cfg.cluster_transport.health_failure_threshold, 3);
    }

    #[test]
    fn memory_tuning_override() {
        let toml_str = r#"
[memory]
overflow_initial_bytes = 134217728
overflow_max_bytes = 2147483648
doc_cache_entries = 8192
"#;
        let cfg: TuningConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(cfg.memory.overflow_initial_bytes, 128 * 1024 * 1024);
        assert_eq!(cfg.memory.overflow_max_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(cfg.memory.doc_cache_entries, 8192);
    }
}
