mod data_plane;
mod engines;
mod memory;
mod network;

pub use data_plane::{DataPlaneTuning, QueryTuning};
pub use engines::{
    DEFAULT_MAX_DEPTH, DEFAULT_MAX_VISITED, GraphTuning, KvTuning, SparseTuning, TimeseriesToning,
    VectorTuning,
};
pub use memory::MemoryTuning;
pub use network::{BridgeTuning, ClusterTransportTuning, NetworkTuning, WalTuning};

use serde::{Deserialize, Serialize};

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
        assert_eq!(parsed.query.sort_run_size, 100_000);
        assert_eq!(parsed.vector.flat_index_threshold, 10_000);
        assert_eq!(parsed.sparse.bm25_k1, 1.2);
        assert_eq!(parsed.graph.max_visited, 100_000);
        assert_eq!(parsed.timeseries.memtable_budget_bytes, 64 * 1024 * 1024);
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
        // New MemoryTuning fields.
        assert_eq!(parsed.memory.overflow_initial_bytes, 64 * 1024 * 1024);
        assert_eq!(parsed.memory.overflow_max_bytes, 1024 * 1024 * 1024);
        assert_eq!(parsed.memory.doc_cache_entries, 4096);
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
