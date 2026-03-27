//! Per-engine tuning: Vector, Sparse, Graph, Timeseries.

use serde::{Deserialize, Serialize};

/// Vector engine tuning (HNSW, PQ, IVF).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorTuning {
    #[serde(default = "default_flat_index_threshold")]
    pub flat_index_threshold: usize,
    #[serde(default = "default_seal_threshold")]
    pub seal_threshold: usize,
    #[serde(default = "default_pq_m")]
    pub default_pq_m: usize,
    #[serde(default = "default_ivf_cells")]
    pub default_ivf_cells: usize,
    #[serde(default = "default_ivf_nprobe")]
    pub default_ivf_nprobe: usize,
}

impl Default for VectorTuning {
    fn default() -> Self {
        Self {
            flat_index_threshold: default_flat_index_threshold(),
            seal_threshold: default_seal_threshold(),
            default_pq_m: default_pq_m(),
            default_ivf_cells: default_ivf_cells(),
            default_ivf_nprobe: default_ivf_nprobe(),
        }
    }
}

fn default_flat_index_threshold() -> usize {
    10_000
}
fn default_seal_threshold() -> usize {
    65_536
}
fn default_pq_m() -> usize {
    8
}
fn default_ivf_cells() -> usize {
    256
}
fn default_ivf_nprobe() -> usize {
    16
}

/// Sparse/metadata engine tuning (BM25, GSI, HyperLogLog).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseTuning {
    #[serde(default = "default_bm25_k1")]
    pub bm25_k1: f32,
    #[serde(default = "default_bm25_b")]
    pub bm25_b: f32,
    #[serde(default = "default_max_gsis_per_collection")]
    pub max_gsis_per_collection: usize,
    #[serde(default = "default_hll_m")]
    pub hll_registers: usize,
    #[serde(default = "default_hll_p")]
    pub hll_precision: u32,
}

impl Default for SparseTuning {
    fn default() -> Self {
        Self {
            bm25_k1: default_bm25_k1(),
            bm25_b: default_bm25_b(),
            max_gsis_per_collection: default_max_gsis_per_collection(),
            hll_registers: default_hll_m(),
            hll_precision: default_hll_p(),
        }
    }
}

fn default_bm25_k1() -> f32 {
    1.2
}
fn default_bm25_b() -> f32 {
    0.75
}
fn default_max_gsis_per_collection() -> usize {
    4
}
fn default_hll_m() -> usize {
    256
}
fn default_hll_p() -> u32 {
    8
}

/// Default cap on visited nodes during BFS traversals.
/// Prevents supernode fan-out explosion from consuming unbounded memory.
pub const DEFAULT_MAX_VISITED: usize = 100_000;

/// Default maximum BFS traversal depth.
pub const DEFAULT_MAX_DEPTH: usize = 10;

/// Graph engine tuning (traversal limits, LCC algorithm).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphTuning {
    #[serde(default = "default_max_visited")]
    pub max_visited: usize,
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
    #[serde(default = "default_lcc_high_degree_threshold")]
    pub lcc_high_degree_threshold: usize,
    #[serde(default = "default_lcc_sample_pairs")]
    pub lcc_sample_pairs: usize,
}

impl Default for GraphTuning {
    fn default() -> Self {
        Self {
            max_visited: default_max_visited(),
            max_depth: default_max_depth(),
            lcc_high_degree_threshold: default_lcc_high_degree_threshold(),
            lcc_sample_pairs: default_lcc_sample_pairs(),
        }
    }
}

fn default_max_visited() -> usize {
    DEFAULT_MAX_VISITED
}
fn default_max_depth() -> usize {
    DEFAULT_MAX_DEPTH
}
fn default_lcc_high_degree_threshold() -> usize {
    2_000
}
fn default_lcc_sample_pairs() -> usize {
    10_000
}

/// Timeseries engine tuning (memtable budgets, block sizes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesToning {
    #[serde(default = "default_memtable_budget_bytes")]
    pub memtable_budget_bytes: usize,
    #[serde(default = "default_total_budget_bytes")]
    pub total_budget_bytes: usize,
    #[serde(default = "default_ts_block_size")]
    pub block_size: usize,
}

impl Default for TimeseriesToning {
    fn default() -> Self {
        Self {
            memtable_budget_bytes: default_memtable_budget_bytes(),
            total_budget_bytes: default_total_budget_bytes(),
            block_size: default_ts_block_size(),
        }
    }
}

fn default_memtable_budget_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_total_budget_bytes() -> usize {
    100 * 1024 * 1024
}
fn default_ts_block_size() -> usize {
    1024
}

/// KV engine tuning (hash table, expiry wheel, slab allocator).
///
/// Controls the per-core hash table parameters, incremental rehash behavior,
/// expiry wheel tick rate, and the per-tick reap budget that prevents reactor
/// stalls during mass-expiry events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTuning {
    /// Default initial hash table capacity per collection (number of slots).
    /// Should be a power of two. Larger values reduce early rehash churn for
    /// collections that will grow quickly. Default: 16384.
    #[serde(default = "default_kv_capacity")]
    pub default_capacity: usize,

    /// Hash table load factor threshold that triggers incremental rehash.
    /// When `entries / capacity > rehash_load_factor`, the table begins
    /// doubling. Range: 0.5–0.9. Default: 0.75 (standard Robin Hood threshold).
    #[serde(default = "default_kv_rehash_load_factor")]
    pub rehash_load_factor: f32,

    /// Number of entries rehashed per PUT during incremental rehash.
    /// Higher values complete rehash faster but add per-PUT latency.
    /// Default: 8.
    #[serde(default = "default_kv_rehash_batch_size")]
    pub rehash_batch_size: usize,

    /// Default inline value threshold in bytes. Values at or below this size
    /// are stored directly in the hash entry (no pointer chase). Larger values
    /// overflow to slab-allocated Binary Tuples. Default: 64.
    #[serde(default = "default_kv_inline_threshold")]
    pub default_inline_threshold: usize,

    /// Maximum expirations processed per reactor tick (event loop iteration).
    /// Prevents mass-expiry events (e.g., 10M keys with identical TTL) from
    /// stalling the TPC core. Expired-but-not-yet-reaped keys are invisible
    /// to GET (lazy fallback). Default: 1024.
    #[serde(default = "default_kv_expiry_reap_budget")]
    pub expiry_reap_budget: usize,

    /// Expiry wheel tick interval in milliseconds. Determines the granularity
    /// of TTL expiration. Lower = more precise but more CPU overhead.
    /// Default: 1000 (1 second).
    #[serde(default = "default_kv_expiry_tick_ms")]
    pub expiry_tick_ms: u64,
}

impl Default for KvTuning {
    fn default() -> Self {
        Self {
            default_capacity: default_kv_capacity(),
            rehash_load_factor: default_kv_rehash_load_factor(),
            rehash_batch_size: default_kv_rehash_batch_size(),
            default_inline_threshold: default_kv_inline_threshold(),
            expiry_reap_budget: default_kv_expiry_reap_budget(),
            expiry_tick_ms: default_kv_expiry_tick_ms(),
        }
    }
}

fn default_kv_capacity() -> usize {
    16_384
}
fn default_kv_rehash_load_factor() -> f32 {
    0.75
}
fn default_kv_rehash_batch_size() -> usize {
    8
}
fn default_kv_inline_threshold() -> usize {
    64
}
fn default_kv_expiry_reap_budget() -> usize {
    1024
}
fn default_kv_expiry_tick_ms() -> u64 {
    1000
}
