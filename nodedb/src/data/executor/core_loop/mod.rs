use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;

use nodedb_bridge::buffer::{Consumer, Producer};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;
use crate::engine::sparse::doc_cache::DocCache;
use crate::engine::sparse::inverted::InvertedIndex;
use crate::engine::vector::collection::VectorCollection;
use crate::types::{Lsn, TenantId};

use super::task::ExecutionTask;

pub(in crate::data::executor) mod deferred;
mod event_emit;
mod maintenance;
mod response;
#[cfg(test)]
pub(crate) mod tests;
mod tick;

/// Per-core event loop for the Data Plane.
///
/// Each CPU core runs one `CoreLoop`. It owns:
/// - SPSC consumer for incoming requests from the Control Plane
/// - SPSC producer for outgoing responses to the Control Plane
/// - Per-core `SparseEngine` (redb) for point lookups and range scans
/// - Per-tenant `TenantCrdtEngine` instances (lazy-initialized)
/// - Task queue for pending execution
///
/// This type is intentionally `!Send` — pinned to a single core.
pub struct CoreLoop {
    pub(in crate::data::executor) core_id: usize,

    /// SPSC channel: receives requests from Control Plane.
    pub(in crate::data::executor) request_rx: Consumer<BridgeRequest>,

    /// SPSC channel: sends responses to Control Plane.
    pub(crate) response_tx: Producer<BridgeResponse>,

    /// Pending tasks ordered by priority then arrival.
    pub(crate) task_queue: VecDeque<ExecutionTask>,

    /// Current watermark LSN for this core's shard data.
    pub(crate) watermark: Lsn,

    /// redb-backed sparse/metadata engine for this core.
    pub(crate) sparse: SparseEngine,

    /// Per-tenant CRDT engines, lazily initialized on first access.
    pub(in crate::data::executor) crdt_engines: HashMap<TenantId, TenantCrdtEngine>,

    /// Per-collection vector collections, lazily initialized on first insert.
    pub(in crate::data::executor) vector_collections: HashMap<String, VectorCollection>,

    /// Background HNSW builder: send requests.
    pub(in crate::data::executor) build_tx: Option<crate::engine::vector::builder::BuildSender>,
    /// Background HNSW builder: receive completed builds.
    pub(in crate::data::executor) build_rx:
        Option<crate::engine::vector::builder::CompleteReceiver>,

    /// Per-collection HNSW parameters set via DDL. If a collection has no
    /// entry here, `HnswParams::default()` is used on first insert.
    pub(in crate::data::executor) vector_params:
        HashMap<String, crate::engine::vector::hnsw::HnswParams>,

    /// redb-backed graph edge storage for this core.
    pub(in crate::data::executor) edge_store: EdgeStore,

    /// In-memory CSR adjacency index, rebuilt from edge_store on startup.
    pub(in crate::data::executor) csr: CsrIndex,

    /// Full-text inverted index (BM25), shares redb with sparse engine.
    pub(in crate::data::executor) inverted: InvertedIndex,

    /// Per-collection spatial R-tree indexes, keyed by "{tid}:{collection}:{field}".
    /// Lazily initialized when a spatial query or geometry insert first targets a field.
    pub(in crate::data::executor) spatial_indexes:
        std::collections::HashMap<String, nodedb_spatial::RTree>,

    /// Reverse map from R-tree entry ID → document ID, keyed by (index_key, entry_id).
    /// Needed because RTreeEntry only stores a u64 hash, not the original doc_id.
    pub(in crate::data::executor) spatial_doc_map: std::collections::HashMap<(String, u64), String>,

    /// Base data directory for this core (used for sort spill temp files).
    pub(in crate::data::executor) data_dir: std::path::PathBuf,

    /// vShards that are paused for write operations (during Phase 3 migration cutover).
    pub(in crate::data::executor) paused_vshards: std::collections::HashSet<crate::types::VShardId>,

    /// Nodes that have been explicitly deleted via PointDelete cascade.
    /// Used for edge referential integrity — EdgePut to a deleted node is rejected
    /// with `RejectedDanglingEdge`. Cleared periodically or on compaction.
    pub(in crate::data::executor) deleted_nodes: std::collections::HashSet<String>,

    /// Idempotency key deduplication: maps processed idempotency keys to
    /// whether they succeeded (true) or failed (false). Uses `VecDeque`
    /// for FIFO eviction order alongside `HashMap` for O(1) lookup.
    /// Bounded to 16,384 entries.
    pub(in crate::data::executor) idempotency_cache: HashMap<u64, bool>,
    /// FIFO order of idempotency keys for correct eviction (oldest first).
    pub(in crate::data::executor) idempotency_order: std::collections::VecDeque<u64>,

    /// Column statistics store for CBO. Shares redb with sparse engine.
    /// Updated incrementally on PointPut. Read by DataFusion optimizer.
    pub(in crate::data::executor) stats_store: crate::engine::sparse::stats::StatsStore,

    /// Incremental aggregate cache: maps `(collection, group_key, agg_op)` →
    /// partial aggregate state. Updated on writes (PointPut increments counts/sums),
    /// cleared on schema change. Turns O(N) full-scan aggregates into O(1) cache
    /// lookups for repeated dashboard/analytics queries.
    ///
    /// Key: `"{collection}\0{group_by_fields}\0{agg_ops}"`.
    /// Value: cached result rows as JSON.
    pub(in crate::data::executor) aggregate_cache: HashMap<String, Vec<u8>>,

    /// Last time periodic maintenance (compaction, edge sweep) was run.
    pub(in crate::data::executor) last_maintenance: Option<std::time::Instant>,

    /// Per-collection full index config (includes index_type, PQ params, IVF params).
    /// Stored alongside vector_params for collections that use non-default index types.
    pub(in crate::data::executor) index_configs:
        HashMap<String, crate::engine::vector::index_config::IndexConfig>,

    /// IVF-PQ indexes for collections configured with `index_type = "ivf_pq"`.
    pub(in crate::data::executor) ivf_indexes:
        HashMap<String, crate::engine::vector::ivf::IvfPqIndex>,

    /// Compaction interval (how often `maybe_run_maintenance` triggers).
    pub(in crate::data::executor) compaction_interval: std::time::Duration,

    /// Tombstone ratio threshold for auto-compaction (0.0–1.0).
    pub(in crate::data::executor) compaction_tombstone_threshold: f64,

    /// Per-core LRU document cache for O(1) hot-key point lookups.
    /// Invalidated write-through on PointPut/Delete/Update.
    pub(in crate::data::executor) doc_cache: DocCache,

    /// Per-collection columnar timeseries memtables (!Send, per-core owned).
    pub(in crate::data::executor) columnar_memtables:
        HashMap<String, crate::engine::timeseries::columnar_memtable::ColumnarMemtable>,

    /// Per-collection max WAL LSN that has been ingested into the memtable.
    /// Used by the WAL catch-up deduplication: if a catch-up record's LSN
    /// is <= this value, the Data Plane skips it (already ingested).
    pub(in crate::data::executor) ts_max_ingested_lsn: HashMap<String, u64>,

    /// Last time any timeseries ingest was processed on this core.
    /// Used by idle flush: if no ingest for 5 seconds, `maybe_run_maintenance`
    /// flushes all non-empty memtables to disk partitions.
    pub(in crate::data::executor) last_ts_ingest: Option<std::time::Instant>,

    /// Per-collection timeseries partition registries for this core.
    pub(in crate::data::executor) ts_registries:
        HashMap<String, crate::engine::timeseries::partition_registry::PartitionRegistry>,

    /// Continuous aggregate manager for this core. Fires on memtable flush.
    pub(in crate::data::executor) continuous_agg_mgr:
        crate::engine::timeseries::continuous_agg::ContinuousAggregateManager,

    /// Checkpoint coordinator: incremental dirty page flushing across engines.
    /// Replaces timer-based checkpoint with I/O-budget-aware progressive flush.
    pub(in crate::data::executor) checkpoint_coordinator:
        crate::storage::checkpoint::CheckpointCoordinator,

    /// L1 segment compaction config for the storage layer.
    pub(in crate::data::executor) segment_compaction_config:
        crate::storage::compaction::CompactionConfig,

    /// Per-collection document index configurations.
    /// Maps "{tenant_id}:{collection}" → CollectionConfig.
    /// Populated via RegisterDocumentCollection plans.
    pub(in crate::data::executor) doc_configs:
        HashMap<String, crate::engine::document::store::CollectionConfig>,

    /// Query execution tuning parameters (sort run size, stream chunk size, etc.).
    /// Set at core spawn time from config; never changed at runtime.
    pub(in crate::data::executor) query_tuning: nodedb_types::config::tuning::QueryTuning,

    /// Graph engine tuning parameters (max_visited, max_depth, LCC thresholds).
    /// Set at core spawn time from config; never changed at runtime.
    pub(in crate::data::executor) graph_tuning: nodedb_types::config::tuning::GraphTuning,

    /// Per-core KV engine: hash tables + expiry wheel. `!Send`.
    pub(in crate::data::executor) kv_engine: crate::engine::kv::KvEngine,

    /// Shared system metrics — Arc is safe for `!Send` since all fields are atomic.
    pub(in crate::data::executor) metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,

    /// Event bus producer: emits WriteEvents to the Event Plane.
    /// One per core, `!Send` once pinned. `None` if Event Plane is disabled.
    pub(in crate::data::executor) event_producer: Option<crate::event::bus::EventProducer>,

    /// Monotonic sequence counter for events emitted by this core.
    /// Incremented on every successful event emission.
    pub(in crate::data::executor) event_sequence: u64,
}

impl CoreLoop {
    /// Create a core loop with its SPSC channel endpoints and engine storage.
    ///
    /// `data_dir` is the base data directory; each core gets its own redb file
    /// at `{data_dir}/sparse/core-{core_id}.redb`.
    pub fn open(
        core_id: usize,
        request_rx: Consumer<BridgeRequest>,
        response_tx: Producer<BridgeResponse>,
        data_dir: &Path,
    ) -> crate::Result<Self> {
        let sparse_path = data_dir.join(format!("sparse/core-{core_id}.redb"));
        let sparse = SparseEngine::open(&sparse_path)?;

        let graph_path = data_dir.join(format!("graph/core-{core_id}.redb"));
        let edge_store = EdgeStore::open(&graph_path)?;
        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&edge_store)?;

        // Inverted index shares the sparse engine's redb database.
        let inverted = InvertedIndex::open(sparse.db().clone())?;

        // Column statistics store shares the sparse engine's redb database.
        let stats_store = crate::engine::sparse::stats::StatsStore::open(sparse.db().clone())?;

        Ok(Self {
            core_id,
            request_rx,
            response_tx,
            task_queue: VecDeque::with_capacity(256),
            watermark: Lsn::ZERO,
            sparse,
            crdt_engines: HashMap::new(),
            vector_collections: HashMap::new(),
            build_tx: None,
            build_rx: None,
            vector_params: HashMap::new(),
            edge_store,
            csr,
            inverted,
            data_dir: data_dir.to_path_buf(),
            paused_vshards: std::collections::HashSet::new(),
            deleted_nodes: std::collections::HashSet::new(),
            idempotency_cache: HashMap::new(),
            idempotency_order: std::collections::VecDeque::new(),
            stats_store,
            aggregate_cache: HashMap::new(),
            last_maintenance: None,
            compaction_interval: std::time::Duration::from_secs(600),
            compaction_tombstone_threshold: 0.2,
            index_configs: HashMap::new(),
            ivf_indexes: HashMap::new(),
            doc_cache: DocCache::new(
                nodedb_types::config::tuning::QueryTuning::default().doc_cache_entries,
            ),
            columnar_memtables: HashMap::new(),
            ts_max_ingested_lsn: HashMap::new(),
            last_ts_ingest: None,
            ts_registries: HashMap::new(),
            continuous_agg_mgr:
                crate::engine::timeseries::continuous_agg::ContinuousAggregateManager::new(),
            checkpoint_coordinator: {
                let mut coord = crate::storage::checkpoint::CheckpointCoordinator::new(
                    crate::storage::checkpoint::CheckpointConfig::default(),
                );
                coord.register_engine("sparse");
                coord.register_engine("vector");
                coord.register_engine("crdt");
                coord.register_engine("timeseries");
                coord
            },
            segment_compaction_config: crate::storage::compaction::CompactionConfig::default(),
            spatial_indexes: std::collections::HashMap::new(),
            spatial_doc_map: std::collections::HashMap::new(),
            doc_configs: HashMap::new(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            kv_engine: crate::engine::kv::KvEngine::from_tuning(
                crate::engine::kv::current_ms(),
                &nodedb_types::config::tuning::KvTuning::default(),
            ),
            metrics: None,
            event_producer: None,
            event_sequence: 0,
        })
    }

    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Set the last timeseries ingest timestamp (for testing idle flush).
    pub fn set_last_ts_ingest(&mut self, value: Option<std::time::Instant>) {
        self.last_ts_ingest = value;
    }

    pub fn pending_count(&self) -> usize {
        self.task_queue.len()
    }

    pub fn advance_watermark(&mut self, lsn: Lsn) {
        self.watermark = lsn;
    }
}
