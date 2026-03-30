use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;

use tracing::warn;

use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;
use crate::engine::sparse::doc_cache::DocCache;
use crate::engine::sparse::inverted::InvertedIndex;
use crate::engine::vector::collection::VectorCollection;
use crate::types::{Lsn, TenantId};

use super::task::{ExecutionTask, TaskState};

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

    /// Set compaction parameters (called after open, before event loop).
    pub fn set_compaction_config(
        &mut self,
        interval: std::time::Duration,
        tombstone_threshold: f64,
    ) {
        self.compaction_interval = interval;
        self.compaction_tombstone_threshold = tombstone_threshold;
    }

    /// Set shared system metrics reference (called after open, before event loop).
    pub fn set_metrics(&mut self, metrics: Arc<crate::control::metrics::SystemMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Set checkpoint coordinator config (called after open, before event loop).
    pub fn set_checkpoint_config(&mut self, config: crate::storage::checkpoint::CheckpointConfig) {
        self.checkpoint_coordinator =
            crate::storage::checkpoint::CheckpointCoordinator::new(config);
        self.checkpoint_coordinator.register_engine("sparse");
        self.checkpoint_coordinator.register_engine("vector");
        self.checkpoint_coordinator.register_engine("crdt");
        self.checkpoint_coordinator.register_engine("timeseries");
    }

    /// Set L1 segment compaction config.
    pub fn set_segment_compaction_config(
        &mut self,
        config: crate::storage::compaction::CompactionConfig,
    ) {
        self.segment_compaction_config = config;
    }

    /// Set query execution tuning parameters (called after open, before event loop).
    ///
    /// Also resizes the doc cache if `doc_cache_entries` differs from the current size.
    /// Resizing clears all cached entries.
    pub fn set_query_tuning(&mut self, tuning: nodedb_types::config::tuning::QueryTuning) {
        if tuning.doc_cache_entries != self.query_tuning.doc_cache_entries {
            self.doc_cache = DocCache::new(tuning.doc_cache_entries);
        }
        self.query_tuning = tuning;
    }

    /// Set the Event Plane producer (called after open, before event loop).
    pub fn set_event_producer(&mut self, producer: crate::event::bus::EventProducer) {
        self.event_producer = Some(producer);
    }

    /// Emit a write event to the Event Plane.
    ///
    /// Called after a successful write (PointPut, PointDelete, PointUpdate,
    /// BatchInsert, BulkDelete, etc.). The Data Plane NEVER blocks here —
    /// if the ring buffer is full, the event is dropped and the Event Plane
    /// will detect the gap via sequence numbers and replay from WAL.
    pub(in crate::data::executor) fn emit_write_event(
        &mut self,
        task: &super::task::ExecutionTask,
        collection: &str,
        op: crate::event::WriteOp,
        row_id: &str,
        new_value: Option<&[u8]>,
        old_value: Option<&[u8]>,
    ) {
        let producer = match self.event_producer.as_mut() {
            Some(p) => p,
            None => return, // Event Plane not configured.
        };

        self.event_sequence += 1;

        let event = crate::event::WriteEvent {
            sequence: self.event_sequence,
            collection: Arc::from(collection),
            op,
            row_id: crate::event::types::RowId::new(row_id),
            lsn: self.watermark,
            tenant_id: task.request.tenant_id,
            vshard_id: task.request.vshard_id,
            source: crate::event::EventSource::User,
            new_value: new_value.map(Arc::from),
            old_value: old_value.map(Arc::from),
        };

        producer.emit(event);
    }

    /// Apply secondary index extraction for a document.
    ///
    /// Shared by `execute_document_batch_insert` and `execute_point_put`.
    pub(in crate::data::executor) fn apply_secondary_indexes(
        &mut self,
        tid: u32,
        collection: &str,
        doc: &serde_json::Value,
        doc_id: &str,
        index_paths: &[crate::engine::document::store::IndexPath],
    ) {
        for index_path in index_paths {
            let values = crate::engine::document::store::extract_index_values(
                doc,
                &index_path.path,
                index_path.is_array,
            );
            for v in values {
                if let Err(e) = self
                    .sparse
                    .index_put(tid, collection, &index_path.path, &v, doc_id)
                {
                    tracing::warn!(
                        core = self.core_id,
                        %collection,
                        doc_id = %doc_id,
                        path = %index_path.path,
                        error = %e,
                        "secondary index extraction failed"
                    );
                }
            }
        }
    }

    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Pause writes to a vShard (during Phase 3 migration cutover).
    pub fn pause_vshard(&mut self, vshard: crate::types::VShardId) {
        self.paused_vshards.insert(vshard);
    }

    /// Resume writes to a vShard after cutover.
    pub fn resume_vshard(&mut self, vshard: crate::types::VShardId) {
        self.paused_vshards.remove(&vshard);
    }

    /// Check if a vShard is paused for writes.
    pub fn is_vshard_paused(&self, vshard: crate::types::VShardId) -> bool {
        self.paused_vshards.contains(&vshard)
    }

    /// Sweep dangling edges: detect edges whose source or destination
    /// node has been deleted (present in `deleted_nodes`).
    ///
    /// Called periodically from the idle loop. Removes dangling edges
    /// from both the CSR and persistent edge store. Returns the number
    /// of edges removed.
    pub fn sweep_dangling_edges(&mut self) -> usize {
        if self.deleted_nodes.is_empty() {
            return 0;
        }
        let mut removed = 0;
        let deleted: Vec<String> = self.deleted_nodes.iter().cloned().collect();
        for node in &deleted {
            let edges = self.csr.remove_node_edges(node);
            if edges > 0 {
                if let Err(e) = self.edge_store.delete_edges_for_node(node) {
                    tracing::warn!(
                        core = self.core_id,
                        node = %node,
                        error = %e,
                        "sweep: failed to delete edges from store"
                    );
                }
                removed += edges;
            }
        }
        if removed > 0 {
            tracing::info!(
                core = self.core_id,
                removed,
                deleted_nodes = deleted.len(),
                "dangling edge sweep complete"
            );
        }
        removed
    }

    /// Drain incoming requests from the SPSC bridge into the task queue.
    pub fn drain_requests(&mut self) {
        let mut batch = Vec::new();
        self.request_rx.drain_into(&mut batch, 64);
        for br in batch {
            self.task_queue.push_back(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task and send the response back via SPSC.
    pub fn poll_one(&mut self) -> bool {
        let Some(mut task) = self.task_queue.pop_front() else {
            return false;
        };

        if let Some(key) = task.request.idempotency_key
            && let Some(&succeeded) = self.idempotency_cache.get(&key)
        {
            let response = if succeeded {
                self.response_ok(&task)
            } else {
                self.response_error(&task, ErrorCode::DuplicateWrite)
            };
            if let Err(e) = self
                .response_tx
                .try_push(BridgeResponse { inner: response })
            {
                warn!(core = self.core_id, error = %e, "failed to send idempotent response");
            }
            return true;
        }

        let response = if task.is_expired() {
            task.state = TaskState::Failed;
            Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(ErrorCode::DeadlineExceeded),
            }
        } else {
            task.state = TaskState::Running;
            let resp = self.execute(&task);
            task.state = TaskState::Completed;
            resp
        };

        if let Some(key) = task.request.idempotency_key {
            let succeeded = response.status == Status::Ok;
            if self.idempotency_cache.len() >= 16_384
                && let Some(oldest_key) = self.idempotency_order.pop_front()
            {
                self.idempotency_cache.remove(&oldest_key);
            }
            self.idempotency_cache.insert(key, succeeded);
            self.idempotency_order.push_back(key);
        }

        if self.deleted_nodes.len() > 100_000 {
            self.deleted_nodes.clear();
        }

        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core = self.core_id, error = %e, "failed to send response — response queue full");
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    pub fn tick(&mut self) -> usize {
        self.poll_build_completions();
        self.drain_requests();
        let mut processed = 0;
        while !self.task_queue.is_empty() {
            let batched = self.poll_write_batch();
            if batched > 0 {
                processed += batched;
                continue;
            }
            if self.poll_one() {
                processed += 1;
            } else {
                break;
            }
        }
        processed
    }

    pub(in crate::data::executor) fn response_ok(&self, task: &ExecutionTask) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    pub(in crate::data::executor) fn response_with_payload(
        &self,
        task: &ExecutionTask,
        payload: Vec<u8>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    pub(in crate::data::executor) fn response_partial(
        &self,
        task: &ExecutionTask,
        payload: Vec<u8>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(payload),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    pub(in crate::data::executor) fn response_error(
        &self,
        task: &ExecutionTask,
        error_code: impl Into<ErrorCode>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: Some(error_code.into()),
        }
    }

    pub(in crate::data::executor) fn vector_index_key(
        tenant_id: u32,
        collection: &str,
        field_name: &str,
    ) -> String {
        if field_name.is_empty() {
            format!("{tenant_id}:{collection}")
        } else {
            format!("{tenant_id}:{collection}:{field_name}")
        }
    }

    pub(in crate::data::executor) fn get_crdt_engine(
        &mut self,
        tenant_id: TenantId,
    ) -> crate::Result<&mut TenantCrdtEngine> {
        if !self.crdt_engines.contains_key(&tenant_id) {
            tracing::debug!(core = self.core_id, %tenant_id, "creating CRDT engine for tenant");
            let engine =
                TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())?;
            self.crdt_engines.insert(tenant_id, engine);
        }
        Ok(self
            .crdt_engines
            .get_mut(&tenant_id)
            .expect("just inserted"))
    }

    pub fn pending_count(&self) -> usize {
        self.task_queue.len()
    }

    pub fn advance_watermark(&mut self, lsn: Lsn) {
        self.watermark = lsn;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::bridge::physical_plan::{DocumentOp, MetaOp};
    use crate::types::*;
    use nodedb_bridge::buffer::RingBuffer;
    use std::time::{Duration, Instant};

    fn make_core() -> (CoreLoop, Producer<BridgeRequest>, Consumer<BridgeResponse>) {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(0, req_rx, resp_tx, dir.path()).unwrap();
        std::mem::forget(dir);
        (core, req_tx, resp_rx)
    }

    pub fn make_core_with_dir(
        dir: &std::path::Path,
    ) -> (CoreLoop, Producer<BridgeRequest>, Consumer<BridgeResponse>) {
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(0, req_rx, resp_tx, dir).unwrap();
        (core, req_tx, resp_rx)
    }

    fn make_request(plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        }
    }

    #[test]
    fn empty_tick_processes_nothing() {
        let (mut core, _, _) = make_core();
        assert_eq!(core.tick(), 0);
    }

    #[test]
    fn expired_task_returns_deadline_exceeded() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();
        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    deadline: Instant::now() - Duration::from_secs(1),
                    ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                        rls_filters: Vec::new(),
                    }))
                },
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(resp.inner.error_code, Some(ErrorCode::DeadlineExceeded));
    }

    #[test]
    fn watermark_in_response() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();
        core.advance_watermark(Lsn::new(99));
        core.sparse.put(1, "x", "y", b"data").unwrap();
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                    rls_filters: Vec::new(),
                })),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.watermark_lsn, Lsn::new(99));
    }

    #[test]
    fn cancel_removes_pending_task() {
        let (mut core, mut req_tx, _resp_rx) = make_core();
        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(10),
                    deadline: Instant::now() + Duration::from_secs(60),
                    ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                        rls_filters: Vec::new(),
                    }))
                },
            })
            .unwrap();
        core.drain_requests();
        assert_eq!(core.pending_count(), 1);

        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(99),
                    priority: Priority::Critical,
                    consistency: ReadConsistency::Eventual,
                    ..make_request(PhysicalPlan::Meta(MetaOp::Cancel {
                        target_request_id: RequestId::new(10),
                    }))
                },
            })
            .unwrap();
        assert_eq!(core.tick(), 2);
    }
}
