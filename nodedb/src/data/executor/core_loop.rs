use std::collections::{HashMap, VecDeque};
use std::path::Path;

use tracing::warn;

use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;
use crate::engine::sparse::inverted::InvertedIndex;
use crate::engine::vector::hnsw::HnswIndex;
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

    /// Per-collection HNSW vector indexes, lazily initialized on first insert.
    pub(in crate::data::executor) vector_indexes: HashMap<String, HnswIndex>,

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

    /// Base data directory for this core (used for sort spill temp files).
    pub(in crate::data::executor) data_dir: std::path::PathBuf,

    /// vShards that are paused for write operations (during Phase 3 migration cutover).
    pub(in crate::data::executor) paused_vshards: std::collections::HashSet<crate::types::VShardId>,

    /// Nodes that have been explicitly deleted via PointDelete cascade.
    /// Used for edge referential integrity — EdgePut to a deleted node is rejected
    /// with `RejectedDanglingEdge`. Cleared periodically or on compaction.
    pub(in crate::data::executor) deleted_nodes: std::collections::HashSet<String>,

    /// Idempotency key deduplication map: maps processed idempotency keys to
    /// whether they succeeded (true) or failed (false). Bounded to prevent
    /// unbounded memory growth — oldest entries evicted when capacity exceeded.
    pub(in crate::data::executor) idempotency_cache: HashMap<u64, bool>,
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
        let csr = CsrIndex::rebuild_from(&edge_store)?;

        // Inverted index shares the sparse engine's redb database.
        let inverted = InvertedIndex::open(sparse.db().clone())?;

        Ok(Self {
            core_id,
            request_rx,
            response_tx,
            task_queue: VecDeque::with_capacity(256),
            watermark: Lsn::ZERO,
            sparse,
            crdt_engines: HashMap::new(),
            vector_indexes: HashMap::new(),
            vector_params: HashMap::new(),
            edge_store,
            csr,
            inverted,
            data_dir: data_dir.to_path_buf(),
            paused_vshards: std::collections::HashSet::new(),
            deleted_nodes: std::collections::HashSet::new(),
            idempotency_cache: HashMap::new(),
        })
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

    /// Drain incoming requests from the SPSC bridge into the task queue.
    pub fn drain_requests(&mut self) {
        let mut batch = Vec::new();
        self.request_rx.drain_into(&mut batch, 64);
        for br in batch {
            self.task_queue.push_back(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task and send the response back via SPSC.
    ///
    /// Returns `true` if a task was processed, `false` if the queue was empty.
    pub fn poll_one(&mut self) -> bool {
        let Some(mut task) = self.task_queue.pop_front() else {
            return false;
        };

        // Idempotency key deduplication: if this request carries a key
        // that was already processed, return a cached Ok/Error without
        // re-executing. Prevents duplicate writes from retries.
        if let Some(key) = task.request.idempotency_key {
            if let Some(&succeeded) = self.idempotency_cache.get(&key) {
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

        // Record idempotency key result for future dedup.
        if let Some(key) = task.request.idempotency_key {
            let succeeded = response.status == Status::Ok;
            // Evict oldest entries if cache grows too large (16K entries).
            if self.idempotency_cache.len() >= 16_384 {
                if let Some(&first_key) = self.idempotency_cache.keys().next() {
                    self.idempotency_cache.remove(&first_key);
                }
            }
            self.idempotency_cache.insert(key, succeeded);
        }

        // Cap deleted_nodes to prevent unbounded memory growth.
        // Old entries are safe to evict — cascade delete already cleaned edges.
        if self.deleted_nodes.len() > 100_000 {
            self.deleted_nodes.clear();
        }

        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(
                core = self.core_id,
                error = %e,
                "failed to send response — response queue full"
            );
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    ///
    /// Returns the number of tasks processed.
    pub fn tick(&mut self) -> usize {
        self.drain_requests();
        let mut processed = 0;
        while self.poll_one() {
            processed += 1;
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

    pub(in crate::data::executor) fn response_error(
        &self,
        task: &ExecutionTask,
        error_code: ErrorCode,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: Some(error_code),
        }
    }

    /// Build a tenant-scoped vector index key.
    ///
    /// When `field_name` is non-empty, the key incorporates it to support
    /// multiple named vector fields per collection. An empty `field_name`
    /// produces the backward-compatible `{tenant_id}:{collection}` key.
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

    /// Get or create a CRDT engine for the given tenant.
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

    /// Write HNSW checkpoints for all vector indexes to disk.
    ///
    /// Called periodically from the TPC event loop (e.g., every 5 minutes
    /// or when idle). Each index is serialized to a file at
    /// `{data_dir}/vector-ckpt/{index_key}.ckpt`.
    ///
    /// After checkpointing, WAL replay only needs to process entries
    /// since the checkpoint — not the entire history.
    pub fn checkpoint_vector_indexes(&self) -> usize {
        if self.vector_indexes.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create vector checkpoint dir"
            );
            return 0;
        }

        let mut checkpointed = 0;
        for (key, index) in &self.vector_indexes {
            if index.is_empty() {
                continue;
            }
            let bytes = index.checkpoint_to_bytes();
            if bytes.is_empty() {
                continue;
            }
            // Write to temp file, then rename for atomicity.
            let ckpt_path = ckpt_dir.join(format!("{key}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{key}.ckpt.tmp"));
            if std::fs::write(&tmp_path, &bytes).is_ok()
                && std::fs::rename(&tmp_path, &ckpt_path).is_ok()
            {
                checkpointed += 1;
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.vector_indexes.len(),
                "vector indexes checkpointed"
            );
        }
        checkpointed
    }

    /// Load HNSW checkpoints from disk on startup, before WAL replay.
    ///
    /// For each checkpoint file, loads the index. WAL replay then only
    /// needs to process entries after the checkpoint LSN.
    pub fn load_vector_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if !ckpt_dir.exists() {
            return;
        }

        let entries = match std::fs::read_dir(&ckpt_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            let key = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if key.is_empty() {
                continue;
            }

            if let Ok(bytes) = std::fs::read(&path) {
                if let Some(index) = crate::engine::vector::hnsw::HnswIndex::from_checkpoint(&bytes)
                {
                    tracing::info!(
                        core = self.core_id,
                        %key,
                        vectors = index.len(),
                        "loaded vector checkpoint"
                    );
                    self.vector_indexes.insert(key, index);
                    loaded += 1;
                }
            }
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "vector checkpoints loaded");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
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
                    ..make_request(PhysicalPlan::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                    })
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
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                }),
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
                    ..make_request(PhysicalPlan::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                    })
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
                    ..make_request(PhysicalPlan::Cancel {
                        target_request_id: RequestId::new(10),
                    })
                },
            })
            .unwrap();
        assert_eq!(core.tick(), 2);
    }
}
