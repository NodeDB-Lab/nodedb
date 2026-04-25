//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! Write flow:
//! 1. Handler serializes write as [`ReplicatedWrite`]
//! 2. Handler proposes to Raft via [`RaftLoop::propose`]
//! 3. Handler registers a waiter in [`ProposeTracker`] keyed by (group_id, log_index)
//! 4. Raft replicates to quorum and commits
//! 5. [`DistributedApplier`] receives committed entries, queues for async execution
//! 6. Background task dispatches each write to the local Data Plane
//! 7. If a waiter exists (leader path), sends the response; otherwise just applies (follower)

/// Type alias for the Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
pub type RaftProposer =
    dyn Fn(u16, Vec<u8>) -> std::result::Result<(u64, u64), crate::Error> + Send + Sync;

fn default_pq_m() -> usize {
    crate::engine::vector::index_config::DEFAULT_PQ_M
}
fn default_ivf_cells() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_CELLS
}
fn default_ivf_nprobe() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_NPROBE
}

// ── Replicated write envelope ───────────────────────────────────────

/// A write operation serialized for Raft replication.
///
/// Mirrors the write variants of [`PhysicalPlan`] but uses only types that
/// are trivially serializable (no `Arc`, no `Instant`).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReplicatedWrite {
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
    },
    PointInsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        #[serde(default)]
        if_absent: bool,
    },
    PointDelete {
        collection: String,
        document_id: String,
    },
    PointUpdate {
        collection: String,
        document_id: String,
        updates: Vec<(String, crate::bridge::physical_plan::UpdateValue)>,
    },
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        #[serde(default)]
        field_name: String,
        /// User PK bytes (UTF-8 of the document id) when the insert
        /// originates from a PK-bearing path; `None` for headless
        /// inserts. Followers re-derive the surrogate via
        /// `assigner.assign(collection, &pk_bytes)` (or
        /// `assign_anonymous(collection)` when `None`).
        #[serde(default)]
        pk_bytes: Option<Vec<u8>>,
    },
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    },
    VectorDelete {
        collection: String,
        vector_id: u32,
    },
    SetVectorParams {
        collection: String,
        m: usize,
        ef_construction: usize,
        metric: String,
        #[serde(default)]
        index_type: String,
        #[serde(default = "default_pq_m")]
        pq_m: usize,
        #[serde(default = "default_ivf_cells")]
        ivf_cells: usize,
        #[serde(default = "default_ivf_nprobe")]
        ivf_nprobe: usize,
    },
    CrdtApply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
    },
    EdgePut {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
    },
    EdgeDelete {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
    },
    KvPut {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
    },
    KvDelete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        ttl_ms: u64,
    },
    KvExpire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
    },
    KvPersist {
        collection: String,
        key: Vec<u8>,
    },
    KvIncr {
        collection: String,
        key: Vec<u8>,
        delta: i64,
        ttl_ms: u64,
    },
    KvIncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
    },
    KvCas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
    },
    KvGetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
    },
    KvRegisterSortedIndex {
        collection: String,
        index_name: String,
        sort_columns: Vec<(String, String)>,
        key_column: String,
        window_type: String,
        window_timestamp_column: String,
        window_start_ms: u64,
        window_end_ms: u64,
    },
    KvDropSortedIndex {
        index_name: String,
    },
}

/// Metadata carried alongside the write for routing on the receiving node.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedEntry {
    pub tenant_id: u32,
    pub vshard_id: u16,
    pub write: ReplicatedWrite,
}

impl ReplicatedEntry {
    /// Serialize to bytes for Raft log entry data.
    pub fn to_bytes(&self) -> Vec<u8> {
        zerompk::to_msgpack_vec(self).expect("ReplicatedEntry serialization cannot fail")
    }

    /// Deserialize from Raft log entry data bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        zerompk::from_msgpack(data).ok()
    }
}
