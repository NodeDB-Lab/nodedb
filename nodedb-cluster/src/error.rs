// SPDX-License-Identifier: BUSL-1.1

use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClusterError>;

/// Errors specific to the Calvin sequencer and transaction-class layer.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CalvinError {
    #[error("write set is empty; a Calvin transaction must write at least one key")]
    EmptyWriteSet,

    #[error(
        "write set resolves to a single vshard ({vshard}); \
         use the single-shard fast path instead"
    )]
    SingleVshardTxn { vshard: u32 },

    /// A sequencer-layer error. See [`crate::calvin::sequencer::error::SequencerError`]
    /// for the full variant set.
    #[error("sequencer error: {0}")]
    Sequencer(#[from] crate::calvin::sequencer::error::SequencerError),
}

/// Error emitted when applying or validating a `MigrationCheckpoint` entry.
#[derive(Debug, Error)]
pub enum MigrationCheckpointError {
    #[error(
        "crc32c mismatch on migration checkpoint for {migration_id}: expected {expected:#010x} got {actual:#010x}"
    )]
    Crc32cMismatch {
        migration_id: uuid::Uuid,
        expected: u32,
        actual: u32,
    },
    #[error("codec error persisting migration checkpoint: {detail}")]
    Codec { detail: String },
    #[error("storage error persisting migration checkpoint: {detail}")]
    Storage { detail: String },
}

/// Error emitted during in-flight migration recovery at startup.
#[derive(Debug, Error)]
pub enum MigrationRecoveryError {
    #[error("compensation failed for migration {migration_id} step {step}: {detail}")]
    CompensationFailed {
        migration_id: uuid::Uuid,
        step: usize,
        detail: String,
    },
    #[error("storage error during migration recovery: {detail}")]
    Storage { detail: String },
    #[error("codec error during migration recovery: {detail}")]
    Codec { detail: String },
}

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("raft error: {0}")]
    Raft(#[from] nodedb_raft::RaftError),

    #[error("vshard {vshard_id} not mapped to any raft group")]
    VShardNotMapped { vshard_id: u32 },

    #[error("raft group {group_id} not found on this node")]
    GroupNotFound { group_id: u64 },

    #[error(
        "learner {node_id} in group {group_id} not caught up (match_index={match_index}, commit_index={commit_index}); refusing to promote"
    )]
    LearnerNotCaughtUp {
        group_id: u64,
        node_id: u64,
        match_index: u64,
        commit_index: u64,
    },

    #[error("migration in progress for vshard {vshard_id}")]
    MigrationInProgress { vshard_id: u32 },

    #[error("migration refused: estimated pause {estimated_us}µs exceeds budget {budget_us}µs")]
    MigrationPauseBudgetExceeded { estimated_us: u64, budget_us: u64 },

    #[error("node {node_id} not reachable")]
    NodeUnreachable { node_id: u64 },

    #[error("ghost stub not found: node={node_id} on shard={shard_id}")]
    GhostNotFound { node_id: String, shard_id: u32 },

    #[error("transport error: {detail}")]
    Transport { detail: String },

    /// A shard RPC did not answer within its allotted timeout.
    ///
    /// Distinct from `Transport`: a timeout means the peer may still be
    /// alive but slow (retriable after backoff), whereas `Transport` covers
    /// connection-level failures suggesting the peer is gone.
    #[error("shard {vshard_id} RPC timed out after {elapsed_ms}ms")]
    ShardTimeout { vshard_id: u32, elapsed_ms: u64 },

    /// Terminal error carried by a streaming `ExecuteStreamEnd` frame.
    ///
    /// Preserves the typed shape end-to-end so the coordinator can map a
    /// pre-row `NotLeader` / `DescriptorMismatch` back to a retryable error
    /// (retry only applies before the first row — see the coordinator's
    /// `dispatch_remote_stream`). `detail` is the `Debug` rendering for logs.
    #[error("streaming execution terminal error: {detail}")]
    StreamTerminal {
        error: Box<crate::rpc_codec::TypedClusterError>,
        detail: String,
    },

    #[error("storage error: {detail}")]
    Storage { detail: String },

    #[error("codec error: {detail}")]
    Codec { detail: String },

    #[error(
        "unsupported wire version: got {got}, this node accepts [{supported_min}..={supported_max}]"
    )]
    UnsupportedWireVersion {
        got: u8,
        supported_min: u8,
        supported_max: u8,
    },

    #[error("circuit open for node {node_id}: peer has {failures} consecutive failures")]
    CircuitOpen { node_id: u64, failures: u32 },

    #[error("raft group {group_id} disappeared while waiting for conf change commit")]
    JoinGroupDisappeared { group_id: u64 },

    #[error("conf change commit timeout on group {group_id} (waited for index {log_index})")]
    JoinCommitTimeout { group_id: u64, log_index: u64 },

    #[error("not the leader for raft group {group_id}; cannot serve a linearizable read")]
    ReadIndexNotLeader { group_id: u64 },

    #[error("leadership confirmation for raft group {group_id} timed out after {waited_ms}ms")]
    ReadIndexTimeout { group_id: u64, waited_ms: u64 },

    #[error("invalid cluster configuration: {detail}")]
    Config { detail: String },

    #[error("migration checkpoint error: {0}")]
    MigrationCheckpoint(#[from] MigrationCheckpointError),

    #[error("migration recovery error: {0}")]
    MigrationRecovery(#[from] MigrationRecoveryError),

    /// A shard RPC was routed to a node that no longer owns the target vShard.
    ///
    /// This surfaces when vShard ownership has transferred (rebalance or split
    /// cut-over) after the coordinator computed its routing plan. The coordinator
    /// must refresh its routing table and retry against the new owner.
    ///
    /// `expected_owner_node` is `Some` when the receiving shard knows who the
    /// current owner is, and `None` when it does not (e.g. during a brief
    /// transition window). Either way, the coordinator should re-derive the owner
    /// from its local routing table — `expected_owner_node` is advisory only.
    #[error(
        "vshard {vshard_id} misrouted: this node is no longer the owner\
         {}", if let Some(n) = expected_owner_node { format!("; current owner may be node {n}") } else { String::new() }
    )]
    WrongOwner {
        vshard_id: u32,
        expected_owner_node: Option<u64>,
    },

    #[error("calvin error: {0}")]
    Calvin(#[from] CalvinError),

    #[error(
        "snapshot CRC mismatch for group {group_id}: stored {stored:#010x}, computed {computed:#010x}"
    )]
    SnapshotCrcMismatch {
        group_id: u64,
        stored: u32,
        computed: u32,
    },

    #[error("snapshot offset regression for group {group_id}: expected {expected}, got {actual}")]
    SnapshotOffsetRegression {
        group_id: u64,
        expected: u64,
        actual: u64,
    },

    #[error("partial snapshot file corrupt for group {group_id}: {detail}")]
    PartialSnapshotCorrupt { group_id: u64, detail: String },

    #[error("partial snapshot cleanup failed for group {group_id}: {detail}")]
    PartialSnapshotCleanupFailed { group_id: u64, detail: String },

    #[error("snapshot apply to local state machine failed for group {group_id}: {detail}")]
    SnapshotApplyFailed { group_id: u64, detail: String },

    #[error("mirror error: {0}")]
    Mirror(#[from] crate::mirror::MirrorError),

    #[error("bsp barrier error: {0}")]
    BspBarrier(#[from] crate::distributed_graph::BspBarrierError),

    #[error("vector gather error: {0}")]
    VectorGather(#[from] crate::distributed_vector::VectorGatherError),

    #[error("spatial gather error: {0}")]
    SpatialGather(#[from] crate::distributed_spatial::SpatialGatherError),

    #[error("bm25 gather error: {0}")]
    Bm25Gather(#[from] crate::distributed_document::Bm25GatherError),

    #[error("timeseries gather error: {0}")]
    TsGather(#[from] crate::distributed_timeseries::TsGatherError),
}
