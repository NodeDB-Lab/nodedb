// SPDX-License-Identifier: BUSL-1.1

//! The shard error wire enum. Variant order is the wire ABI: append only.

use super::raft::RaftErrorWire;
use crate::rpc_codec::data_plane_error::DataPlaneErrorCode;
use crate::rpc_codec::execute::TypedClusterError;

/// A `ClusterError` carried across a node hop.
///
/// One variant per `ClusterError` variant whose fields cross the wire. The
/// coordinator-side error families (gather, barrier, mirror, Calvin,
/// migration) cross as [`Self::Untyped`] with their message.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum ShardErrorWire {
    Raft {
        error: RaftErrorWire,
    },
    VShardNotMapped {
        vshard_id: u32,
    },
    GroupNotFound {
        group_id: u64,
    },
    LearnerNotCaughtUp {
        group_id: u64,
        node_id: u64,
        match_index: u64,
        commit_index: u64,
    },
    MigrationInProgress {
        vshard_id: u32,
    },
    MigrationPauseBudgetExceeded {
        estimated_us: u64,
        budget_us: u64,
    },
    NodeUnreachable {
        node_id: u64,
    },
    GhostNotFound {
        node_id: String,
        shard_id: u32,
    },
    Transport {
        detail: String,
    },
    ShardTimeout {
        vshard_id: u32,
        elapsed_ms: u64,
    },
    StreamTerminal {
        error: TypedClusterError,
        detail: String,
    },
    Storage {
        detail: String,
    },
    DataPlane {
        code: DataPlaneErrorCode,
    },
    Codec {
        detail: String,
    },
    UnsupportedWireVersion {
        got: u8,
        supported_min: u8,
        supported_max: u8,
    },
    CircuitOpen {
        node_id: u64,
        failures: u32,
    },
    JoinGroupDisappeared {
        group_id: u64,
    },
    JoinCommitTimeout {
        group_id: u64,
        log_index: u64,
    },
    ReadIndexNotLeader {
        group_id: u64,
    },
    ReadIndexTimeout {
        group_id: u64,
        waited_ms: u64,
    },
    Config {
        detail: String,
    },
    WrongOwner {
        vshard_id: u32,
        expected_owner_node: Option<u64>,
    },
    SnapshotCrcMismatch {
        group_id: u64,
        stored: u32,
        computed: u32,
    },
    SnapshotOffsetRegression {
        group_id: u64,
        expected: u64,
        actual: u64,
    },
    PartialSnapshotCorrupt {
        group_id: u64,
        detail: String,
    },
    PartialSnapshotCleanupFailed {
        group_id: u64,
        detail: String,
    },
    SnapshotApplyFailed {
        group_id: u64,
        detail: String,
    },
    /// An error whose type has no wire mirror. `detail` is its message.
    Untyped {
        detail: String,
    },
    /// A shard's classified local-execution error, in its typed wire form.
    ShardExecution {
        error: TypedClusterError,
        detail: String,
    },
    Unanswered {
        node_id: u64,
        detail: String,
    },
}
