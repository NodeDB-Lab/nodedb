// SPDX-License-Identifier: BUSL-1.1

//! Wire types carried by the execute RPC family.
//!
//! Field order and enum variant order are the wire ABI: append only.

use nodedb_types::id::{TxnId, VShardId};

use crate::rpc_codec::data_plane_error::DataPlaneErrorCode;

/// A single (collection, version) entry sent by the caller to let the receiver
/// validate descriptor freshness before executing the plan.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DescriptorVersionEntry {
    pub collection: String,
    pub version: u64,
}

/// Send an already-planned `PhysicalPlan` to a remote node for execution.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteRequest {
    /// zerompk-encoded PhysicalPlan (via nodedb::bridge::physical_plan::wire::encode).
    pub plan_bytes: Vec<u8>,
    /// Tenant ID authenticated on the originating node; trusted on the receiver.
    pub tenant_id: u64,
    /// Database scope authenticated on the originating node; trusted on the receiver.
    /// `0` maps to `DatabaseId::DEFAULT` (the built-in `default` database).
    pub database_id: u64,
    /// Milliseconds remaining until the caller's deadline.
    /// 0 means the deadline has already expired — receiver returns DeadlineExceeded.
    pub deadline_remaining_ms: u64,
    /// Distributed trace ID for observability (16-byte W3C-compatible TraceId).
    pub trace_id: [u8; 16],
    /// Caller's view of descriptor versions for every collection touched by the plan.
    pub descriptor_versions: Vec<DescriptorVersionEntry>,
    /// Transaction context for the plan, when this leg executes inside a session
    /// transaction (e.g. a multi-node graph-MATCH leg). `None` for the common
    /// non-transactional dispatch. Lets the receiver resolve the per-transaction
    /// staging overlay for the id on the remote node.
    pub txn_id: Option<TxnId>,
    /// The vShard whose owning core runs a vShard-scoped plan (a transaction
    /// meta-op such as `StageWrite` or `ResolveTxn`). The receiver sends such a
    /// plan to that one core. `None` for every other plan, which fans across
    /// all local cores. Every vShard id is valid, so absence is `None`, never
    /// a sentinel id.
    pub vshard_id: Option<VShardId>,
    /// Raft groups a linearizable read leg observes. The receiver takes a read
    /// index for each and applies through it before it reads. Empty for a
    /// write or a read that accepts this replica as it is.
    pub read_groups: Vec<u64>,
}

/// Response to an `ExecuteRequest`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteResponse {
    pub success: bool,
    /// Raw Data Plane response payloads, one per result set.
    pub payloads: Vec<Vec<u8>>,
    pub error: Option<TypedClusterError>,
    /// Max read watermark LSN observed by the executing node's cores; 0 for
    /// writes/errors. Mirrors [`ExecuteStreamChunk::watermark_lsn`]: raw `u64`
    /// on the wire, converted to `Lsn` at the coordinator via `Lsn::new`.
    pub watermark_lsn: u64,
    /// The read versions the executing node's cores observed, one per vShard
    /// read. A write reports the versions it recorded. Empty for errors other
    /// than a read that found no row. The
    /// comparand for cross-shard OCC read validation, distinct from the
    /// core-global `watermark_lsn`.
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

/// Typed error returned by the remote executor.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum TypedClusterError {
    NotLeader {
        group_id: u64,
        leader_node_id: Option<u64>,
        leader_addr: Option<String>,
        term: u64,
    },
    DescriptorMismatch {
        collection: String,
        expected_version: u64,
        actual_version: u64,
    },
    DeadlineExceeded {
        elapsed_ms: u64,
    },
    /// Catch-all. `code` is a `nodedb_types::error::ErrorCode` as u32.
    Internal {
        code: u32,
        message: String,
    },
    /// Verbatim Data-Plane verdict from the executing shard, so the
    /// coordinator renders the same SQLSTATE local execution renders.
    /// Appended last: variant order is the wire ABI.
    DataPlane {
        code: DataPlaneErrorCode,
    },
    /// A Control-Plane constraint refusal (`crate::Error::RejectedConstraint`
    /// on the executing node), carried verbatim so the coordinator renders
    /// the same SQLSTATE (23502 vs 23505) local execution would. Without
    /// this, `constraint` collapsed into `Internal`'s bare numeric code and
    /// a NOT NULL refusal on a remote shard read back as unique_violation.
    RejectedConstraint {
        collection: String,
        constraint: String,
        detail: String,
    },
    /// A Calvin transaction the sequencer aborted, with the verdict's reason.
    /// The coordinator rebuilds the error a local submit returns, so a routed
    /// abort keeps its SQLSTATE and message. Appended last: variant order is
    /// the wire ABI.
    CalvinAborted {
        reason: crate::calvin::AbortReason,
    },
}

/// One streamed chunk of an `ExecuteStreamRequest` result.
///
/// Mirrors a `RowBatch` on the coordinator side: `payload` is a standalone
/// msgpack array of row elements (the exact bytes the Data Plane produced for a
/// single scan frame); `watermark_lsn` is that frame's read watermark. A
/// streaming response is a sequence of these followed by exactly one
/// [`ExecuteStreamEnd`].
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteStreamChunk {
    pub payload: Vec<u8>,
    pub watermark_lsn: u64,
}

/// Terminal frame of an `ExecuteStreamRequest` result.
///
/// `error: None` is a clean EOF (all chunks delivered). `error: Some(e)` is a
/// terminal failure — any chunks already delivered are valid, but the result is
/// incomplete and the consumer must surface the error.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteStreamEnd {
    pub error: Option<TypedClusterError>,
}

impl ExecuteResponse {
    pub fn ok(
        payloads: Vec<Vec<u8>>,
        watermark_lsn: u64,
        read_versions: Vec<nodedb_types::ShardVersion>,
    ) -> Self {
        Self {
            success: true,
            payloads,
            error: None,
            watermark_lsn,
            read_versions,
        }
    }
    /// A refusal that still observed the versions a read validates: a read
    /// that found no row.
    pub fn refused_with_versions(
        error: TypedClusterError,
        watermark_lsn: u64,
        read_versions: Vec<nodedb_types::ShardVersion>,
    ) -> Self {
        Self {
            success: false,
            payloads: vec![],
            error: Some(error),
            watermark_lsn,
            read_versions,
        }
    }

    pub fn err(error: TypedClusterError) -> Self {
        Self {
            success: false,
            payloads: vec![],
            error: Some(error),
            watermark_lsn: 0,
            read_versions: Vec::new(),
        }
    }
}

/// Numeric code for `TypedClusterError::Internal` when plan bytes fail to decode.
pub const PLAN_DECODE_FAILED: u32 = 0x_CE00_0001;
