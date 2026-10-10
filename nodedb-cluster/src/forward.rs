// SPDX-License-Identifier: BUSL-1.1

//! Physical-plan execution trait for leader-based request routing.
//!
//! [`PlanExecutor`]: the physical-plan execution path introduced in C-β.
//! The legacy [`RequestForwarder`] SQL-string path was deleted in C-δ.6.

use crate::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

// ── Physical-plan execution (C-β) ────────────────────────────────────────────

/// Sink for streamed result chunks driven by [`PlanExecutor::execute_plan_streaming`].
///
/// The transport server provides a `ChunkSink` whose `send_chunk` writes one
/// `RPC_EXECUTE_STREAM_CHUNK` envelope to the QUIC stream and awaits the write
/// inline, so QUIC flow control back-pressures the producer (the local stream
/// pull) when the coordinator falls behind.
///
/// `send_chunk` returning `Err` means the client / coordinator is gone (the
/// stream was reset or finished early): the producer MUST stop pulling and
/// return without writing a terminal frame.
///
/// `read_versions` are that frame's read versions, one per vShard it read,
/// distinct from the core-global `watermark_lsn`. The shuffle fan-out sink
/// folds them so the producer can report what its scan observed for
/// cross-shard OCC validation. The plain `ExecuteStream` sink carries only
/// `watermark_lsn` on the wire and ignores them.
pub trait ChunkSink: Send {
    fn send_chunk(
        &mut self,
        payload: Vec<u8>,
        watermark_lsn: u64,
        read_versions: Vec<nodedb_types::ShardVersion>,
    ) -> impl std::future::Future<Output = crate::error::Result<()>> + Send;
}

/// Trait for executing a pre-planned `PhysicalPlan` on the local Data Plane.
///
/// Implemented in `nodedb/src/control/exec_receiver.rs` by `LocalPlanExecutor`.
/// The cluster RPC handler calls this when it receives an `ExecuteRequest`.
///
/// Responsibilities:
/// 1. Validate that `deadline_remaining_ms > 0`.
/// 2. For each `DescriptorVersionEntry`, verify the local descriptor version matches.
/// 3. Decode `plan_bytes` via `nodedb::bridge::physical_plan::wire::decode`.
/// 4. Dispatch through the local SPSC bridge.
/// 5. Collect response payloads.
/// 6. Map errors to `TypedClusterError`.
pub trait PlanExecutor: Send + Sync + 'static {
    fn execute_plan(
        &self,
        req: ExecuteRequest,
    ) -> impl std::future::Future<Output = ExecuteResponse> + Send;

    /// Streaming sibling of [`execute_plan`](Self::execute_plan).
    ///
    /// Executes the plan and pushes each result frame to `sink` as it arrives.
    /// Returns `None` on a clean EOF (all chunks delivered), or `Some(err)` on a
    /// terminal failure — validation rejection, decode failure, deadline, or an
    /// error pulled from the underlying result stream. A `send_chunk` error
    /// (coordinator gone) is NOT a terminal error: the producer stops and
    /// returns `None`, since there is no live peer to receive a terminal frame.
    fn execute_plan_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> impl std::future::Future<Output = Option<TypedClusterError>> + Send;
}

/// No-op executor for single-node mode or testing.
pub struct NoopPlanExecutor;

impl PlanExecutor for NoopPlanExecutor {
    async fn execute_plan(&self, _req: ExecuteRequest) -> ExecuteResponse {
        ExecuteResponse::err(TypedClusterError::Internal {
            code: 0,
            message: "plan execution not available (single-node mode)".into(),
        })
    }

    async fn execute_plan_streaming(
        &self,
        _req: ExecuteRequest,
        _sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        Some(TypedClusterError::Internal {
            code: 0,
            message: "streaming plan execution not available (single-node mode)".into(),
        })
    }
}
