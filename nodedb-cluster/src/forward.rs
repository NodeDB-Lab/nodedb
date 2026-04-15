//! Physical-plan execution trait for leader-based request routing.
//!
//! [`PlanExecutor`]: the physical-plan execution path introduced in C-β.
//! The legacy [`RequestForwarder`] SQL-string path was deleted in C-δ.6.

use crate::rpc_codec::{ExecuteRequest, ExecuteResponse};

// ── Physical-plan execution (C-β) ────────────────────────────────────────────

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
}

/// No-op executor for single-node mode or testing.
pub struct NoopPlanExecutor;

impl PlanExecutor for NoopPlanExecutor {
    async fn execute_plan(&self, _req: ExecuteRequest) -> ExecuteResponse {
        use crate::rpc_codec::TypedClusterError;
        ExecuteResponse::err(TypedClusterError::Internal {
            code: 0,
            message: "plan execution not available (single-node mode)".into(),
        })
    }
}
