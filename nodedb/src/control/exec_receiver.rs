//! Local execution of incoming `ExecuteRequest` RPCs.
//!
//! When a remote node sends an `ExecuteRequest` to this node (because this
//! node is the leader for the target vShard), the [`LocalPlanExecutor`]
//! validates descriptor versions, decodes the `PhysicalPlan`, dispatches
//! it through the local SPSC bridge, and returns an `ExecuteResponse`.
//!
//! Unlike the retired SQL-string forwarding path, this path skips planning
//! entirely — the plan is already encoded by the sender.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nodedb_cluster::forward::PlanExecutor;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

use crate::bridge::envelope::{Priority, Request};
use crate::bridge::physical_plan::wire as plan_wire;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId};

/// Numeric code for `TypedClusterError::Internal` when plan bytes fail to decode.
const PLAN_DECODE_FAILED: u32 = nodedb_cluster::rpc_codec::PLAN_DECODE_FAILED;

/// Executes pre-planned `PhysicalPlan` on the local Data Plane.
pub struct LocalPlanExecutor {
    state: Arc<SharedState>,
    next_request_id: AtomicU64,
}

impl LocalPlanExecutor {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self {
            state,
            // Offset to avoid collision with direct client and forwarded request IDs.
            next_request_id: AtomicU64::new(2_000_000_000),
        }
    }

    fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }
}

impl PlanExecutor for LocalPlanExecutor {
    async fn execute_plan(&self, req: ExecuteRequest) -> ExecuteResponse {
        // ── 1. Deadline check ─────────────────────────────────────────────────
        if req.deadline_remaining_ms == 0 {
            return ExecuteResponse::err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 });
        }

        let deadline = Duration::from_millis(req.deadline_remaining_ms).min(Duration::from_secs(
            self.state.tuning.network.default_deadline_secs,
        ));

        // ── 2. Descriptor version validation ──────────────────────────────────
        //
        // For each (collection, version) pair the caller sent, look up the local
        // descriptor version from SystemCatalog. If any version differs, the
        // caller's plan was built against a stale schema — reject with a typed
        // error so they re-plan against fresh leases.
        let catalog_ref = self.state.credentials.catalog();
        if let Some(catalog) = catalog_ref.as_ref() {
            for entry in &req.descriptor_versions {
                match catalog.get_collection(req.tenant_id, &entry.collection) {
                    Ok(Some(stored)) => {
                        // Version 0 is the pre-B.1 sentinel; treat as 1 (same
                        // floor the drain gate uses).
                        let actual = if stored.descriptor_version == 0 {
                            1
                        } else {
                            stored.descriptor_version
                        };
                        if actual != entry.version {
                            return ExecuteResponse::err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: actual,
                            });
                        }
                    }
                    Ok(None) => {
                        // Collection not found locally — could be a new collection
                        // the follower saw but we haven't applied yet, or a race.
                        // Treat as DescriptorMismatch so the caller re-plans.
                        if entry.version != 0 {
                            return ExecuteResponse::err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: 0,
                            });
                        }
                    }
                    Err(e) => {
                        return ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: format!("catalog lookup failed: {e}"),
                        });
                    }
                }
            }
        }

        // ── 3. Decode the PhysicalPlan ────────────────────────────────────────
        let plan = match plan_wire::decode(&req.plan_bytes) {
            Ok(p) => p,
            Err(e) => {
                return ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: format!("plan decode failed: {e}"),
                });
            }
        };

        // ── 4. Dispatch through local SPSC bridge ─────────────────────────────
        //
        // Build a Request, register a oneshot tracker, dispatch, and await the response.
        let request_id = self.next_request_id();
        let tenant_id = crate::types::TenantId::new(req.tenant_id);

        let request = Request {
            request_id,
            tenant_id,
            // Use the first vshard_id from the plan — the sender already routed
            // this to the correct node. Use 0 as the default if the plan doesn't
            // embed vshard info directly; the Data Plane ignores it for local exec.
            vshard_id: crate::types::VShardId::new(0),
            plan,
            deadline: Instant::now() + deadline,
            priority: Priority::Normal,
            trace_id: req.trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        };

        let rx = self.state.tracker.register_oneshot(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            return ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: format!("dispatch failed: {e}"),
            });
        }

        // ── 5. Collect response payloads ──────────────────────────────────────
        match tokio::time::timeout(deadline, rx).await {
            Ok(Ok(resp)) => {
                if resp.status == crate::bridge::envelope::Status::Error {
                    let msg = resp
                        .error_code
                        .as_ref()
                        .map(|c| format!("{c:?}"))
                        .unwrap_or_else(|| "unknown error".into());
                    ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: msg,
                    })
                } else {
                    ExecuteResponse::ok(vec![resp.payload.to_vec()])
                }
            }
            Ok(Err(_)) => ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "response channel closed".into(),
            }),
            Err(_) => ExecuteResponse::err(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline.as_millis() as u64,
            }),
        }
    }
}
