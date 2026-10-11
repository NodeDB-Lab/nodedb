// SPDX-License-Identifier: BUSL-1.1

//! `RegistryShuffleConsumer` — bridges the cluster `ShuffleConsume` trigger to
//! the node-local grace-hash join over this part's staged shuffle files (E4b).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the consume logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::ShuffleConsumer`] hook. The `RaftLoop` is built
//! `with_shuffle_consumer(Arc::new(RegistryShuffleConsumer { .. }))`.
//!
//! # What it does
//!
//! On `on_shuffle_consume`, the part-owner node:
//! 1. looks up the BUILD inbox `(shuffle_id, part, 0)` and PROBE inbox
//!    `(shuffle_id, part, 1)` from [`SharedState::shuffle_registry`]. A missing
//!    inbox means no producer ever opened that part — a typed error (never a
//!    hang); E4a producers `End` EVERY part (including zero-row) so after the
//!    producers run both inboxes exist;
//! 2. waits for BOTH inboxes to finalize (flush + sync of the staged file),
//!    bounded by `req.deadline_remaining_ms` via [`tokio::time::timeout`] — on
//!    timeout a `DeadlineExceeded` typed error, never an indefinite hang. After
//!    each finalize it checks the inbox's terminal error: a producer that
//!    reported a terminal error fails the whole part fast (no partial join);
//! 3. resolves each inbox's [`staged_path`](nodedb_cluster placeholder) (local
//!    absolute paths);
//! 4. builds a node-local `PhysicalPlan::Query(QueryOp::ShuffleJoinConsume{..})`
//!    carrying those paths + the request's join spec;
//! 5. dispatches it to THIS node's Data Plane over the local SPSC bridge and
//!    collects the FULL join `Response` rows;
//! 6. returns a [`ShuffleConsumeResponse`] with the joined rows (or a typed
//!    error — never a silent drop).
//!
//! # Plane discipline
//!
//! This runs on the part-owner's Control Plane (the Tokio transport reactor). It
//! can await the finalize `Notify` and resolve staged-file paths here. The grace
//! join itself is dispatched to the `!Send` Data Plane through the existing SPSC
//! bridge; this hook never touches storage or io_uring, and never lets the
//! data-plane handler reach back into the Control-Plane registry / Notify.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_cluster::{ShuffleConsumeRequest, ShuffleConsumeResponse, TypedClusterError};
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::bridge::envelope::{ErrorCode, Priority, Request, Status};
use crate::control::cluster::data_plane_error_wire::execution_error_to_typed;
use crate::control::local_dispatch::{
    DispatchCollectError, collect_bounded_response, reject_data_plane_error,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId};

/// Side discriminant for the BUILD (right) inbox.
const SIDE_BUILD: u8 = 0;
/// Side discriminant for the PROBE (left) inbox.
const SIDE_PROBE: u8 = 1;

/// `nodedb`-side implementation of [`nodedb_cluster::ShuffleConsumer`].
///
/// Holds the node's [`SharedState`] so it can reach the shuffle receiver
/// registry (for the staged inboxes), the SPSC dispatcher + request tracker (to
/// run the grace join on the Data Plane), and the network tuning (result-byte
/// ceiling).
pub struct RegistryShuffleConsumer {
    state: Arc<SharedState>,
}

impl RegistryShuffleConsumer {
    /// Build a consumer over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Run the consume, returning the joined rows or a typed error.
    ///
    /// Factored out of the trait method so every early-exit error path is
    /// `?`-propagated here and mapped to one terminal `ShuffleConsumeResponse` at
    /// the call boundary.
    async fn consume(&self, req: ShuffleConsumeRequest) -> Result<Vec<u8>, TypedClusterError> {
        let registry = &self.state.shuffle_registry;

        // 1. Both staged inboxes MUST exist (E4a producers End every part). A
        //    missing inbox means no producer ever opened it — fail fast rather
        //    than waiting forever on a finalize that will never come.
        let build_inbox = registry
            .get((req.shuffle_id, req.part, SIDE_BUILD))
            .ok_or_else(|| TypedClusterError::Internal {
                code: 0,
                message: format!(
                    "shuffle consume: build inbox missing for (shuffle_id={}, part={}, side=0); \
                     no producer opened this part",
                    req.shuffle_id, req.part
                ),
            })?;
        let probe_inbox = registry
            .get((req.shuffle_id, req.part, SIDE_PROBE))
            .ok_or_else(|| TypedClusterError::Internal {
                code: 0,
                message: format!(
                    "shuffle consume: probe inbox missing for (shuffle_id={}, part={}, side=1); \
                     no producer opened this part",
                    req.shuffle_id, req.part
                ),
            })?;

        // 2. Wait for BOTH sides to finalize, bounded by the request deadline.
        //    A single `timeout` wraps both awaits so the total wait — not each
        //    individually — is capped; on expiry we surface a deterministic
        //    DeadlineExceeded rather than hanging.
        let deadline_ms = req.deadline_remaining_ms.max(1);
        let hop_deadline = Instant::now() + Duration::from_millis(deadline_ms);
        let wait_both = async {
            build_inbox.wait_finalized().await;
            probe_inbox.wait_finalized().await;
        };
        if tokio::time::timeout(Duration::from_millis(deadline_ms), wait_both)
            .await
            .is_err()
        {
            return Err(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline_ms,
            });
        }

        // After finalize, a producer-reported terminal error on EITHER side means
        // that side's staged rows are incomplete — fail the whole part fast
        // instead of running a partial (wrong-result) join.
        if let Some(e) = build_inbox.take_error() {
            return Err(e);
        }
        if let Some(e) = probe_inbox.take_error() {
            return Err(e);
        }

        // 3. Resolve the local absolute staged-file paths.
        let build_path = build_inbox.staged_path().to_string_lossy().into_owned();
        let probe_path = probe_inbox.staged_path().to_string_lossy().into_owned();

        // 4. Build the node-local consume plan. The join spec comes straight from
        //    the request; `limit == u64::MAX` maps to `usize::MAX` (no explicit
        //    LIMIT → budget-bounded on the Data Plane).
        let on: Vec<(String, String)> = req
            .on
            .iter()
            .map(|p| (p.left.clone(), p.right.clone()))
            .collect();
        let limit = usize::try_from(req.limit).unwrap_or(usize::MAX);
        let plan = PhysicalPlan::Query(QueryOp::ShuffleJoinConsume {
            build_path,
            probe_path,
            on,
            join_type: req.join_type.clone(),
            limit,
            probe_qualifier: req.probe_qualifier.clone(),
            index_qualifier: req.index_qualifier.clone(),
        });

        // 5. Dispatch to THIS node's Data Plane over the local SPSC bridge and
        //    collect the full join Response rows. This is the same dispatch +
        //    bounded-collect path `LocalPlanExecutor::execute_plan` uses, but the
        //    plan is built locally (it carries node-local paths and must never be
        //    wire-encoded), so we build the `Request` directly rather than
        //    round-tripping through `plan_bytes`.
        //    The hop runs on what is left of the budget the coordinator sent.
        //    This node's default is no ceiling on it.
        let deadline = hop_deadline.saturating_duration_since(Instant::now());

        let request_id = self.state.next_request_id();
        let request = Request {
            request_id,
            tenant_id: TenantId::new(req.tenant_id),
            database_id: DatabaseId::from(req.database_id),
            vshard_id: crate::types::VShardId::new(0),
            plan,
            deadline: hop_deadline,
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId(req.trace_id),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        };

        let mut rx = self.state.tracker.register(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if let Err(e) = dispatch_result {
            return Err(TypedClusterError::Internal {
                code: 0,
                message: format!("shuffle consume dispatch failed: {e}"),
            });
        }

        let max_result_bytes = self.state.tuning.network.max_query_result_bytes as usize;
        match tokio::time::timeout(
            deadline,
            collect_bounded_response(&mut rx, max_result_bytes),
        )
        .await
        {
            Ok(Ok(resp)) => {
                if resp.status == Status::Error {
                    // Carry the shard's verdict across the hop through the one
                    // code-to-typed-error conversion the crate owns, so a shard
                    // refusing an expired task reports the deadline the hop's
                    // own timer reports. An error status with no code fails
                    // closed there as an internal verdict rather than reading
                    // as an empty join result.
                    let error = match reject_data_plane_error(&resp) {
                        Err(error) => error,
                        // `NotFound` is an empty observation elsewhere; this
                        // hop treats every error status as a failed join.
                        Ok(()) => crate::Error::DataPlane(ErrorCode::NotFound),
                    };
                    Err(execution_error_to_typed(error))
                } else {
                    // The payload is a msgpack array of join-result rows — exactly
                    // the `ShuffleConsumeResponse.rows` shape.
                    Ok(resp.payload.to_vec())
                }
            }
            Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
                self.state.tracker.cancel(&request_id);
                Err(TypedClusterError::Internal {
                    code: 0,
                    message: format!(
                        "shuffle consume join result exceeded max_query_result_bytes \
                         ({bytes} > {max_result_bytes} bytes)"
                    ),
                })
            }
            Ok(Err(DispatchCollectError::ChannelClosed)) => Err(TypedClusterError::Internal {
                code: 0,
                message: "shuffle consume response channel closed".into(),
            }),
            Err(_) => {
                self.state.tracker.cancel(&request_id);
                Err(TypedClusterError::DeadlineExceeded {
                    elapsed_ms: deadline.as_millis() as u64,
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::ShuffleConsumer for RegistryShuffleConsumer {
    async fn on_shuffle_consume(&self, req: ShuffleConsumeRequest) -> ShuffleConsumeResponse {
        match self.consume(req).await {
            Ok(rows) => ShuffleConsumeResponse { rows, error: None },
            Err(error) => ShuffleConsumeResponse {
                rows: Vec::new(),
                error: Some(error),
            },
        }
    }
}
