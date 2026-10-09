// SPDX-License-Identifier: BUSL-1.1

//! `RegistryShuffleAggregator` — bridges the cluster `ShuffleAggregateConsume`
//! trigger to the node-local partial-state merge + finalize over this part's
//! single staged side (E5b).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the aggregate
//! consume logic lives here and is exposed to the transport via the
//! [`nodedb_cluster::ShuffleAggregator`] hook. The `RaftLoop` is built
//! `with_shuffle_aggregator(Arc::new(RegistryShuffleAggregator { .. }))`.
//!
//! # What it does
//!
//! This is the SINGLE-SIDED aggregate sibling of [`RegistryShuffleConsumer`].
//! On `on_shuffle_aggregate`, the part-owner node:
//! 1. looks up the ONE producer inbox `(shuffle_id, part, 0)` from
//!    [`SharedState::shuffle_registry`]. A missing inbox means no producer ever
//!    opened that part — a typed error (never a hang); the producers `End` EVERY
//!    part (including zero-row) so after the producers run the inbox exists.
//!    Unlike the join consumer there is NO probe side — only side 0 is waited on;
//! 2. waits for that single inbox to finalize (flush + sync of the staged file),
//!    bounded by `req.deadline_remaining_ms` via [`tokio::time::timeout`] — on
//!    timeout a `DeadlineExceeded` typed error, never an indefinite hang. After
//!    finalize it checks the inbox's terminal error: a producer that reported a
//!    terminal error fails the whole part fast (no partial merge);
//! 3. resolves the inbox's `staged_path` (local absolute path);
//! 4. builds a node-local
//!    `PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume{..})` carrying that
//!    path + the request's aggregate spec;
//! 5. dispatches it to THIS node's Data Plane over the local SPSC bridge and
//!    collects the FULL aggregate `Response` rows;
//! 6. returns a [`ShuffleAggregateConsumeResponse`] with the aggregate rows (or a
//!    typed error — never a silent drop).
//!
//! # Plane discipline
//!
//! This runs on the part-owner's Control Plane (the Tokio transport reactor). It
//! can await the finalize `Notify` and resolve the staged-file path here. The
//! merge + finalize itself is dispatched to the `!Send` Data Plane through the
//! existing SPSC bridge; this hook never touches storage or io_uring, and never
//! lets the data-plane handler reach back into the Control-Plane registry /
//! Notify.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_cluster::{
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, TypedClusterError,
};
use nodedb_physical::physical_plan::{AggregateSpec, PhysicalPlan, QueryOp};

use crate::bridge::envelope::{ErrorCode, Priority, Request, Status};
use crate::control::cluster::data_plane_error_wire::execution_error_to_typed;
use crate::control::local_dispatch::{
    DispatchCollectError, collect_bounded_response, reject_data_plane_error,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId};

/// Side discriminant for the single producer inbox of an aggregate shuffle.
/// The aggregate consumer is SINGLE-SIDED — there is no probe side.
const SIDE_PRODUCER: u8 = 0;

/// `nodedb`-side implementation of [`nodedb_cluster::ShuffleAggregator`].
///
/// SINGLE-SIDED aggregate sibling of [`RegistryShuffleConsumer`]. Holds the
/// node's [`SharedState`] so it can reach the shuffle receiver registry (for the
/// staged inbox), the SPSC dispatcher + request tracker (to run the merge +
/// finalize on the Data Plane), and the network tuning (result-byte ceiling).
pub struct RegistryShuffleAggregator {
    state: Arc<SharedState>,
}

impl RegistryShuffleAggregator {
    /// Build an aggregator over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Run the aggregate consume, returning the finalized rows or a typed error.
    ///
    /// Factored out of the trait method so every early-exit error path is
    /// `?`-propagated here and mapped to one terminal
    /// `ShuffleAggregateConsumeResponse` at the call boundary.
    async fn aggregate(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> Result<Vec<u8>, TypedClusterError> {
        let registry = &self.state.shuffle_registry;

        // 1. The single producer inbox MUST exist (producers End every part). A
        //    missing inbox means no producer ever opened it — fail fast rather
        //    than waiting forever on a finalize that will never come.
        let inbox = registry
            .get((req.shuffle_id, req.part, SIDE_PRODUCER))
            .ok_or_else(|| TypedClusterError::Internal {
                code: 0,
                message: format!(
                    "shuffle aggregate: producer inbox missing for (shuffle_id={}, part={}, \
                     side=0); no producer opened this part",
                    req.shuffle_id, req.part
                ),
            })?;

        // 2. Wait for the single side to finalize, bounded by the request
        //    deadline; on expiry surface a deterministic DeadlineExceeded rather
        //    than hanging.
        let deadline_ms = req.deadline_remaining_ms.max(1);
        let hop_deadline = Instant::now() + Duration::from_millis(deadline_ms);
        if tokio::time::timeout(Duration::from_millis(deadline_ms), inbox.wait_finalized())
            .await
            .is_err()
        {
            return Err(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline_ms,
            });
        }

        // After finalize, a producer-reported terminal error means the staged
        // rows are incomplete — fail the whole part fast instead of merging a
        // partial (wrong-result) state.
        if let Some(e) = inbox.take_error() {
            return Err(e);
        }

        // 3. Resolve the local absolute staged-file path.
        let state_path = inbox.staged_path().to_string_lossy().into_owned();

        // 4. Decode the opaque aggregate spec and map the sort keys into the
        //    owned shape the node-local plan expects.
        let aggregates: Vec<AggregateSpec> =
            zerompk::from_msgpack(&req.aggregates_bytes).map_err(|e| {
                TypedClusterError::Internal {
                    code: 0,
                    message: format!("shuffle aggregate: decode aggregate specs failed: {e}"),
                }
            })?;
        let sort_keys: Vec<nodedb_physical::physical_plan::SortKeySpec> = req
            .sort_keys
            .iter()
            .map(|k| {
                nodedb_physical::physical_plan::SortKeySpec::column(k.column.clone(), k.ascending)
            })
            .collect();
        let limit = usize::try_from(req.limit).unwrap_or(usize::MAX);

        // 5. Build the node-local consume plan. It carries the node-local staged
        //    path and must never be wire-encoded.
        let plan = PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume {
            state_path,
            group_by: req.group_by.clone(),
            aggregates,
            having: req.having.clone(),
            limit,
            sort_keys,
        });

        // 6. Dispatch to THIS node's Data Plane over the local SPSC bridge and
        //    collect the full aggregate Response rows. Same dispatch +
        //    bounded-collect path the join consume hook uses; the plan is built
        //    locally (node-local path, never wire-encoded), so we build the
        //    `Request` directly rather than round-tripping through `plan_bytes`.
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
                message: format!("shuffle aggregate dispatch failed: {e}"),
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
                    // as an empty aggregate result.
                    let error = match reject_data_plane_error(&resp) {
                        Err(error) => error,
                        // `NotFound` is an empty observation elsewhere; this
                        // hop treats every error status as a failed merge.
                        Ok(()) => crate::Error::DataPlane(ErrorCode::NotFound),
                    };
                    Err(execution_error_to_typed(error))
                } else {
                    // The payload is a msgpack array of finalized aggregate rows —
                    // exactly the `ShuffleAggregateConsumeResponse.rows` shape.
                    Ok(resp.payload.to_vec())
                }
            }
            Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
                self.state.tracker.cancel(&request_id);
                Err(TypedClusterError::Internal {
                    code: 0,
                    message: format!(
                        "shuffle aggregate result exceeded max_query_result_bytes \
                         ({bytes} > {max_result_bytes} bytes)"
                    ),
                })
            }
            Ok(Err(DispatchCollectError::ChannelClosed)) => Err(TypedClusterError::Internal {
                code: 0,
                message: "shuffle aggregate response channel closed".into(),
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
impl nodedb_cluster::ShuffleAggregator for RegistryShuffleAggregator {
    async fn on_shuffle_aggregate(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> ShuffleAggregateConsumeResponse {
        match self.aggregate(req).await {
            Ok(rows) => ShuffleAggregateConsumeResponse { rows, error: None },
            Err(error) => ShuffleAggregateConsumeResponse {
                rows: Vec::new(),
                error: Some(error),
            },
        }
    }
}
