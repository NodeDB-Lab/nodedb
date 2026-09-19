// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for an in-transaction `ClusterArrayOp::{Put,
//! Delete}`.
//!
//! The routing wrapper has no Data-Plane handler. It fans out into one
//! `ArrayOp::{Put, Delete}` per owning vShard (`txn_expand`), and every
//! per-shard task is staged into that shard's per-transaction overlay via
//! `MetaOp::StageWrite`, on that shard's own leader. The statement answers
//! with the summed affected count, and a same-transaction `Slice` / `Agg`
//! carrying the transaction id sees the staged cells on every shard.
//!
//! Every per-shard task is buffered BEFORE any shard is staged. The buffer
//! records each shard's vShard (`SessionStore::buffer_write` →
//! `tx_vshards`), and ROLLBACK's overlay teardown (`lifecycle::run_rollback`
//! over `txn_identity`) drops the overlay on every recorded vShard. A shard
//! that fails mid-fan-out therefore leaves the transaction with every shard
//! on record, so the shards that staged before the error are still torn down.

use std::future::Future;

use futures::future::join_all;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::gateway::RouteDecision;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_staged_write};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_task::PhysicalTask;

use super::connection::SessionId;
use super::leader_forward::{forward_to_leader, resolve_leader};
use super::staging_gate::{StagedWriteOutcome, StagingGateError, wrap_stage_write};
use super::store::SessionStore;
use super::txn_expand::expand_cluster_array_write;

/// One shard's share of the fan-out, ready to stage: the vShard, the leader
/// it resolved to, and the per-shard `ArrayOp` plan to wrap.
struct ShardStage {
    vshard_id: VShardId,
    decision: RouteDecision,
    inner: PhysicalPlan,
}

/// Stage an in-transaction `ClusterArrayOp::{Put, Delete}` into every owning
/// shard's per-transaction overlay and answer with the summed count.
///
/// `dispatch` runs once per shard whose leader is this node, with a
/// `MetaOp::StageWrite` task wrapping that shard's `ArrayOp`; remote leaders
/// go through `forward_to_leader`. The shard dispatches run concurrently. Any
/// shard error fails the statement with that error: the transaction's normal
/// error semantics apply, and ROLLBACK tears down every recorded shard.
///
/// Caller invariant: `task.plan` is a `ClusterArrayOp::Put` or `Delete`
/// (`route_in_tx_write` branches on exactly that shape).
pub(super) async fn stage_cluster_array_write<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    task: PhysicalTask,
    dispatch: F,
) -> Result<StagedWriteOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    let (kind, shard_tasks) = expand_cluster_array_write(&task)
        .map_err(StagingGateError::Dispatch)?
        .ok_or_else(|| {
            StagingGateError::Dispatch(crate::Error::Internal {
                detail: "cluster array fan-out staging called on a plan that is not a \
                         ClusterArrayOp::Put or ClusterArrayOp::Delete"
                    .to_owned(),
            })
        })?;

    // Quota admission once for the whole statement, on the original plan,
    // before any shard touches its overlay. Mirrors `stage_write`.
    let identity = sessions.identity(session_id);
    let scope = identity.as_ref().map(|identity| {
        RequestAuthScope::for_database(identity, state.auth_stores(), task.database_id)
    });
    if state.metering_config.enabled
        && let Some(scope) = &scope
    {
        let info = PlanMeteringInfo::extract(&task.plan);
        admit_quota_for_dispatch(state, scope, &info).map_err(StagingGateError::Dispatch)?;
    }

    // Resolve each shard's leader and buffer its task BEFORE staging any
    // shard, so ROLLBACK has every vShard on record whatever happens below.
    let mut stages = Vec::with_capacity(shard_tasks.len());
    for shard_task in shard_tasks {
        stages.push(ShardStage {
            vshard_id: shard_task.vshard_id,
            decision: resolve_leader(&shard_task, state),
            inner: shard_task.plan.clone(),
        });
        sessions.buffer_write(session_id, shard_task);
    }

    let txn_id = sessions.tx_id(session_id);
    let tenant_id = task.tenant_id;
    let database_id = task.database_id;
    let responses = join_all(stages.into_iter().map(|stage| {
        let dispatch = &dispatch;
        async move {
            let ShardStage {
                vshard_id,
                decision,
                inner,
            } = stage;
            let wrap = |plan: PhysicalPlan| {
                wrap_stage_write(tenant_id, vshard_id, database_id, txn_id, plan)
            };
            match decision {
                RouteDecision::Local => dispatch(wrap(inner)).await,
                // The leader's OCC check reads the descriptor version set of
                // the INNER write, so the un-wrapped plan rides alongside.
                remote => {
                    let version_plan = inner.clone();
                    forward_to_leader(state, remote, wrap(inner), &version_plan).await
                }
            }
        }
    }))
    .await;

    let mut affected = 0usize;
    let mut first_ok: Option<Response> = None;
    for resp in responses {
        let resp = resp.map_err(StagingGateError::Dispatch)?;
        if resp.status == Status::Error {
            return Err(StagingGateError::Rejected {
                code: resp.error_code.as_deref().cloned(),
            });
        }
        affected += require_affected_count(resp.payload.as_ref())
            .map_err(StagingGateError::Dispatch)? as usize;
        if first_ok.is_none() {
            first_ok = Some(resp);
        }
    }

    // Metered once for the whole statement, on the original plan, after every
    // shard staged. Mirrors `stage_write`: the overlay work is real engine
    // work done now, and COMMIT skips the buffered per-shard `ArrayOp` tasks
    // (they pass `is_stageable_write`), so nothing is billed twice.
    if let (Some(scope), Some(resp)) = (&scope, &first_ok) {
        meter_staged_write(state, scope, &task.plan, resp);
    }

    Ok(StagedWriteOutcome {
        kind,
        affected,
        payload: Vec::new(),
    })
}
