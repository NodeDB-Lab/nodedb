// SPDX-License-Identifier: BUSL-1.1

//! Set operations over a clone.
//!
//! A clone read resolves one collection per task, and a `SetOp` reads one
//! relation per branch. Each branch that reads a live clone is read through
//! the clone on its own and replaced by its rows. The set operation then
//! merges the rows of every branch on the coordinator, so `UNION`,
//! `INTERSECT` and `EXCEPT` judge complete branch results.

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::envelope::describe_row_context;
use crate::control::local_dispatch::reject_data_plane_error;
use crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn;
use crate::control::server::exchange::ReadScope;
use crate::control::server::exchange::gather::stream_to_response;
use crate::control::server::exchange::resolve::exchange::provider_scan_of_rows;
use crate::control::server::exchange::resolve::{Resolved, resolve_and_materialize};
use crate::control::server::payload_merge::encode_msgpack_array;
use crate::control::server::shared::authorization::{authorize_task_set, gathered_sources};
use crate::control::server::shared::plan_util::extract_collection;
use crate::data::executor::response_codec::flatten_rows_in_context;
use crate::types::TraceId;

use super::entry::{
    CloneReadAnswer, CloneReadInterceptParams, CloneReadOutcome, maybe_intercept_clone_read,
};
use super::merge::wrap_single_map_as_array;

/// Read a task whose set operation has a branch over a live clone.
///
/// Returns `None` when no set operation in the plan reads a live clone. The
/// caller then resolves the task as one read. Otherwise the whole task is
/// authorized, each clone branch is read through the clone and replaced by
/// its rows, and the rewritten plan resolves under the task's read scope.
pub(super) async fn read_set_op_through_clone(
    params: &CloneReadInterceptParams<'_>,
) -> crate::Result<Option<CloneReadAnswer>> {
    if !reaches_set_op(&params.task.plan) {
        return Ok(None);
    }
    // The task's requirements name every branch's collection, so no branch
    // reads before the whole set passes.
    let _authorized = authorize_task_set(
        params.identity,
        std::slice::from_ref(params.task),
        params.permissions,
        params.roles,
        params.emitter,
    )
    .map_err(crate::Error::from)?;
    let mut plan = params.task.plan.clone();
    if !resolve_set_op_branches(params, &mut plan).await? {
        return Ok(None);
    }
    let scope = ReadScope {
        database_id: params.task.database_id,
        tenant_id: params.task.tenant_id,
        trace_id: TraceId::ZERO,
        txn_id: params.task.txn_id,
        linearizable: params.linearizable,
    };
    let answer = match resolve_and_materialize(params.state, params.identity, plan, scope).await? {
        Resolved::Gathered(response, _watermarks, distributed_reads) => CloneReadAnswer {
            response,
            distributed_reads,
        },
        Resolved::Plan(plan) => {
            let response = dispatch_to_data_plane_with_txn(
                params.state,
                params.task.tenant_id,
                params.task.database_id,
                params.task.vshard_id,
                *plan,
                TraceId::ZERO,
                params.task.txn_id,
            )
            .await?;
            CloneReadAnswer {
                response,
                distributed_reads: Vec::new(),
            }
        }
        Resolved::Stream(stream) => CloneReadAnswer {
            response: stream_to_response(stream).await?,
            distributed_reads: Vec::new(),
        },
    };
    reject_data_plane_error(&answer.response)?;
    Ok(Some(answer))
}

/// Whether `plan` is a set operation, or wraps one in the `Exchange` and
/// `PostProcess` nodes the converter puts over a body.
fn reaches_set_op(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Query(QueryOp::SetOp { .. }) => true,
        PhysicalPlan::Query(QueryOp::Exchange(exchange)) => reaches_set_op(&exchange.child),
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => reaches_set_op(input),
        _ => false,
    }
}

/// Replace every set-operation branch in `plan` that reads a live clone with
/// its rows. Returns whether any branch was replaced.
async fn resolve_set_op_branches(
    params: &CloneReadInterceptParams<'_>,
    plan: &mut PhysicalPlan,
) -> crate::Result<bool> {
    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(exchange)) => {
            Box::pin(resolve_set_op_branches(params, exchange.child.as_mut())).await
        }
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => {
            Box::pin(resolve_set_op_branches(params, input.as_mut())).await
        }
        PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => {
            let mut replaced = false;
            for branch in inputs.iter_mut() {
                if reaches_set_op(branch) {
                    replaced |= Box::pin(resolve_set_op_branches(params, branch)).await?;
                } else if let Some(rows) = read_branch(params, branch).await? {
                    *branch = rows;
                    replaced = true;
                }
            }
            Ok(replaced)
        }
        _ => Ok(false),
    }
}

/// The rows of one branch read through the clone, as a `ProviderScan` that
/// carries the branch's requirements. `None` when the branch reads no live
/// clone.
///
/// The future is boxed as a trait object: it calls back into
/// [`maybe_intercept_clone_read`], which reaches this function again.
fn read_branch<'a>(
    params: &'a CloneReadInterceptParams<'a>,
    branch: &'a PhysicalPlan,
) -> Pin<Box<dyn Future<Output = crate::Result<Option<PhysicalPlan>>> + Send + 'a>> {
    Box::pin(async move {
        let database_id = params.task.database_id;
        let Some(collection) = extract_collection(branch) else {
            return Ok(None);
        };
        let vshard_id =
            nodedb_types::CollectionKey::from_relation_name(database_id, collection)?.vshard();
        let task = PhysicalTask {
            tenant_id: params.task.tenant_id,
            vshard_id,
            database_id,
            plan: branch.clone(),
            post_set_op: PostSetOp::None,
            txn_id: params.task.txn_id,
        };
        let outcome = maybe_intercept_clone_read(CloneReadInterceptParams {
            state: params.state,
            task: &task,
            identity: params.identity,
            tenant_id: params.tenant_id,
            permissions: params.permissions,
            roles: params.roles,
            emitter: params.emitter,
            linearizable: params.linearizable,
        })
        .await?;
        let CloneReadOutcome::Handled(CloneReadAnswer { response, .. }) = outcome else {
            return Ok(None);
        };
        // A point read answers one row, and a read that predates the clone
        // answers no payload.
        let payload = wrap_single_map_as_array(response.payload.as_ref().to_vec());
        let payload = if payload.is_empty() {
            encode_msgpack_array(&[])
        } else {
            payload
        };
        let rows = flatten_rows_in_context(&payload, describe_row_context(branch))?;
        let sources = gathered_sources(branch, database_id)?;
        Ok(Some(provider_scan_of_rows(rows, sources)))
    })
}
