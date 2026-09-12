// SPDX-License-Identifier: BUSL-1.1

//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin,
};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::types::TraceId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::response::data_plane_response_to_native;
use super::single_task::dispatch_single_task;
use super::{DispatchCtx, error_to_native, error_to_native_with_sqlstate};
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Dispatch a direct Data Plane operation by opcode.
pub(crate) async fn handle_direct_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    // CRDT Apply allocates a surrogate while planning; authorize the exact
    // collection first.
    if matches!(op, OpCode::CrdtApply) {
        let audit = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &ctx.state.audit,
        ));
        if let Err(error) = crate::control::server::shared::authorization::authorize_collection(
            ctx.identity,
            ctx.database_id(),
            &collection,
            crate::control::security::identity::Permission::Write,
            &ctx.state.permissions,
            &ctx.state.roles,
            &audit,
        ) {
            return error_to_native(seq, &crate::Error::from(error));
        }
    }

    // Per-operation cap enforcement (vector dim, top_k, batch size, etc.).
    if let Err(e) = super::limits::check_op_limits(ctx.state, fields) {
        return error_to_native_with_sqlstate(seq, "0A000", &e);
    }

    // Quota enforcement — reject before planning or dispatch.
    if let Err(e) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &e);
    }

    let mut plan = match super::plan_builder::build_plan(ctx, op, fields, &collection) {
        Ok(p) => p,
        Err(e) => return error_to_native_with_sqlstate(seq, "42601", &e),
    };

    // Apply RLS before any special Control-Plane orchestration can observe the plan.
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        ctx.database_id(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context(),
    ) {
        return error_to_native_with_sqlstate(seq, "42501", &e);
    }

    // Refuse what column redaction cannot cover (redacted-column aggregate, graph traversal).
    if let Err(e) = crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        ctx.database_id(),
        ctx.auth_context(),
        &ctx.state.redaction,
    ) {
        return error_to_native_with_sqlstate(seq, "0A000", &e);
    }

    // Extracted before `plan` moves; a no-op when metering is disabled (the default).
    let plan_metering_info = ctx
        .state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));

    // A spent hard quota refuses the op before it runs; charges below are success-path
    // only. Branches through `dispatch_single_task` are also gated there — harmless.
    if let Some(info) = &plan_metering_info
        && let Err(e) = admit_quota_for_dispatch(ctx.state, &ctx.scope, info)
    {
        return error_to_native_with_sqlstate(seq, "53400", &e);
    }

    // False for `dispatch_single_task`, which meters itself — re-metering here
    // would double-bill a `Staged` dispatch and wrongly bill a `Buffered` one.
    let mut needs_top_level_metering = true;
    // Wrapped in an async block so `return` inside each branch exits only this
    // block, letting the metering call below run exactly once regardless of branch.
    let response: NativeResponse = async {
        // `INSERT ... SELECT` orchestrates on the Control Plane; never reaches the
        // Data Plane as a single op.
        if matches!(
            &plan,
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. })
        ) {
            let task = PhysicalTask {
                tenant_id,
                vshard_id,
                database_id: ctx.database_id(),
                plan: plan.clone(),
                post_set_op: PostSetOp::None,
                txn_id: None,
            };
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &error),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result =
                crate::control::insert_select::run_authorized_insert_select(ctx.state, authorized)
                    .await;
            return match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
                Err(e) => error_to_native(seq, &e),
            };
        }

        // Autocommit `MERGE` orchestrates on the Control Plane; never reaches the
        // Data Plane as a single op.
        if matches!(
            &plan,
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Merge {
                resolved_inserts: None,
                ..
            })
        ) {
            let task = PhysicalTask {
                tenant_id,
                vshard_id,
                database_id: ctx.database_id(),
                plan: plan.clone(),
                post_set_op: PostSetOp::None,
                txn_id: None,
            };
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &error),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result =
                crate::control::merge_orchestrator::run_authorized_merge(ctx.state, authorized)
                    .await;
            return match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
                Err(e) => error_to_native(seq, &e),
            };
        }

        // Autocommit `UPDATE ... FROM <source>` scans the source on its own core and
        // ships it into the plan; never reaches the Data Plane as a single op.
        if matches!(
            &plan,
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                source_rows: None,
                ..
            })
        ) {
            let task = PhysicalTask {
                tenant_id,
                vshard_id,
                database_id: ctx.database_id(),
                plan: plan.clone(),
                post_set_op: PostSetOp::None,
                txn_id: None,
            };
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &error),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result =
                crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                    ctx.state, authorized,
                )
                .await;
            return match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
                Err(e) => error_to_native(seq, &e),
            };
        }

        // A governed predicate resolves to a concrete row set before proposing — see
        // `control::write_resolve`. Local (non-Raft) path skips this.
        if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&plan)
            && ctx.state.async_raft_proposer().is_some()
        {
            let task = PhysicalTask {
                tenant_id,
                vshard_id,
                database_id: ctx.database_id(),
                plan: plan.clone(),
                post_set_op: PostSetOp::None,
                txn_id: None,
            };
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &error),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result = crate::control::write_resolve::run_authorized_write_resolve(
                ctx.state, authorized, resolver,
            )
            .await;
            return match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
                Err(e) => error_to_native(seq, &e),
            };
        }

        // Stamp the connection's active txn id so the Data Plane resolves the staging
        // overlay for read-your-own-writes; `None` outside a transaction block.
        let txn_id = ctx.sessions.tx_id(ctx.peer_addr);

        // Implicit graph-edge extraction: a `_from`/`_to` document mirrors as a
        // `GraphOp::EdgePut` task. No-edge case leaves `tasks` at length 1.
        let mut tasks = vec![PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: ctx.database_id(),
            plan,
            post_set_op: PostSetOp::None,
            txn_id,
        }];
        // Implicit-edge extraction allocates surrogates — authorize the direct-op
        // task before those side effects.
        let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &ctx.state.audit,
        ));
        if let Err(error) = crate::control::server::shared::authorization::authorize_task_set(
            ctx.identity,
            &tasks,
            &ctx.state.permissions,
            &ctx.state.roles,
            &emitter,
        ) {
            return error_to_native(seq, &crate::Error::from(error));
        }

        if let Err(e) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
            ctx.state,
            &mut tasks,
            tenant_id,
            ctx.database_id(),
            TraceId::ZERO,
        )
        .await
        {
            return error_to_native(seq, &e);
        }

        // Covers the row images each settled cross-shard balance was folded from,
        // so Calvin's OCC check aborts on a total folded from a moved image.
        let sum_target_reads =
            match crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
                ctx.state,
                &mut tasks,
                tenant_id,
                ctx.database_id(),
                TraceId::ZERO,
            )
            .await
            {
                Ok(reads) => reads,
                Err(e) => return error_to_native(seq, &e),
            };

        // Follows resolution: consumes the surrogates that passed bound, no lookup of its own.
        if let Err(e) = crate::control::planner::materialized_sum::append_cross_shard_balance_tasks(
            ctx.state,
            &mut tasks,
            tenant_id,
            ctx.database_id(),
        ) {
            return error_to_native(seq, &e);
        }

        // Period-lock reference rows, resolved into the same plan slot as the
        // materialized-sum targets just above.
        if let Err(e) = crate::control::planner::period_lock::resolve_period_lock_targets(
            ctx.state,
            &mut tasks,
            tenant_id,
            ctx.database_id(),
            TraceId::ZERO,
        )
        .await
        {
            return error_to_native(seq, &e);
        }

        // The expanded set is the dispatch authorization boundary.
        let authorized_tasks =
            match crate::control::server::shared::authorization::authorize_task_set(
                ctx.identity,
                &tasks,
                &ctx.state.permissions,
                &ctx.state.roles,
                &emitter,
            ) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &crate::Error::from(error)),
            };

        if tasks.len() == 1 {
            // No-edge fast path. Local-path WAL append lives inside `dispatch_single_task`,
            // shared with the single-shard edge loop.
            let task = match authorized_tasks.into_tasks().into_iter().next() {
                Some(task) => task,
                None => {
                    return sqlstate_error(
                        seq,
                        "XX000",
                        "authorization returned no task capability",
                    );
                }
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            needs_top_level_metering = false;
            return dispatch_single_task(ctx, seq, task).await;
        }

        let _request = ctx.state.tenant_request_guard(tenant_id);
        // Dispatching to Calvin applies the write durably at statement time,
        // escaping the transaction buffer. Inside a block the per-task loop
        // buffers each task instead, and COMMIT flushes the whole buffer
        // through Calvin. A write the gate cannot buffer is refused here.
        let in_txn_block =
            ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock;
        if in_txn_block && !all_writes_bufferable(&tasks) {
            return error_to_native(seq, &crate::Error::CrossShardInExplicitTransaction);
        }
        // Only reads to widen with are those materialized-sum settlement stamped
        // on the source rows its shipped balances folded from.
        let route_to_calvin = !in_txn_block
            && matches!(
                classify_dispatch(
                    &tasks,
                    &crate::control::planner::calvin::read_vshards_of(&sum_target_reads),
                ),
                DispatchClass::MultiShard { .. }
            );
        if route_to_calvin {
            match dispatch_authorized_tasks_to_calvin(
                ctx.state,
                authorized_tasks,
                tenant_id,
                CrossShardTxnMode::Strict,
                TxnDispatchPosition::Autocommit,
                &sum_target_reads,
                None,
            )
            .await
            {
                // No RETURNING possible here, so the Response carries no rows —
                // report one row-affected per task.
                Ok(_apply) => {
                    let mut r = NativeResponse::ok(seq);
                    r.rows_affected = Some(tasks.len() as u64);
                    r
                }
                Err(e) => error_to_native(seq, &e),
            }
        } else {
            // Document task is first; its response is returned to the caller.
            needs_top_level_metering = false;
            let mut doc_response: Option<NativeResponse> = None;
            let mut error: Option<NativeResponse> = None;
            for task in authorized_tasks.into_tasks() {
                let resp = dispatch_single_task(ctx, seq, task).await;
                if resp.status == nodedb_types::protocol::ResponseStatus::Error {
                    error = Some(resp);
                    break;
                }
                if doc_response.is_none() {
                    doc_response = Some(resp);
                }
            }
            error
                .or(doc_response)
                .unwrap_or_else(|| NativeResponse::ok(seq))
        }
    }
    .await;

    // Metered only on success, once, for a branch that dispatched directly —
    // see `needs_top_level_metering`'s declaration above.
    if needs_top_level_metering
        && response.status != nodedb_types::protocol::ResponseStatus::Error
        && let Some(info) = &plan_metering_info
    {
        let rows = response
            .rows
            .as_ref()
            .map(|rows| rows.len() as u64)
            .or(response.rows_affected);
        meter_dispatch(ctx.state, &ctx.scope, info, rows);
    }
    response
}
