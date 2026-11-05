// SPDX-License-Identifier: BUSL-1.1

//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin,
};
use crate::control::server::response_shape::calvin_fold::CalvinFoldCtx;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::txn_route::statement_needs_implicit_txn;
use crate::control::server::shared::write_admission::plan_is_write;
use crate::types::TraceId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::single_task::dispatch_single_task;
use super::sql_loop::session_sequences;
use super::{DispatchCtx, error_to_native, error_to_native_with_sqlstate};
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Dispatch a direct Data Plane operation by opcode.
pub(crate) async fn handle_direct_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = match crate::control::server::shared::request_collection::request_collection(
        ctx.database_id(),
        fields.collection.as_deref().unwrap_or("default"),
    ) {
        Ok(collection) => collection,
        Err(error) => return error_to_native(seq, &error),
    };
    let tenant_id = ctx.tenant_id();

    // CRDT Apply allocates a surrogate while planning, and a KV counter plans
    // its fresh row from the catalog, which can evaluate a DEFAULT. Authorize
    // the exact collection first.
    if matches!(op, OpCode::CrdtApply | OpCode::KvIncr | OpCode::KvIncrFloat) {
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

    // A point delete's plan depends on whether the collection is edge-bearing.
    // The collection's lease is held from before that read until the op's
    // outcome, so the flag cannot change between the plan and the write: the
    // flag's catalog write drains every lease on the version the plan read.
    let _plan_lease = if op == OpCode::PointDelete {
        match crate::control::server::shared::clone_write::collections_write_lease(
            ctx.state,
            tenant_id,
            ctx.database_id(),
            std::iter::once(collection.clone()),
        )
        .await
        {
            Ok(lease) => Some(lease),
            Err(e) => return error_to_native(seq, &e),
        }
    } else {
        None
    };

    // A malformed request is a syntax error. Any other error comes from
    // resolving surrogates at the collection home and keeps its own code.
    let mut plan = match super::plan_builder::build_plan(ctx, op, fields, &collection).await {
        Ok(p) => p,
        Err(e @ crate::Error::BadRequest { .. }) => {
            return error_to_native_with_sqlstate(seq, "42601", &e);
        }
        Err(e) => return error_to_native(seq, &e),
    };
    let vshard_id = ctx.task_vshard(&plan, fields.document_id.as_deref(), &collection);

    // Refuse a read of a redacted column outside the projection, before RLS
    // adds the read policy's own predicates.
    if let Err(e) = crate::control::planner::redaction_refusal::refuse_redacted_expression_reads(
        &plan,
        tenant_id,
        ctx.database_id(),
        &crate::control::planner::redaction_refusal::RedactionReadCheck {
            auth: ctx.auth_context(),
            store: &ctx.state.redaction,
            catalog: ctx.state.credentials.catalog(),
        },
    ) {
        return error_to_native_with_sqlstate(seq, "42501", &e);
    }

    // Apply RLS before any special Control-Plane orchestration can observe the plan.
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        ctx.database_id(),
        &mut plan,
        &ctx.state.rls,
        ctx.state.credentials.catalog(),
        ctx.auth_context(),
    ) {
        return error_to_native_with_sqlstate(seq, "42501", &e);
    }

    // Inside a transaction block a direct read reads the block's snapshot, as
    // a SQL read does.
    match crate::control::server::shared::session::ensure_txn_snapshot(
        ctx.sessions,
        ctx.peer_addr.into(),
        ctx.state,
    )
    .await
    {
        Ok(Some(snapshot)) => crate::control::planner::system_cut::pin_plan_txn_snapshot(
            &mut plan,
            snapshot.statement_cap(),
        ),
        Ok(None) => {}
        Err(e) => return error_to_native(seq, &e),
    }

    // Refuse what column redaction cannot cover (redacted-column aggregate, graph traversal).
    if let Err(e) = crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        ctx.database_id(),
        ctx.auth_context(),
        &ctx.state.redaction,
    ) {
        return error_to_native_with_sqlstate(seq, "42501", &e);
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

    // Inside a transaction block every write runs through the statement
    // loop the SQL path uses, which stages it into the block or refuses it.
    let in_txn_block = ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock;

    // False for `dispatch_single_task`, which meters itself — re-metering here
    // will double-bill a `Staged` dispatch and wrongly bill a `Buffered` one.
    let mut needs_top_level_metering = true;
    // Wrapped in an async block so `return` inside each branch exits only this
    // block, letting the metering call below run exactly once regardless of branch.
    let response: NativeResponse = async {
        // Plans that orchestrate on the Control Plane never reach the Data
        // Plane as a single op. Their orchestrators apply at once, so inside a
        // block they expand and stage through the statement loop below.
        if !in_txn_block
            && let Some(response) =
                super::direct_orchestrated::dispatch_orchestrated(ctx, seq, &plan, vshard_id).await
        {
            return response;
        }

        // A graph algorithm or neighbors read runs where the graph's partitions
        // live, not on the one node the collection routes to.
        if let Some(response) =
            super::graph_owner::dispatch_graph_owner_read(ctx, seq, &plan, vshard_id).await
        {
            return response;
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
        // The lease gates the direct op on a drained collection and lives
        // until the op's outcome.
        let lease = match crate::control::server::shared::clone_write::write_lease(
            ctx.state,
            tenant_id,
            ctx.database_id(),
            &tasks[0].plan,
        )
        .await
        {
            Ok(lease) => std::sync::Arc::new(lease),
            Err(e) => return error_to_native(seq, &e),
        };

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
                txn_id,
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
        // materialized-sum targets right above.
        if let Err(e) = crate::control::planner::period_lock::resolve_period_lock_targets(
            ctx.state,
            &mut tasks,
            tenant_id,
            ctx.database_id(),
            txn_id,
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

        // A write that fires a BEFORE, INSTEAD OF or SYNC AFTER body, a MERGE
        // into an edge-bearing collection, or a join-expanding write into the
        // source of a cross-shard materialized sum, runs in one transaction, as
        // the SQL path's does. So does every write inside a block: the loop
        // expands a join or page write, takes a clone's copy-on-write steps,
        // and stages each write into the block or refuses it with 25001. The
        // loop clone-checks, authorizes and meters each task itself.
        let block_write = in_txn_block && tasks.iter().any(|task| plan_is_write(&task.plan));
        let needs_implicit_txn = match statement_needs_implicit_txn(ctx.state, &tasks) {
            Ok(needs) => needs,
            Err(error) => return error_to_native(seq, &error),
        };
        if block_write || needs_implicit_txn {
            let _request = ctx.state.tenant_request_guard(tenant_id);
            needs_top_level_metering = false;
            return super::direct_txn::dispatch_direct_in_txn(
                ctx,
                seq,
                super::direct_txn::DirectWrite {
                    tasks,
                    sum_target_reads,
                    lease: std::sync::Arc::clone(&lease),
                },
            )
            .await;
        }

        // A delete or update on an edge-bearing collection commits its edge
        // cleanup in the same Calvin transaction, as the SQL path's does.
        use super::edge_recon_gate::{
            EdgeReconResult, try_edge_recon_dispatch, try_multi_shard_predicate_recon_dispatch,
            try_settled_recon_dispatch,
        };
        let (tasks, authorized_tasks) =
            match try_edge_recon_dispatch(ctx, seq, tasks, authorized_tasks, &sum_target_reads)
                .await
            {
                EdgeReconResult::Outcome(outcome) => return outcome.into_response(),
                EdgeReconResult::NotFired(tasks, authorized) => (tasks, authorized),
            };
        // A zero-net settlement on one vShard ships no task, and its reads
        // still guard the balance. A predicate write takes the coordinator.
        let (tasks, authorized_tasks) =
            match try_settled_recon_dispatch(ctx, seq, tasks, authorized_tasks, &sum_target_reads)
                .await
            {
                EdgeReconResult::Outcome(outcome) => return outcome.into_response(),
                EdgeReconResult::NotFired(tasks, authorized) => (tasks, authorized),
            };

        if tasks.len() == 1 && sum_target_reads.is_empty() {
            // No-edge fast path. Local-path WAL append lives inside `dispatch_single_task`,
            // shared with the single-shard edge loop.
            let task = match authorized_tasks.into_tasks().into_iter().next() {
                Some(task) => task,
                None => {
                    return sqlstate_error(
                        seq,
                        nodedb_types::error::sqlstate::INTERNAL_ERROR,
                        "authorization returned no task capability",
                    );
                }
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            needs_top_level_metering = false;
            return dispatch_single_task(ctx, seq, task).await;
        }

        let _request = ctx.state.tenant_request_guard(tenant_id);
        // Only reads to widen with are those materialized-sum settlement stamped
        // on the source rows its shipped balances folded from.
        let sum_read_vshards =
            match crate::control::planner::calvin::read_vshards_of(&sum_target_reads) {
                Ok(vshards) => vshards,
                Err(error) => return error_to_native(seq, &error),
            };
        // A block's writes returned above, so these tasks apply autocommit. A
        // set with settlement reads commits through Calvin on any vShard count:
        // the per-task loop checks no read.
        let route_to_calvin = !sum_target_reads.is_empty()
            || matches!(
                classify_dispatch(&tasks, &sum_read_vshards),
                DispatchClass::MultiShard { .. }
            );
        if route_to_calvin {
            // The settled recon route took a recon plan on one vShard, so one
            // here spans vShards. A predicate write names the rows it matched
            // only after the coordinator reads them.
            let (tasks, authorized_tasks) = match try_multi_shard_predicate_recon_dispatch(
                ctx,
                seq,
                tasks,
                authorized_tasks,
                &sum_target_reads,
            )
            .await
            {
                EdgeReconResult::Outcome(outcome) => return outcome.into_response(),
                EdgeReconResult::NotFired(tasks, authorized) => (tasks, authorized),
            };
            let plans: Vec<PhysicalPlan> = tasks.iter().map(|task| task.plan.clone()).collect();
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
                // The count and verb come from the applied response the
                // batch's count-bearing plan reported, never from the task
                // count: an implicit-edge cleanup task is not a row.
                Ok(apply) => {
                    let database_id = ctx.database_id();
                    let sequences = session_sequences(ctx, database_id);
                    super::conversion::calvin_native_response(
                        seq,
                        apply,
                        &plans,
                        &CalvinFoldCtx {
                            projection: None,
                            state: ctx.state,
                            tenant_id,
                            database_id,
                            auth: ctx.auth_context(),
                            sequences: Some(&sequences),
                        },
                    )
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
