// SPDX-License-Identifier: BUSL-1.1

//! The single-task dispatch primitive the native SQL loop calls.
//!
//! Decides HOW one already-planned task reaches an engine — Control-Plane
//! orchestrators, Exchange resolution, then the gateway — while `sql_loop.rs`
//! decides what to do with the answers.

use nodedb_types::TraceId;

use crate::bridge::envelope::Response;
use crate::control::server::exchange::DistributedReadCapture;
use crate::control::server::exchange::resolve::{Resolved, resolve_and_materialize};
use crate::types::{Lsn, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;
use super::sql_gateway::dispatch_task_via_gateway;

/// Dispatch a single `PhysicalTask`, returning the response plus per-shard
/// watermark LSNs a single-node fan gather observed.
///
/// Multi-arm writes run via Control-Plane orchestrators; everything else
/// routes through `dispatch_task_via_gateway`.
pub(super) async fn dispatch_task(
    ctx: &DispatchCtx<'_>,
    mut task: PhysicalTask,
) -> crate::Result<(Response, Vec<(VShardId, Lsn)>, Vec<DistributedReadCapture>)> {
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. },
    ) = &task.plan
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp =
            crate::control::insert_select::run_authorized_insert_select(ctx.state, authorized)
                .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Autocommit `MERGE` orchestrates on the Control Plane (`control::merge_orchestrator`).
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::Merge {
            target_collection: _,
            source_collection: _,
            source_alias: _,
            target_join_col: _,
            source_join_col: _,
            clauses: _,
            returning: _,
            resolved_inserts: None,
            source_rows: _,
            rls_filters: _,
            rls_write_check: _,
            resolved_sum_targets: _,
            declared_primary_key: _,
        },
    ) = &task.plan
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp =
            crate::control::merge_orchestrator::run_authorized_merge(ctx.state, authorized).await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Autocommit `UPDATE ... FROM <source>` scans the source on its own core and
    // ships it into the plan, since the source's vShard can live on a different core.
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
            target_collection: _,
            source_collection: _,
            source_alias: _,
            target_join_col: _,
            source_join_col: _,
            updates: _,
            target_filters: _,
            returning: _,
            source_rows: None,
            rls_filters: _,
            rls_write_check: _,
            resolved_sum_targets: _,
            declared_primary_key: _,
        },
    ) = &task.plan
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp = crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
            ctx.state, authorized,
        )
        .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // A governed predicate resolves to a concrete row set before proposing
    // (`control::write_resolve`); local (non-Raft) path skips this.
    if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan)
        && ctx.state.async_raft_proposer().is_some()
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp = crate::control::write_resolve::run_authorized_write_resolve(
            ctx.state, authorized, resolver,
        )
        .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Native DROP uses the same reversible all-core protocol as pgwire.
    if matches!(
        task.plan,
        crate::bridge::envelope::PhysicalPlan::Array(
            nodedb_physical::physical_plan::ArrayOp::DropArray { .. }
        )
    ) {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let task = authorized.into_physical_task();
        let resp = crate::control::array_catalog::ddl::run_authorized_drop(
            ctx.state,
            task.tenant_id,
            task.database_id,
            task.plan,
            TraceId::ZERO,
        )
        .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Materialize catalog providers and resolve Exchange nodes before dispatch.
    match resolve_and_materialize(
        ctx.state,
        ctx.identity,
        task.database_id,
        task.tenant_id,
        task.plan,
        TraceId::ZERO,
        task.txn_id,
    )
    .await?
    {
        Resolved::Gathered(resp, shard_watermarks, dist_reads) => {
            return Ok((resp, shard_watermarks, dist_reads));
        }
        Resolved::Plan(resolved_plan) => {
            let resolved_plan = *resolved_plan;
            task.plan = resolved_plan;
        }
        // Native path materializes the stream into a Response, preserving gather-then-return.
        Resolved::Stream(s) => {
            let resp = crate::control::server::exchange::gather::stream_to_response(s).await?;
            return Ok((resp, Vec::new(), Vec::new()));
        }
    }

    // Everything else routes through the gateway when available, or local SPSC otherwise.
    let resp = dispatch_task_via_gateway(ctx, task).await?;
    Ok((resp, Vec::new(), Vec::new()))
}
