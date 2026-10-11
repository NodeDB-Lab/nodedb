// SPDX-License-Identifier: BUSL-1.1

//! Direct operations that orchestrate on the Control Plane and never reach the
//! Data Plane as a single op: `INSERT ... SELECT`, autocommit `MERGE`,
//! autocommit `UPDATE ... FROM <source>`, and governed-predicate writes.

use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::txn_route::statement_needs_implicit_txn;
use crate::types::VShardId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::response::data_plane_response_to_native;
use super::{DispatchCtx, error_to_native};

/// A task carrying `plan` with no post-set op and no transaction.
fn bare_task(ctx: &DispatchCtx<'_>, plan: &PhysicalPlan, vshard_id: VShardId) -> PhysicalTask {
    PhysicalTask {
        tenant_id: ctx.tenant_id(),
        vshard_id,
        database_id: ctx.database_id(),
        plan: plan.clone(),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

/// Run `plan` on the Control Plane when it needs orchestration there.
///
/// Returns `None` when the plan dispatches as a single Data Plane op.
pub(super) async fn dispatch_orchestrated(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    plan: &PhysicalPlan,
    vshard_id: VShardId,
) -> Option<NativeResponse> {
    let tenant_id = ctx.tenant_id();

    // `INSERT ... SELECT` orchestrates on the Control Plane; never reaches the
    // Data Plane as a single op.
    if matches!(
        plan,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. })
    ) {
        let task = bare_task(ctx, plan, vshard_id);
        // A copy into the source of a cross-shard materialized sum runs in the
        // implicit transaction, which ships each balance to its target.
        if !statement_needs_implicit_txn(ctx.state, std::slice::from_ref(&task)) {
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return Some(error_to_native(seq, &error)),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result =
                crate::control::insert_select::run_authorized_insert_select(ctx.state, authorized)
                    .await;
            return Some(match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, plan, &resp),
                Err(e) => error_to_native(seq, &e),
            });
        }
    }

    // Autocommit `MERGE` orchestrates on the Control Plane; never reaches the
    // Data Plane as a single op.
    if matches!(
        plan,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Merge {
            resolved_inserts: None,
            ..
        })
    ) {
        let task = bare_task(ctx, plan, vshard_id);
        // A MERGE into an edge-bearing collection, or into the source of a
        // cross-shard materialized sum, runs in the implicit transaction. It
        // stages each removed row's edge tasks and ships each balance to its
        // target.
        if !statement_needs_implicit_txn(ctx.state, std::slice::from_ref(&task)) {
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return Some(error_to_native(seq, &error)),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result =
                crate::control::merge_orchestrator::run_authorized_merge(ctx.state, authorized)
                    .await;
            return Some(match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, plan, &resp),
                Err(e) => error_to_native(seq, &e),
            });
        }
    }

    // Autocommit `UPDATE ... FROM <source>` scans the source on its own core and
    // ships it into the plan; never reaches the Data Plane as a single op.
    if matches!(
        plan,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
            source_rows: None,
            ..
        })
    ) {
        let task = bare_task(ctx, plan, vshard_id);
        // An update of the source of a cross-shard materialized sum runs in the
        // implicit transaction, which ships each balance to its target.
        if !statement_needs_implicit_txn(ctx.state, std::slice::from_ref(&task)) {
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(authorized) => authorized,
                Err(error) => return Some(error_to_native(seq, &error)),
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            let result =
                crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                    ctx.state, authorized,
                )
                .await;
            return Some(match result {
                Ok(resp) => data_plane_response_to_native(ctx, seq, plan, &resp),
                Err(e) => error_to_native(seq, &e),
            });
        }
    }

    // A governed predicate resolves to a concrete row set before proposing — see
    // `control::write_resolve`.
    if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(plan) {
        let task = bare_task(ctx, plan, vshard_id);
        let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
            Ok(authorized) => authorized,
            Err(error) => return Some(error_to_native(seq, &error)),
        };
        let _request = ctx.state.tenant_request_guard(tenant_id);
        let result = crate::control::write_resolve::run_authorized_write_resolve(
            ctx.state, authorized, resolver,
        )
        .await;
        return Some(match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, plan, &resp),
            Err(e) => error_to_native(seq, &e),
        });
    }

    None
}
