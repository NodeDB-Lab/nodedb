// SPDX-License-Identifier: BUSL-1.1

//! Task authorization for the native protocol's Control-Plane orchestrated
//! plans. The SQL path's single-task dispatch lives in
//! `shared::statement_exec::dispatch`.

use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;

/// Authorize one task with no clone-write check. Used only by the
/// Control-Plane orchestrator branches, whose plan shapes (`InsertSelect`,
/// `Merge`, `UpdateFromJoin`, a governed predicate resolution, array DDL, a
/// cluster array op, a graph owner op) are never clone-write shapes.
pub(super) fn authorize_native_task(
    ctx: &DispatchCtx<'_>,
    task: &PhysicalTask,
) -> crate::Result<crate::control::server::shared::authorization::AuthorizedTask> {
    crate::control::server::shared::statement_exec::authorize_one_task(
        ctx.state,
        ctx.identity,
        task,
    )
}
