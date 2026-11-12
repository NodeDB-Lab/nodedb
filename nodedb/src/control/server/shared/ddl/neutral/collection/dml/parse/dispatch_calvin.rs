// SPDX-License-Identifier: BUSL-1.1

//! The Calvin route of one autocommit collection-DML statement.
//!
//! A statement outside a transaction block commits as one Calvin
//! transaction when it spans vShards, holds settlement reads, holds several
//! writes, or carries a `RETURNING` list that can fail. The direct path
//! evaluates a list after its write is stored. A Calvin participant
//! evaluates it before it votes, so an error in the list stores nothing. A
//! list that cannot fail keeps the direct path, as it does over every other
//! protocol.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin, read_vshards_of,
};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::authorization::AuthorizedTaskSet;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::returning::plan_carries_returning_contract;
use crate::control::server::shared::session::announced_returning::with_announced_returning;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::statement_exec::fallible_returning::returning_commits_through_calvin;
use crate::control::state::SharedState;
use crate::types::TenantId;

use crate::control::server::shared::ddl::neutral::refusal::response_verdict;

use super::dispatch_write::{ReturningShape, error_to_ddl};
use super::types::ddl_err;

/// One autocommit statement, authorized, before it takes a route.
pub(super) struct AutocommitStatement<'a> {
    pub tasks: &'a [PhysicalTask],
    pub authorized: AuthorizedTaskSet,
    /// The row images the statement's cross-shard balances were settled
    /// from.
    pub reads: &'a [ReadSetEntry],
    pub has_returning: bool,
    /// The statement holds several writes Calvin commits as one.
    pub several_writes: bool,
}

/// The route an autocommit statement took.
pub(super) enum AutocommitRoute {
    /// Calvin committed the statement. Its `RETURNING` rows, if any.
    Calvin(Vec<DdlResult>),
    /// No Calvin route applies. The caller dispatches each task directly.
    Direct(AuthorizedTaskSet),
}

/// Commit `statement` through Calvin when a Calvin route applies, else hand
/// its authorized tasks back for the direct path.
///
/// The participants answer the `RETURNING` columns the statement announced,
/// and the decision reads them, so both run with them in scope.
pub(super) async fn route_autocommit(
    state: &SharedState,
    tenant_id: TenantId,
    shape: &ReturningShape<'_>,
    statement: AutocommitStatement<'_>,
) -> Result<AutocommitRoute, DdlError> {
    with_announced_returning(
        Some(shape.output_schema),
        route(state, tenant_id, shape, statement),
    )
    .await
}

async fn route(
    state: &SharedState,
    tenant_id: TenantId,
    shape: &ReturningShape<'_>,
    statement: AutocommitStatement<'_>,
) -> Result<AutocommitRoute, DdlError> {
    let AutocommitStatement {
        tasks,
        authorized,
        reads,
        has_returning,
        several_writes,
    } = statement;
    let read_vshards = read_vshards_of(reads).map_err(|error| DdlError::from_error(&error))?;
    // A statement with settlement reads commits through Calvin on any vShard
    // count: the direct route checks no read.
    let through_calvin = several_writes
        || !reads.is_empty()
        || matches!(
            classify_dispatch(tasks, &read_vshards),
            DispatchClass::MultiShard { .. }
        )
        || returning_commits_through_calvin(state, tenant_id, tasks);
    if !through_calvin {
        return Ok(AutocommitRoute::Direct(authorized));
    }
    // Every participant answers the rows its contract froze. A list with no
    // contract has no frozen rows to answer, so it is refused before the
    // write commits.
    let contract = tasks
        .iter()
        .find(|task| plan_carries_returning_contract(&task.plan));
    if has_returning && contract.is_none() {
        return Err(ddl_err(
            "0A000",
            "RETURNING is not supported on a write that commits through Calvin on this \
             route; remove the RETURNING clause",
        ));
    }
    let answer = dispatch_authorized_tasks_to_calvin(
        state,
        authorized,
        tenant_id,
        CrossShardTxnMode::Strict,
        TxnDispatchPosition::Autocommit,
        reads,
        None,
    )
    .await
    .map_err(|error| error_to_ddl(&error))?;
    let Some(response) = answer else {
        return Ok(AutocommitRoute::Calvin(Vec::new()));
    };
    response_verdict(&response)?;
    let (true, Some(contract)) = (has_returning, contract) else {
        return Ok(AutocommitRoute::Calvin(Vec::new()));
    };
    let mut rows: Option<ShapedRows> = None;
    shape.fold_frozen(&contract.plan, response.payload.as_bytes(), &mut rows)?;
    Ok(AutocommitRoute::Calvin(
        rows.map(DdlResult::Rows).into_iter().collect(),
    ))
}
