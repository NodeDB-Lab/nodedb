// SPDX-License-Identifier: BUSL-1.1

//! The implicit-edge OLLP/Calvin gate for native direct ops.
//!
//! The gate itself is shared (`shared::statement_exec::try_edge_recon`).
//! This adapter reads the session's transaction state and renders the
//! applied batch as a native frame.

use nodedb_types::protocol::NativeResponse;

use crate::control::server::shared::authorization::AuthorizedTaskSet;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::statement_exec::{
    EdgeRecon, try_edge_recon, try_multi_shard_predicate_recon,
};
use nodedb_physical::physical_task::PhysicalTask;

use super::{DispatchCtx, SqlOutcome, error_to_native};

/// Route `tasks` through the implicit-edge OLLP/Calvin path when the shared
/// gate fires for them. The caller returns a fired outcome at once: the
/// tasks are consumed.
pub(super) async fn try_edge_recon_dispatch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    reads: &[ReadSetEntry],
) -> EdgeReconResult {
    // The native `handle_begin` / `handle_commit` / `handle_rollback` drive
    // the same `SessionStore` state machine pgwire uses.
    let in_txn_block = ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock;
    let routed = try_edge_recon(
        ctx.state,
        ctx.identity,
        in_txn_block,
        tasks,
        authorized,
        reads,
    )
    .await;
    outcome(ctx, seq, routed)
}

/// Route `tasks` through the OLLP/Calvin path when the shared multi-shard
/// gate fires for them. The caller has classified the statement as
/// multi-shard and outside a transaction block.
pub(super) async fn try_multi_shard_predicate_recon_dispatch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    reads: &[ReadSetEntry],
) -> EdgeReconResult {
    let routed =
        try_multi_shard_predicate_recon(ctx.state, ctx.identity, tasks, authorized, reads).await;
    outcome(ctx, seq, routed)
}

fn outcome(ctx: &DispatchCtx<'_>, seq: u64, routed: crate::Result<EdgeRecon>) -> EdgeReconResult {
    match routed {
        Ok(EdgeRecon::NotFired(tasks, authorized)) => EdgeReconResult::NotFired(tasks, authorized),
        // A RETURNING dependent write surfaces its deleted or updated rows. A
        // plain write reports the count its own mutation returned.
        Ok(EdgeRecon::Applied(applied)) => {
            EdgeReconResult::Outcome(resp(super::conversion::calvin_native_response(
                seq,
                applied.apply_result,
                &applied.plans,
                ctx.state,
                applied.database_id,
                ctx.tenant_id(),
                ctx.auth_context(),
            )))
        }
        Err(error) => EdgeReconResult::Outcome(resp(error_to_native(seq, &error))),
    }
}

/// Result returned by [`try_edge_recon_dispatch`].
pub(super) enum EdgeReconResult {
    /// Gate did not fire; caller receives the task list back and continues
    /// normal dispatch.
    NotFired(Vec<PhysicalTask>, AuthorizedTaskSet),
    /// Gate fired; caller must return this outcome immediately.
    Outcome(SqlOutcome),
}

fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
}
