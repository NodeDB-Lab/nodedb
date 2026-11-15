// SPDX-License-Identifier: BUSL-1.1

//! A native request routed `RequestRoute::Sequential`: its statements run in
//! order through the sequential request runner.

use nodedb_types::protocol::NativeResponse;

use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::sql_split::Statement;
use crate::control::server::shared::statement_exec::{
    RequestAnswer, SequentialRequest, StatementError, StatementExec, run_sequential_request,
};
use crate::types::{DatabaseId, TraceId};

use super::sql_fold::{answer_to_native, statement_error_to_native};
use super::sql_loop::session_sequences;
use super::{DispatchCtx, error_to_native};

/// Run `statements` in order on the session of `ctx`.
///
/// Outside a transaction block they run on a private implicit transaction.
/// Inside the session's block they run on that block. An error inside the
/// block leaves the block failed, as any statement error does.
///
/// The frame is the sum of every statement's answer, as a request the
/// one-set path ran answers.
pub(super) async fn run_native_sequential(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    statements: &[Statement],
    database_id: DatabaseId,
) -> NativeResponse {
    if let Err(error) = ctx.state.check_tenant_quota(ctx.tenant_id()) {
        return error_to_native(seq, &error);
    }
    let _request = ctx.state.tenant_request_guard(ctx.tenant_id());
    // Each statement plans under the session's planning overrides, as a
    // statement of the one-set path does.
    crate::control::server::shared::planning_overrides::apply_planning_session_overrides(
        ctx.query_ctx,
        ctx.sessions,
        ctx.state,
        ctx.peer_addr,
        ctx.tenant_id(),
    );
    let session = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let sequences = session_sequences(ctx, database_id);
    let exec = StatementExec {
        state: ctx.state,
        identity: ctx.identity,
        scope: &ctx.scope,
        schemas: &[],
        database_id,
        sequences: &sequences,
    };
    let ran = run_sequential_request(
        &SequentialRequest {
            exec: &exec,
            query_ctx: ctx.query_ctx,
            statements,
            trace_id: TraceId::ZERO,
        },
        &session,
    )
    .await
    .map_err(StatementError::from)
    .and_then(RequestAnswer::summed);
    match ran {
        Ok(answer) => answer_to_native(seq, answer),
        Err(error) => {
            ctx.sessions.fail_transaction(ctx.peer_addr);
            statement_error_to_native(seq, error)
        }
    }
}
