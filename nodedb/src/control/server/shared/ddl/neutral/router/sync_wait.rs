// SPDX-License-Identifier: BUSL-1.1

//! The wait of a statement the router answers itself for its sync mirrors.
//!
//! The planned write paths read their tasks to find a sync database. A write the
//! router runs holds no such task list, so each write arm wraps its answer here.

use crate::control::mirror::source::sync::await_database_sync;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// `result`, once every sync mirror of `database_id` holds the write it answers.
/// A failed statement waits for nothing, and the timeout says the write committed.
pub(super) async fn confirmed(
    state: &SharedState,
    database_id: DatabaseId,
    result: Result<Vec<DdlResult>, DdlError>,
) -> Result<Vec<DdlResult>, DdlError> {
    let answer = result?;
    await_database_sync(state, database_id)
        .await
        .map_err(|error| DdlError::from_error(&error))?;
    Ok(answer)
}

/// [`confirmed`] for a statement a transaction block stages. A statement in a
/// block committed nothing, so its wait happens at the COMMIT.
pub(super) async fn confirmed_unless_staged(
    state: &SharedState,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
    result: Result<Vec<DdlResult>, DdlError>,
) -> Result<Vec<DdlResult>, DdlError> {
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        return result;
    }
    confirmed(state, database_id, result).await
}
