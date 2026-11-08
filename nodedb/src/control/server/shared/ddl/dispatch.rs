// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch.
//!
//! Yields a protocol-neutral [`DdlResult`] / [`DdlError`] instead of pgwire
//! `Response` types, so native, http, and pgwire entrypoints share one router
//! with no pgwire dependency in the routing layer.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::result::{DdlError, DdlResult};

/// Try to handle a SQL statement as a Control Plane DDL command, returning a
/// protocol-neutral result.
///
/// `txn_ctx` carries the connection's session store + address so that
/// write-shaped DML reached through this router (UPSERT/INSERT, `COPY FROM`)
/// can route in-transaction writes through the same staging gate pgwire-SQL
/// and native already use.
///
/// Returns `None` when the statement is not a recognized DDL command (the
/// caller falls through to the SQL planner).
pub async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    super::neutral::try_dispatch(state, identity, sql, database_id, txn_ctx).await
}

/// Mark the open transaction block failed when a router statement answered
/// with an error, as an error of a planned statement does.
///
/// Outside a block this changes nothing.
pub fn abort_block_on_error<T>(txn_ctx: &DmlTxnCtx<'_>, result: &Result<T, DdlError>) {
    if result.is_err() {
        txn_ctx.sessions.fail_transaction(txn_ctx.session_id);
    }
}
