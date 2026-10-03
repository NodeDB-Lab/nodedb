// SPDX-License-Identifier: BUSL-1.1

//! Savepoint control adapters for the native protocol: SAVEPOINT, RELEASE
//! SAVEPOINT, ROLLBACK TO SAVEPOINT — thin shims over the protocol-neutral
//! savepoint orchestrator in `control/server/shared/session/savepoint_ops.rs`.
//!
//! The overlay marker capture/decode and the `SessionStore` savepoint stack
//! live in the neutral core; native only parses the statement, shares the same
//! Data-Plane dispatch seam as COMMIT/ROLLBACK, and maps the neutral error to a
//! native SQLSTATE frame (`25P01` / `25P02` / `3B001` / `XX000`).

use nodedb_types::protocol::NativeResponse;

use crate::control::server::shared::session::savepoint_ops::{self, SavepointError};

use super::DispatchCtx;
use super::transaction::NativeTxnDp;
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Map a neutral savepoint error to a native error frame.
fn savepoint_error_to_native(seq: u64, e: &SavepointError) -> NativeResponse {
    match e {
        SavepointError::NoActiveTransaction => sqlstate_error(
            seq,
            "25P01",
            "SAVEPOINT can only be used in transaction blocks",
        ),
        SavepointError::TransactionAborted => sqlstate_error(
            seq,
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        ),
        SavepointError::NotFound { message } => sqlstate_error(seq, "3B001", message.clone()),
        SavepointError::OverlayDispatch { message } => {
            sqlstate_error(seq, "XX000", message.clone())
        }
    }
}

/// Handle SAVEPOINT <name>.
pub(crate) async fn handle_savepoint(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql_trimmed: &str,
) -> NativeResponse {
    let sp_name = sql_trimmed.split_whitespace().nth(1).unwrap_or("sp");
    let dp = NativeTxnDp { state: ctx.state };
    match savepoint_ops::run_savepoint(
        ctx.sessions,
        ctx.peer_addr.into(),
        ctx.tenant_id(),
        &dp,
        sp_name,
    )
    .await
    {
        Ok(()) => NativeResponse::status_row(seq, "SAVEPOINT"),
        Err(e) => savepoint_error_to_native(seq, &e),
    }
}

/// Handle RELEASE SAVEPOINT <name>.
pub(crate) fn handle_release_savepoint(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql_trimmed: &str,
) -> NativeResponse {
    let sp_name = sql_trimmed.split_whitespace().last().unwrap_or("sp");
    match savepoint_ops::run_release_savepoint(ctx.sessions, ctx.peer_addr.into(), sp_name) {
        Ok(()) => NativeResponse::status_row(seq, "RELEASE"),
        Err(e) => savepoint_error_to_native(seq, &e),
    }
}

/// Handle ROLLBACK TO SAVEPOINT <name>.
pub(crate) async fn handle_rollback_to_savepoint(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql_trimmed: &str,
) -> NativeResponse {
    let sp_name = sql_trimmed.split_whitespace().last().unwrap_or("sp");
    let dp = NativeTxnDp { state: ctx.state };
    match savepoint_ops::run_rollback_to_savepoint(
        ctx.sessions,
        ctx.peer_addr.into(),
        ctx.tenant_id(),
        &dp,
        sp_name,
    )
    .await
    {
        Ok(()) => NativeResponse::status_row(seq, "ROLLBACK"),
        Err(e) => savepoint_error_to_native(seq, &e),
    }
}
