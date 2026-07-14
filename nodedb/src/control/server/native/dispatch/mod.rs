// SPDX-License-Identifier: BUSL-1.1

//! Per-opcode dispatch handlers for the native protocol.

mod auth;
mod conversion;
mod ctx;
mod direct_ops;
mod edge_recon_gate;
mod limits;
mod plan_builder;
mod session_ops;
mod sql;
mod sql_admin;
mod sql_gateway;
mod sql_loop;
mod streaming;
mod transaction;
mod transaction_savepoint;

pub(crate) use auth::{handle_auth, handle_ping};
pub(crate) use conversion::{
    ddl_result_to_native, error_to_native, shape_error_to_native, to_native_columns_rows,
};
pub(crate) use ctx::DispatchCtx;
pub(crate) use direct_ops::{handle_direct_op, handle_graph_match};
pub(crate) use session_ops::{handle_reset, handle_set, handle_show};
pub(crate) use sql::{handle_sql, handle_sql_streaming};
pub(crate) use streaming::{SqlOutcome, SqlStream};
pub(crate) use transaction::{NativeTxnDp, handle_begin, handle_commit, handle_rollback};
