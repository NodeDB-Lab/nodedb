// SPDX-License-Identifier: BUSL-1.1

//! Per-opcode dispatch handlers for the native protocol.

mod admission_op;
mod auth;
mod cluster_array;
mod conversion;
mod ctx;
mod direct_ops;
mod edge_recon_gate;
mod graph_match;
mod limits;
mod plan_builder;
pub(crate) mod raw_dispatch;
pub(crate) mod response;
mod session_ops;
mod single_task;
mod sql;
mod sql_admin;
mod sql_dispatch_task;
mod sql_gateway;
mod sql_loop;
mod streaming;
mod transaction;
mod transaction_savepoint;

pub(crate) use admission_op::admission_operation;
pub(crate) use auth::{NativeAuthOutcome, handle_auth, handle_ping};
pub(crate) use conversion::{
    apply_dml_outcome, ddl_result_to_native, dml_fold_error_to_native, error_code_to_native,
    error_response_to_native, error_to_native, error_to_native_with_sqlstate,
    shape_error_to_native, to_native_columns_rows,
};
pub(crate) use ctx::DispatchCtx;
pub(crate) use direct_ops::handle_direct_op;
pub(crate) use graph_match::handle_graph_match;
pub(crate) use session_ops::{handle_reset, handle_set, handle_show, show_all};
pub(crate) use sql::{handle_sql, handle_sql_streaming};
pub(crate) use streaming::{SqlOutcome, SqlStream};
pub(crate) use transaction::{NativeTxnDp, handle_begin, handle_commit, handle_rollback};
