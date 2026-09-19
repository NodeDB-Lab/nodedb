// SPDX-License-Identifier: BUSL-1.1

//! `ClusterArray` plan dispatch for the native protocol.
//!
//! `sql_loop.rs` and `single_task.rs` intercept a `PhysicalPlan::ClusterArray`
//! task right after their own in-transaction routing gate and delegate to the
//! shared, protocol-neutral core
//! (`shared::cluster_array_dispatch::execute_cluster_array`), then convert
//! the outcome into native wire columns/rows or an affected count. Metering
//! is not applied on this path, matching pgwire's `ClusterArray`
//! short-circuit, which also does not meter it.

use nodedb_types::Value;

use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::server::shared::cluster_array_dispatch::{
    ClusterArrayShaped, execute_cluster_array,
};

use super::DispatchCtx;
use super::conversion::to_native_columns_rows;

/// What one `ClusterArrayOp` answers with, in native wire shape.
pub(crate) enum ClusterArrayOutcome {
    /// A read's rows (`Slice` / `Agg`), converted to native columns/rows.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        notice: Option<String>,
    },
    /// A write's affected row count (`Put` / `Delete`).
    Affected(u64),
}

/// Execute a single `ClusterArrayOp` via the shared core and convert its
/// outcome into native wire shape.
pub(crate) async fn dispatch_cluster_array_task(
    ctx: &DispatchCtx<'_>,
    authorized: AuthorizedTask,
    projection: Option<&OutputSchema>,
) -> crate::Result<ClusterArrayOutcome> {
    match execute_cluster_array(ctx.state, ctx.auth_context(), authorized, projection).await? {
        ClusterArrayShaped::Rows(mut shaped) => {
            let notice = shaped.notice.take();
            let (columns, rows) = to_native_columns_rows(&shaped);
            Ok(ClusterArrayOutcome::Rows {
                columns,
                rows,
                notice,
            })
        }
        ClusterArrayShaped::Affected(outcome) => {
            Ok(ClusterArrayOutcome::Affected(outcome.affected))
        }
    }
}
