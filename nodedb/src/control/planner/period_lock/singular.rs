// SPDX-License-Identifier: BUSL-1.1

//! Period-value extraction for the single-row and batch-body gated shapes:
//! `PointPut` / `PointInsert` / `Upsert` / `PointDelete` / `BatchInsert`.

use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget};

use super::lookup::lookup_period_surrogate;
use crate::control::planner::materialized_sum::recon::recon_point_row;
use crate::control::planner::materialized_sum::{join_value_from_body, resolve_one_target};
use crate::control::security::catalog::PeriodLockDef;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// The period value a single-row gated write carries, or `None` when the
/// period column is absent — the same "not gated" outcome `check_period_lock`
/// reaches for a row with no period column. Never called for `BatchInsert`,
/// which resolves through [`resolve_batch_period_values`] instead.
///
/// A body-carrying write (`PointPut`/`PointInsert`/`Upsert`) reads the value
/// off its submitted body. `PointDelete` carries no body, so its value comes
/// off the row it is about to remove.
pub(super) async fn singular_period_value(
    state: &SharedState,
    op: &DocumentOp,
    collection: &str,
    def: &PeriodLockDef,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<Option<String>> {
    match op {
        DocumentOp::PointPut { value, .. }
        | DocumentOp::PointInsert { value, .. }
        | DocumentOp::Upsert { value, .. } => Ok(join_value_from_body(value, &def.period_column)),
        DocumentOp::PointDelete {
            document_id,
            surrogate,
            ..
        } => {
            let read = recon_point_row(
                state,
                tenant_id,
                database_id,
                collection,
                document_id,
                *surrogate,
            )
            .await?;
            Ok(read
                .rows
                .as_ref()
                .and_then(|row| row.get(def.period_column.as_str()))
                .and_then(|v| v.as_str())
                .map(str::to_string))
        }
        // `period_lock_gated_collection` only routes these eight variants
        // here, and the caller handles `BatchInsert`, `PointUpdate`,
        // `BulkUpdate`, and `BulkDelete` before reaching this function —
        // every other variant is unreachable.
        DocumentOp::BatchInsert { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => Ok(None),
    }
}

/// Resolve every DISTINCT period value across a page of row bodies to its
/// reference row's surrogate, one entry per value — mirroring how the
/// materialized-sum resolution dedupes a page onto one entry per distinct
/// join value.
pub(super) async fn resolve_batch_period_values(
    state: &SharedState,
    bodies: &[&[u8]],
    def: &PeriodLockDef,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<ResolvedSumTarget>> {
    let mut resolved: Vec<ResolvedSumTarget> = Vec::new();
    for body in bodies {
        let Some(period_key) = join_value_from_body(body, &def.period_column) else {
            continue;
        };
        resolve_one_target(&mut resolved, &def.ref_table, period_key, async |key| {
            lookup_period_surrogate(state, &def.ref_table, key, tenant_id, database_id, trace_id)
                .await
        })
        .await?;
    }
    Ok(resolved)
}
