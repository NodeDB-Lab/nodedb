// SPDX-License-Identifier: BUSL-1.1

//! Period-value extraction for `PointUpdate`, whose write touches both a
//! pre-image and a post-image.

use nodedb_physical::physical_plan::{ResolvedSumTarget, UpdateValue};

use super::lookup::{PeriodLockScope, lookup_period_surrogate};
use crate::control::planner::materialized_sum::recon::recon_point_row;
use crate::control::planner::materialized_sum::resolve_one_target;
use crate::control::security::catalog::PeriodLockDef;

/// Resolve the period value(s) a `PointUpdate` addresses: the stored row's
/// current period, and the period its assignments would produce, deduped when
/// they are the same value.
///
/// A key matching no stored row resolves nothing — the update rewrites no row
/// and `execute_point_update` reports zero rows affected without ever reaching
/// `check_period_lock`.
pub(super) async fn resolve_update_period_values(
    scope: &PeriodLockScope<'_>,
    collection: &str,
    document_id: &str,
    surrogate: nodedb_types::Surrogate,
    updates: &[(String, UpdateValue)],
    def: &PeriodLockDef,
) -> crate::Result<Vec<ResolvedSumTarget>> {
    let read = recon_point_row(
        scope.state,
        scope.tenant_id,
        scope.database_id,
        collection,
        document_id,
        surrogate,
    )
    .await?;
    let Some(old_row) = read.rows else {
        return Ok(Vec::new());
    };
    let new_row = crate::query::apply_update_assignments(&old_row, updates)?;

    // `resolve_one_target` dedupes on `(target, value)` itself, so the old
    // and new images resolve through the same call whether or not they carry
    // the same period value.
    let mut resolved: Vec<ResolvedSumTarget> = Vec::new();
    for row in [&old_row, &new_row] {
        let Some(value) = row.get(def.period_column.as_str()).and_then(|v| v.as_str()) else {
            continue;
        };
        // No reference row names this period — `resolve_one_target` leaves
        // it unresolved so `check_period_lock` reports the unknown-period
        // refusal for it.
        resolve_one_target(
            &mut resolved,
            &def.ref_table,
            value.to_string(),
            async |key| {
                lookup_period_surrogate(
                    scope.state,
                    &def.ref_table,
                    key,
                    scope.tenant_id,
                    scope.database_id,
                    scope.trace_id,
                )
                .await
            },
        )
        .await?;
    }
    Ok(resolved)
}
