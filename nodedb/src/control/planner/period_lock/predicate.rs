// SPDX-License-Identifier: BUSL-1.1

//! Period-value extraction for the predicate-driven shapes `BulkUpdate` /
//! `BulkDelete`, which name their rows by predicate rather than by body or
//! key.

use nodedb_physical::physical_plan::{ResolvedSumTarget, UpdateValue};

use super::lookup::{PeriodLockScope, lookup_period_surrogate};
use crate::control::planner::materialized_sum::ResolvedTargets;
use crate::control::planner::materialized_sum::recon::recon_scan_rows;
use crate::control::security::catalog::PeriodLockDef;

/// What a predicate-driven statement does to each row it matches — decides
/// whether the resolution also needs the row's POST-image.
pub(super) enum PeriodLockEffect {
    /// The rows are rewritten by the statement's assignments.
    Assign,
    /// The rows are removed; there is no post-image.
    Remove,
}

/// Resolve the period value(s) a `BulkUpdate` / `BulkDelete` addresses, from a
/// reconnaissance scan of the same predicate — mirroring
/// `materialized_sum::predicate::resolve_predicate_sum_targets`.
///
/// A `BulkUpdate` resolves both images of every matched row (the period it
/// currently holds, and the period its assignments would produce), exactly
/// like `resolve_update_period_values`. A `BulkDelete` resolves only the
/// pre-image, its one and only image.
pub(super) async fn resolve_predicate_period_values(
    scope: &PeriodLockScope<'_>,
    collection: &str,
    filters: Vec<u8>,
    updates: &[(String, UpdateValue)],
    effect: PeriodLockEffect,
    def: &PeriodLockDef,
) -> crate::Result<Vec<ResolvedSumTarget>> {
    let read = recon_scan_rows(
        scope.state,
        scope.tenant_id,
        scope.database_id,
        collection,
        filters,
    )
    .await?;

    // `ResolvedTargets` dedupes on `(target, value)` itself, so every row's
    // pre- and post-image resolves through the same call regardless of how
    // many rows or images share a period value.
    let mut resolved = ResolvedTargets::new();
    for row in &read.rows {
        resolve_period_value(scope, def, row, &mut resolved).await?;
        if matches!(effect, PeriodLockEffect::Assign) {
            let new_row = crate::query::apply_update_assignments(row, updates)?;
            resolve_period_value(scope, def, &new_row, &mut resolved).await?;
        }
    }
    Ok(resolved.into_vec())
}

/// Resolve one row's period value into `resolved`, when it carries the
/// configured period column at all. No reference row naming the period
/// leaves it unresolved so `check_period_lock` reports the unknown-period
/// refusal for it.
async fn resolve_period_value(
    scope: &PeriodLockScope<'_>,
    def: &PeriodLockDef,
    row: &serde_json::Value,
    resolved: &mut ResolvedTargets,
) -> crate::Result<()> {
    let Some(value) = row.get(def.period_column.as_str()).and_then(|v| v.as_str()) else {
        return Ok(());
    };
    resolved
        .resolve(&def.ref_table, value.to_string(), async |key| {
            lookup_period_surrogate(
                scope.state,
                &def.ref_table,
                key,
                scope.tenant_id,
                scope.database_id,
                scope.trace_id,
            )
            .await
        })
        .await
}
