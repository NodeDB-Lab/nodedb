// SPDX-License-Identifier: BUSL-1.1

//! The shared "dedupe, resolve, push" step every materialized-sum and
//! period-lock resolution loop performs once per candidate `(target, value)`
//! pair.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::Surrogate;

/// Resolve one `(target, value)` pair into `resolved`, deduping on the pair
/// and skipping the push when `lookup` names no row.
///
/// One entry per DISTINCT `(target, value)` pair: a page or scan that touches
/// the same target row many times resolves it once, so every caller checks
/// `resolved` before spending a lookup on a value it already holds.
///
/// `lookup` differs by caller: a period-lock lookup can name no reference row
/// (`Ok(None)` skips the push, leaving the value unresolved for
/// `check_period_lock` to refuse), while a materialized-sum lookup always
/// resolves or fails outright — its `Err` propagates through the `?` below
/// before `Ok(None)` could ever apply.
pub async fn resolve_one_target(
    resolved: &mut Vec<ResolvedSumTarget>,
    target: &str,
    value: String,
    lookup: impl AsyncFnOnce(&str) -> crate::Result<Option<Surrogate>>,
) -> crate::Result<()> {
    if resolved.iter().any(|entry| entry.addresses(target, &value)) {
        return Ok(());
    }
    if let Some(surrogate) = lookup(&value).await? {
        resolved.push(ResolvedSumTarget::new(target, value, surrogate));
    }
    Ok(())
}
