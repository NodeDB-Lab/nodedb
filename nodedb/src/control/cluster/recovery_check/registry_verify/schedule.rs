//! `ScheduleRegistry` verifier.
//!
//! Checks that the in-memory `ScheduleRegistry` is consistent with
//! the `_system.schedules` redb table.
//!
//! **What it checks:**
//! - Every schedule in redb has a matching entry in memory
//!   (key = `{tenant_id}|{name}`, value encodes `enabled` and
//!   `cron_expr` so an ALTER SCHEDULE mutation surfaces as a
//!   value mismatch).
//! - Every schedule in memory has a backing redb row (ghost
//!   entries from a buggy load_from path).
//!
//! **What it does NOT check:**
//! - Whether the cron expression is valid (parsing is a runtime
//!   concern, not a catalog coherence concern).
//! - Whether the SQL body references a live collection or function.

use crate::control::security::catalog::SystemCatalog;
use crate::event::scheduler::ScheduleRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_schedules(
    registry: &ScheduleRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_schedules()?
        .into_iter()
        .map(|s| {
            let key = format!("{}|{}", s.tenant_id, s.name);
            let value = format!("en={},cron={}", s.enabled, s.cron_expr);
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .list_all()
        .into_iter()
        .map(|s| {
            let key = format!("{}|{}", s.tenant_id, s.name);
            let value = format!("en={},cron={}", s.enabled, s.cron_expr);
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "schedules",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "schedules",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "schedules",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_schedules(registry: &ScheduleRegistry, catalog: &SystemCatalog) -> crate::Result<()> {
    registry.clear_and_reload(catalog)
}
