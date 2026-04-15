//! `TriggerRegistry` verifier.

use crate::control::security::catalog::SystemCatalog;
use crate::control::trigger::TriggerRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_triggers(
    registry: &TriggerRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    // Value = `(descriptor_version, enabled, priority)`.
    // `descriptor_version` is bumped by the applier on any
    // mutation, so divergence on it implies either a missed
    // apply or a load_from bug. `enabled` and `priority` are
    // included so ALTER-style field changes that keep the
    // version stable still surface.
    let mut expected: Vec<(String, String)> = catalog
        .load_all_triggers()?
        .into_iter()
        .map(|t| {
            let key = format!("{}|{}|{}", t.tenant_id, t.collection, t.name);
            let value = format!(
                "v={},en={},pri={}",
                t.descriptor_version, t.enabled, t.priority
            );
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .snapshot_all()
        .into_iter()
        .map(|t| {
            let key = format!("{}|{}|{}", t.tenant_id, t.collection, t.name);
            let value = format!(
                "v={},en={},pri={}",
                t.descriptor_version, t.enabled, t.priority
            );
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "triggers",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "triggers",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "triggers",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair path: `TriggerRegistry::load_all` does not clear
/// existing entries, so we build a fresh registry, load into
/// it, and use the installed-during-apply methods on the
/// original registry to flush-and-replace. The simplest way
/// is to expose a `clear_and_install_all` method on the
/// registry — added in the same file.
pub fn repair_triggers(registry: &TriggerRegistry, catalog: &SystemCatalog) -> crate::Result<()> {
    let fresh_rows = catalog.load_all_triggers()?;
    registry.clear_and_install_all(fresh_rows);
    Ok(())
}
