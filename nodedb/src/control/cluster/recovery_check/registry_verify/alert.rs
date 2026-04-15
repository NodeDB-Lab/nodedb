//! `AlertRegistry` verifier.
//!
//! Checks that the in-memory `AlertRegistry` is consistent with
//! the `_system.alert_rules` redb table.
//!
//! **What it checks:**
//! - Every alert rule in redb has a matching entry in memory
//!   (key = `{tenant_id}|{name}`, value encodes `enabled` and
//!   `collection` so mutations to either field surface).
//! - Every alert rule in memory has a backing redb row.
//!
//! **What it does NOT check:**
//! - Whether the source collection exists or is active. That
//!   cross-entity check is deferred to a future integrity pass.
//!   The verifier strictly covers load_from coherence.

use crate::control::security::catalog::SystemCatalog;
use crate::event::alert::AlertRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_alerts(
    registry: &AlertRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_alert_rules()?
        .into_iter()
        .map(|a| {
            let key = format!("{}|{}", a.tenant_id, a.name);
            let value = format!("en={},coll={}", a.enabled, a.collection);
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .list_all()
        .into_iter()
        .map(|a| {
            let key = format!("{}|{}", a.tenant_id, a.name);
            let value = format!("en={},coll={}", a.enabled, a.collection);
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "alert_rules",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "alert_rules",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "alert_rules",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_alerts(registry: &AlertRegistry, catalog: &SystemCatalog) -> crate::Result<()> {
    registry.clear_and_reload(catalog)
}
