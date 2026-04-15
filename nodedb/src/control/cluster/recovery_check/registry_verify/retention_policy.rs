//! `RetentionPolicyRegistry` verifier.
//!
//! Checks that the in-memory `RetentionPolicyRegistry` is consistent
//! with the `_system.retention_policies` redb table.
//!
//! **What it checks:**
//! - Every policy in redb has a matching entry in memory
//!   (key = `{tenant_id}|{name}`, value encodes `enabled` and
//!   `collection` so mutations to either field surface).
//! - Every policy in memory has a backing redb row.
//!
//! **What it does NOT check:**
//! - Whether the target collection exists or is active. The spec
//!   notes that a deactivated collection is a warning, and a missing
//!   collection is an error — but those cross-entity checks require
//!   the collections table and are deferred to a future integrity pass.
//!   This verifier strictly covers load_from coherence.

use crate::control::security::catalog::SystemCatalog;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_retention_policies(
    registry: &RetentionPolicyRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_retention_policies()?
        .into_iter()
        .map(|p| {
            let key = format!("{}|{}", p.tenant_id, p.name);
            let value = format!("en={},coll={}", p.enabled, p.collection);
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .list_all()
        .into_iter()
        .map(|p| {
            let key = format!("{}|{}", p.tenant_id, p.name);
            let value = format!("en={},coll={}", p.enabled, p.collection);
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "retention_policies",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "retention_policies",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "retention_policies",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_retention_policies(
    registry: &RetentionPolicyRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    registry.clear_and_reload(catalog)
}
