//! `RlsPolicyStore` verifier.
//!
//! Checks that the in-memory `RlsPolicyStore` is consistent with
//! the `_system.rls_policies` redb table.
//!
//! **What it checks:**
//! - Every policy in redb has a matching entry in the in-memory store
//!   (key = `{tenant_id}|{collection}|{name}`, value encodes
//!   `enabled` flag so enable/disable mutations surface).
//! - Every policy in memory has a matching row in redb (ghost entries
//!   from a buggy load_from path).
//!
//! **What it does NOT check:**
//! - Whether the target collection is active or even exists — that
//!   cross-entity check is deferred to a future integrity pass.
//!   The verifier strictly covers load_from coherence.

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::rls::RlsPolicyStore;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_rls_policies(
    store: &RlsPolicyStore,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_rls_policies()?
        .into_iter()
        .map(|p| {
            let key = format!("{}|{}|{}", p.tenant_id, p.collection, p.name);
            let value = format!("en={}", p.enabled);
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = store
        .list_all_flat()
        .into_iter()
        .map(|p| {
            let key = format!("{}|{}|{}", p.tenant_id, p.collection, p.name);
            let value = format!("en={}", p.enabled);
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "rls_policies",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "rls_policies",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "rls_policies",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear in-memory store and reload from redb.
pub fn repair_rls_policies(store: &RlsPolicyStore, catalog: &SystemCatalog) -> crate::Result<()> {
    store.clear_and_reload(catalog)
}
