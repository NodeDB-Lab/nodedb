//! `RoleStore` verifier.
//!
//! `RoleStore::load_from` converts `StoredRole` into
//! `CustomRole`. We compare by `name` key with the value
//! encoding `tenant_id` + parent role — these are the fields
//! the rest of the system relies on.

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::role::RoleStore;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_roles(store: &RoleStore, catalog: &SystemCatalog) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_roles()?
        .into_iter()
        .map(|r| {
            let value = format!("{}|{}", r.tenant_id, r.parent);
            (r.name, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = store
        .list_roles()
        .into_iter()
        .map(|r| {
            let parent = r.parent.unwrap_or_default();
            let value = format!("{}|{}", r.tenant_id.as_u32(), parent);
            (r.name, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "roles",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "roles",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "roles",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear the in-memory role map and re-run `load_from`.
pub fn repair_roles(store: &RoleStore, catalog: &SystemCatalog) -> crate::Result<()> {
    store.clear_and_reload(catalog)
}
