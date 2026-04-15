//! `PermissionStore` verifier — covers both grants and
//! ownership maps.

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::permission::PermissionStore;
use crate::control::security::permission::types::{format_permission, owner_key, parse_permission};

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

/// Verify `PermissionStore` against `catalog`. Returns the
/// list of divergences (unrepaired at this point). Caller
/// reports them and drives the repair by re-loading.
pub fn verify_permissions(
    store: &PermissionStore,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut out: Vec<Divergence> = Vec::new();

    // ── Grants ──────────────────────────────────────────
    let mut expected_grants: Vec<(String, String)> = catalog
        .load_all_permissions()?
        .into_iter()
        .filter_map(|sp| {
            // Drop permission strings the in-memory store
            // couldn't parse — the `load_from` path silently
            // skips these, so it would be a false positive to
            // flag them as divergent here.
            parse_permission(&sp.permission).map(|_| {
                let key = format!("{}|{}|{}", sp.target, sp.grantee, sp.permission);
                (key, String::new())
            })
        })
        .collect();
    expected_grants.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual_grants: Vec<(String, String)> = store
        .snapshot_grants()
        .into_iter()
        .map(|g| {
            let key = format!(
                "{}|{}|{}",
                g.target,
                g.grantee,
                format_permission(g.permission)
            );
            (key, String::new())
        })
        .collect();
    actual_grants.sort_by(|a, b| a.0.cmp(&b.0));

    let grant_diff = diff_sorted(&expected_grants, &actual_grants, |_, _| true);
    for (key, _) in &grant_diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "permissions.grants",
            key: key.clone(),
        }));
    }
    for (key, _) in &grant_diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "permissions.grants",
            key: key.clone(),
        }));
    }

    // ── Owners ──────────────────────────────────────────
    let mut expected_owners: Vec<(String, String)> = catalog
        .load_all_owners()?
        .into_iter()
        .map(|o| {
            let key = owner_key(&o.object_type, o.tenant_id, &o.object_name);
            (key, o.owner_username)
        })
        .collect();
    expected_owners.sort_by(|a, b| a.0.cmp(&b.0));

    let actual_owners = store.snapshot_owners();
    // `snapshot_owners` already returns sorted by key.

    let owner_diff = diff_sorted(&expected_owners, &actual_owners, |a, b| a == b);
    for (key, _) in &owner_diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "permissions.owners",
            key: key.clone(),
        }));
    }
    for (key, _) in &owner_diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "permissions.owners",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &owner_diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "permissions.owners",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }

    Ok(out)
}

/// Repair path: swap the in-memory PermissionStore state with
/// a fresh re-load from the same catalog. We construct a new
/// `PermissionStore`, call `load_from`, then copy its grants
/// and owners into the caller's store. Because `PermissionStore`
/// uses interior `RwLock`s on both `grants` and `owners`, we
/// can repair the contents without replacing the struct itself
/// — callers keep their `&PermissionStore` reference.
pub fn repair_permissions(store: &PermissionStore, catalog: &SystemCatalog) -> crate::Result<()> {
    let fresh = PermissionStore::new();
    fresh.load_from(catalog)?;
    // Swap grants/owners wholesale by replicating the fresh
    // snapshot back into the original store. This uses the
    // existing replication-path helpers so every invariant the
    // `install_replicated_*` methods enforce is preserved.
    store.clear_and_install_from(&fresh);
    Ok(())
}
