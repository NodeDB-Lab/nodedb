// SPDX-License-Identifier: BUSL-1.1

//! Catalog-local repair for recoverable referential-integrity violations.
//!
//! Missing owner rows are reconstructed from primary records. Owner rows that
//! reference a missing user are reassigned through the tenant's authoritative
//! ownership fallback, updating the primary record and `StoredOwner` together.
//! Grants to missing users are revoked rather than transferred. Any violation
//! that cannot be repaired deterministically remains fatal to startup.

use tracing::{info, warn};

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::{StoredOwner, object_type};

use super::divergence::{Divergence, DivergenceKind};

/// Attempt every deterministic repair represented by `violations`.
///
/// Returns `(remaining, healed)`. Registry caches are intentionally not
/// mutated here; the caller reloads divergent registries from the repaired
/// catalog before startup proceeds.
pub fn heal_orphan_rows(
    catalog: &SystemCatalog,
    violations: Vec<Divergence>,
) -> (Vec<Divergence>, usize) {
    let mut remaining = Vec::with_capacity(violations.len());
    let mut healed = 0usize;

    for divergence in violations {
        let repaired = match &divergence.kind {
            DivergenceKind::OrphanRow {
                kind,
                key,
                expected_parent_kind: "owner",
            } => repair_missing_owner(catalog, kind, key),
            DivergenceKind::DanglingReference {
                from_kind: "owner",
                from_key,
                to_kind: "user",
                to_key,
            } => repair_dangling_owner(catalog, from_key, to_key),
            DivergenceKind::DanglingReference {
                from_kind: "permission",
                from_key,
                to_kind: "user",
                to_key,
            } => revoke_dangling_grant(catalog, from_key, to_key),
            _ => false,
        };

        if repaired {
            healed += 1;
        } else {
            remaining.push(divergence);
        }
    }

    (remaining, healed)
}

fn repair_missing_owner(catalog: &SystemCatalog, kind: &str, key: &str) -> bool {
    let Some((database_id, tenant_id, name)) = parse_object_key(key) else {
        return false;
    };
    let Some(owner_username) = primary_row_owner(catalog, kind, database_id, tenant_id, &name)
    else {
        return false;
    };
    let stored = StoredOwner {
        database_id,
        object_type: kind.to_string(),
        object_name: name.clone(),
        tenant_id,
        owner_username: owner_username.clone(),
    };
    match catalog.put_owner(&stored) {
        Ok(()) => {
            info!(kind, tenant_id, object = %name, owner = %owner_username,
                "catalog sanity check: reconstructed missing owner row");
            true
        }
        Err(error) => {
            warn!(kind, tenant_id, object = %name, %error,
                "catalog sanity check: could not reconstruct missing owner row");
            false
        }
    }
}

fn repair_dangling_owner(catalog: &SystemCatalog, from_key: &str, missing_user: &str) -> bool {
    let Some((kind, database_id, tenant_id, name)) = parse_owner_reference(from_key) else {
        return false;
    };
    if kind != object_type::INDEX
        && let Some(primary_owner) = primary_row_owner(catalog, kind, database_id, tenant_id, &name)
        && user_exists_in_tenant(catalog, tenant_id, &primary_owner)
    {
        let owner = StoredOwner {
            database_id,
            object_type: kind.to_string(),
            object_name: name.clone(),
            tenant_id,
            owner_username: primary_owner.clone(),
        };
        return match catalog.put_owner(&owner) {
            Ok(()) => {
                info!(kind, tenant_id, object = %name, owner = %primary_owner,
                    "catalog sanity check: restored owner row from canonical primary owner");
                true
            }
            Err(error) => {
                warn!(kind, tenant_id, object = %name, owner = %primary_owner, %error,
                    "catalog sanity check: could not restore canonical owner row");
                false
            }
        };
    }

    let replacement = match catalog.resolve_ownership_fallback(tenant_id, missing_user) {
        Ok(Some(username)) => username,
        Ok(None) => return false,
        Err(error) => {
            warn!(tenant_id, owner = missing_user, %error,
                "catalog sanity check: could not resolve ownership fallback");
            return false;
        }
    };

    match catalog.rewrite_object_owner(kind, database_id, tenant_id, &name, &replacement) {
        Ok(()) => {
            info!(kind, tenant_id, object = %name, owner = %replacement,
                "catalog sanity check: reassigned dangling owner reference");
            true
        }
        Err(error) => {
            warn!(kind, tenant_id, object = %name, owner = %replacement, %error,
                "catalog sanity check: could not reassign dangling owner reference");
            false
        }
    }
}

fn user_exists_in_tenant(catalog: &SystemCatalog, tenant_id: u64, username: &str) -> bool {
    match catalog.load_all_users() {
        Ok(users) => users
            .into_iter()
            .any(|user| user.tenant_id == tenant_id && user.username == username),
        Err(error) => {
            warn!(tenant_id, user = username, %error,
                "catalog sanity check: could not validate canonical owner");
            false
        }
    }
}

fn revoke_dangling_grant(catalog: &SystemCatalog, from_key: &str, missing_user: &str) -> bool {
    let grantee = format!("user:{missing_user}");
    let grants = match catalog.load_all_permissions() {
        Ok(grants) => grants,
        Err(error) => {
            warn!(user = missing_user, %error,
                "catalog sanity check: could not load dangling grants");
            return false;
        }
    };
    let Some(grant) = grants.into_iter().find(|grant| {
        grant.grantee == grantee && format!("{}:{}", grant.target, grant.grantee) == from_key
    }) else {
        return false;
    };

    match catalog.delete_permission(&grant.target, &grant.grantee, &grant.permission) {
        Ok(()) => {
            info!(target = %grant.target, grantee = %grant.grantee,
                permission = %grant.permission,
                "catalog sanity check: revoked grant to missing user");
            true
        }
        Err(error) => {
            warn!(target = %grant.target, grantee = %grant.grantee,
                permission = %grant.permission, %error,
                "catalog sanity check: could not revoke grant to missing user");
            false
        }
    }
}

fn parse_object_key(key: &str) -> Option<(u64, u64, String)> {
    let mut parts = key.splitn(3, ':');
    let database_id = parts.next()?.parse().ok()?;
    let tenant_id = parts.next()?.parse().ok()?;
    Some((database_id, tenant_id, parts.next()?.to_string()))
}

fn parse_owner_reference(key: &str) -> Option<(&str, u64, u64, String)> {
    let mut parts = key.splitn(4, ':');
    let kind = parts.next()?;
    let database_id = parts.next()?.parse().ok()?;
    let tenant_id = parts.next()?.parse().ok()?;
    let name = parts.next()?.to_string();
    Some((kind, database_id, tenant_id, name))
}

fn primary_row_owner(
    catalog: &SystemCatalog,
    kind: &str,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) -> Option<String> {
    match kind {
        object_type::COLLECTION => catalog
            .get_collection(nodedb_types::DatabaseId::new(database_id), tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::FUNCTION => catalog
            .get_function(tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::PROCEDURE => catalog
            .get_procedure(tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::TRIGGER => catalog
            .get_trigger(tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::MATERIALIZED_VIEW => catalog
            .get_materialized_view(tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::SEQUENCE => catalog
            .get_sequence(tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::SCHEDULE => catalog
            .load_all_schedules()
            .ok()?
            .into_iter()
            .find(|stored| stored.tenant_id == tenant_id && stored.name == name)
            .map(|stored| stored.owner),
        object_type::CHANGE_STREAM => catalog
            .get_change_stream(tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        object_type::CONTINUOUS_AGGREGATE => catalog
            .get_continuous_aggregate(database_id, tenant_id, name)
            .ok()
            .flatten()
            .map(|stored| stored.owner),
        _ => None,
    }
}
