//! DROP COLLECTION DDL.
//!
//! Supported forms (tokens are case-insensitive):
//!
//! - `DROP COLLECTION <n>` — soft-delete (flip `is_active`).
//! - `DROP COLLECTION <n> PURGE` — hard-delete via
//!   `CatalogEntry::PurgeCollection`. Requires admin.
//! - `DROP COLLECTION <n> CASCADE` / `... PURGE CASCADE` /
//!   `... CASCADE FORCE` — accept the keyword; the recursive dependent
//!   enumeration lives in the apply path Until the enumerator lands,
//!   handlers reject with a clear "dependents must be dropped
//!   individually" message rather than silently succeeding.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// Flags parsed off the `DROP COLLECTION <name> [IF EXISTS] [PURGE] [CASCADE [FORCE]]`
/// trailing tokens.
#[derive(Debug, Default, Clone, Copy)]
struct DropFlags {
    purge: bool,
    cascade: bool,
    cascade_force: bool,
}

fn parse_drop_flags(parts: &[&str]) -> DropFlags {
    let mut f = DropFlags::default();
    let upper: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();
    for (i, tok) in upper.iter().enumerate() {
        match tok.as_str() {
            "PURGE" => f.purge = true,
            "CASCADE" => {
                f.cascade = true;
                if upper.get(i + 1).map(String::as_str) == Some("FORCE") {
                    f.cascade_force = true;
                }
            }
            "FORCE" if i > 0 && upper[i - 1] == "CASCADE" => {
                // already handled above
            }
            _ => {}
        }
    }
    f
}

/// DROP COLLECTION <name> [PURGE] [CASCADE [FORCE]]
///
/// Marks collection as inactive (or hard-deletes on PURGE).
/// Requires owner or admin.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP COLLECTION <name>"));
    }

    let flags = parse_drop_flags(parts);

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Dependent-object check. When CASCADE is NOT specified we refuse
    // the drop if anything points at this collection. The cascade-
    // proposal path (atomic batched Delete* + PurgeCollection) has not
    // landed yet, so CASCADE itself is still rejected — but now with
    // the enumerated dependent list in hand, so the rejection is
    // specific instead of a generic "not yet supported".
    let dependents: Vec<crate::control::cascade::Dependent> = if let Some(catalog) =
        state.credentials.catalog()
    {
        let mut visited = std::collections::HashSet::new();
        crate::control::cascade::collect_dependents(catalog, tenant_id.as_u32(), name, &mut visited)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
    } else {
        Vec::new()
    };

    if !dependents.is_empty() && !flags.cascade {
        let deps_list: Vec<String> = dependents
            .iter()
            .map(|d| format!("{}:{}", d.kind.as_str(), d.name))
            .collect();
        return Err(sqlstate_error(
            "2BP01",
            &format!(
                "cannot drop collection '{name}': {} dependent object(s) exist ({}); \
                 drop them individually or retry with CASCADE (batched-cascade propose \
                 not yet implemented — CASCADE currently rejected to avoid orphaned rows)",
                dependents.len(),
                deps_list.join(", ")
            ),
        ));
    }

    if flags.cascade {
        return Err(sqlstate_error(
            "0A000",
            "DROP COLLECTION ... CASCADE requires atomic batched Delete* + PurgeCollection \
             in one metadata-raft commit — that proposer surface has not landed yet. \
             Drop dependents individually in the meantime.",
        ));
    }
    let _ = flags.cascade_force; // same gate

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("collection", tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    let is_admin = identity.is_superuser
        || identity.has_role(&crate::control::security::identity::Role::TenantAdmin);

    if !is_owner && !is_admin {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // PURGE requires admin — it bypasses the retention safety net,
    // which an owner alone should not be able to invoke.
    if flags.purge && !is_admin {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser or tenant_admin may DROP COLLECTION ... PURGE",
        ));
    }

    // Existence + idempotency check. The matrix:
    //
    // | catalog state       | DROP (soft)                 | DROP PURGE             |
    // |---------------------|-----------------------------|------------------------|
    // | active              | proceed                     | proceed (upgrade)      |
    // | soft-deleted        | idempotent OK — already     | proceed (upgrade to    |
    // |                     |   soft-deleted              |   hard-delete)         |
    // | absent (purged/NA)  | 42P01 "does not exist"      | idempotent OK —        |
    // |                     |                             |   already purged       |
    //
    // The two idempotency branches short-circuit with a success tag
    // and skip the audit pair + propose — re-running a drop that's
    // already a no-op should not spawn extra raft rounds or audit
    // noise.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), name) {
            Ok(Some(coll)) if coll.is_active => {}
            Ok(Some(_)) if flags.purge => {}
            Ok(Some(_)) => {
                return Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))]);
            }
            Ok(None) if flags.purge => {
                return Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))]);
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    // Audit the user's intent BEFORE mutating the catalog. Ordering
    // is load-bearing for forensic completeness: if the process
    // crashes between the audit durable-write and the catalog row
    // delete, restart leaves the audit record present + the row
    // still present, so the purge can be retried cleanly with full
    // history. The alternative (audit after delete) loses the trail
    // on a crash window.
    let action = if flags.purge {
        format!("requested purge of collection '{name}'")
    } else {
        format!("requested drop of collection '{name}'")
    };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &action,
    );

    // Propose the drop through the metadata raft group. The applier
    // on every node decodes the entry, performs the appropriate
    // mutation, and (for PurgeCollection) triggers the async
    // storage-reclaim dispatch on every node symmetrically.
    let entry = if flags.purge {
        crate::control::catalog_entry::CatalogEntry::PurgeCollection {
            tenant_id: tenant_id.as_u32(),
            name: name.to_string(),
        }
    } else {
        crate::control::catalog_entry::CatalogEntry::DeactivateCollection {
            tenant_id: tenant_id.as_u32(),
            name: name.to_string(),
        }
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        // Single-node / no-cluster fallback: apply the catalog mutation
        // directly, matching what the applier would have done on a
        // clustered deployment.
        if flags.purge {
            catalog
                .delete_collection(tenant_id.as_u32(), name)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        } else if let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), name) {
            coll.is_active = false;
            catalog
                .put_collection(&coll)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
    }

    // Cascade: drop implicit sequences (SERIAL/BIGSERIAL fields create {coll}_{field}_seq).
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(seqs) = catalog.load_sequences_for_tenant(tenant_id.as_u32())
    {
        let prefix = format!("{name}_");
        let suffix = "_seq";
        for seq in &seqs {
            if seq.name.starts_with(&prefix) && seq.name.ends_with(suffix) {
                catalog
                    .delete_sequence(tenant_id.as_u32(), &seq.name)
                    .map_err(|e| {
                        sqlstate_error(
                            "XX000",
                            &format!("failed to drop sequence '{}': {e}", seq.name),
                        )
                    })?;
                // Best-effort: registry removal is non-critical since catalog
                // is the source of truth and the sequence won't be reloaded.
                let _ = state
                    .sequence_registry
                    .remove(tenant_id.as_u32(), &seq.name);
            }
        }
    }

    // Emit a second audit record with the completion status so the
    // intent + outcome pair is visible to auditors. If the process
    // dies after propose returned but before this line, the pre-propose
    // intent record alone is enough to reconstruct the history.
    let completion = if flags.purge {
        format!("purged collection '{name}' (log_index={log_index})")
    } else {
        format!("dropped collection '{name}' (log_index={log_index})")
    };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &completion,
    );

    Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
}
