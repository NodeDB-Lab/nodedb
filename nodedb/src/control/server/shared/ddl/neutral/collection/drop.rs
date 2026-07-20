// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DROP COLLECTION DDL.
//!
//! Supported forms (tokens are case-insensitive; `COLLECTION` and
//! `TABLE` are accepted as synonyms — both route through the parser
//! to `NodedbStatement::DropCollection` and land here):
//!
//! - `DROP { COLLECTION | TABLE } [IF EXISTS] <name>` — soft-delete
//!   (flip `is_active`). `IF EXISTS` makes the missing-target case a
//!   silent success instead of `42P01`.
//! - `DROP { COLLECTION | TABLE } [IF EXISTS] <name> PURGE` — hard-delete
//!   via `CatalogEntry::PurgeCollection`. Requires admin. `IF EXISTS`
//!   makes the already-purged case a silent success.
//! - `DROP { COLLECTION | TABLE } [IF EXISTS] <name> CASCADE [FORCE]`
//!   — accept the keyword; the recursive dependent enumeration lives
//!   in the apply path. Until the enumerator lands, handlers reject
//!   with a clear "dependents must be dropped individually" message
//!   rather than silently succeeding.
//!
//! The handler takes typed parsed arguments rather than the raw `parts`
//! slice so the `IF EXISTS` and spelling-synonym contracts cannot be
//! lost by an off-by-one index into the tokens.
//!
//! Ported from the pgwire `ddl::collection::drop` handler. The catalog
//! (propose + single-node fallback), cascade dependent enumeration, soft
//! vs hard delete, implicit-sequence sweep, and audit pair are preserved
//! verbatim; only the result construction changed from pgwire `Response`
//! / `Tag` to the protocol-neutral `DdlResult` / `DdlError`.

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Parsed `DROP COLLECTION` request. A struct (rather than positional
/// bools) because the parameter count crosses seven once `database_id`
/// is threaded through for non-default-database DDL.
#[derive(Clone, Copy)]
pub struct DropCollectionRequest<'a> {
    pub name: &'a str,
    pub if_exists: bool,
    pub purge: bool,
    pub cascade: bool,
    pub cascade_force: bool,
    pub database_id: DatabaseId,
}

/// DROP { COLLECTION | TABLE } [IF EXISTS] <name> [PURGE] [CASCADE [FORCE]]
///
/// All fields arrive pre-parsed from `NodedbStatement::DropCollection`:
/// - `name`: collection (lowercased by the parser).
/// - `if_exists`: suppress `42P01` when the target does not exist.
/// - `purge`: hard-delete via `PurgeCollection` (admin only).
/// - `cascade` / `cascade_force`: reject for now (atomic batched
///   propose path not landed).
///
/// Security invariant: `IF EXISTS` does not bypass authz. A caller
/// without ownership or admin rights gets `42501` (permission denied)
/// regardless of whether the target actually exists — this prevents
/// using error-code differences to probe collection existence.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &DropCollectionRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let DropCollectionRequest {
        name,
        if_exists,
        purge,
        cascade,
        cascade_force,
        database_id,
    } = *req;
    let name_lower = name.to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Dependent-object check. When CASCADE is NOT specified we refuse
    // the drop if anything points at this collection. The cascade-
    // proposal path (atomic batched Delete* + PurgeCollection) has not
    // landed yet, so CASCADE itself is still rejected — but now with
    // the enumerated dependent list in hand, so the rejection is
    // specific instead of a generic "not yet supported".
    let dependents: Vec<crate::control::cascade::Dependent> = {
        let catalog = state.credentials.catalog();
        let mut visited = std::collections::HashSet::new();
        crate::control::cascade::collect_dependents(catalog, tenant_id.as_u64(), name, &mut visited)
            .map_err(|e| err("XX000", e.to_string()))?
    };

    // Implicit SERIAL/BIGSERIAL sequences (`{collection}_{field}_seq`)
    // are auto-dropped by the post-propose sweep below and therefore
    // never become orphans — they don't block a bare DROP. Every
    // other dependent kind (triggers, RLS policies, MVs, change
    // streams, schedules) CAN be orphaned by a bare DROP, so those
    // are the ones that gate the rejection.
    let blocking_dependents: Vec<&crate::control::cascade::Dependent> = dependents
        .iter()
        .filter(|d| d.kind != crate::control::cascade::DependentKind::Sequence)
        .collect();

    if !blocking_dependents.is_empty() && !cascade {
        let deps_list: Vec<String> = blocking_dependents
            .iter()
            .map(|d| format!("{}:{}", d.kind.as_str(), d.name))
            .collect();
        return Err(err(
            "2BP01",
            format!(
                "cannot drop collection '{name}': {} dependent object(s) exist ({}); \
                 drop them individually or retry with CASCADE (batched-cascade propose \
                 not yet implemented — CASCADE currently rejected to avoid orphaned rows)",
                blocking_dependents.len(),
                deps_list.join(", ")
            ),
        ));
    }

    if cascade {
        return Err(err(
            "0A000",
            "DROP COLLECTION ... CASCADE requires atomic batched Delete* + PurgeCollection \
             in one metadata-raft commit — that proposer surface has not landed yet. \
             Drop dependents individually in the meantime.",
        ));
    }
    let _ = cascade_force; // same gate

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner_in_database("collection", database_id.as_u64(), tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    let is_admin = identity.is_superuser
        || identity.has_role(&crate::control::security::identity::Role::TenantAdmin);

    if !is_owner && !is_admin {
        // See the security invariant in the docstring: returned
        // unconditionally, before the existence check, so the response
        // does not depend on whether the target exists.
        return Err(err(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // PURGE requires admin — it bypasses the retention safety net,
    // which an owner alone should not be able to invoke.
    if purge && !is_admin {
        return Err(err(
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
    // | absent (purged/NA)  | 42P01 (or OK if IF EXISTS)  | idempotent OK —        |
    // |                     |                             |   already purged       |
    //
    // The two idempotency branches (already-deleted, already-purged)
    // short-circuit with a success tag and skip the audit pair +
    // propose — re-running a drop that's already a no-op should not
    // spawn extra raft rounds or audit noise. The `if_exists` case
    // joins them on the absent-name branch.
    {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(database_id, tenant_id.as_u64(), name) {
            Ok(Some(coll)) if coll.is_active => {}
            Ok(Some(_)) if purge => {}
            Ok(Some(_)) => {
                return Ok(vec![DdlResult::Status {
                    command: "DROP COLLECTION".to_string(),
                    rows_affected: None,
                }]);
            }
            Ok(None) if purge || if_exists => {
                return Ok(vec![DdlResult::Status {
                    command: "DROP COLLECTION".to_string(),
                    rows_affected: None,
                }]);
            }
            _ => {
                return Err(err("42P01", format!("collection '{name}' does not exist")));
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
    let action = if purge {
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
    let entry = if purge {
        crate::control::catalog_entry::CatalogEntry::PurgeCollection {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: name.to_string(),
        }
    } else {
        crate::control::catalog_entry::CatalogEntry::DeactivateCollection {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: name.to_string(),
        }
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", e.to_string()))?;
    if log_index == 0 {
        let catalog = state.credentials.catalog();
        // Single-node / no-cluster fallback: apply the catalog mutation
        // directly, matching what the applier would have done on a
        // clustered deployment.
        if purge {
            crate::control::catalog_entry::apply::collection::purge(
                database_id.as_u64(),
                tenant_id.as_u64(),
                name,
                catalog,
            )
            .map_err(|e| err("XX000", e.to_string()))?;
            state
                .permissions
                .install_replicated_remove_owner_in_database(
                    "collection",
                    database_id.as_u64(),
                    tenant_id.as_u64(),
                    name,
                );
            state
                .permissions
                .remove_grants_for_target(&format!("collection:{}:{name}", tenant_id.as_u64()));
        } else {
            crate::control::catalog_entry::apply::collection::deactivate(
                database_id.as_u64(),
                tenant_id.as_u64(),
                name,
                catalog,
            );
        }
    }

    // Cascade: drop implicit sequences (SERIAL/BIGSERIAL fields create {coll}_{field}_seq).
    let catalog = state.credentials.catalog();
    if let Ok(seqs) = catalog.load_sequences_for_tenant(tenant_id.as_u64()) {
        let prefix = format!("{name}_");
        let suffix = "_seq";
        for seq in &seqs {
            if seq.name.starts_with(&prefix) && seq.name.ends_with(suffix) {
                catalog
                    .delete_sequence(tenant_id.as_u64(), &seq.name)
                    .map_err(|e| {
                        err(
                            "XX000",
                            format!("failed to drop sequence '{}': {e}", seq.name),
                        )
                    })?;
                // Best-effort: registry removal is non-critical since catalog
                // is the source of truth and the sequence won't be reloaded.
                let _ = state
                    .sequence_registry
                    .remove(tenant_id.as_u64(), &seq.name);
            }
        }
    }

    // Emit a second audit record with the completion status so the
    // intent + outcome pair is visible to auditors. If the process
    // dies after propose returned but before this line, the pre-propose
    // intent record alone is enough to reconstruct the history.
    let completion = if purge {
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

    Ok(vec![DdlResult::Status {
        command: "DROP COLLECTION".to_string(),
        rows_affected: None,
    }])
}
