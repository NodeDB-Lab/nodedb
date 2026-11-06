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
//! The catalog (propose + single-node fallback), cascade dependent
//! enumeration, soft vs hard delete, implicit-sequence sweep, and audit pair
//! run here. The result is the protocol-neutral `DdlResult` / `DdlError`.

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
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
/// - `name`: collection, decoded by the parser via `check_identifier`
///   (a quoted name keeps its case, a bare name lowercases).
/// - `if_exists`: suppress `42P01` when the target does not exist.
/// - `purge`: hard-delete via `PurgeCollection` (admin only).
/// - `cascade` / `cascade_force`: reject for now (atomic batched
///   propose path not landed).
///
/// Security invariant: `IF EXISTS` does not bypass authz. A caller
/// without ownership or admin rights gets `42501` (permission denied)
/// regardless of whether the target actually exists — this prevents
/// using error-code differences to probe collection existence.
pub async fn drop_collection(
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
        crate::control::cascade::collect_dependents(
            catalog,
            database_id,
            tenant_id.as_u64(),
            name,
            &mut visited,
        )
        .map_err(|e| DdlError::from_error(&e))?
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
        .get_owner("collection", database_id.as_u64(), tenant_id, name)
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
    // which an owner alone cannot invoke.
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
    // propose — re-running a drop that's already a no-op must not
    // spawn extra raft rounds or audit noise. The `if_exists` case
    // joins them on the absent-name branch.
    {
        let catalog = state.credentials.catalog();
        if catalog
            .get_materialized_view(database_id.as_u64(), tenant_id.as_u64(), name)
            .map_err(|error| DdlError::from_error(&error))?
            .is_some()
        {
            return Err(err(
                "2BP01",
                format!(
                    "collection '{name}' is owned by a materialized view; drop the materialized view"
                ),
            ));
        }
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

    // Best-effort operator trail: this row is buffered and can be lost in a
    // crash. The durable record is the proposer's `ActionIntent` row, which
    // syncs before the propose below and refuses the statement if it fails.
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
            // Frozen by the proposer's stamp.
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        }
    } else {
        crate::control::catalog_entry::CatalogEntry::DeactivateCollection {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: name.to_string(),
            // Sentinel: the proposer freezes the real version and HLC through
            // `descriptor_stamp::stamp` before the entry is replicated.
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    };
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|error| DdlError::from_error(&error))?;

    // Cascade: drop implicit sequences (SERIAL/BIGSERIAL fields create
    // {coll}_{field}_seq). Each delete replicates, so every node's catalog
    // and sequence registry lose it.
    let catalog = state.credentials.catalog();
    let seqs = catalog
        .load_sequences_for_tenant(database_id.as_u64(), tenant_id.as_u64())
        .map_err(|e| DdlError::from_error_in_context("failed to list sequences", &e))?;
    let prefix = format!("{name}_");
    for seq in seqs
        .iter()
        .filter(|seq| seq.name.starts_with(&prefix) && seq.name.ends_with("_seq"))
    {
        let entry = crate::control::catalog_entry::CatalogEntry::DeleteSequence {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: seq.name.clone(),
            // Frozen by the proposer's stamp.
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        };
        super::super::replicate::propose_and_apply_async(state, &entry).await?;
    }

    // Emit a second audit record with the completion status so the
    // intent + outcome pair is visible to auditors. If the process
    // dies after propose returned but before this line, the pre-propose
    // intent record alone is enough to reconstruct the history.
    let completion = if purge {
        format!(
            "purged collection '{name}' (log_index={})",
            outcome.log_index()
        )
    } else {
        format!(
            "dropped collection '{name}' (log_index={})",
            outcome.log_index()
        )
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
