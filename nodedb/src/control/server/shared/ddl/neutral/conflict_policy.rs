// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral handlers for CRDT conflict-policy DDL.
//!
//! - `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`
//! - `SHOW CONFLICT POLICY ON <name>`
//!
//! Conflict policies are persisted on the collection's catalog record
//! (`StoredCollection::conflict_policy`, mirroring `bitemporal`) instead of
//! living only in the in-memory per-core `PolicyRegistry`. The read-modify-
//! write cycle now targets the catalog directly: read the durable policy (or
//! `CollectionPolicy::ephemeral()` when none has ever been persisted),
//! apply the partial update, and write back via `CatalogEntry::PutCollection`
//! — the existing `PutCollection` post-apply hook re-broadcasts
//! `DocumentOp::Register` to every Data Plane core (live) and boot
//! rehydration replays the same path, so the policy survives a restart.
//! The SQLSTATE codes and messages are unchanged from the previous
//! Data-Plane-round-trip implementation.

use serde_json::{Map, Value as JsonValue};

use nodedb_crdt::policy::{CollectionPolicy, ConflictPolicy};
use nodedb_sql::ddl_ast::alter_ops::{ConflictPolicyKind, ConstraintKindKeyword};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::catalog::propose_and_apply;
use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Handle `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`.
///
/// Implements a read-modify-write cycle against the collection's catalog
/// record (mirroring `alter_collection_set_retention`):
/// 1. Read `StoredCollection::conflict_policy` (or `CollectionPolicy::ephemeral()`
///    when none has ever been persisted).
/// 2. Replace the targeted constraint-kind field.
/// 3. Write the updated policy back via `CatalogEntry::PutCollection` and bump
///    `schema_version`. The `PutCollection` post-apply hook re-broadcasts
///    `DocumentOp::Register` to every Data Plane core, rehydrating the
///    per-core `PolicyRegistry` — durably, so the policy survives a restart.
pub async fn alter_set_on_conflict(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    policy_kind: &ConflictPolicyKind,
    constraint_kind: &ConstraintKindKeyword,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(database_id, tenant_id, collection)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{collection}' not found")))?;

    // Step 1: read the durable policy, falling back to the same ephemeral
    // default the in-memory `PolicyRegistry` uses for an unregistered
    // collection.
    let mut policy: CollectionPolicy = match &coll.conflict_policy {
        Some(json) => sonic_rs::from_str(json).map_err(|e| err("XX000", e.to_string()))?,
        None => CollectionPolicy::ephemeral(),
    };

    // Step 2: apply the partial update.
    let new_conflict_policy = resolve_policy_kind(policy_kind);
    apply_conflict_policy(&mut policy, constraint_kind, new_conflict_policy);

    // Step 3: persist on the catalog record and re-broadcast.
    let policy_json = sonic_rs::to_string(&policy).map_err(|e| err("XX000", e.to_string()))?;
    coll.conflict_policy = Some(policy_json);
    let entry = CatalogEntry::PutCollection(Box::new(coll));
    propose_and_apply(state, &entry)?;
    state.schema_version.bump();

    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String("OK".to_string()));
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["result".to_string()],
        vec![row],
    ))])
}

/// Handle `SHOW CONFLICT POLICY ON <collection>`.
///
/// Returns one row with a single `policy` column containing the JSON-serialized
/// `CollectionPolicy`, read straight from the durable catalog record. Falls
/// back to the ephemeral default when no policy has ever been persisted.
pub async fn show_conflict_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let coll = catalog
        .get_collection(database_id, tenant_id, collection)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{collection}' not found")))?;

    let policy: CollectionPolicy = match &coll.conflict_policy {
        Some(json) => sonic_rs::from_str(json).map_err(|e| err("XX000", e.to_string()))?,
        None => CollectionPolicy::ephemeral(),
    };
    let text = sonic_rs::to_string(&policy).map_err(|e| err("XX000", e.to_string()))?;

    let mut row = Map::new();
    row.insert("policy".to_string(), JsonValue::String(text));
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["policy".to_string()],
        vec![row],
    ))])
}

fn resolve_policy_kind(kind: &ConflictPolicyKind) -> ConflictPolicy {
    match kind {
        ConflictPolicyKind::LastWriterWins => ConflictPolicy::LastWriterWins,
        ConflictPolicyKind::RenameSuffix => ConflictPolicy::RenameSuffix,
        ConflictPolicyKind::CascadeDefer => ConflictPolicy::CascadeDefer {
            max_retries: 3,
            ttl_secs: 300,
        },
        ConflictPolicyKind::EscalateToDlq => ConflictPolicy::EscalateToDlq,
    }
}

fn apply_conflict_policy(
    policy: &mut CollectionPolicy,
    kind: &ConstraintKindKeyword,
    conflict_policy: ConflictPolicy,
) {
    match kind {
        ConstraintKindKeyword::Unique => policy.unique = conflict_policy,
        ConstraintKindKeyword::ForeignKey => policy.foreign_key = conflict_policy,
        ConstraintKindKeyword::NotNull => policy.not_null = conflict_policy,
        ConstraintKindKeyword::Check => policy.check = conflict_policy,
    }
}
