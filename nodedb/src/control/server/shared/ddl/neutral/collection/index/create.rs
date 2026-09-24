// SPDX-License-Identifier: BUSL-1.1

//! `CREATE [UNIQUE] INDEX` on a collection field.
//!
//! CREATE INDEX mutates the owning [`StoredCollection`]'s `indexes` vector and
//! commits a `CatalogEntry::PutCollection`. The replicated applier's
//! `put_async` post-apply hook fans out a fresh `Register` to every node's
//! Data Plane (including this leader), so `doc_configs` reflects the new index
//! before the next write arrives.
//!
//! The index is also registered in the catalog index registry, which is what
//! `SHOW INDEXES` lists and `DROP INDEX` resolves. The ownership row backs
//! authorization and is filed under the collection's own database so a
//! database-scoped owner lookup finds it.
//!
//! [`StoredCollection`]: crate::control::security::catalog::StoredCollection

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::{IndexBuildState, IndexKind, StoredIndex};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::index_registry::{
    IndexRegistration, propose_index_record,
};
use crate::control::server::shared::session::ddl_buffer;
use crate::control::server::shared::session::ddl_effect::{DeferredDdlEffect, SecondaryIndexBuild};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::super::result::{DdlError, DdlResult};
use super::build::build_secondary_index;
use super::commit::{commit_collection_mutation, err};

/// Normalize a user-supplied field reference into the canonical JSON path
/// used by the sparse-index extraction (`$.field` / `$.nested.field`).
/// Plain column names gain the `$.` prefix; already-prefixed paths are
/// returned unchanged.
fn normalize_index_field(field: &str) -> String {
    if field.starts_with("$.") || field.starts_with('$') {
        field.to_string()
    } else {
        format!("$.{field}")
    }
}

/// Parsed `CREATE INDEX` request.
#[derive(Clone, Copy)]
pub struct CreateIndexRequest<'a> {
    pub is_unique: bool,
    pub index_name_opt: Option<&'a str>,
    pub collection: &'a str,
    pub field: &'a str,
    pub case_insensitive: bool,
    pub where_condition: Option<&'a str>,
    pub database_id: DatabaseId,
    /// `IF NOT EXISTS` — an index of this name that already exists makes the
    /// statement a successful no-op instead of SQLSTATE 42710. Only the
    /// name-already-taken checks are relaxed; every other failure (missing
    /// collection, permission denied, catalog read fault, backfill error)
    /// still surfaces.
    pub if_not_exists: bool,
}

/// Successful completion tag for `CREATE INDEX`, shared by the real create
/// path and the `IF NOT EXISTS` no-op so a client cannot tell them apart.
fn create_index_ok() -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: "CREATE INDEX".to_string(),
        rows_affected: None,
    }]
}

/// CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON <collection> (<field>) [WHERE condition]
///
/// Creates an index by appending a [`StoredIndex`] to the collection's
/// `indexes` vector and committing the mutation through `PutCollection`.
/// UNIQUE enforces uniqueness at write pre-commit. COLLATE NOCASE lowercases
/// the indexed value. WHERE defines a partial index predicate.
///
/// All fields are pre-parsed by the `nodedb-sql` AST layer.
pub async fn create_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateIndexRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateIndexRequest {
        is_unique,
        index_name_opt,
        collection,
        field,
        case_insensitive,
        where_condition,
        database_id,
        if_not_exists,
    } = *req;
    if collection.is_empty() {
        return Err(err(
            "42601",
            "CREATE INDEX requires at least: ON <collection> (<field>)",
        ));
    }

    // Auto-generate name if omitted.
    let index_name = match index_name_opt {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => format!("idx_{}_{}", collection, field),
    };

    let where_condition = where_condition.map(|s| s.to_string());
    let tenant_id = identity.tenant_id;

    // Verify collection exists, capture it, and check CREATE permission.
    let catalog = state.credentials.catalog();
    let mut coll = match catalog.get_collection(database_id, tenant_id.as_u64(), collection) {
        Ok(Some(c)) if c.is_active => c,
        _ => {
            return Err(err(
                "42P01",
                format!("collection '{collection}' does not exist"),
            ));
        }
    };

    let is_owner = coll.owner == identity.username;
    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(err(
            "42501",
            "permission denied: must be collection owner or admin to create indexes",
        ));
    }

    // Reject duplicates within this collection. `IF NOT EXISTS` turns this
    // into a no-op, matching CREATE COLLECTION IF NOT EXISTS.
    if coll.indexes.iter().any(|i| i.name == index_name) {
        if if_not_exists {
            return Ok(create_index_ok());
        }
        return Err(err(
            "42710",
            format!("index '{index_name}' already exists on '{collection}'"),
        ));
    }

    // Reject a name already taken by an index of any kind in this database:
    // the registry is keyed by name, so two kinds sharing one name would make
    // exactly one of them droppable. The registry read itself must still fail
    // loudly — only a genuine name collision is absorbed by `IF NOT EXISTS`.
    if let Some(existing) = catalog
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), &index_name)
        .map_err(|e| err("XX000", e.to_string()))?
    {
        if if_not_exists {
            return Ok(create_index_ok());
        }
        return Err(err(
            "42710",
            format!(
                "index '{index_name}' already exists on '{}' ({})",
                existing.collection,
                existing.kind.display_type()
            ),
        ));
    }

    let index_owner = coll.owner.clone();
    let canonical_field = normalize_index_field(field);
    let is_array = canonical_field.ends_with("[]");
    let extraction_path = canonical_field
        .strip_suffix("[]")
        .unwrap_or(&canonical_field)
        .to_string();

    // A key-value collection's index lives in the KV engine, which indexes
    // one top-level field by equality. Refuse what it cannot build before
    // any catalog entry is written.
    if coll.collection_type.is_kv() {
        super::kv_index::kv_field(
            collection,
            &canonical_field,
            &super::kv_index::KvIndexOptions {
                unique: is_unique,
                case_insensitive,
                predicate: where_condition.as_deref(),
            },
        )?;
    }

    // Two-phase Building→Ready pipeline. Phase 1: stamp `Building` and
    // commit — readers skip the index (planner filters to Ready), writers
    // dual-write (extraction iterates every registered path regardless of
    // state). Phase 2: backfill existing rows, fail on UNIQUE violations,
    // then commit a second PutCollection flipping to `Ready`. The planner
    // only rewrites queries to IndexLookup once Phase 2 commits, so the
    // index is never observable in a half-built state.
    coll.indexes.push(StoredIndex {
        name: index_name.clone(),
        field: canonical_field.clone(),
        unique: is_unique,
        case_insensitive,
        predicate: where_condition.clone(),
        state: IndexBuildState::Building,
        owner: index_owner.clone(),
    });

    commit_collection_mutation(state, &coll, database_id).await?;

    // Phase 2 and 3: backfill on every node, then flip to Ready. Inside an
    // explicit transaction the build waits for COMMIT, after the Building
    // entry lands; a ROLLBACK discards it with the entry.
    let build = SecondaryIndexBuild {
        tenant_id,
        database_id,
        collection: collection.to_string(),
        index_name: index_name.clone(),
        extraction_path: extraction_path.clone(),
        is_array,
        unique: is_unique,
        case_insensitive,
        predicate: where_condition.clone(),
    };
    if !ddl_buffer::defer_effect(DeferredDdlEffect::SecondaryIndexBuild(build.clone())) {
        build_secondary_index(state, &build).await?;
    }

    // Identity record: what SHOW INDEXES lists and DROP INDEX resolves.
    propose_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: &index_name,
            kind: IndexKind::Secondary,
            collection,
            fields: vec![canonical_field.clone()],
        },
    )?;

    // Ownership record backs authorization for later ALTER / DROP.
    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        IndexKind::Secondary.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
        &index_owner,
    )?;

    let kind = if is_unique { "unique index" } else { "index" };
    let ci = if case_insensitive {
        " COLLATE NOCASE"
    } else {
        ""
    };
    let cond = where_condition
        .as_deref()
        .map(|c| format!(" WHERE {c}"))
        .unwrap_or_default();
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created {kind} '{index_name}' on '{collection}' ({canonical_field}){ci}{cond}"),
    );

    Ok(create_index_ok())
}
