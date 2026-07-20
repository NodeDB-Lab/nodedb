// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral index DDL: CREATE INDEX, DROP INDEX.
//!
//! CREATE/DROP INDEX mutate the owning [`StoredCollection`]'s `indexes`
//! vector and commit a `CatalogEntry::PutCollection`. The replicated
//! applier's `put_async` post-apply hook fans out a fresh `Register` to
//! every node's Data Plane (including this leader), so `doc_configs`
//! reflects the new index before the next write arrives. The `indexes`
//! ownership keys (`permissions.propose_owner("index", ...)`) continue
//! to back SHOW INDEXES (served by the protocol-neutral DDL router).
//!
//! Ported from the pgwire `ddl::collection::index` handler. The async
//! data-plane pipeline (two-phase Building→Ready backfill, peer fan-out,
//! `dispatch_register_from_stored`, owner propose/delete) is preserved
//! verbatim; only the result construction changed from pgwire `Response`
//! / `Tag` to the protocol-neutral `DdlResult` / `DdlError`.

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::{IndexBuildState, StoredIndex};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::TraceId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

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

/// Commit a mutated [`StoredCollection`] through the replicated metadata
/// Raft group (cluster) or straight to the local `SystemCatalog`
/// (single-node fallback), then re-dispatch a `Register` to this node's
/// Data Plane so the new index vector lands in `doc_configs` immediately.
async fn commit_collection_mutation(
    state: &SharedState,
    coll: &crate::control::security::catalog::StoredCollection,
    database_id: DatabaseId,
) -> Result<(), DdlError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", e.to_string()))?;
    if log_index == 0 {
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_collection(database_id, coll)
                .map_err(|e| err("XX000", e.to_string()))?;
        }
        // Single-node path bypasses the applier post-apply hook, so the
        // Register refresh has to be fired here. In cluster mode the
        // applier's `put_async` does it on every node.
        super::dispatch_register_from_stored(state, coll)
            .await
            .map_err(|e| err("XX000", e.to_string()))?;
    }
    Ok(())
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
}

/// CREATE [UNIQUE] INDEX [name] ON <collection> (<field>) [WHERE condition]
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

    // Reject duplicates within this collection.
    if coll.indexes.iter().any(|i| i.name == index_name) {
        return Err(err(
            "42710",
            format!("index '{index_name}' already exists on '{collection}'"),
        ));
    }

    let index_owner = coll.owner.clone();
    let canonical_field = normalize_index_field(field);
    let is_array = canonical_field.ends_with("[]");
    let extraction_path = canonical_field
        .strip_suffix("[]")
        .unwrap_or(&canonical_field)
        .to_string();

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

    // Phase 2: dispatch the backfill op. This runs on the local Data
    // Plane (single-node) or the leader (cluster — distributed backfill
    // across vShards is handled inside the handler by the existing scan
    // primitive, which is vShard-local per core). UNIQUE violations here
    // surface as a Data Plane error; we propagate as SQLSTATE 23505 and
    // leave the index in `Building` so a subsequent retry can DROP + try
    // with a wider data fix.
    let vshard = crate::types::VShardId::from_collection_in_database(database_id, collection);
    let backfill_plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::BackfillIndex {
            collection: collection.to_string(),
            path: extraction_path.clone(),
            is_array,
            unique: is_unique,
            case_insensitive,
            predicate: where_condition.clone(),
        },
    );
    let backfill_resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        backfill_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", e.to_string()))?;

    if backfill_resp.status == crate::bridge::envelope::Status::Error {
        let detail = match backfill_resp.error_code.as_deref() {
            Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
            Some(other) => format!("{other:?}"),
            None => String::from_utf8_lossy(&backfill_resp.payload).into_owned(),
        };
        let code = if detail.to_lowercase().contains("unique") {
            "23505"
        } else {
            "XX000"
        };
        return Err(err(code, detail));
    }

    // Phase 2b: fan the same backfill op to every other cluster node.
    // `execute_backfill_index` is vShard-local per core, so without
    // this step non-coordinator nodes never populate the index for
    // the rows they host — the silent-miss bug. Single-node and
    // peerless clusters short-circuit inside the helper.
    super::index_fanout::backfill_on_peers(
        state,
        super::index_fanout::PeerBackfill {
            tenant_id,
            database_id,
            collection,
            path: &extraction_path,
            is_array,
            unique: is_unique,
            case_insensitive,
            predicate: where_condition.as_deref(),
        },
    )
    .await?;

    // Phase 3: flip to Ready. Re-read the collection so any concurrent
    // mutation (e.g. another DDL on the same collection — blocked by
    // descriptor drain in cluster mode, serialized by pgwire session in
    // single-node) is folded in before we rewrite the index vector.
    if let Some(latest) = catalog
        .get_collection(database_id, tenant_id.as_u64(), collection)
        .ok()
        .flatten()
    {
        let mut ready_coll = latest;
        for idx in ready_coll.indexes.iter_mut() {
            if idx.name == index_name {
                idx.state = IndexBuildState::Ready;
            }
        }
        commit_collection_mutation(state, &ready_coll, database_id).await?;
    }

    // Ownership record backs SHOW INDEXES — keep the existing ledger.
    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        "index",
        tenant_id,
        &index_name,
        &index_owner,
    )
    .map_err(|e| err(&e.sqlstate, e.message))?;

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

    Ok(vec![DdlResult::Status {
        command: "CREATE INDEX".to_string(),
        rows_affected: None,
    }])
}

/// DROP INDEX <name>
pub async fn drop_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 3 {
        return Err(err("42601", "syntax: DROP INDEX <name>"));
    }

    let index_name = parts[2].to_string();
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner_in_database("index", database_id.as_u64(), tenant_id, &index_name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(err(
            "42501",
            "permission denied: must be index owner or admin",
        ));
    }

    // Locate the owning collection via catalog scan. Every index lives on
    // exactly one collection; scanning is cheap relative to Raft commit.
    let catalog = state.credentials.catalog();
    let collections = catalog
        .load_collections_for_tenant(database_id, tenant_id.as_u64())
        .map_err(|e| err("XX000", e.to_string()))?;
    let mut owning = collections
        .into_iter()
        .find(|c| c.indexes.iter().any(|i| i.name == index_name));

    if let Some(coll) = owning.as_mut() {
        let dropped_field = coll
            .indexes
            .iter()
            .find(|i| i.name == index_name)
            .map(|i| i.field.clone());
        coll.indexes.retain(|i| i.name != index_name);
        commit_collection_mutation(state, coll, database_id).await?;

        // Purge existing index entries from the sparse engine so stale
        // rows don't leak into future lookups on a re-created index of
        // the same name. Best-effort — the Data Plane itself is the
        // authority, so a failure here is logged rather than propagated.
        if let Some(field) = dropped_field {
            let vshard =
                crate::types::VShardId::from_collection_in_database(database_id, &coll.name);
            let plan = crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::DropIndex {
                    collection: coll.name.clone(),
                    field,
                },
            );
            if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                state,
                tenant_id,
                database_id,
                vshard,
                plan,
                TraceId::ZERO,
            )
            .await
            {
                tracing::warn!(
                    index = %index_name,
                    collection = %coll.name,
                    error = %e,
                    "failed to dispatch DropIndex to Data Plane (non-fatal)"
                );
            }
        }
    } else {
        // No owning collection found — still tear down the ownership
        // record so repeated DROP INDEX is idempotent even for legacy
        // indexes created before catalog-backed storage.
        tracing::debug!(
            index = %index_name,
            "DROP INDEX: no owning collection in catalog, removing ownership record only"
        );
    }

    crate::control::server::shared::ddl::owner::propose_delete_owner(
        state,
        "index",
        tenant_id,
        &index_name,
    )
    .map_err(|e| err(&e.sqlstate, e.message))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped index '{index_name}'"),
    );

    Ok(vec![DdlResult::Status {
        command: "DROP INDEX".to_string(),
        rows_affected: None,
    }])
}
