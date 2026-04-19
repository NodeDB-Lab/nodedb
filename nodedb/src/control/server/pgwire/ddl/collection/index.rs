//! Index DDL: CREATE INDEX, DROP INDEX, SHOW INDEXES.
//!
//! CREATE/DROP INDEX mutate the owning [`StoredCollection`]'s `indexes`
//! vector and commit a `CatalogEntry::PutCollection`. The replicated
//! applier's `put_async` post-apply hook fans out a fresh `Register` to
//! every node's Data Plane (including this leader), so `doc_configs`
//! reflects the new index before the next write arrives. The `indexes`
//! ownership keys (`permissions.propose_owner("index", ...)`) continue
//! to back SHOW INDEXES.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::{IndexBuildState, StoredIndex};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

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
) -> Result<(), pgwire::error::PgWireError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_collection(coll)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
        // Single-node path bypasses the applier post-apply hook, so the
        // Register refresh has to be fired here. In cluster mode the
        // applier's `put_async` does it on every node.
        super::create::dispatch_register_from_stored(state, coll).await;
    }
    Ok(())
}

/// CREATE [UNIQUE] INDEX <name> ON <collection> (<field>) [WHERE condition]
///
/// Creates an index by appending a [`StoredIndex`] to the collection's
/// `indexes` vector and committing the mutation through `PutCollection`.
/// UNIQUE enforces uniqueness at write pre-commit. COLLATE NOCASE lowercases
/// the indexed value. WHERE defines a partial index predicate.
pub async fn create_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Detect UNIQUE modifier.
    let is_unique = upper.contains("UNIQUE INDEX");
    let idx_offset = if is_unique { 3 } else { 2 }; // skip "CREATE UNIQUE INDEX" vs "CREATE INDEX"

    if parts.len() < idx_offset + 2 {
        return Err(sqlstate_error(
            "42601",
            "CREATE INDEX requires at least: ON <collection> (<field>)",
        ));
    }

    // Detect whether index name is present or omitted.
    // If parts[idx_offset] is "ON", the name was omitted.
    let (index_name_owned, on_offset) = if parts[idx_offset].eq_ignore_ascii_case("ON") {
        // No name — will auto-generate after parsing field.
        (String::new(), idx_offset)
    } else {
        // Named: parts[idx_offset] is the name, parts[idx_offset+1] should be ON.
        if parts.len() < idx_offset + 3 {
            return Err(sqlstate_error(
                "42601",
                &format!(
                    "CREATE INDEX {}: expected ON <collection> (<field>) after index name",
                    parts[idx_offset]
                ),
            ));
        }
        if !parts[idx_offset + 1].eq_ignore_ascii_case("ON") {
            return Err(sqlstate_error("42601", "expected ON after index name"));
        }
        (parts[idx_offset].to_lowercase(), idx_offset + 1)
    };

    if parts.len() <= on_offset + 1 {
        return Err(sqlstate_error("42601", "expected collection name after ON"));
    }

    // Handle both "collection (field)", "collection(field)", and "collection FIELDS field".
    let (collection, field) = {
        let raw = parts[on_offset + 1];
        if let Some(paren_pos) = raw.find('(') {
            let coll = &raw[..paren_pos];
            let fld = raw[paren_pos..].trim_matches(|c| c == '(' || c == ')');
            (coll.to_string(), fld.to_string())
        } else if parts.len() > on_offset + 2 {
            let next = parts[on_offset + 2];
            if next.eq_ignore_ascii_case("FIELDS") && parts.len() > on_offset + 3 {
                // FIELDS keyword form: ON collection FIELDS field
                (raw.to_string(), parts[on_offset + 3].to_string())
            } else {
                // Parenthesized form: ON collection (field)
                let fld = next.trim_matches(|c| c == '(' || c == ')');
                (raw.to_string(), fld.to_string())
            }
        } else {
            return Err(sqlstate_error(
                "42601",
                &format!(
                    "missing field specification for index on '{}': use (<field>) or FIELDS <field>",
                    raw
                ),
            ));
        }
    };

    // Auto-generate name if omitted.
    let index_name = if index_name_owned.is_empty() {
        format!("idx_{}_{}", collection, field)
    } else {
        index_name_owned
    };

    // Parse optional WHERE condition for conditional indexes.
    let where_condition = upper
        .find(" WHERE ")
        .map(|pos| sql[pos + 7..].trim().to_string());

    // Parse optional COLLATE NOCASE for case-insensitive indexes.
    let case_insensitive = upper.contains("COLLATE NOCASE") || upper.contains("COLLATE CI");
    let tenant_id = identity.tenant_id;

    // Verify collection exists, capture it, and check CREATE permission.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error(
            "XX000",
            "catalog unavailable: CREATE INDEX requires persisted collections",
        ));
    };
    let mut coll = match catalog.get_collection(tenant_id.as_u32(), &collection) {
        Ok(Some(c)) if c.is_active => c,
        _ => {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            ));
        }
    };

    let is_owner = coll.owner == identity.username;
    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: must be collection owner or admin to create indexes",
        ));
    }

    // Reject duplicates within this collection.
    if coll.indexes.iter().any(|i| i.name == index_name) {
        return Err(sqlstate_error(
            "42710",
            &format!("index '{index_name}' already exists on '{collection}'"),
        ));
    }

    let index_owner = coll.owner.clone();
    let canonical_field = normalize_index_field(&field);
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

    commit_collection_mutation(state, &coll).await?;

    // Phase 2: dispatch the backfill op. This runs on the local Data
    // Plane (single-node) or the leader (cluster — distributed backfill
    // across vShards is handled inside the handler by the existing scan
    // primitive, which is vShard-local per core). UNIQUE violations here
    // surface as a Data Plane error; we propagate as SQLSTATE 23505 and
    // leave the index in `Building` so a subsequent retry can DROP + try
    // with a wider data fix.
    let vshard = crate::types::VShardId::from_collection(&collection);
    let backfill_plan = crate::bridge::envelope::PhysicalPlan::Document(
        crate::bridge::physical_plan::DocumentOp::BackfillIndex {
            collection: collection.clone(),
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
        vshard,
        backfill_plan,
        0,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if backfill_resp.status == crate::bridge::envelope::Status::Error {
        let detail = match &backfill_resp.error_code {
            Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
            Some(other) => format!("{other:?}"),
            None => String::from_utf8_lossy(&backfill_resp.payload).into_owned(),
        };
        let code = if detail.to_lowercase().contains("unique") {
            "23505"
        } else {
            "XX000"
        };
        return Err(sqlstate_error(code, &detail));
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
            collection: &collection,
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
        .get_collection(tenant_id.as_u32(), &collection)
        .ok()
        .flatten()
    {
        let mut ready_coll = latest;
        for idx in ready_coll.indexes.iter_mut() {
            if idx.name == index_name {
                idx.state = IndexBuildState::Ready;
            }
        }
        commit_collection_mutation(state, &ready_coll).await?;
    }

    // Ownership record backs SHOW INDEXES — keep the existing ledger.
    super::super::owner_propose::propose_owner(
        state,
        "index",
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

    Ok(vec![Response::Execution(Tag::new("CREATE INDEX"))])
}

/// DROP INDEX <name>
pub async fn drop_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP INDEX <name>"));
    }

    let index_name = parts[2].to_string();
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("index", tenant_id, &index_name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: must be index owner or admin",
        ));
    }

    // Locate the owning collection via catalog scan. Every index lives on
    // exactly one collection; scanning is cheap relative to Raft commit.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error(
            "XX000",
            "catalog unavailable: DROP INDEX requires persisted collections",
        ));
    };
    let collections = catalog
        .load_collections_for_tenant(tenant_id.as_u32())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
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
        commit_collection_mutation(state, coll).await?;

        // Purge existing index entries from the sparse engine so stale
        // rows don't leak into future lookups on a re-created index of
        // the same name. Best-effort — the Data Plane itself is the
        // authority, so a failure here is logged rather than propagated.
        if let Some(field) = dropped_field {
            let vshard = crate::types::VShardId::from_collection(&coll.name);
            let plan = crate::bridge::envelope::PhysicalPlan::Document(
                crate::bridge::physical_plan::DocumentOp::DropIndex {
                    collection: coll.name.clone(),
                    field,
                },
            );
            if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                state, tenant_id, vshard, plan, 0,
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

    super::super::owner_propose::propose_delete_owner(state, "index", tenant_id, &index_name)?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped index '{index_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP INDEX"))])
}

/// SHOW INDEXES [ON <collection>]
///
/// Lists indexes for the current tenant (optionally filtered by collection).
pub fn show_indexes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    // Parse optional ON <collection> filter.
    let filter_collection = if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("INDEXES")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        Some(parts[3])
    } else {
        None
    };

    let schema = Arc::new(vec![
        text_field("index_name"),
        text_field("type"),
        text_field("owner"),
    ]);

    // List all index types for this tenant.
    let index_types = [
        ("index", "btree"),
        ("vector_index", "vector"),
        ("fulltext_index", "fulltext"),
        ("spatial_index", "spatial"),
    ];

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (owner_type, display_type) in &index_types {
        let indexes = state.permissions.list_owners(owner_type, tenant_id);
        for (index_name, owner) in &indexes {
            if let Some(coll) = filter_collection
                && !index_name.starts_with(coll)
            {
                continue;
            }

            encoder
                .encode_field(index_name)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(display_type)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(owner)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
