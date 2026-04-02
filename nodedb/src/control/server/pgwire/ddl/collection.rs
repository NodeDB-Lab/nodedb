//! Collection DDL: CREATE COLLECTION, DROP COLLECTION, SHOW COLLECTIONS.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};
use super::schema_validation::{extract_vector_fields, parse_fields_clause};

pub use super::schema_validation::validate_document_schema;

/// CREATE COLLECTION <name> [FIELDS (<field> <type>, ...)]
///
/// Creates a collection owned by the current user in the current tenant.
pub fn create_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE COLLECTION <name> [FIELDS (<field> <type>, ...)]",
        ));
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Check if collection already exists.
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(existing)) = catalog.get_collection(tenant_id.as_u32(), name)
        && existing.is_active
    {
        return Err(sqlstate_error(
            "42P07",
            &format!("collection '{name}' already exists"),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Detect storage mode: WITH storage = 'strict' | 'columnar' | 'kv'.
    let upper = sql_upper_from_parts(parts);
    let collection_type = if upper.contains("STORAGE") && upper.contains("STRICT") {
        let schema = parse_typed_schema(sql).map_err(|e| sqlstate_error("42601", &e))?;
        nodedb_types::CollectionType::strict(schema)
    } else if upper.contains("STORAGE") && upper.contains("COLUMNAR") {
        // Infer columnar profile from column modifiers or explicit profile keyword.
        // Priority: column modifiers (TIME_KEY, SPATIAL_INDEX) > WITH profile = '...'
        let schema = parse_typed_schema(sql).ok();
        let partition_by =
            extract_with_value(sql, "partition_by").unwrap_or_else(|| "1h".to_string());

        // Check column modifiers first.
        let time_key_col = schema.as_ref().and_then(|s| {
            s.columns
                .iter()
                .find(|c| c.is_time_key())
                .map(|c| c.name.clone())
        });
        let spatial_col = schema.as_ref().and_then(|s| {
            s.columns
                .iter()
                .find(|c| c.is_spatial_index())
                .map(|c| c.name.clone())
        });

        if let Some(time_key) = time_key_col {
            nodedb_types::CollectionType::timeseries(time_key, partition_by)
        } else if let Some(geom_col) = spatial_col {
            nodedb_types::CollectionType::spatial(geom_col)
        } else if upper.contains("PROFILE") && upper.contains("TIMESERIES") {
            // Fallback: explicit profile keyword. Find first TIMESTAMP column.
            let time_key = schema
                .as_ref()
                .and_then(|s| {
                    s.columns
                        .iter()
                        .find(|c| c.column_type == nodedb_types::columnar::ColumnType::Timestamp)
                        .map(|c| c.name.clone())
                })
                .unwrap_or_else(|| "timestamp".to_string());
            nodedb_types::CollectionType::timeseries(time_key, partition_by)
        } else if upper.contains("PROFILE") && upper.contains("SPATIAL") {
            // Fallback: explicit profile keyword. Find first GEOMETRY column.
            let geom_col = schema
                .as_ref()
                .and_then(|s| {
                    s.columns
                        .iter()
                        .find(|c| c.column_type == nodedb_types::columnar::ColumnType::Geometry)
                        .map(|c| c.name.clone())
                })
                .unwrap_or_else(|| "geom".to_string());
            nodedb_types::CollectionType::spatial(geom_col)
        } else {
            nodedb_types::CollectionType::columnar()
        }
    } else if super::kv::is_kv_storage_mode(&upper) {
        super::kv::parse_kv_collection(sql, &upper)?
    } else {
        nodedb_types::CollectionType::document()
    };

    // Parse optional FIELDS clause: CREATE COLLECTION name FIELDS (field type, ...)
    let (fields, serial_fields) = parse_fields_clause(parts);

    // For strict/columnar/kv collections, serialize the schema as JSON in timeseries_config
    // (reused for schema storage until StoredCollection gets a dedicated schema field).
    let schema_json = match &collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            sonic_rs::to_string(schema).ok()
        }
        nodedb_types::CollectionType::KeyValue(config) => sonic_rs::to_string(config).ok(),
        _ => None,
    };

    // Parse accounting options: WITH APPEND_ONLY, WITH HASH_CHAIN, WITH BALANCED ON (...).
    let append_only = upper.contains("APPEND_ONLY");
    let hash_chain = upper.contains("HASH_CHAIN");
    if hash_chain && !append_only {
        return Err(sqlstate_error("42601", "HASH_CHAIN requires APPEND_ONLY"));
    }
    let balanced = parse_balanced_clause(&upper).map_err(|e| sqlstate_error("42601", &e))?;

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type,
        timeseries_config: schema_json,
        is_active: true,
        append_only,
        hash_chain,
        balanced,
        last_chain_hash: None,
    };

    // Persist to catalog.
    if let Some(catalog) = state.credentials.catalog() {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Set ownership.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .set_owner(
            "collection",
            tenant_id,
            name,
            &identity.username,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // If vector fields are declared, dispatch SetVectorParams for each.
    let vector_fields = extract_vector_fields(&coll.fields);
    if !vector_fields.is_empty() {
        for (field_name, _dim, metric) in &vector_fields {
            // Use default HNSW params (m=16, ef=200) with the declared metric.
            // The field_name becomes the named vector field key.
            tracing::info!(
                %name,
                field = %field_name,
                %metric,
                "auto-configuring vector field"
            );
            // Note: SetVectorParams is dispatched later when the first insert
            // arrives, because the Data Plane core is selected by vShard routing
            // at dispatch time. The catalog stores the declaration; the Data Plane
            // honors it on first insert via the field_name in VectorInsert.
        }
    }

    // Auto-create implicit sequences for SERIAL/BIGSERIAL fields.
    for field_name in &serial_fields {
        let seq_name = format!("{name}_{field_name}_seq");
        let mut seq_def = crate::control::security::catalog::sequence_types::StoredSequence::new(
            tenant_id.as_u32(),
            seq_name.clone(),
            identity.username.clone(),
        );
        seq_def.created_at = now;
        if let Some(catalog) = state.credentials.catalog() {
            let _ = catalog.put_sequence(&seq_def);
        }
        let _ = state.sequence_registry.create(seq_def);
        tracing::info!(collection = %name, field = %field_name, sequence = %seq_name, "auto-created SERIAL sequence");
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))])
}

/// Dispatch a `DocumentOp::Register` to the Data Plane after collection creation.
///
/// Tells the Data Plane core about the collection's storage mode (schemaless vs strict)
/// so it encodes documents correctly. For schemaless collections this is optional
/// (MessagePack is the default), but for strict collections it's required (Binary Tuple
/// encoding needs the schema).
pub async fn dispatch_register_if_needed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) {
    let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
    let tenant_id = identity.tenant_id;

    // Look up the just-created collection to get its type.
    let Some(catalog) = state.credentials.catalog() else {
        return;
    };
    let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), &name) else {
        return;
    };

    // Determine storage mode from collection type — exhaustive match
    // ensures new CollectionType variants get a compile error here.
    let storage_mode = match &coll.collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            crate::bridge::physical_plan::StorageMode::Strict {
                schema: schema.clone(),
            }
        }
        nodedb_types::CollectionType::KeyValue(config) => {
            crate::bridge::physical_plan::StorageMode::Strict {
                schema: config.schema.clone(),
            }
        }
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        | nodedb_types::CollectionType::Columnar(_) => {
            crate::bridge::physical_plan::StorageMode::Schemaless
        }
    };

    // Parse index paths from FIELDS clause (if any).
    let (fields, _serial_fields) = super::schema_validation::parse_fields_clause(parts);
    let index_paths: Vec<String> = fields
        .iter()
        .map(|(name, _ty)| format!("$.{name}"))
        .collect();

    let _ = sql; // Reserved for future CRDT detection from SQL.
    let crdt_enabled = false;

    // Build accounting enforcement options from the stored collection metadata.
    let accounting = crate::bridge::physical_plan::AccountingOptions {
        append_only: coll.append_only,
        hash_chain: coll.hash_chain,
        balanced: coll
            .balanced
            .as_ref()
            .map(|b| crate::bridge::physical_plan::BalancedDef {
                group_key_column: b.group_key_column.clone(),
                entry_type_column: b.entry_type_column.clone(),
                debit_value: b.debit_value.clone(),
                credit_value: b.credit_value.clone(),
                amount_column: b.amount_column.clone(),
            }),
    };

    let vshard = crate::types::VShardId::from_collection(&name);
    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        crate::bridge::physical_plan::DocumentOp::Register {
            collection: name.clone(),
            index_paths,
            crdt_enabled,
            storage_mode,
            accounting,
        },
    );

    if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        tracing::warn!(
            %name,
            error = %e,
            "failed to dispatch Register to Data Plane (non-fatal)"
        );
    }
}

/// DROP COLLECTION <name>
///
/// Marks collection as inactive. Requires owner or admin.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP COLLECTION <name>"));
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("collection", tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // Mark as inactive in catalog.
    if let Some(catalog) = state.credentials.catalog() {
        if let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), name) {
            coll.is_active = false;
            catalog
                .put_collection(&coll)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        } else {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{name}' does not exist"),
            ));
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

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
}

/// CREATE [UNIQUE] INDEX <name> ON <collection> (<field>) [WHERE condition]
///
/// Creates an index owned by the collection's owner.
/// UNIQUE enforces uniqueness on the indexed field. WHERE makes it conditional.
pub fn create_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Detect UNIQUE modifier.
    let is_unique = upper.contains("UNIQUE INDEX");
    let idx_offset = if is_unique { 3 } else { 2 }; // skip "CREATE UNIQUE INDEX" vs "CREATE INDEX"

    if parts.len() < idx_offset + 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE [UNIQUE] INDEX <name> ON <collection> (<field>) [WHERE ...]",
        ));
    }

    let index_name = parts[idx_offset];
    if !parts[idx_offset + 1].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[idx_offset + 2];
    let field = parts[idx_offset + 3].trim_matches(|c| c == '(' || c == ')');

    // Parse optional WHERE condition for conditional indexes.
    let where_condition = upper
        .find(" WHERE ")
        .map(|pos| sql[pos + 7..].trim().to_string());

    // Parse optional COLLATE NOCASE for case-insensitive indexes.
    let case_insensitive = upper.contains("COLLATE NOCASE") || upper.contains("COLLATE CI");
    let tenant_id = identity.tenant_id;

    // Verify collection exists and user has CREATE permission.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), collection) {
            Ok(Some(coll)) if coll.is_active => {
                // Check: must be collection owner, superuser, or tenant_admin.
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
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{collection}' does not exist"),
                ));
            }
        }
    }

    // Index ownership inherits from the collection owner.
    let catalog = state.credentials.catalog();
    let index_owner = if let Some(cat) = catalog {
        cat.get_collection(tenant_id.as_u32(), collection)
            .ok()
            .flatten()
            .map(|c| c.owner)
            .unwrap_or_else(|| identity.username.clone())
    } else {
        identity.username.clone()
    };

    state
        .permissions
        .set_owner(
            "index",
            tenant_id,
            index_name,
            &index_owner,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

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
        &format!("created {kind} '{index_name}' on '{collection}' ({field}){ci}{cond}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE INDEX"))])
}

/// DROP INDEX <name>
pub fn drop_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP INDEX <name>"));
    }

    let index_name = parts[2];
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("index", tenant_id, index_name)
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

    // Remove ownership record.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .remove_owner("index", tenant_id, index_name, catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped index '{index_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP INDEX"))])
}

/// DESCRIBE <collection> — show fields, types, and schema info.
pub fn describe_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 2 {
        return Err(sqlstate_error("42601", "syntax: DESCRIBE <collection>"));
    }

    let name_lower = parts[1].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => return Err(sqlstate_error("XX000", "catalog not available")),
    };

    let coll = match catalog.get_collection(tenant_id.as_u32(), name) {
        Ok(Some(c)) if c.is_active => c,
        _ => {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{name}' not found"),
            ));
        }
    };

    let schema = Arc::new(vec![
        text_field("field"),
        text_field("type"),
        text_field("nullable"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    // Always has an 'id' field.
    encoder
        .encode_field(&"id")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&"TEXT")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&"false")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    rows.push(Ok(encoder.take_row()));

    if coll.fields.is_empty() {
        encoder
            .encode_field(&"document")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"JSON")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"true")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    } else {
        for (field_name, field_type) in &coll.fields {
            encoder
                .encode_field(field_name)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(field_type)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&"true")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    // Show storage mode info.
    if coll.collection_type.is_strict()
        || coll.collection_type.is_columnar()
        || coll.collection_type.is_kv()
    {
        encoder
            .encode_field(&"__storage")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.collection_type.as_str())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"false")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    // Timeseries-specific info: show collection_type and config.
    if coll.collection_type.is_timeseries() {
        encoder
            .encode_field(&"__collection_type")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"timeseries")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"false")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));

        if let Some(config) = coll.get_timeseries_config() {
            for (key, value) in config.as_object().into_iter().flatten() {
                let val_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                encoder
                    .encode_field(&format!("__ts_{key}"))
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&val_str)
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&"config")
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                rows.push(Ok(encoder.take_row()));
            }
        }
    }

    // KV-specific info: show TTL policy and key type.
    if let Some(kv_config) = coll.collection_type.kv_config() {
        if let Some(pk) = kv_config.primary_key_column() {
            encoder
                .encode_field(&"__kv_key")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&format!("{} ({})", pk.name, pk.column_type))
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&"false")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
        if let Some(ttl) = &kv_config.ttl {
            let ttl_str = match ttl {
                nodedb_types::KvTtlPolicy::FixedDuration { duration_ms } => {
                    format!("INTERVAL '{duration_ms}ms'")
                }
                nodedb_types::KvTtlPolicy::FieldBased { field, offset_ms } => {
                    format!("{field} + INTERVAL '{offset_ms}ms'")
                }
            };
            encoder
                .encode_field(&"__kv_ttl")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&ttl_str)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&"false")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW COLLECTIONS
///
/// Lists all active collections for the current tenant.
pub fn show_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("owner"),
        int8_field("created_at"),
    ]);

    let collections = if let Some(catalog) = state.credentials.catalog() {
        if identity.is_superuser {
            catalog
                .load_all_collections()
                .unwrap_or_default()
                .into_iter()
                .filter(|c| c.is_active)
                .collect::<Vec<_>>()
        } else {
            catalog
                .load_collections_for_tenant(tenant_id.as_u32())
                .unwrap_or_default()
        }
    } else {
        Vec::new()
    };

    let mut rows = Vec::with_capacity(collections.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for coll in &collections {
        encoder
            .encode_field(&coll.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(coll.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
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

/// ALTER TABLE <name> ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT ...]
pub fn alter_table_add_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let table_name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "ALTER TABLE requires a table name"))?
        .to_lowercase();
    let tenant_id = identity.tenant_id;

    // Find column def after ADD [COLUMN].
    let upper = sql.to_uppercase();
    let add_pos = upper
        .find("ADD COLUMN ")
        .map(|p| p + 11)
        .or_else(|| upper.find("ADD ").map(|p| p + 4))
        .ok_or_else(|| sqlstate_error("42601", "expected ADD [COLUMN]"))?;

    let col_def_str = sql[add_pos..].trim();
    let column = parse_origin_column_def(col_def_str).map_err(|e| sqlstate_error("42601", &e))?;
    let column_name = column.name.clone(); // Save before potential move.

    // Validate: new column must be nullable or have a default.
    if !column.nullable && column.default.is_none() {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                column.name
            ),
        ));
    }

    // Verify collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &table_name) {
            Ok(Some(coll)) if coll.is_active => {
                // Update the stored schema if it's a strict collection.
                if coll.collection_type.is_strict()
                    && let Some(config_json) = &coll.timeseries_config
                    && let Ok(mut schema) =
                        sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(config_json)
                {
                    if schema.columns.iter().any(|c| c.name == column.name) {
                        return Err(sqlstate_error(
                            "42P07",
                            &format!("column '{}' already exists", column.name),
                        ));
                    }
                    schema.columns.push(column);
                    schema.version = schema.version.saturating_add(1);

                    let mut updated = coll;
                    updated.timeseries_config = sonic_rs::to_string(&schema).ok();
                    catalog
                        .put_collection(&updated)
                        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{table_name}' does not exist"),
                ));
            }
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER TABLE '{table_name}' ADD COLUMN '{column_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TABLE"))])
}

/// Reconstruct uppercase SQL from split parts for keyword detection.
fn sql_upper_from_parts(parts: &[&str]) -> String {
    parts.join(" ").to_uppercase()
}

/// Parse `BALANCED ON (group_key = col, debit = 'DEBIT', credit = 'CREDIT', amount = col)`
/// from the uppercase SQL string. Returns `None` if not present.
fn parse_balanced_clause(
    upper: &str,
) -> Result<Option<crate::control::security::catalog::BalancedConstraintDef>, String> {
    let Some(pos) = upper.find("BALANCED ON") else {
        return Ok(None);
    };
    let after = &upper[pos + "BALANCED ON".len()..];
    let after = after.trim_start();
    let Some(paren_start) = after.find('(') else {
        return Err("BALANCED ON requires parenthesized options: (group_key = col, ...)".into());
    };
    let Some(paren_end) = after.find(')') else {
        return Err("BALANCED ON: missing closing parenthesis".into());
    };
    let inner = &after[paren_start + 1..paren_end];

    let mut group_key = None;
    let mut entry_type = None;
    let mut debit = None;
    let mut credit = None;
    let mut amount = None;

    for part in inner.split(',') {
        let part = part.trim();
        if let Some((key, value)) = part.split_once('=') {
            let key = key.trim().to_uppercase();
            let value = value.trim().trim_matches('\'').trim_matches('"');
            match key.as_str() {
                "GROUP_KEY" => group_key = Some(value.to_lowercase()),
                "ENTRY_TYPE" => entry_type = Some(value.to_lowercase()),
                "DEBIT" => debit = Some(value.to_string()),
                "CREDIT" => credit = Some(value.to_string()),
                "AMOUNT" => amount = Some(value.to_lowercase()),
                other => return Err(format!("BALANCED ON: unknown option '{other}'")),
            }
        }
    }

    let group_key = group_key.ok_or("BALANCED ON: missing group_key")?;
    let debit = debit.ok_or("BALANCED ON: missing debit")?;
    let credit = credit.ok_or("BALANCED ON: missing credit")?;
    let amount = amount.ok_or("BALANCED ON: missing amount")?;
    let entry_type = entry_type.unwrap_or_else(|| "entry_type".to_string());

    // Validate column names are safe identifiers (alphanumeric + underscore).
    for (label, col) in [
        ("group_key", group_key.as_str()),
        ("entry_type", entry_type.as_str()),
        ("amount", amount.as_str()),
    ] {
        if col.is_empty() || !col.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "BALANCED ON: {label} must be a valid column name, got '{col}'"
            ));
        }
    }

    Ok(Some(
        crate::control::security::catalog::BalancedConstraintDef {
            group_key_column: group_key,
            entry_type_column: entry_type,
            debit_value: debit,
            credit_value: credit,
            amount_column: amount,
        },
    ))
}

/// Extract a value from a WITH clause: `key = 'value'`.
///
/// Searches the SQL for `key = 'value'` or `key = "value"` patterns.
fn extract_with_value(sql: &str, key: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let key_upper = key.to_uppercase();
    let pos = upper.find(&key_upper)?;
    let after = sql[pos + key.len()..].trim_start();
    let after = after.strip_prefix('=')?;
    let after = after.trim_start();
    let val = after.trim_start_matches('\'').trim_start_matches('"');
    let end = val
        .find('\'')
        .or_else(|| val.find('"'))
        .or_else(|| val.find(','))
        .or_else(|| val.find(')'))
        .unwrap_or(val.len());
    let result = val[..end].trim().to_string();
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Parse column definitions from a CREATE COLLECTION SQL statement into a StrictSchema.
///
/// Extracts the parenthesized column list: `(id BIGINT NOT NULL PRIMARY KEY, name TEXT, ...)`.
/// Auto-generates `_rowid` PK if no PK column is declared.
pub(super) fn parse_typed_schema(
    sql: &str,
) -> Result<nodedb_types::columnar::StrictSchema, String> {
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    // Find parenthesized column definitions.
    let paren_start = sql
        .find('(')
        .ok_or("expected column definitions in parentheses")?;

    // Find matching close paren (handle nested parens for VECTOR(dim)).
    let mut depth = 0;
    let mut paren_end = None;
    for (i, b) in sql.bytes().enumerate().skip(paren_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let paren_end = paren_end.ok_or("unmatched parenthesis")?;
    let col_defs_str = &sql[paren_start + 1..paren_end];

    // Split by top-level commas.
    let mut columns = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in col_defs_str.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                let part = col_defs_str[start..i].trim();
                if !part.is_empty() {
                    columns.push(parse_origin_column_def(part)?);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = col_defs_str[start..].trim();
    if !last.is_empty() {
        columns.push(parse_origin_column_def(last)?);
    }

    if columns.is_empty() {
        return Err("at least one column required".into());
    }

    // Auto-generate _rowid PK if none declared.
    if !columns.iter().any(|c| c.primary_key) {
        columns.insert(
            0,
            ColumnDef::required("_rowid", ColumnType::Int64).with_primary_key(),
        );
    }

    StrictSchema::new(columns).map_err(|e| e.to_string())
}

/// Parse a single column definition: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr]`
fn parse_origin_column_def(s: &str) -> Result<nodedb_types::columnar::ColumnDef, String> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    let upper = s.to_uppercase();
    let tokens: Vec<&str> = s.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err(format!(
            "column definition requires name and type, got: '{s}'"
        ));
    }

    let name = tokens[0].to_lowercase();

    // Find the type string (may span tokens for VECTOR(dim)).
    let keywords = [
        " NOT ",
        " NULL",
        " PRIMARY ",
        " DEFAULT ",
        " TIME_KEY",
        " SPATIAL_INDEX",
    ];
    let type_end = keywords
        .iter()
        .filter_map(|kw| upper[name.len()..].find(kw))
        .min()
        .map(|p| p + name.len())
        .unwrap_or(s.len());
    let type_str = s[name.len()..type_end].trim();

    let column_type: ColumnType = type_str
        .parse()
        .map_err(|e: nodedb_types::columnar::ColumnTypeParseError| e.to_string())?;

    let is_not_null = upper.contains("NOT NULL");
    let is_pk = upper.contains("PRIMARY KEY");
    let nullable = !is_not_null && !is_pk;

    let default = if let Some(pos) = upper.find("DEFAULT ") {
        let after_default = s[pos + 8..].trim();
        let end = keywords
            .iter()
            .filter_map(|kw| after_default.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_default.len());
        let expr = after_default[..end].trim();
        if expr.is_empty() {
            None
        } else {
            Some(expr.to_string())
        }
    } else {
        None
    };

    let mut col = if nullable {
        ColumnDef::nullable(name, column_type)
    } else {
        ColumnDef::required(name, column_type)
    };
    if is_pk {
        col = col.with_primary_key();
    }
    if let Some(d) = default {
        col = col.with_default(d);
    }

    // Column modifiers: TIME_KEY, SPATIAL_INDEX.
    if upper.contains("TIME_KEY") {
        col.modifiers
            .push(nodedb_types::columnar::ColumnModifier::TimeKey);
    }
    if upper.contains("SPATIAL_INDEX") {
        col.modifiers
            .push(nodedb_types::columnar::ColumnModifier::SpatialIndex);
    }

    Ok(col)
}
