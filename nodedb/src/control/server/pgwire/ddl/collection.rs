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

    // Detect storage mode: WITH storage = 'strict' | 'columnar'.
    let upper = sql_upper_from_parts(parts);
    let collection_type = if upper.contains("STORAGE") && upper.contains("STRICT") {
        nodedb_types::CollectionType::strict(
            // Schema will be parsed below and stored in timeseries_config
            // as JSON until StoredCollection gets a proper strict_schema field.
            nodedb_types::columnar::StrictSchema {
                columns: Vec::new(),
                version: 1,
            },
        )
    } else if upper.contains("STORAGE") && upper.contains("COLUMNAR") {
        nodedb_types::CollectionType::columnar()
    } else {
        nodedb_types::CollectionType::document()
    };

    // Parse optional FIELDS clause: CREATE COLLECTION name FIELDS (field type, ...)
    let fields = parse_fields_clause(parts);

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type,
        timeseries_config: None,
        is_active: true,
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

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))])
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
    if coll.collection_type.is_strict() || coll.collection_type.is_columnar() {
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

/// Reconstruct uppercase SQL from split parts for keyword detection.
fn sql_upper_from_parts(parts: &[&str]) -> String {
    parts.join(" ").to_uppercase()
}
