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

    let name = parts[2];
    let tenant_id = identity.tenant_id;

    // Check if collection already exists.
    if let Some(catalog) = state.credentials.catalog() {
        if let Ok(Some(existing)) = catalog.get_collection(tenant_id.as_u32(), name) {
            if existing.is_active {
                return Err(sqlstate_error(
                    "42P07",
                    &format!("collection '{name}' already exists"),
                ));
            }
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Parse optional FIELDS clause: CREATE COLLECTION name FIELDS (field type, ...)
    let fields = parse_fields_clause(parts);

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        fields,
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

    let name = parts[2];
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

/// CREATE INDEX <name> ON <collection> (<field>)
///
/// Creates an index owned by the collection's owner.
pub fn create_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // CREATE INDEX <name> ON <collection> (<field>)
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE INDEX <name> ON <collection> (<field>)",
        ));
    }

    let index_name = parts[2];
    if !parts[3].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[4];
    let field = parts[5].trim_matches(|c| c == '(' || c == ')');
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

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created index '{index_name}' on '{collection}' ({field})"),
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

    let schema = Arc::new(vec![text_field("index_name"), text_field("owner")]);

    // List all index owners for this tenant.
    let indexes = state.permissions.list_owners("index", tenant_id);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (index_name, owner) in &indexes {
        // If filtering by collection, only show indexes whose name starts with the collection.
        // Convention: index names are typically "<collection>_<field>_idx".
        if let Some(coll) = filter_collection {
            if !index_name.starts_with(coll) {
                continue;
            }
        }

        encoder
            .encode_field(index_name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Parse FIELDS clause from CREATE COLLECTION parts.
///
/// Syntax: `CREATE COLLECTION name FIELDS (field1 type1, field2 type2, ...)`
/// Returns empty vec if no FIELDS clause.
fn parse_fields_clause(parts: &[&str]) -> Vec<(String, String)> {
    let fields_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("FIELDS"));
    let fields_idx = match fields_idx {
        Some(i) => i,
        None => return Vec::new(),
    };

    let rest = parts[fields_idx + 1..].join(" ");
    let rest = rest.trim();
    let inner = if rest.starts_with('(') && rest.ends_with(')') {
        &rest[1..rest.len() - 1]
    } else {
        rest
    };

    inner
        .split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            let mut tokens = pair.split_whitespace();
            let name = tokens.next()?.to_string();
            let type_name = tokens.next().unwrap_or("text").to_uppercase();
            Some((name, type_name))
        })
        .collect()
}

/// Validate a JSON document against a collection's declared schema.
///
/// Returns Ok(()) if valid, or Err with a descriptive message.
/// Empty fields = schemaless (always valid).
pub fn validate_document_schema(
    fields: &[(String, String)],
    doc: &serde_json::Value,
) -> Result<(), String> {
    if fields.is_empty() {
        return Ok(());
    }

    let obj = match doc.as_object() {
        Some(o) => o,
        None => return Err("document must be a JSON object".into()),
    };

    for (field_name, type_name) in fields {
        if let Some(val) = obj.get(field_name) {
            if !val.is_null() && !type_matches(type_name, val) {
                return Err(format!(
                    "field '{}' expected type {}, got {}",
                    field_name,
                    type_name,
                    json_type_name(val)
                ));
            }
        }
    }

    Ok(())
}

fn type_matches(type_name: &str, val: &serde_json::Value) -> bool {
    match type_name {
        "VARCHAR" | "TEXT" | "STRING" => val.is_string(),
        "INT" | "INT4" | "INTEGER" | "INT2" | "SMALLINT" | "INT8" | "BIGINT" => {
            val.is_i64() || val.is_u64()
        }
        "FLOAT" | "FLOAT4" | "REAL" | "FLOAT8" | "DOUBLE" => val.is_f64() || val.is_i64(),
        "BOOL" | "BOOLEAN" => val.is_boolean(),
        "JSON" | "JSONB" => val.is_object() || val.is_array(),
        "BYTEA" | "BYTES" => val.is_string(),
        "TIMESTAMP" | "TIMESTAMPTZ" => val.is_string(),
        _ => true,
    }
}

fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}
