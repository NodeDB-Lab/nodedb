// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DESCRIBE COLLECTION and SHOW COLLECTIONS DDL.
//!
//! Ported from the pgwire `ddl::collection::describe` handlers. The catalog
//! reads, row ordering, and error paths are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral `DdlResult` over `ShapedRows`.

use nodedb_types::DatabaseId;
use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// Push a `(field, type, nullable)` row into `rows`.
fn push_describe_row(
    rows: &mut Vec<Map<String, JsonValue>>,
    field: &str,
    ty: &str,
    nullable: &str,
) {
    let mut row = Map::new();
    row.insert("field".to_string(), JsonValue::String(field.to_string()));
    row.insert("type".to_string(), JsonValue::String(ty.to_string()));
    row.insert(
        "nullable".to_string(),
        JsonValue::String(nullable.to_string()),
    );
    rows.push(row);
}

/// DESCRIBE <collection> — show fields, types, and schema info.
pub fn describe_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 2 {
        return Err(DdlError::new("42601", "syntax: DESCRIBE <collection>"));
    }

    let name_lower = parse_ident_token(parts[1])?;
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();

    let coll = match catalog.get_collection(database_id, tenant_id.as_u64(), name) {
        Ok(Some(c)) if c.is_active => c,
        _ => {
            return Err(DdlError::new(
                "42P01",
                format!("collection '{name}' not found"),
            ));
        }
    };

    let columns = vec![
        "field".to_string(),
        "type".to_string(),
        "nullable".to_string(),
    ];
    let mut rows = Vec::new();

    // Synthesize the implicit 'id' field only when the collection does not
    // already declare one — a strict collection created with an explicit
    // `id ... PRIMARY KEY` stores `id` in `coll.fields`, so emitting the
    // synthetic row too would list `id` twice with contradictory nullability.
    let declares_id = coll
        .fields
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("id"));
    if !declares_id {
        push_describe_row(&mut rows, "id", "TEXT", "false");
    }
    if coll.fields.is_empty() {
        push_describe_row(&mut rows, "document", "JSON", "true");
    } else {
        for (field_name, field_type) in &coll.fields {
            let upper = field_type.to_uppercase();
            let nullable = if upper.contains("PRIMARY KEY") || upper.contains("NOT NULL") {
                "false"
            } else {
                "true"
            };
            push_describe_row(&mut rows, field_name, field_type, nullable);
        }
    }

    // Show storage mode info.
    if coll.collection_type.is_strict()
        || coll.collection_type.is_columnar_family()
        || coll.collection_type.is_kv()
    {
        push_describe_row(
            &mut rows,
            "__storage",
            coll.collection_type.as_str(),
            "false",
        );
    }

    // Timeseries-specific info: show collection_type and config.
    if coll.collection_type.is_timeseries() {
        push_describe_row(&mut rows, "__collection_type", "timeseries", "false");

        if let Some(config) = coll.get_timeseries_config() {
            for (key, value) in config.as_object().into_iter().flatten() {
                let val_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                push_describe_row(&mut rows, &format!("__ts_{key}"), &val_str, "config");
            }
        }
    }

    // KV-specific info: show TTL policy and key type.
    if let Some(kv_config) = coll.collection_type.kv_config() {
        if let Some(pk) = kv_config.primary_key_column() {
            push_describe_row(
                &mut rows,
                "__kv_key",
                &format!("{} ({})", pk.name, pk.column_type),
                "false",
            );
        }
        if let Some(ttl) = &kv_config.ttl {
            let ttl_str = match ttl {
                nodedb_types::KvTtlPolicy::FixedDuration { duration_ms } => {
                    format!("INTERVAL '{duration_ms}ms'")
                }
                nodedb_types::KvTtlPolicy::FieldBased { field, offset_ms } => {
                    format!("{field} + INTERVAL '{offset_ms}ms'")
                }
                _ => "UNKNOWN TTL POLICY".to_string(),
            };
            push_describe_row(&mut rows, "__kv_ttl", &ttl_str, "false");
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// SHOW COLLECTIONS
///
/// Lists all active collections for the current tenant.
pub fn show_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let columns = vec![
        "name".to_string(),
        "owner".to_string(),
        "created_at".to_string(),
        "partition_strategy".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    let collections = {
        let catalog = state.credentials.catalog();
        if identity.is_superuser {
            catalog
                .load_all_collections(database_id)
                .unwrap_or_default()
                .into_iter()
                .filter(|c| c.is_active)
                .collect::<Vec<_>>()
        } else {
            catalog
                .load_collections_for_tenant(database_id, tenant_id.as_u64())
                .unwrap_or_default()
        }
    };

    // Array (`CREATE ARRAY`) collections live in the dedicated
    // `array_catalog`, not in `StoredCollection` — `CollectionType` has no
    // Array variant (Array uses its own DDL family and schema model, never
    // `WITH (engine='array')`). Without this merge, arrays are silently
    // absent from `SHOW COLLECTIONS`, unlike every other engine. Merge in
    // the array_catalog entries visible to this identity so introspection
    // is uniform across all eight engines.
    let array_entries: Vec<crate::control::array_catalog::entry::ArrayCatalogEntry> = state
        .array_catalog
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .all_entries()
        .into_iter()
        .filter(|e| identity.is_superuser || e.array_id.tenant_id == tenant_id)
        .collect();

    let mut rows = Vec::with_capacity(collections.len() + array_entries.len());

    for coll in &collections {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(coll.name.clone()));
        row.insert("owner".to_string(), JsonValue::String(coll.owner.clone()));
        row.insert(
            "created_at".to_string(),
            JsonValue::String((coll.created_at as i64).to_string()),
        );
        row.insert(
            "partition_strategy".to_string(),
            JsonValue::String(coll.partition_strategy.as_str().to_string()),
        );
        rows.push(row);
    }

    for entry in &array_entries {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(entry.name.clone()));
        row.insert("owner".to_string(), JsonValue::String(String::new()));
        row.insert(
            "created_at".to_string(),
            JsonValue::String(entry.created_at_ms.to_string()),
        );
        row.insert(
            "partition_strategy".to_string(),
            JsonValue::String("array".to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
