//! The `create_collection` pgwire handler.
//!
//! Parses the CREATE COLLECTION DDL, builds the full
//! `StoredCollection` record, and replicates it through the
//! metadata raft group. Every sibling concern (Data Plane
//! register, enforcement-option parsing, generated-column specs)
//! lives in its own file under `create/`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;
use super::super::super::schema_validation::{extract_vector_fields, parse_fields_clause};
use super::super::helpers::{extract_with_value, parse_typed_schema, sql_upper_from_parts};
use super::enforcement::parse_balanced_clause;

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

    // Collection names must be alphanumeric, hyphens, or underscores only.
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "invalid collection name '{name}': only letters, digits, '-', and '_' are allowed"
            ),
        ));
    }

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

    let upper = sql_upper_from_parts(parts);

    // Columnar schema columns are captured separately because `ColumnarProfile` only stores
    // the profile-specific key (time_key / geometry_column), not the full column list.
    // DataFusion needs all column names+types to build the Arrow schema for planning.
    let mut columnar_schema_columns: Vec<(String, String)> = Vec::new();

    let collection_type = if upper.contains("TYPE") && upper.contains("KEY_VALUE") {
        super::super::super::kv::parse_kv_collection(sql, &upper)?
    } else if upper.contains("TYPE") && upper.contains("STRICT") {
        let schema = parse_typed_schema(sql).map_err(|e| sqlstate_error("42601", &e))?;
        nodedb_types::CollectionType::strict(schema)
    } else if upper.contains("TYPE") && upper.contains("COLUMNAR") {
        resolve_columnar(sql, &upper, &mut columnar_schema_columns)
    } else {
        // No TYPE keyword → schemaless document.
        nodedb_types::CollectionType::document()
    };

    // Parse optional FIELDS clause: CREATE COLLECTION name FIELDS (field type, ...)
    // For columnar collections using typed DDL syntax (no FIELDS keyword), fall back
    // to the columns captured from the typed schema above.
    let (mut fields, serial_fields) = parse_fields_clause(parts);
    if fields.is_empty() && !columnar_schema_columns.is_empty() {
        fields = columnar_schema_columns;
    }

    // For strict/columnar/kv collections, serialize the schema as JSON in timeseries_config
    // (reused for schema storage until StoredCollection gets a dedicated schema field).
    let schema_json = match &collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            sonic_rs::to_string(schema).ok()
        }
        nodedb_types::CollectionType::KeyValue(config) => sonic_rs::to_string(config).ok(),
        _ => None,
    };

    // Parse enforcement options: WITH APPEND_ONLY, WITH HASH_CHAIN, WITH BALANCED ON (...).
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
        period_lock: None,
        retention_period: None,
        legal_holds: Vec::new(),
        state_constraints: Vec::new(),
        transition_checks: Vec::new(),
        type_guards: Vec::new(),
        check_constraints: Vec::new(),
        materialized_sums: Vec::new(),
        lvc_enabled: false,
        permission_tree_def: None,
    };

    // Persist through the replicated metadata Raft group (group 0).
    //
    // In cluster mode this proposes a `CatalogEntry::PutCollection`
    // wrapped in an opaque `MetadataEntry::CatalogDdl { payload }`;
    // every node's `MetadataCommitApplier` decodes the payload and
    // writes the `StoredCollection` into its local `SystemCatalog`
    // redb so every subsequent reader (planner, dispatch, DESCRIBE,
    // pg_catalog) sees the new collection on every node.
    //
    // `propose_catalog_entry` returns `Ok(0)` when no cluster is
    // configured — the single-node fallback below does the direct
    // `put_collection` write that the legacy handler used to do
    // unconditionally.
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Ownership replicates through the parent `PutCollection`
    // post_apply on every node — handlers no longer call
    // `set_owner` directly here. The owner field is already
    // carried in `coll.owner` and installed in
    // `post_apply::collection::put`.

    // If vector fields are declared, log their auto-configuration.
    // SetVectorParams is dispatched later when the first insert
    // arrives, because the Data Plane core is selected by vShard
    // routing at dispatch time.
    let vector_fields = extract_vector_fields(&coll.fields);
    if !vector_fields.is_empty() {
        for (field_name, _dim, metric) in &vector_fields {
            tracing::info!(
                %name,
                field = %field_name,
                %metric,
                "auto-configuring vector field"
            );
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
        tracing::info!(
            collection = %name,
            field = %field_name,
            sequence = %seq_name,
            "auto-created SERIAL sequence"
        );
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))])
}

/// Parse columnar schema, capture column list, and resolve the profile.
///
/// Requires explicit `WITH profile = 'timeseries'` or `WITH profile = 'spatial'`.
/// Column modifiers (TIME_KEY, SPATIAL_INDEX) without explicit profile
/// default to plain columnar — no auto-promotion.
fn resolve_columnar(
    sql: &str,
    _upper: &str,
    columnar_schema_columns: &mut Vec<(String, String)>,
) -> nodedb_types::CollectionType {
    let schema = parse_typed_schema(sql).ok();
    let partition_by = extract_with_value(sql, "partition_by").unwrap_or_else(|| "1h".to_string());

    if let Some(ref s) = schema {
        *columnar_schema_columns = s
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.column_type.to_string()))
            .collect();
    }

    let profile_val = extract_with_value(sql, "profile")
        .map(|v| v.to_uppercase())
        .unwrap_or_default();

    if profile_val == "TIMESERIES" {
        let time_key = schema
            .as_ref()
            .and_then(|s| {
                s.columns
                    .iter()
                    .find(|c| {
                        c.is_time_key()
                            || c.column_type == nodedb_types::columnar::ColumnType::Timestamp
                    })
                    .map(|c| c.name.clone())
            })
            .unwrap_or_else(|| "timestamp".to_string());
        return nodedb_types::CollectionType::timeseries(time_key, partition_by);
    }

    if profile_val == "SPATIAL" {
        let geom_col = schema
            .as_ref()
            .and_then(|s| {
                s.columns
                    .iter()
                    .find(|c| {
                        c.is_spatial_index()
                            || c.column_type == nodedb_types::columnar::ColumnType::Geometry
                    })
                    .map(|c| c.name.clone())
            })
            .unwrap_or_else(|| "geom".to_string());
        return nodedb_types::CollectionType::spatial(geom_col);
    }

    // No auto-promotion: TIME_KEY or SPATIAL_INDEX without explicit
    // profile is plain columnar.
    nodedb_types::CollectionType::columnar()
}
