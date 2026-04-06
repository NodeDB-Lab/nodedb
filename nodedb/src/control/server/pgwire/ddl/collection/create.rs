//! CREATE COLLECTION DDL: creation, registration, and related helpers.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::super::schema_validation::{extract_vector_fields, parse_fields_clause};
use super::helpers::{extract_with_value, parse_typed_schema, sql_upper_from_parts};

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

    // Detect storage mode: WITH storage = 'strict' | 'columnar' | 'kv'.
    let upper = sql_upper_from_parts(parts);

    // For columnar collections the typed schema (e.g. `(ts TIMESTAMP TIME_KEY, cpu FLOAT)`)
    // is parsed to extract profile hints. We also capture its columns so they can be stored
    // in `fields` for DataFusion schema registration — `ColumnarProfile` only keeps the
    // profile-specific keys (time_key, geometry_column), not the full column list.
    let mut columnar_schema_columns: Vec<(String, String)> = Vec::new();

    let collection_type = if upper.contains("STORAGE") && upper.contains("STRICT") {
        let schema = parse_typed_schema(sql).map_err(|e| sqlstate_error("42601", &e))?;
        nodedb_types::CollectionType::strict(schema)
    } else if upper.contains("STORAGE")
        && (upper.contains("COLUMNAR") || upper.contains("TIMESERIES") || upper.contains("SPATIAL"))
    {
        // Infer columnar profile from column modifiers or explicit profile keyword.
        // Priority: column modifiers (TIME_KEY, SPATIAL_INDEX) > WITH profile = '...'
        let schema = parse_typed_schema(sql).ok();
        let partition_by =
            extract_with_value(sql, "partition_by").unwrap_or_else(|| "1h".to_string());

        // Capture column names/types for fields storage before consuming schema.
        if let Some(ref s) = schema {
            columnar_schema_columns = s
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.column_type.to_string()))
                .collect();
        }

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
    } else if super::super::kv::is_kv_storage_mode(&upper) {
        super::super::kv::parse_kv_collection(sql, &upper)?
    } else {
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
        materialized_sums: Vec::new(),
        lvc_enabled: false,
        permission_tree_def: None,
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
    let (fields, _serial_fields) = super::super::schema_validation::parse_fields_clause(parts);
    let index_paths: Vec<String> = fields
        .iter()
        .map(|(name, _ty)| format!("$.{name}"))
        .collect();

    let _ = sql; // Reserved for future CRDT detection from SQL.
    let crdt_enabled = false;

    // Build enforcement options from the stored collection metadata.
    let enforcement = crate::bridge::physical_plan::EnforcementOptions {
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
        period_lock: coll.period_lock.as_ref().map(|pl| {
            crate::bridge::physical_plan::PeriodLockConfig {
                period_column: pl.period_column.clone(),
                ref_table: pl.ref_table.clone(),
                ref_pk: pl.ref_pk.clone(),
                status_column: pl.status_column.clone(),
                allowed_statuses: pl.allowed_statuses.clone(),
            }
        }),
        retention: coll.retention_period.as_ref().and_then(|s| {
            crate::data::executor::enforcement::retention::parse_retention_period(s).ok()
        }),
        has_legal_hold: !coll.legal_holds.is_empty(),
        state_constraints: coll.state_constraints.clone(),
        transition_checks: coll.transition_checks.clone(),
        materialized_sum_sources: find_materialized_sum_bindings(
            catalog,
            tenant_id.as_u32(),
            &name,
        ),
        generated_columns: build_generated_column_specs(&coll),
    };

    let vshard = crate::types::VShardId::from_collection(&name);
    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        crate::bridge::physical_plan::DocumentOp::Register {
            collection: name.clone(),
            index_paths,
            crdt_enabled,
            storage_mode,
            enforcement: Box::new(enforcement),
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

/// Find all materialized sum bindings where `source_collection == collection_name`.
///
/// Scans all collections for the tenant and extracts bindings from their
/// `materialized_sums` definitions. These are placed on the SOURCE collection's
/// `EnforcementOptions` so the Data Plane fires the trigger on INSERT.
fn find_materialized_sum_bindings(
    catalog: &crate::control::security::catalog::types::SystemCatalog,
    tenant_id: u32,
    collection_name: &str,
) -> Vec<crate::bridge::physical_plan::MaterializedSumBinding> {
    let all_collections = catalog
        .load_collections_for_tenant(tenant_id)
        .unwrap_or_default();

    let mut bindings = Vec::new();
    for target_coll in &all_collections {
        for def in &target_coll.materialized_sums {
            if def.source_collection == collection_name {
                bindings.push(crate::bridge::physical_plan::MaterializedSumBinding {
                    target_collection: def.target_collection.clone(),
                    target_column: def.target_column.clone(),
                    join_column: def.join_column.clone(),
                    value_expr: def.value_expr.clone(),
                });
            }
        }
    }
    bindings
}

/// Build generated column specs from the stored collection's schema.
///
/// Checks both strict-schema `ColumnDef` entries (via `timeseries_config`,
/// which is reused for schema storage) and schemaless `FieldDefinition`
/// entries (via `field_defs`).
fn build_generated_column_specs(
    coll: &crate::control::security::catalog::StoredCollection,
) -> Vec<crate::bridge::physical_plan::GeneratedColumnSpec> {
    let mut specs = Vec::new();

    // Strict/columnar collections store their schema JSON in timeseries_config
    // (reused for schema storage until StoredCollection gets a dedicated field).
    let schema_json = coll.timeseries_config.as_deref().unwrap_or("");
    if let Ok(schema) = sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(schema_json) {
        for col in &schema.columns {
            if let Some(ref expr_json) = col.generated_expr
                && let Ok(expr) = sonic_rs::from_str::<crate::bridge::expr_eval::SqlExpr>(expr_json)
            {
                specs.push(crate::bridge::physical_plan::GeneratedColumnSpec {
                    name: col.name.clone(),
                    expr,
                    depends_on: col.generated_deps.clone(),
                });
            }
        }
    }

    // Schemaless/document collections: generated columns live in FieldDefinitions.
    for field_def in &coll.field_defs {
        if field_def.is_generated
            && !field_def.value_expr.is_empty()
            && let Ok(expr) =
                sonic_rs::from_str::<crate::bridge::expr_eval::SqlExpr>(&field_def.value_expr)
            && !specs.iter().any(|s| s.name == field_def.name)
        {
            specs.push(crate::bridge::physical_plan::GeneratedColumnSpec {
                name: field_def.name.clone(),
                expr,
                depends_on: field_def.generated_deps.clone(),
            });
        }
    }

    specs
}

#[cfg(test)]
mod tests {
    /// Collection name validation: allowed chars are [a-zA-Z0-9_-].
    fn validate_name(name: &str) -> bool {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    #[test]
    fn valid_collection_names() {
        assert!(validate_name("docs"));
        assert!(validate_name("my_collection"));
        assert!(validate_name("my-collection"));
        assert!(validate_name("Collection123"));
        assert!(validate_name("a"));
    }

    #[test]
    fn invalid_collection_names_rejected() {
        // Semicolons are sent by psql in multi-statement queries —
        // must be rejected with a clear error, not stored silently.
        assert!(!validate_name("docs;"));
        assert!(!validate_name("bad;name"));
        assert!(!validate_name("bad name"));
        assert!(!validate_name("bad.name"));
        assert!(!validate_name("bad/name"));
        assert!(!validate_name(""));
        // Trailing semicolon is the most common real-world case.
        assert!(!validate_name("events;"));
        assert!(!validate_name("orders;"));
    }
}
