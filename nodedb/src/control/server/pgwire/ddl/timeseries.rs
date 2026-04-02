//! DDL handlers for timeseries collections.
//!
//! - `CREATE TIMESERIES name [WITH (...)]`
//! - `SHOW PARTITIONS FOR name`
//! - `ALTER TIMESERIES name SET (...)`

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};
use super::timeseries_helpers::{format_bytes, parse_column_defs, parse_with_clause};

/// CREATE TIMESERIES <name> [WITH (key = 'value', ...)]
pub fn create_timeseries(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE TIMESERIES <name> [WITH (key = 'value', ...)]",
        ));
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id;

    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(_)) = catalog.get_collection(tenant_id.as_u32(), &name)
    {
        return Err(sqlstate_error(
            "42P07",
            &format!("collection '{name}' already exists"),
        ));
    }

    let config_json = parse_with_clause(parts);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Parse column definitions from CREATE TIMESERIES name (...) syntax.
    // Falls back to (timestamp, value) if no columns specified.
    let fields = parse_column_defs(parts).unwrap_or_else(|| {
        vec![
            ("timestamp".into(), "TIMESTAMP".into()),
            ("value".into(), "FLOAT".into()),
        ]
    });

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.clone(),
        owner: identity.username.clone(),
        created_at: now,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type: nodedb_types::CollectionType::timeseries("timestamp", "1h"),
        timeseries_config: config_json,
        is_active: true,
        append_only: false,
        hash_chain: false,
        balanced: None,
        last_chain_hash: None,
        period_lock: None,
        retention_period: None,
        legal_holds: Vec::new(),
        state_constraints: Vec::new(),
        transition_checks: Vec::new(),
    };

    if let Some(catalog) = state.credentials.catalog() {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Initialize partition registry for this timeseries collection.
    if let Some(registries) = state.timeseries_registries() {
        let config = nodedb_types::timeseries::TieredPartitionConfig::origin_defaults();
        let registry =
            crate::engine::timeseries::partition_registry::PartitionRegistry::new(config);
        let key = format!("{}:{}", tenant_id.as_u32(), name);
        let mut regs =
            crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
        regs.insert(key, registry);
    }

    tracing::info!(
        collection = name,
        tenant = tenant_id.as_u32(),
        "timeseries collection created"
    );

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "CREATE TIMESERIES",
    ))])
}

/// SHOW PARTITIONS FOR <name>
pub fn show_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SHOW PARTITIONS FOR <collection>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Verify collection exists and is timeseries.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &name) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(sqlstate_error(
                    "42809",
                    &format!("'{name}' is not a timeseries collection"),
                ));
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    let schema = Arc::new(vec![
        text_field("partition"),
        int8_field("min_ts"),
        int8_field("max_ts"),
        int8_field("rows"),
        text_field("size"),
        text_field("state"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    if let Some(registries) = state.timeseries_registries() {
        let regs = crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
        let key = format!("{}:{}", tenant_id.as_u32(), name);
        if let Some(registry) = regs.get(&key) {
            for (_, entry) in registry.iter() {
                if !entry.meta.is_queryable() {
                    continue;
                }
                encoder
                    .encode_field(&entry.dir_name)
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&entry.meta.min_ts)
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&entry.meta.max_ts)
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&(entry.meta.row_count as i64))
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&format_bytes(entry.meta.size_bytes))
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&format!("{:?}", entry.meta.state))
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

/// ALTER TIMESERIES <name> SET (key = 'value', ...)
pub fn alter_timeseries(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 || parts[3].to_uppercase() != "SET" {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER TIMESERIES <name> SET (key = 'value', ...)",
        ));
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id;

    if let Some(catalog) = state.credentials.catalog() {
        let mut coll = catalog
            .get_collection(tenant_id.as_u32(), &name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
            .ok_or_else(|| {
                sqlstate_error("42P01", &format!("collection '{name}' does not exist"))
            })?;

        if !coll.collection_type.is_timeseries() {
            return Err(sqlstate_error(
                "42809",
                &format!("'{name}' is not a timeseries collection"),
            ));
        }

        let new_config = parse_with_clause(parts);
        if let Some(cfg) = new_config {
            coll.timeseries_config = Some(cfg);
        }

        // Update partition registry interval if partition_by changed.
        if let Some(registries) = state.timeseries_registries() {
            let key = format!("{}:{}", tenant_id.as_u32(), name);
            let mut regs =
                crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
            if let Some(registry) = regs.get_mut(&key)
                && let Some(config) = coll.get_timeseries_config()
                && let Some(partition_by) = config.get("partition_by").and_then(|v| v.as_str())
                && let Ok(interval) =
                    nodedb_types::timeseries::PartitionInterval::parse(partition_by)
            {
                registry.set_partition_interval(interval);
            }
        }

        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    } else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    }

    tracing::info!(collection = name, "timeseries config updated");

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "ALTER TIMESERIES",
    ))])
}

/// REWRITE PARTITIONS FOR <name>
///
/// Triggers an async background rewrite of all sealed partitions
/// for a timeseries collection. Non-blocking — returns immediately.
/// Useful for reclaiming space after column drops or applying new compression.
pub fn rewrite_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // REWRITE PARTITIONS FOR <name>
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REWRITE PARTITIONS FOR <collection>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Verify collection exists and is timeseries.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &name) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(sqlstate_error(
                    "42809",
                    &format!("'{name}' is not a timeseries collection"),
                ));
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    // Collect partition directories to rewrite.
    let partitions_to_rewrite: Vec<String> = if let Some(registries) = state.timeseries_registries()
    {
        let key = format!("{}:{}", tenant_id.as_u32(), name);
        let regs = crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
        if let Some(registry) = regs.get(&key) {
            registry
                .iter()
                .filter(|(_, e)| {
                    e.meta.state == nodedb_types::timeseries::PartitionState::Sealed
                        || e.meta.state == nodedb_types::timeseries::PartitionState::Merged
                })
                .map(|(_, e)| e.dir_name.clone())
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let sealed_count = partitions_to_rewrite.len();
    tracing::info!(
        collection = name,
        sealed_partitions = sealed_count,
        "REWRITE PARTITIONS scheduled (async, non-blocking)"
    );

    if sealed_count > 0 {
        let wal_dir = state.wal.wal_dir();
        let ts_base = wal_dir
            .parent()
            .unwrap_or(wal_dir)
            .join("timeseries")
            .to_path_buf();
        let collection_name = name.clone();

        tokio::task::spawn_blocking(move || {
            let mut rewritten = 0usize;
            for dir_name in &partitions_to_rewrite {
                let partition_dir = ts_base.join(dir_name);
                if !partition_dir.exists() {
                    continue;
                }
                match crate::engine::timeseries::merge::merge_partitions(
                    &ts_base,
                    std::slice::from_ref(&partition_dir),
                    &format!("{dir_name}.rewrite"),
                ) {
                    Ok(result) => {
                        let rewrite_dir = ts_base.join(format!("{dir_name}.rewrite"));
                        let backup_dir = ts_base.join(format!("{dir_name}.old"));
                        if std::fs::rename(&partition_dir, &backup_dir).is_ok()
                            && std::fs::rename(&rewrite_dir, &partition_dir).is_ok()
                        {
                            let _ = std::fs::remove_dir_all(&backup_dir);
                            // Write updated metadata to partition.meta (on-disk source of truth).
                            let meta_path = partition_dir.join("partition.meta");
                            let _ = std::fs::write(
                                &meta_path,
                                sonic_rs::to_vec_pretty(&result.meta).unwrap_or_default(),
                            );
                            rewritten += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            partition = dir_name,
                            error = %e,
                            "rewrite failed for partition"
                        );
                    }
                }
            }
            tracing::info!(
                collection = collection_name,
                rewritten,
                total = sealed_count,
                "REWRITE PARTITIONS completed"
            );
        });
    }

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "REWRITE PARTITIONS",
    ))])
}
