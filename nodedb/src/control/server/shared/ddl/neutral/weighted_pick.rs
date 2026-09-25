// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral WEIGHTED_PICK SQL table-valued function for weighted random
//! selection.
//!
//! `SELECT * FROM WEIGHTED_PICK('collection', weight => 'weight_col', count => N
//!     [, SEED => 'seed_string'] [, AUDIT => TRUE] [, WITH REPLACEMENT])`
//!
//! Workflow:
//! 1. Scan the collection (optionally filtered by WHERE).
//! 2. Extract weight values from the weight column.
//! 3. Build alias table (O(N) setup).
//! 4. Sample `count` items using alias method (O(1) per pick).
//! 5. Optionally log the pick to `_system.random_audit`.
//! 6. Return selected rows with (pick_index, key, weight) columns.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::engine::random::alias::AliasTable;
use crate::engine::random::csprng::SeedableRng;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::KvOp;

use super::super::result::{DdlError, DdlResult};
use super::read_gate::CollectionReadGate;

/// Handle `SELECT * FROM WEIGHTED_PICK('collection', weight => 'col', count => N, ...)`
pub async fn weighted_pick(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "WEIGHTED_PICK")?;
    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            "WEIGHTED_PICK requires at least 3 arguments: (collection, weight => 'col', count => N)",
        ));
    }

    // A string literal is data: the collection name resolves exactly as
    // written, with no case folding.
    let collection = unquote(&args[0]);

    // Parse named parameters from remaining args.
    let mut weight_col = String::new();
    let mut count = 1usize;
    let mut seed: Option<String> = None;
    let mut audit = false;
    let mut with_replacement = false;

    for arg in &args[1..] {
        let trimmed = arg.trim();
        let upper = trimmed.to_uppercase();

        if let Some(val) = strip_named_param(&upper, trimmed, "WEIGHT") {
            weight_col = unquote(&val);
        } else if let Some(val) = strip_named_param(&upper, trimmed, "COUNT") {
            count = val.trim().parse().map_err(|_| {
                ddl_err(
                    "42601",
                    format!("count must be a positive integer, got '{val}'"),
                )
            })?;
        } else if let Some(val) = strip_named_param(&upper, trimmed, "SEED") {
            seed = Some(unquote(&val));
        } else if upper.contains("AUDIT") {
            if upper.contains("TRUE") {
                audit = true;
            }
        } else if upper.contains("WITH REPLACEMENT") || upper.contains("REPLACEMENT") {
            with_replacement = true;
        }
    }

    if weight_col.is_empty() {
        return Err(ddl_err(
            "42601",
            "WEIGHTED_PICK: missing 'weight' parameter",
        ));
    }
    if count == 0 {
        return Err(ddl_err("42601", "WEIGHTED_PICK: count must be >= 1"));
    }

    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);

    // `collection` is a caller argument, so the scan it names is authorized and
    // row-filtered here — a pick is a read of every row in the collection. The
    // weight a row is drawn with, and reported with, is the stored weight
    // column, so a redaction rule on it is refused rather than masked: a masked
    // weight would change which row is drawn, not just how it prints.
    let gate = CollectionReadGate::open(state, identity, DatabaseId::DEFAULT, &collection)?;
    gate.refuse_if_field_redacted(&collection, &weight_col, "the sampling weight")?;

    // Step 1: Scan the collection to get all entries.
    let entries = scan_all_entries(state, &gate, tenant_id, vshard, &collection).await?;
    if entries.is_empty() {
        return Ok(vec![empty_pick_rows()]);
    }

    // Step 2: Extract weights and build alias table.
    let mut weights: Vec<f64> = Vec::with_capacity(entries.len());
    let mut keys: Vec<String> = Vec::with_capacity(entries.len());

    for (key_str, row) in entries {
        let weight = row_weight(&row, &weight_col).unwrap_or(0.0);
        if weight < 0.0 {
            return Err(ddl_err(
                "42601",
                format!("WEIGHTED_PICK: negative weight {weight} for key '{key_str}'"),
            ));
        }
        keys.push(key_str);
        weights.push(weight);
    }

    let table = AliasTable::new(&weights)
        .ok_or_else(|| ddl_err("42601", "WEIGHTED_PICK: all weights are zero or empty"))?;

    // Step 3: Sample.
    let mut rng = match &seed {
        Some(s) => SeedableRng::from_seed_str(s),
        None => SeedableRng::from_entropy(),
    };

    let selected_indices = if with_replacement {
        table.sample_with_replacement(&mut rng, count)
    } else {
        table.sample_without_replacement(&mut rng, count)
    };

    // Step 4: Optional audit trail.
    if audit {
        let selected_keys: Vec<&str> = selected_indices.iter().map(|&i| keys[i].as_str()).collect();
        let selected_weights: Vec<f64> = selected_indices.iter().map(|&i| weights[i]).collect();
        let audit_entry = serde_json::json!({
            "collection": collection,
            "seed": seed.as_deref().unwrap_or("csprng"),
            "selected_keys": selected_keys,
            "selected_weights": selected_weights,
            "timestamp": unix_epoch_secs(),
            "tenant_id": tenant_id.as_u64(),
            "count": selected_indices.len(),
            "with_replacement": with_replacement,
        });

        // Write audit entry to _system.random_audit collection.
        let audit_key = format!(
            "{}:{}",
            tenant_id.as_u64(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let audit_value = nodedb_types::json_to_msgpack(&audit_entry)
            .map_err(|e| ddl_err("XX000", format!("WEIGHTED_PICK: audit entry encode: {e}")))?;
        let audit_key_bytes = audit_key.into_bytes();
        let audit_surrogate = state
            .surrogate_assigner
            .assign(
                crate::types::DatabaseId::DEFAULT,
                tenant_id,
                "_system_random_audit",
                &audit_key_bytes,
            )
            .map_err(|e| ddl_err("XX000", format!("WEIGHTED_PICK: audit surrogate bind: {e}")))?;
        let audit_plan = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                DatabaseId::DEFAULT,
                "_system_random_audit",
            ),
            key: audit_key_bytes,
            value: audit_value,
            ttl_ms: 0,
            surrogate: audit_surrogate,
            returning: None,
            rls_filters: Vec::new(),
        });
        // The caller asked for an audited pick, so the pick is answered only
        // once its audit record is durable: Raft in cluster mode, else the
        // write funnel's `AppendHere`. A pick returned without its record is
        // an unaudited pick reported as an audited one.
        let resp = crate::control::server::dispatch_utils::dispatch_durable_autocommit_write(
            state,
            crate::control::server::dispatch_utils::AutocommitWrite {
                tenant_id,
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::from_collection_in_database(
                    DatabaseId::DEFAULT,
                    "_system_random_audit",
                ),
                plan: audit_plan,
                trace_id: TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id: None,
            },
        )
        .await
        .map_err(|e| ddl_err("XX000", format!("WEIGHTED_PICK: audit write: {e}")))?;
        if resp.status != crate::bridge::envelope::Status::Ok {
            return Err(ddl_err(
                "XX000",
                format!(
                    "WEIGHTED_PICK: the audit write was refused: {:?}",
                    resp.error_code
                ),
            ));
        }
    }

    // Step 5: Build response rows.
    let mut rows = Vec::with_capacity(selected_indices.len());
    for (pick_idx, &item_idx) in selected_indices.iter().enumerate() {
        let mut row = Map::new();
        row.insert(
            "pick_index".to_string(),
            JsonValue::String((pick_idx + 1).to_string()),
        );
        row.insert("key".to_string(), JsonValue::String(keys[item_idx].clone()));
        row.insert(
            "weight".to_string(),
            JsonValue::String(weights[item_idx].to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        pick_columns(),
        rows,
    ))])
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Scan every row of a KV collection as `(key, row)`. A KV scan row is a
/// document map carrying the row's `key` beside its value fields.
async fn scan_all_entries(
    state: &SharedState,
    gate: &CollectionReadGate<'_>,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    collection: &str,
) -> Result<Vec<(String, serde_json::Value)>, DdlError> {
    let mut plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        cursor: Vec::new(),
        count: 100_000,
        filters: Vec::new(),
        projection: Vec::new(),
        computed_columns: Vec::new(),
        match_pattern: None,
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
    });
    gate.inject_rls(&mut plan)?;

    let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // A refused scan is not an empty collection: a pick from it draws from
    // rows the scan never returned.
    if resp.status != Status::Ok {
        return Err(ddl_err(
            "XX000",
            format!(
                "WEIGHTED_PICK: the scan of '{collection}' was refused: {:?}",
                resp.error_code
            ),
        ));
    }

    // KV scan returns a flat msgpack array of entry maps.
    let payload_text = crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
    let json: serde_json::Value = sonic_rs::from_str(&payload_text).map_err(|e| {
        ddl_err(
            "XX000",
            format!("WEIGHTED_PICK: the scan of '{collection}' returned undecodable rows: {e}"),
        )
    })?;

    let entries = match json {
        serde_json::Value::Array(arr) => arr,
        other => {
            return Err(ddl_err(
                "XX000",
                format!("WEIGHTED_PICK: the scan of '{collection}' returned {other}, not rows"),
            ));
        }
    };

    let mut all_entries = Vec::with_capacity(entries.len());
    for row in entries {
        let key = scan_row_key(&row, collection)?;
        all_entries.push((key, row));
    }

    Ok(all_entries)
}

/// The key of one KV scan row. A row without a text `key` fails the pick: a
/// skipped row changes the row set the pick draws from.
fn scan_row_key(row: &serde_json::Value, collection: &str) -> Result<String, DdlError> {
    row.get("key")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            ddl_err(
                "XX000",
                format!("WEIGHTED_PICK: a scan row of '{collection}' has no text 'key'"),
            )
        })
}

/// The numeric weight column of one KV scan row.
fn row_weight(row: &serde_json::Value, weight_col: &str) -> Option<f64> {
    let v = row.get(weight_col)?;
    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
}

/// Parse a named parameter like "weight => 'col'" or "SEED => 'value'".
fn strip_named_param(upper: &str, original: &str, name: &str) -> Option<String> {
    let prefix = format!("{name} =>");
    let prefix_eq = format!("{name}=>");
    let prefix_space = format!("{name} ");

    if upper.starts_with(&prefix) {
        Some(original[prefix.len()..].trim().to_string())
    } else if upper.starts_with(&prefix_eq) {
        Some(original[prefix_eq.len()..].trim().to_string())
    } else if upper.starts_with(&prefix_space) && original.contains("=>") {
        let after_arrow = original.split("=>").nth(1)?;
        Some(after_arrow.trim().to_string())
    } else {
        None
    }
}

/// Column names for the WEIGHTED_PICK result set.
fn pick_columns() -> Vec<String> {
    vec![
        "pick_index".to_string(),
        "key".to_string(),
        "weight".to_string(),
    ]
}

/// Empty (no-rows) result set with the WEIGHTED_PICK schema.
fn empty_pick_rows() -> DdlResult {
    DdlResult::Rows(ShapedRows::text_rows(pick_columns(), Vec::new()))
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// Current time as Unix epoch seconds (for audit timestamps).
fn unix_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
