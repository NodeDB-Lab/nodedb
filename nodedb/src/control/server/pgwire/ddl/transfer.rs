//! Atomic transfer SQL functions: TRANSFER (fungible) and TRANSFER_ITEM (non-fungible).
//!
//! `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
//!   — Atomically: source.field -= amount, dest.field += amount.
//!   — Fails with INSUFFICIENT_BALANCE if source.field < amount.
//!   — Returns: `{ source_balance, dest_balance }`.
//!
//! `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
//!   — Atomically: remove item from source owner, add to dest owner.
//!   — Fails with NOT_FOUND if source doesn't own the item.
//!   — Returns: `{ item_id, from, to }`.
//!
//! Both are implemented as Control Plane orchestration: read → validate → TransactionBatch.
//! The TransactionBatch executes atomically on the Data Plane with undo-log rollback.

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::bridge::physical_plan::{KvOp, MetaOp};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// Handle `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
pub async fn transfer(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = super::kv_atomic::parse_function_args(sql, "TRANSFER")?;
    if args.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "TRANSFER requires 5 arguments: (collection, source_key, dest_key, field, amount)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let source_key = unquote(&args[1]);
    let dest_key = unquote(&args[2]);
    let field = unquote(&args[3]);
    let amount_str = args[4].trim().to_string();
    let amount: f64 = amount_str.parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!("TRANSFER: amount must be a number, got '{amount_str}'"),
        )
    })?;

    if amount <= 0.0 {
        return Err(sqlstate_error("42601", "TRANSFER: amount must be positive"));
    }

    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(&collection);

    // Step 1: Read source value.
    let source_value = read_kv_value(state, tenant_id, vshard, &collection, &source_key).await?;
    let dest_value = read_kv_value(state, tenant_id, vshard, &collection, &dest_key).await?;

    // Step 2: Extract field values and validate.
    let source_balance = extract_numeric_field(&source_value, &field).ok_or_else(|| {
        sqlstate_error(
            "42846",
            &format!(
                "TRANSFER: source key '{}' field '{}' is not numeric or missing",
                source_key, field
            ),
        )
    })?;

    let dest_balance = extract_numeric_field(&dest_value, &field).unwrap_or(0.0);

    if source_balance < amount {
        return Err(sqlstate_error(
            "23514",
            &format!(
                "insufficient balance: {source_key}.{field} = {source_balance}, need {amount}"
            ),
        ));
    }

    // Step 3: Build new values with updated field.
    let new_source = update_numeric_field(&source_value, &field, source_balance - amount)?;
    let new_dest = update_numeric_field(&dest_value, &field, dest_balance + amount)?;

    // Step 4: Atomic write via TransactionBatch.
    // Deterministic lock ordering: lexicographic lower key first.
    let (first_key, first_val, second_key, second_val) = if source_key <= dest_key {
        (&source_key, &new_source, &dest_key, &new_dest)
    } else {
        (&dest_key, &new_dest, &source_key, &new_source)
    };

    let plans = vec![
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.clone(),
            key: first_key.as_bytes().to_vec(),
            value: first_val.clone(),
            ttl_ms: 0,
        }),
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.clone(),
            key: second_key.as_bytes().to_vec(),
            value: second_val.clone(),
            ttl_ms: 0,
        }),
    ];

    let batch_plan = PhysicalPlan::Meta(MetaOp::TransactionBatch { plans });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, batch_plan, 0,
    )
    .await
    {
        Ok(_) => {
            let result = serde_json::json!({
                "source_key": source_key,
                "dest_key": dest_key,
                "field": field,
                "amount": amount,
                "source_balance": source_balance - amount,
                "dest_balance": dest_balance + amount,
            });
            respond_json("transfer", &result.to_string())
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Handle `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
pub async fn transfer_item(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = super::kv_atomic::parse_function_args(sql, "TRANSFER_ITEM")?;
    if args.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "TRANSFER_ITEM requires 5 arguments: (source_collection, dest_collection, item_id, source_owner, dest_owner)",
        ));
    }

    let source_collection = unquote(&args[0]).to_lowercase();
    let dest_collection = unquote(&args[1]).to_lowercase();
    let item_id = unquote(&args[2]);
    let source_owner = unquote(&args[3]);
    let dest_owner = unquote(&args[4]);

    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(&source_collection);

    // Step 1: Verify source owns the item.
    let item_key = format!("{source_owner}:{item_id}");
    let source_value = read_kv_value(state, tenant_id, vshard, &source_collection, &item_key).await;

    if source_value.is_err() || source_value.as_ref().is_ok_and(|v| v.is_empty()) {
        return Err(sqlstate_error(
            "02000",
            &format!(
                "TRANSFER_ITEM: item '{}' not found for owner '{}'",
                item_id, source_owner
            ),
        ));
    }
    let item_data = source_value.unwrap();

    // Step 2: Build atomic batch — delete from source, insert at dest.
    let dest_key = format!("{dest_owner}:{item_id}");

    let plans = vec![
        PhysicalPlan::Kv(KvOp::Delete {
            collection: source_collection.clone(),
            keys: vec![item_key.as_bytes().to_vec()],
        }),
        PhysicalPlan::Kv(KvOp::Put {
            collection: dest_collection.clone(),
            key: dest_key.as_bytes().to_vec(),
            value: item_data,
            ttl_ms: 0,
        }),
    ];

    let batch_plan = PhysicalPlan::Meta(MetaOp::TransactionBatch { plans });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, batch_plan, 0,
    )
    .await
    {
        Ok(_) => {
            let result = serde_json::json!({
                "item_id": item_id,
                "from": source_owner,
                "to": dest_owner,
            });
            respond_json("transfer_item", &result.to_string())
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Read a KV value from the Data Plane.
async fn read_kv_value(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    collection: &str,
    key: &str,
) -> PgWireResult<Vec<u8>> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: collection.to_string(),
        key: key.as_bytes().to_vec(),
        rls_filters: Vec::new(),
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            Ok(resp.payload.to_vec())
        }
        Ok(_) => Err(sqlstate_error(
            "02000",
            &format!("key '{key}' not found in collection '{collection}'"),
        )),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Extract a numeric field from a MessagePack-encoded KV value.
fn extract_numeric_field(value: &[u8], field: &str) -> Option<f64> {
    let doc: serde_json::Value = rmp_serde::from_slice(value).ok()?;
    let v = doc.get(field)?;
    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
}

/// Update a numeric field in a MessagePack-encoded KV value.
fn update_numeric_field(value: &[u8], field: &str, new_value: f64) -> PgWireResult<Vec<u8>> {
    let mut doc: serde_json::Value = rmp_serde::from_slice(value)
        .map_err(|e| sqlstate_error("XX000", &format!("failed to decode value: {e}")))?;

    if let Some(obj) = doc.as_object_mut() {
        // Preserve integer type if the new value has no fractional part.
        if new_value.fract() == 0.0 && new_value >= i64::MIN as f64 && new_value <= i64::MAX as f64
        {
            obj.insert(field.to_string(), serde_json::json!(new_value as i64));
        } else {
            obj.insert(field.to_string(), serde_json::json!(new_value));
        }
    }

    rmp_serde::to_vec(&doc)
        .map_err(|e| sqlstate_error("XX000", &format!("failed to encode value: {e}")))
}

fn respond_json(col_name: &str, json_text: &str) -> PgWireResult<Vec<Response>> {
    let schema = std::sync::Arc::new(vec![super::super::types::text_field(col_name)]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&json_text.to_string());
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

fn sqlstate_error(code: &str, message: &str) -> pgwire::error::PgWireError {
    super::super::types::sqlstate_error(code, message)
}
