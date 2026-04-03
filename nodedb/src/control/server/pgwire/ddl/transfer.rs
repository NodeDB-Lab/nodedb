//! Atomic transfer SQL functions: TRANSFER (fungible) and TRANSFER_ITEM (non-fungible).
//!
//! `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
//!   — Atomically: source.field -= amount, dest.field += amount.
//!   — Fails with INSUFFICIENT_BALANCE if source.field < amount.
//!   — Returns: `{ source_key, dest_key, field, amount, source_balance, dest_balance }`.
//!
//! `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
//!   — Atomically: remove item from source owner, add to dest owner.
//!   — Fails with NOT_FOUND if source doesn't own the item.
//!   — Returns: `{ item_key, dest_key, source_collection, dest_collection }`.
//!
//! Both dispatch to the Data Plane as dedicated KvOp variants. The entire
//! read-validate-write executes in a single TPC core pass — no TOCTOU race.

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::{KvOp, PhysicalPlan};
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

    // Dispatch to Data Plane — entire read+validate+write is atomic (single TPC core).
    let plan = PhysicalPlan::Kv(KvOp::Transfer {
        collection,
        source_key: source_key.into_bytes(),
        dest_key: dest_key.into_bytes(),
        field,
        amount,
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            respond_json("transfer", &payload_text)
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

    // Cross-collection transfers must be on the same vshard.
    // Validate this upfront to prevent silent failures.
    let vshard_src = VShardId::from_collection(&source_collection);
    let vshard_dst = VShardId::from_collection(&dest_collection);
    if source_collection != dest_collection && vshard_src != vshard_dst {
        return Err(sqlstate_error(
            "0A000",
            &format!(
                "TRANSFER_ITEM: cross-shard transfer not supported \
                 (source '{}' and dest '{}' map to different vShards)",
                source_collection, dest_collection
            ),
        ));
    }

    let tenant_id = identity.tenant_id;
    let item_key = format!("{source_owner}:{item_id}");
    let dest_key = format!("{dest_owner}:{item_id}");

    // Dispatch to Data Plane — verify + delete + insert is atomic.
    let plan = PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection,
        dest_collection,
        item_key: item_key.into_bytes(),
        dest_key: dest_key.into_bytes(),
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_src, plan, 0,
    )
    .await
    {
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            respond_json("transfer_item", &payload_text)
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

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
