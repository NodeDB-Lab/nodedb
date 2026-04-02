//! `SELECT VERIFY_HASH_CHAIN('collection')`
//!
//! Scans documents in the collection, verifies each hash chain link.
//! Returns `{valid: true/false, entries: N, broken_at: index, last_hash: ...}`.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::VShardId;

use super::super::super::types::{sqlstate_error, text_field};
use super::helpers::extract_function_args;

pub async fn verify_hash_chain(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "VERIFY_HASH_CHAIN")?;
    if args.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "VERIFY_HASH_CHAIN requires (collection)",
        ));
    }

    let collection = args[0]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_lowercase();

    // Scan all documents.
    let vshard = VShardId::from_collection(&collection);
    let scan_plan = PhysicalPlan::Document(crate::bridge::physical_plan::DocumentOp::Scan {
        collection: collection.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
    });

    let scan_resp = dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard, scan_plan, 0)
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("scan failed: {e}")))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&payload_json)
        .map_err(|e| sqlstate_error("22P02", &format!("invalid JSON in scan response: {e}")))?;

    // Walk the chain: each doc should have `_chain_hash` field.
    let mut prev_hash = crate::data::executor::enforcement::hash_chain::GENESIS_HASH.to_string();
    let mut entries = 0usize;
    let mut valid = true;
    let mut broken_at: Option<usize> = None;

    for (i, doc) in docs.iter().enumerate() {
        let obj = match doc.as_object() {
            Some(o) => o,
            None => continue,
        };

        let doc_id = obj
            .get("id")
            .or_else(|| obj.get("_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let stored_hash = obj
            .get("_chain_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if stored_hash.is_empty() {
            valid = false;
            broken_at = Some(i);
            break;
        }

        // Recompute the hash from the document contents (without _chain_hash).
        let mut doc_for_hash = doc.clone();
        if let Some(obj) = doc_for_hash.as_object_mut() {
            obj.remove("_chain_hash");
        }
        let doc_bytes = serde_json::to_vec(&doc_for_hash)
            .map_err(|e| sqlstate_error("XX000", &format!("failed to serialize document: {e}")))?;

        let expected = crate::data::executor::enforcement::hash_chain::compute_chain_hash(
            &prev_hash, doc_id, &doc_bytes,
        );

        if expected != stored_hash {
            valid = false;
            broken_at = Some(i);
            break;
        }

        prev_hash = stored_hash.to_string();
        entries += 1;
    }

    let result = serde_json::json!({
        "valid": valid,
        "entries": entries,
        "broken_at": broken_at,
        "last_hash": prev_hash,
    });

    let schema = Arc::new(vec![text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&result.to_string())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}
