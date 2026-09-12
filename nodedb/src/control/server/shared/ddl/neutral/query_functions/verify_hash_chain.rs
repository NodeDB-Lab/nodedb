// SPDX-License-Identifier: BUSL-1.1

//! `SELECT VERIFY_HASH_CHAIN('collection')`
//!
//! Scans documents in the collection, verifies each hash chain link.
//! Returns `{valid: true/false, entries: N, broken_at: index, last_hash: ...}`.

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{err, extract_function_args, single_result, unwrap_scan_doc_with_id};

pub async fn verify_hash_chain(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "VERIFY_HASH_CHAIN")?;
    if args.is_empty() {
        return Err(err("42601", "VERIFY_HASH_CHAIN requires (collection)"));
    }

    let collection = args[0]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_lowercase();

    // `collection` is a caller argument, so the scan it names is authorized and
    // row-filtered here. Each link is recomputed over the whole document body,
    // so any redaction rule on the collection is refused: hashing a masked row
    // would report an intact chain as broken.
    let gate = CollectionReadGate::open(state, identity, database_id, &collection)?;
    gate.require_document_engine(&collection, "VERIFY_HASH_CHAIN")?;
    gate.refuse_if_any_redaction(&collection, "the hash chain")?;

    // Scan all documents.
    let vshard = VShardId::from_collection_in_database(database_id, &collection);
    let mut scan_plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    gate.inject_rls(&mut scan_plan)?;

    let scan_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        scan_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", &format!("scan failed: {e}")))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = sonic_rs::from_str(&payload_json)
        .map_err(|e| err("22P02", &format!("invalid JSON in scan response: {e}")))?;

    // Walk the chain: each doc should have `_chain_hash` field.
    let mut prev_hash = crate::data::executor::enforcement::hash_chain::GENESIS_HASH.to_string();
    let mut entries = 0usize;
    let mut valid = true;
    let mut broken_at: Option<usize> = None;

    for (i, doc) in docs.into_iter().enumerate() {
        // The raw document-scan codec wraps each row as `{"id": <doc PK>,
        // "data": {..fields incl. _chain_hash..}}`. `doc_id` must be the
        // *wrapper's* id — the same `document_id` the original INSERT fed
        // into `compute_chain_hash` — not a same-named field inside the
        // document body, which may not exist.
        let (wrapper_id, obj) = unwrap_scan_doc_with_id(doc);
        let doc_id = if !wrapper_id.is_empty() {
            wrapper_id
        } else {
            obj.get("id")
                .or_else(|| obj.get("_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };

        let stored_hash = obj
            .get("_chain_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if stored_hash.is_empty() {
            valid = false;
            broken_at = Some(i);
            break;
        }

        // Recompute the hash from the document contents (without _chain_hash).
        let mut doc_for_hash = serde_json::Value::Object(obj);
        if let Some(obj) = doc_for_hash.as_object_mut() {
            obj.remove("_chain_hash");
        }
        let doc_bytes = sonic_rs::to_vec(&doc_for_hash)
            .map_err(|e| err("XX000", &format!("failed to serialize document: {e}")))?;

        let expected = crate::data::executor::enforcement::hash_chain::compute_chain_hash(
            &prev_hash, &doc_id, &doc_bytes,
        );

        if expected != stored_hash {
            valid = false;
            broken_at = Some(i);
            break;
        }

        prev_hash = stored_hash;
        entries += 1;
    }

    let result = serde_json::json!({
        "valid": valid,
        "entries": entries,
        "broken_at": broken_at,
        "last_hash": prev_hash,
    });

    Ok(single_result(&result.to_string()))
}
