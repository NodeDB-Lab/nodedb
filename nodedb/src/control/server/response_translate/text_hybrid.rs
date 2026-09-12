// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → user-PK translation for full-text (`TextOp::Search`) and
//! hybrid / RRF (`TextOp::HybridSearch`, `TextOp::HybridSearchTriple`)
//! search responses.
//!
//! `TextOp::Search` hits carry the standard `{id, data}` document-scan
//! envelope, keyed by `StorageKey::for_surrogate(surrogate)` hex — the document
//! body itself already carries the user's PK as an ordinary field (it was
//! written verbatim from the user's INSERT), so the resolved value only
//! needs injecting when the body has no `id` field of its own (a headless
//! FTS-indexed row with no document ever written, in which case there is no
//! PK to resolve either).
//!
//! `TextOp::HybridSearch` / `HybridSearchTriple` hits never fetch a document
//! body at all — the row is just `{doc_id, <score alias>, vector_rank?,
//! text_rank?}` with `doc_id` set to the raw surrogate hex (or a
//! `__local_<id>` sentinel for a vector-leg hit with no surrogate binding).
//! This is the genuine gap: without this translator `SELECT id` against a
//! hybrid query has no `id` field to read at all. Both paths resolve through
//! the same [`super::vector::resolve_surrogate_pk`] catalog call the vector
//! translator uses.

use nodedb_types::{DatabaseId, TenantId};
use serde_json::Value as JsonValue;

use crate::control::state::SharedState;
use crate::data::executor::response_codec::decode_payload_to_json;

use super::hit_key::parse_surrogate_hex;
use super::vector::resolve_surrogate_pk;

/// Decode the DP-side JSON/msgpack array of `TextOp::Search` /
/// `PhraseSearch`-shaped hits (`{id: <surrogate hex>, data: {...}}`), and for
/// any row whose `data` object has no `id` field of its own, resolve the
/// surrogate to the user PK via the catalog and inject it into `data`. Rows
/// whose body already carries an `id` (the common case) are left untouched;
/// an unresolved or headless surrogate is left untouched (no fabricated PK).
/// On any decode failure the payload is returned unchanged.
pub fn translate_text_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }

    let text = decode_payload_to_json(payload);
    let Ok(JsonValue::Array(mut rows)) = sonic_rs::from_str::<JsonValue>(&text) else {
        return payload.to_vec();
    };

    for row in &mut rows {
        let JsonValue::Object(map) = row else {
            continue;
        };
        let Some(JsonValue::String(hex_id)) = map.get("id").cloned() else {
            continue;
        };
        let data_has_id = matches!(
            map.get("data"),
            Some(JsonValue::Object(inner)) if inner.contains_key("id")
        );
        if data_has_id {
            continue;
        }
        let Some(surrogate) = parse_surrogate_hex(&hex_id) else {
            continue;
        };
        if let Some(pk) = resolve_surrogate_pk(state, database_id, tenant_id, collection, surrogate)
            && let Some(JsonValue::Object(inner)) = map.get_mut("data")
        {
            inner.insert("id".to_string(), JsonValue::String(pk));
        }
    }

    match sonic_rs::to_string(&JsonValue::Array(rows)) {
        Ok(s) => s.into_bytes(),
        Err(_) => payload.to_vec(),
    }
}

/// Decode the DP-side JSON/msgpack array of `HybridSearchHit`-shaped rows
/// (`{doc_id: <surrogate hex or __local_ sentinel>, <score alias>: f64,
/// vector_rank?, text_rank?}`), resolve each row's `doc_id` surrogate to the
/// user PK via the catalog, and inject it as `id` — the field name every
/// `SELECT id` projection looks up. A storage key must never reach a client,
/// so `doc_id` itself is rewritten to the row's identity too: the catalog PK
/// when one is declared, else the surrogate's decimal string. A `__local_`
/// sentinel (no surrogate binding) passes through untouched in both fields.
/// On any decode failure the payload is returned unchanged.
pub fn translate_hybrid_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }

    let text = decode_payload_to_json(payload);
    let Ok(JsonValue::Array(mut rows)) = sonic_rs::from_str::<JsonValue>(&text) else {
        return payload.to_vec();
    };

    for row in &mut rows {
        let JsonValue::Object(map) = row else {
            continue;
        };
        let Some(JsonValue::String(hex_id)) = map.get("doc_id").cloned() else {
            continue;
        };
        let Some(surrogate) = parse_surrogate_hex(&hex_id) else {
            continue;
        };
        let identity = resolve_surrogate_pk(state, database_id, tenant_id, collection, surrogate)
            .unwrap_or_else(|| surrogate.as_u32().to_string());
        map.insert("id".to_string(), JsonValue::String(identity.clone()));
        map.insert("doc_id".to_string(), JsonValue::String(identity));
    }

    match sonic_rs::to_string(&JsonValue::Array(rows)) {
        Ok(s) => s.into_bytes(),
        Err(_) => payload.to_vec(),
    }
}
