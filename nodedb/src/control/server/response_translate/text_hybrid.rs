// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → user-PK translation for full-text (`TextOp::Search`) and
//! hybrid / RRF (`TextOp::HybridSearch`, `TextOp::HybridSearchTriple`)
//! search responses.
//!
//! `TextOp::Search` hits carry the standard `{id, data}` document-scan
//! envelope, keyed by `StorageKey::for_surrogate(surrogate)` hex — the Data
//! Plane already gives every body its identity under the collection's
//! identity column (the declared key, else `id`), so the resolved value only
//! needs injecting, under that same column, when the row has no body (a
//! headless FTS-indexed row with no document ever written).
//!
//! `TextOp::HybridSearch` / `HybridSearchTriple` hits never fetch a document
//! body at all — the row is just `{doc_id, <score alias>, vector_rank?,
//! text_rank?}` with `doc_id` set to the raw surrogate hex (or a
//! `__local_<id>` sentinel for a vector-leg hit with no surrogate binding).
//! This is the genuine gap: without this translator `SELECT id` against a
//! hybrid query has no `id` field to read at all. Both paths resolve through
//! [`super::pk_resolve::resolve_surrogate_pks`], the resolver the vector
//! translator uses.

use nodedb_types::{DatabaseId, Surrogate, TenantId};
use serde_json::Value as JsonValue;

use crate::control::state::SharedState;
use crate::data::executor::response_codec::decode_payload_to_json;

use super::hit_key::parse_surrogate_hex;
use super::pk_resolve::resolve_surrogate_pks;

/// Decode the DP-side JSON/msgpack array of `TextOp::Search` /
/// `PhraseSearch` / `BM25ScoreScan` rows (`{id: <surrogate hex>, data: {...}}`), and for
/// any row whose `data` object lacks the collection's identity column, resolve
/// the surrogate to the user PK via the catalog and inject it into `data`
/// under that column. A declared-key row never gains an `id` beside its key.
/// Rows that hold the column (the common case) are left untouched. An
/// unresolved or headless surrogate is left untouched (no fabricated PK).
/// On a decode failure, or a catalog error resolving the identity column,
/// the payload is returned unchanged: a row never gains a wrongly named
/// column. A resolver error, or a `collection` without its database's
/// form, fails the call.
pub async fn translate_text_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    if payload.is_empty() {
        return Ok(payload.to_vec());
    }

    let bare = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?.name();
    let text = decode_payload_to_json(payload);
    let Ok(JsonValue::Array(mut rows)) = sonic_rs::from_str::<JsonValue>(&text) else {
        return Ok(payload.to_vec());
    };
    let Ok(identity_column) = identity_column(state, database_id, tenant_id, bare) else {
        return Ok(payload.to_vec());
    };

    let mut unnamed: Vec<(usize, Surrogate)> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let JsonValue::Object(map) = row else {
            continue;
        };
        let Some(JsonValue::String(hex_id)) = map.get("id") else {
            continue;
        };
        let data_has_identity = matches!(
            map.get("data"),
            Some(JsonValue::Object(inner)) if inner.contains_key(&identity_column)
        );
        if data_has_identity {
            continue;
        }
        if let Some(surrogate) = parse_surrogate_hex(hex_id) {
            unnamed.push((index, surrogate));
        }
    }
    if unnamed.is_empty() {
        return Ok(payload.to_vec());
    }
    let keys = resolve_surrogate_pks(
        state,
        database_id,
        tenant_id,
        collection,
        unnamed.iter().map(|(_, surrogate)| *surrogate),
    )
    .await?;
    for (index, surrogate) in &unnamed {
        if let Some(key) = keys.get(*surrogate)
            && let Some(JsonValue::Object(map)) = rows.get_mut(*index)
            && let Some(JsonValue::Object(inner)) = map.get_mut("data")
        {
            inner.insert(identity_column.clone(), JsonValue::String(key.to_owned()));
        }
    }

    Ok(match sonic_rs::to_string(&JsonValue::Array(rows)) {
        Ok(s) => s.into_bytes(),
        Err(_) => payload.to_vec(),
    })
}

/// The column the rows of the collection named `bare` render their
/// identity under: its declared key, per `document_declared_key`, else `id`.
/// The Data Plane's register config comes from the same function.
fn identity_column(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    bare: &str,
) -> crate::Result<String> {
    let declared_key = state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), bare)?
        .and_then(|stored| {
            crate::control::planner::catalog_adapter::document_declared_key(&stored)
        });
    Ok(declared_key.unwrap_or_else(|| nodedb_types::DEFAULT_IDENTITY_COLUMN.to_string()))
}

/// Decode the DP-side JSON/msgpack array of `HybridSearchHit`-shaped rows
/// (`{doc_id: <surrogate hex or __local_ sentinel>, <score alias>: f64,
/// vector_rank?, text_rank?}`), resolve each row's `doc_id` surrogate to the
/// user PK via the catalog, and inject it as `id` — the field name every
/// `SELECT id` projection looks up. A storage key must never reach a client,
/// so `doc_id` itself is rewritten to the row's identity too: the catalog PK
/// when one is declared, else the surrogate's decimal string. A `__local_`
/// sentinel (no surrogate binding) passes through untouched in both fields.
/// On a decode failure the payload is returned unchanged. A resolver error
/// fails the call.
pub async fn translate_hybrid_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    if payload.is_empty() {
        return Ok(payload.to_vec());
    }

    let text = decode_payload_to_json(payload);
    let Ok(JsonValue::Array(mut rows)) = sonic_rs::from_str::<JsonValue>(&text) else {
        return Ok(payload.to_vec());
    };

    let bound: Vec<(usize, Surrogate)> = rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| match row {
            JsonValue::Object(map) => match map.get("doc_id") {
                Some(JsonValue::String(hex_id)) => {
                    parse_surrogate_hex(hex_id).map(|surrogate| (index, surrogate))
                }
                _ => None,
            },
            _ => None,
        })
        .collect();
    let keys = resolve_surrogate_pks(
        state,
        database_id,
        tenant_id,
        collection,
        bound.iter().map(|(_, surrogate)| *surrogate),
    )
    .await?;
    for (index, surrogate) in &bound {
        let Some(JsonValue::Object(map)) = rows.get_mut(*index) else {
            continue;
        };
        let identity = keys
            .get(*surrogate)
            .map(str::to_owned)
            .unwrap_or_else(|| surrogate.as_u32().to_string());
        map.insert("id".to_string(), JsonValue::String(identity.clone()));
        map.insert("doc_id".to_string(), JsonValue::String(identity));
    }

    Ok(match sonic_rs::to_string(&JsonValue::Array(rows)) {
        Ok(s) => s.into_bytes(),
        Err(_) => payload.to_vec(),
    })
}
