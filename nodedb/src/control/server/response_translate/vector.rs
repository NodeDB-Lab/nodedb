// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → user-PK translation for vector search responses.
//!
//! The Data Plane emits each hit's `id` as its `HybridFusionKey` wire
//! string: the hex storage key of a bound row's surrogate, or the
//! `__local_<id>` sentinel for a headless row (no surrogate binding). It
//! leaves `doc_id` as `None`. The Control Plane runs this translator at the
//! response boundary so pgwire / HTTP / native clients still see
//! human-readable document identifiers without the engine ever consulting
//! the catalog.
//!
//! Behaviour:
//!  - non-msgpack payloads (already JSON, empty, or non-array) round-
//!    trip unchanged.
//!  - decode failures are non-fatal — the original payload is returned
//!    so the client still sees the raw search hits.
//!
//! [`resolve_surrogate_pk`] is the single catalog lookup shared by this
//! translator and the full-text / hybrid translators in `text_hybrid.rs` —
//! see `dispatch.rs` for the plan-to-translator routing.

use nodedb_types::DatabaseId;
use nodedb_types::Surrogate;
use nodedb_types::TenantId;
use serde::{Deserialize, Serialize};

use super::hit_key::parse_surrogate_hex;
use crate::bridge::scan_filter::ScanFilter;
use crate::control::state::SharedState;

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct Hit {
    /// The `HybridFusionKey` wire string: a hex storage key, or the
    /// `__local_<id>` sentinel for a headless (surrogate-less) row.
    id: String,
    distance: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Vec<u8>>,
}

/// Apply RLS post-filter at the Control-Plane security boundary, then
/// truncate to `top_k` and strip the body bytes that the Data Plane
/// attached purely for the predicate evaluation. A no-op when
/// `rls_filters` is empty.
fn apply_rls_filter(hits: &mut Vec<Hit>, rls_filters: &[u8], top_k: usize) {
    // When filters are empty the Data Plane never attaches a body, so there
    // is nothing to evaluate or strip — return immediately.
    if rls_filters.is_empty() {
        return;
    }
    let filters: Vec<ScanFilter> = match zerompk::from_msgpack(rls_filters) {
        Ok(f) => f,
        Err(_) => {
            // fail-closed: drop everything if filters are corrupt.
            tracing::warn!("RLS filter decode failed at CP boundary — denying all hits");
            hits.clear();
            return;
        }
    };
    // RLS is a security boundary: fail closed. A division/modulo-by-zero
    // evaluating a filter against one hit drops that hit rather than
    // showing it, the same way a filter that evaluates to
    // `false` already excludes it — matching the "deny on any doubt"
    // posture the corrupt-filter branch above already uses. This
    // response-translation layer has no natural way to fail the whole
    // request without larger dispatch-chain changes, and dropping hits is
    // the strictly safer outcome for an RLS filter regardless.
    hits.retain(|h| match h.body.as_deref() {
        Some(body) => ScanFilter::all_match_binary(&filters, body).unwrap_or(false),
        None => false,
    });
    if hits.len() > top_k {
        hits.truncate(top_k);
    }
    for h in hits.iter_mut() {
        h.body = None;
    }
}

/// Resolve one surrogate to its user-facing primary key via the catalog.
///
/// Shared by the vector, full-text, and hybrid (RRF) response translators —
/// every search-hit path resolves the internal `u32` surrogate back to the
/// user's PK through this single catalog call. Returns `None` when the
/// catalog has no PK mapping for the surrogate (headless row, or a document
/// that was never written) — callers must leave the row's identifier
/// untouched in that case rather than fabricate a value.
pub(crate) fn resolve_surrogate_pk(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    surrogate: Surrogate,
) -> Option<String> {
    let catalog = state.credentials.catalog();
    let pk_bytes = catalog
        .get_pk_for_surrogate(database_id, tenant_id, collection, surrogate)
        .ok()??;
    String::from_utf8(pk_bytes).ok()
}

/// Decode the DP-side msgpack array of `VectorSearchHit`, fill each
/// row's `doc_id` from the catalog using `id` as the surrogate, and
/// re-encode. On any decode failure the payload is returned unchanged.
pub fn translate_vector_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    rls_filters: &[u8],
    top_k: usize,
) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }
    let first = payload[0];
    if first == b'[' || first == b'{' || first == b'"' {
        return payload.to_vec();
    }

    let mut hits: Vec<Hit> = match zerompk::from_msgpack(payload) {
        Ok(h) => h,
        Err(_) => return payload.to_vec(),
    };

    apply_rls_filter(&mut hits, rls_filters, top_k);

    for hit in &mut hits {
        if hit.doc_id.is_some() {
            continue;
        }
        // A headless hit (no surrogate binding) has no PK to resolve; leave
        // it untouched exactly as `text_hybrid.rs` does for the same sentinel.
        let Some(surrogate) = parse_surrogate_hex(&hit.id) else {
            continue;
        };
        if let Some(pk) = resolve_surrogate_pk(state, database_id, tenant_id, collection, surrogate)
        {
            hit.doc_id = Some(pk);
        }
    }

    // Slow-path column projection: when `body` is a msgpack-encoded payload
    // map, decode it and surface fields alongside `id` / `distance` so client
    // SQL projections like `SELECT id, label, vector_distance(...)` see the
    // payload columns. The base hit fields stay top-level; payload fields are
    // serialized as JSON-style siblings.
    //
    // `body` is BARE (standard) msgpack: the Data Plane normalizes every
    // attached body through the shared sparse-body normalizer, which resolves a
    // vector-primary collection's `zerompk` TAGGED sidecar to the same shape a
    // classic document body already has. That is the one contract — the gather
    // path's `flatten_vector_hits_to_relational_rows` reads it identically.
    // Decoding with the tagged `Value` codec here instead would read exactly
    // one of the two collection kinds and silently drop the columns of the
    // other.
    use std::collections::BTreeMap;
    let flattened: Vec<BTreeMap<String, serde_json::Value>> = hits
        .iter()
        .map(|h| {
            let mut obj: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            obj.insert("distance".into(), serde_json::json!(h.distance));
            // Flatten body fields first so the document's own `id` field wins
            // over the internal surrogate integer. Body fields are only present
            // when the Data Plane performed the slow-path fetch.
            if let Some(ref body) = h.body
                && let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(body)
            {
                for (k, v) in map {
                    obj.insert(k, v);
                }
            }
            // A storage key never reaches a client: a bound hit's client
            // identity is its surrogate's decimal string; a headless hit has
            // no surrogate, so the sentinel is the only identity it carries.
            let identity = match parse_surrogate_hex(&h.id) {
                Some(surrogate) => surrogate.as_u32().to_string(),
                None => h.id.clone(),
            };
            // Fall back to catalog-resolved doc_id or the decimal identity
            // when the body didn't provide an "id" field (e.g. skip_payload_fetch).
            if !obj.contains_key("id") {
                if let Some(ref doc) = h.doc_id {
                    obj.insert("id".into(), serde_json::json!(doc));
                } else {
                    obj.insert("id".into(), serde_json::json!(identity));
                }
            }
            // Always expose internal surrogate for debugging / join use.
            obj.insert("_surrogate".into(), serde_json::json!(identity));
            obj
        })
        .collect();
    if let Ok(s) = sonic_rs::to_string(&flattened) {
        return s.into_bytes();
    }
    zerompk::to_msgpack_vec(&hits).unwrap_or_else(|_| payload.to_vec())
}
