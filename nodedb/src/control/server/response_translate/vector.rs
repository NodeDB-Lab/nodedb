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
//! A hit names its row through [`super::pk_resolve::resolve_surrogate_pks`],
//! the one resolver every search translator runs. See `dispatch.rs` for the
//! plan-to-translator routing.

use nodedb_types::DatabaseId;
use nodedb_types::Surrogate;
use nodedb_types::TenantId;
use serde::{Deserialize, Serialize};

use super::hit_key::parse_surrogate_hex;
use super::pk_resolve::resolve_surrogate_pks;
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

/// The residual filter and row bound a vector search's hits are cut to.
pub struct VectorHitCut<'a> {
    /// Serialized `Vec<ScanFilter>`: the statement's `WHERE` conjuncts and
    /// any read policy. Empty when none applies.
    pub rls_filters: &'a [u8],
    pub top_k: usize,
    /// Drop each body after the filter. `true` when the Data Plane attached
    /// the body only for the predicate, as the pure-vector fast path does.
    pub strip_bodies: bool,
}

/// Apply the residual filter, then truncate to `top_k`. With
/// `strip_bodies`, drop the body bytes the Data Plane attached only for the
/// predicate. A no-op when `rls_filters` is empty.
fn apply_rls_filter(
    hits: &mut Vec<Hit>,
    rls_filters: &[u8],
    top_k: usize,
    strip_bodies: bool,
    crs: &nodedb_crs::CrsContext<'_>,
) {
    // When filters are empty the Data Plane ranks exactly `top_k` and
    // attaches a body only for projection, so there is nothing to do.
    if rls_filters.is_empty() {
        return;
    }
    admit_hits(hits, rls_filters, top_k, crs);
    if strip_bodies {
        for h in hits.iter_mut() {
            h.body = None;
        }
    }
}

/// Keep the hits whose body passes `rls_filters`, nearest first, cut to
/// `top_k`.
///
/// The Data Plane ranks a wider window when a residual filter applies: the
/// statement's `WHERE` conjuncts and any read policy. The filter and the cut
/// run here, on the Control Plane.
fn admit_hits(
    hits: &mut Vec<Hit>,
    rls_filters: &[u8],
    top_k: usize,
    crs: &nodedb_crs::CrsContext<'_>,
) {
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
        Some(body) => ScanFilter::all_match_binary(&filters, body, crs).unwrap_or(false),
        None => false,
    });
    if hits.len() > top_k {
        hits.truncate(top_k);
    }
}

/// Apply a vector search's residual filter and `top_k` cut to its raw hit
/// payload, keeping each body for the rows built from it.
///
/// A search read as the body of a post-processing tail reaches the gather
/// path, not the response translator. The gather path calls this so the
/// tail sees the rows a root search returns. A payload that does not decode
/// fails the statement while a filter applies: no hit can be vouched for.
pub(crate) fn filter_vector_hits(
    payload: &[u8],
    rls_filters: &[u8],
    top_k: usize,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    if rls_filters.is_empty() {
        return Ok(payload.to_vec());
    }
    let mut hits: Vec<Hit> =
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("vector search hits do not decode: {e}"),
        })?;
    admit_hits(&mut hits, rls_filters, top_k, crs);
    zerompk::to_msgpack_vec(&hits).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("vector search hits do not encode: {e}"),
    })
}

/// Decode the DP-side msgpack array of `VectorSearchHit`, flatten each hit
/// into a row, and name each row whose body carries no `id` by its primary
/// key. A payload that is not a hit array returns unchanged.
pub async fn translate_vector_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    cut: VectorHitCut<'_>,
) -> crate::Result<Vec<u8>> {
    if payload.is_empty() {
        return Ok(payload.to_vec());
    }
    // A payload that opens with no msgpack array marker carries no hit
    // array: JSON text starts with `[`, `{` or `"`, and a msgpack scalar or
    // map has its own marker. It passes through. A msgpack array that does
    // not decode as hits is an error.
    if !is_msgpack_array(payload[0]) {
        return Ok(payload.to_vec());
    }

    let mut hits: Vec<Hit> =
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("vector search hits do not decode: {e}"),
        })?;

    apply_rls_filter(
        &mut hits,
        cut.rls_filters,
        cut.top_k,
        cut.strip_bodies,
        &crate::control::crs_context::crs_context_for(state),
    );

    let FlattenedHits { mut rows, unnamed } = flatten_hits(&hits)?;
    let keys = resolve_surrogate_pks(
        state,
        database_id,
        tenant_id,
        collection,
        unnamed.iter().map(|(_, surrogate)| *surrogate),
    )
    .await?;
    for (index, surrogate) in &unnamed {
        if let (Some(key), Some(row)) = (keys.get(*surrogate), rows.get_mut(*index)) {
            row.insert("id".into(), serde_json::json!(key));
        }
    }
    sonic_rs::to_string(&rows)
        .map(String::into_bytes)
        .map_err(|e| crate::Error::Serialization {
            format: "json".into(),
            detail: format!("vector search rows do not encode: {e}"),
        })
}

/// Whether `marker` opens a msgpack array: fixarray, array 16 or array 32.
fn is_msgpack_array(marker: u8) -> bool {
    matches!(marker, 0x90..=0x9f | 0xdc | 0xdd)
}

/// One flattened hit row, keyed by column.
type HitRow = std::collections::BTreeMap<String, serde_json::Value>;

/// The rows `flatten_hits` builds, and the rows the caller names.
struct FlattenedHits {
    rows: Vec<HitRow>,
    /// The index and surrogate of each row that carries no `id` of its own.
    unnamed: Vec<(usize, Surrogate)>,
}

/// Flatten `hits` into rows. Also returns the index and surrogate of each
/// row that carries no `id` of its own, for the caller to name. A stored
/// body that does not decode fails the call.
fn flatten_hits(hits: &[Hit]) -> crate::Result<FlattenedHits> {
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
    let mut rows: Vec<HitRow> = Vec::with_capacity(hits.len());
    let mut unnamed: Vec<(usize, Surrogate)> = Vec::new();
    for h in hits {
        let mut obj = HitRow::new();
        obj.insert("distance".into(), serde_json::json!(h.distance));
        // Body fields go first, so the document's own `id` field wins over
        // the surrogate. Only a slow-path fetch attaches a body. A stored
        // body that does not decode is corruption and fails the response.
        if let Some(ref body) = h.body {
            let decoded =
                nodedb_types::json_from_msgpack(body).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "vector hit {} carries a stored body that does not decode: {e}",
                        h.id
                    ),
                })?;
            if let serde_json::Value::Object(map) = decoded {
                for (k, v) in map {
                    obj.insert(k, v);
                }
            }
        }
        // A storage key never reaches a client: a bound hit's identity is its
        // surrogate's decimal string. A headless hit carries only the sentinel.
        let surrogate = parse_surrogate_hex(&h.id);
        let identity = match surrogate {
            Some(surrogate) => surrogate.as_u32().to_string(),
            None => h.id.clone(),
        };
        // A row with no `id` of its own takes its key, named by the caller,
        // and keeps its decimal identity when no binding names it.
        if !obj.contains_key("id") {
            match (&h.doc_id, surrogate) {
                (Some(doc), _) => {
                    obj.insert("id".into(), serde_json::json!(doc));
                }
                (None, Some(surrogate)) => {
                    unnamed.push((rows.len(), surrogate));
                    obj.insert("id".into(), serde_json::json!(identity));
                }
                (None, None) => {
                    obj.insert("id".into(), serde_json::json!(identity));
                }
            }
        }
        // The surrogate is exposed for join use.
        obj.insert("_surrogate".into(), serde_json::json!(identity));
        rows.push(obj);
    }
    Ok(FlattenedHits { rows, unnamed })
}
