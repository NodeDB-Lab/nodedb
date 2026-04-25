//! Surrogate → user-PK translation for vector search responses.
//!
//! The Data Plane emits each hit's `id` as the bound `Surrogate.as_u32()`
//! (or the local node id for headless rows) and leaves `doc_id` as
//! `None`. The Control Plane runs this translator at the response
//! boundary so pgwire / HTTP / native clients still see human-readable
//! document identifiers without the engine ever consulting the catalog.
//!
//! Behaviour:
//!  - non-msgpack payloads (already JSON, empty, or non-array) round-
//!    trip unchanged.
//!  - decode failures are non-fatal — the original payload is returned
//!    so the client still sees the raw search hits.

use nodedb_types::Surrogate;
use serde::{Deserialize, Serialize};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;
use crate::control::state::SharedState;

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct Hit {
    id: u32,
    distance: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc_id: Option<String>,
}

/// Decode the DP-side msgpack array of `VectorSearchHit`, fill each
/// row's `doc_id` from the catalog using `id` as the surrogate, and
/// re-encode. On any decode failure the payload is returned unchanged.
pub fn translate_vector_search_payload(
    payload: &[u8],
    state: &SharedState,
    collection: &str,
) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }
    // Already-JSON payloads (e.g. empty literal `[]`) skip the round-
    // trip — there's nothing to enrich.
    let first = payload[0];
    if first == b'[' || first == b'{' || first == b'"' {
        return payload.to_vec();
    }

    let mut hits: Vec<Hit> = match zerompk::from_msgpack(payload) {
        Ok(h) => h,
        Err(_) => return payload.to_vec(),
    };

    let catalog = match state.credentials.catalog().as_ref() {
        Some(c) => c,
        None => return payload.to_vec(),
    };

    for hit in &mut hits {
        if hit.doc_id.is_some() {
            continue;
        }
        if let Ok(Some(pk_bytes)) = catalog.get_pk_for_surrogate(collection, Surrogate::new(hit.id))
            && let Ok(s) = String::from_utf8(pk_bytes)
        {
            hit.doc_id = Some(s);
        }
    }

    zerompk::to_msgpack_vec(&hits).unwrap_or_else(|_| payload.to_vec())
}

/// Convenience wrapper: inspect the executed plan; if it produced
/// vector hits, apply surrogate→PK translation. Otherwise return the
/// payload untouched.
pub fn translate_if_vector(payload: &[u8], plan: &PhysicalPlan, state: &SharedState) -> Vec<u8> {
    let collection = match plan {
        PhysicalPlan::Vector(VectorOp::Search { collection, .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch { collection, .. }) => collection,
        _ => return payload.to_vec(),
    };
    translate_vector_search_payload(payload, state, collection)
}
