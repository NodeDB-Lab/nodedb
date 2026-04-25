//! CRDT delta application endpoint.
//!
//! POST /collections/{name}/crdt/apply
//! Request: `{ "doc_id": "...", "delta": "hex_encoded_bytes" }`
//! Response: `{ "status": "ok" }`

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::CrdtOp;
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};
use crate::control::server::pgwire::types::hex_decode;

use super::document::{dispatch_plan, extract_request_id};

/// POST /collections/{name}/crdt/apply
///
/// Apply a CRDT delta to a document in the collection.
///
/// Request body:
/// ```json
/// {
///   "doc_id": "doc-1",
///   "delta": "hex_encoded_delta_bytes"
/// }
/// ```
pub async fn crdt_apply(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(collection): Path<String>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    let doc_id = body
        .get("doc_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::BadRequest("missing 'doc_id' field".into()))?;

    let delta_str = body
        .get("delta")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::BadRequest("missing 'delta' field (hex-encoded bytes)".into()))?;

    // Decode delta from hex.
    let delta = hex_decode(delta_str)
        .ok_or_else(|| ApiError::BadRequest("invalid hex in 'delta' field".into()))?;

    let _trace_id = extract_request_id(&headers);

    let surrogate = state
        .shared
        .surrogate_assigner
        .assign(&collection, doc_id.as_bytes())
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.clone(),
        document_id: doc_id.to_string(),
        delta,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate,
    });

    state.shared.tenant_request_start(identity.tenant_id);
    let result = dispatch_plan(&state, identity.tenant_id, &collection, plan).await;
    state.shared.tenant_request_end(identity.tenant_id);

    result?;

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "collection": collection,
        "doc_id": doc_id,
    })))
}
