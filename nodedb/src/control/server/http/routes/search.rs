//! Vector search endpoint.
//!
//! POST /collections/{name}/search
//! Request: `{ "vector": [0.1, 0.2, ...], "top_k": 10, "filter": { ... } }`
//! Response: `{ "status": "ok", "results": [{"id": "...", "distance": 0.123}, ...] }`

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};

use super::document::dispatch_plan;

/// POST /collections/{name}/search
///
/// Request body:
/// ```json
/// {
///   "vector": [0.1, 0.2, 0.3, ...],
///   "top_k": 10,
///   "filter": null
/// }
/// ```
///
/// Response:
/// ```json
/// {
///   "status": "ok",
///   "collection": "docs",
///   "results": [
///     {"id": "doc-1", "distance": 0.123},
///     {"id": "doc-2", "distance": 0.456}
///   ]
/// }
/// ```
pub async fn vector_search(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(collection): Path<String>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    // Check tenant quota.
    state
        .shared
        .check_tenant_quota(identity.tenant_id)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    // Parse query vector.
    let vector_arr = body
        .get("vector")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ApiError::BadRequest("missing 'vector' array field".into()))?;

    let query_vector: Vec<f32> = vector_arr
        .iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_f64()
                .map(|f| f as f32)
                .ok_or_else(|| ApiError::BadRequest(format!("vector[{i}] is not a number")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if query_vector.is_empty() {
        return Err(ApiError::BadRequest("vector must not be empty".into()));
    }

    // Parse top_k (default 10).
    let top_k = body.get("top_k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

    if top_k == 0 || top_k > 10_000 {
        return Err(ApiError::BadRequest(
            "top_k must be between 1 and 10000".into(),
        ));
    }

    let plan = PhysicalPlan::Vector(VectorOp::Search {
        collection: collection.clone(),
        query_vector: Arc::from(query_vector.as_slice()),
        top_k,
        ef_search: 0,
        filter_bitmap: None,
        field_name: String::new(),
    });

    state.shared.tenant_request_start(identity.tenant_id);
    let result = dispatch_plan(&state, identity.tenant_id, &collection, plan).await;
    state.shared.tenant_request_end(identity.tenant_id);

    let payload = result?;

    // Parse the search results from the Data Plane response.
    let results: serde_json::Value = if payload.is_empty() {
        serde_json::json!([])
    } else {
        serde_json::from_slice(&payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&payload).into_owned())
        })
    };

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "collection": collection,
        "results": results,
    })))
}
