//! Document CRUD endpoints.
//!
//! - POST   /collections/{name}/documents         — insert/upsert
//! - GET    /collections/{name}/documents/{id}     — point get
//! - DELETE /collections/{name}/documents/{id}     — delete

use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

/// Extract X-Request-Id from headers, or generate one.
pub(super) fn extract_request_id(headers: &HeaderMap) -> u64 {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        })
}

/// Default request deadline for HTTP API operations.
const HTTP_DEADLINE: Duration = Duration::from_secs(30);

/// Compute vShard ID from collection name.
fn collection_vshard(collection: &str) -> VShardId {
    VShardId::from_collection(collection)
}

/// Dispatch a physical plan and await the response.
pub(super) async fn dispatch_plan(
    state: &AppState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
) -> Result<Vec<u8>, ApiError> {
    dispatch_plan_with_trace(state, tenant_id, collection, plan, 0).await
}

/// Dispatch a physical plan with trace ID and await the response.
pub(super) async fn dispatch_plan_with_trace(
    state: &AppState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
    trace_id: u64,
) -> Result<Vec<u8>, ApiError> {
    let vshard_id = collection_vshard(collection);
    let request_id = RequestId::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );

    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + HTTP_DEADLINE,
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
    };

    let rx = state.shared.tracker.register_oneshot(request_id);

    match state.shared.dispatcher.lock() {
        Ok(mut d) => d
            .dispatch(request)
            .map_err(|e| ApiError::Internal(e.to_string()))?,
        Err(p) => p
            .into_inner()
            .dispatch(request)
            .map_err(|e| ApiError::Internal(e.to_string()))?,
    };

    let resp = tokio::time::timeout(HTTP_DEADLINE, rx)
        .await
        .map_err(|_| ApiError::Internal("request timed out".into()))?
        .map_err(|_| ApiError::Internal("response channel closed".into()))?;

    if resp.status != Status::Ok {
        let detail = if let Some(ref code) = resp.error_code {
            format!("{code:?}")
        } else {
            String::from_utf8_lossy(&resp.payload).into_owned()
        };
        return Err(ApiError::Internal(detail));
    }

    Ok(resp.payload.to_vec())
}

/// POST /collections/{name}/documents
///
/// Request body: `{ "id": "doc-1", "data": { ... } }`
/// Or with auto-generated ID: `{ "data": { ... } }`
///
/// Response: `{ "status": "ok", "id": "doc-1" }`
pub async fn insert_document(
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

    // Extract document ID (required or auto-generated).
    let document_id = match body.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => nodedb_types::id_gen::uuid_v7(),
    };

    // Extract document data.
    let data = body
        .get("data")
        .ok_or_else(|| ApiError::BadRequest("missing 'data' field".into()))?;

    // Schema validation: check document against collection's declared fields.
    if let Some(catalog) = state.shared.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(identity.tenant_id.as_u32(), &collection)
        && let Err(e) = crate::control::server::pgwire::ddl::collection::validate_document_schema(
            &coll.fields,
            data,
        )
    {
        return Err(ApiError::BadRequest(format!("schema validation: {e}")));
    }

    let value =
        serde_json::to_vec(data).map_err(|e| ApiError::BadRequest(format!("invalid data: {e}")))?;

    let plan = PhysicalPlan::PointPut {
        collection: collection.clone(),
        document_id: document_id.clone(),
        value,
    };

    let trace_id = extract_request_id(&headers);
    state.shared.tenant_request_start(identity.tenant_id);
    let result =
        dispatch_plan_with_trace(&state, identity.tenant_id, &collection, plan, trace_id).await;
    state.shared.tenant_request_end(identity.tenant_id);

    result?;

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "id": document_id,
        "collection": collection,
    })))
}

/// GET /collections/{name}/documents/{id}
///
/// Response: `{ "status": "ok", "id": "doc-1", "data": { ... } }`
pub async fn get_document(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((collection, document_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    let plan = PhysicalPlan::PointGet {
        collection: collection.clone(),
        document_id: document_id.clone(),
    };

    state.shared.tenant_request_start(identity.tenant_id);
    let result = dispatch_plan(&state, identity.tenant_id, &collection, plan).await;
    state.shared.tenant_request_end(identity.tenant_id);

    let payload = result?;

    if payload.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "document '{document_id}' not found in '{collection}'"
        )));
    }

    // Parse the payload as JSON (Data Plane returns JSON-encoded documents).
    let data: serde_json::Value = serde_json::from_slice(&payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(&payload).into_owned())
    });

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "id": document_id,
        "collection": collection,
        "data": data,
    })))
}

/// DELETE /collections/{name}/documents/{id}
///
/// Response: `{ "status": "ok", "id": "doc-1", "deleted": true }`
pub async fn delete_document(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((collection, document_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    let plan = PhysicalPlan::PointDelete {
        collection: collection.clone(),
        document_id: document_id.clone(),
    };

    state.shared.tenant_request_start(identity.tenant_id);
    let result = dispatch_plan(&state, identity.tenant_id, &collection, plan).await;
    state.shared.tenant_request_end(identity.tenant_id);

    result?;

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "id": document_id,
        "collection": collection,
        "deleted": true,
    })))
}
