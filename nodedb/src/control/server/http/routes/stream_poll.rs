//! HTTP poll endpoint for change stream consumption.
//!
//! `GET /v1/streams/{stream}/poll?group={group}&limit=100&partition=3`
//!
//! Returns a JSON batch of events from the stream buffer, starting after
//! the consumer group's committed offsets. Does NOT auto-commit.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use super::super::auth::AppState;
use crate::event::cdc::consume::{ConsumeError, ConsumeParams, ConsumeResult, consume_stream};

/// Query parameters.
#[derive(Deserialize, Default)]
pub struct PollParams {
    /// Consumer group name (required).
    pub group: Option<String>,
    /// Maximum events to return. Default: 100.
    pub limit: Option<usize>,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u16>,
    /// Tenant ID. Default: 1.
    pub tenant_id: Option<u32>,
}

/// Response body.
#[derive(Serialize)]
pub struct PollResponse {
    /// Events in this batch.
    pub events: Vec<serde_json::Value>,
    /// Per-partition latest LSN in this batch.
    pub partition_offsets: std::collections::BTreeMap<String, u64>,
    /// Total events returned.
    pub count: usize,
}

/// `GET /v1/streams/{stream}/poll`
pub async fn poll_stream(
    Path(stream_name): Path<String>,
    Query(params): Query<PollParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let group = match params.group {
        Some(g) => g.to_lowercase(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing 'group' query parameter"})),
            )
                .into_response();
        }
    };

    let tenant_id = params.tenant_id.unwrap_or(1);
    let limit = params.limit.unwrap_or(100).min(10_000);
    let stream_name = stream_name.to_lowercase();

    let consume_params = ConsumeParams {
        tenant_id,
        stream_name: &stream_name,
        group_name: &group,
        partition: params.partition,
        limit,
    };

    let result = match consume_stream(&state.shared, &consume_params) {
        Ok(r) => r,
        Err(ConsumeError::RemotePartition { leader_node, .. }) => {
            // Forward to remote node.
            match crate::event::cdc::consume::consume_remote(
                &state.shared,
                &consume_params,
                leader_node,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            }
        }
        Err(ConsumeError::BufferEmpty(_)) => ConsumeResult {
            events: Vec::new(),
            partition_offsets: Vec::new(),
        },
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let events: Vec<serde_json::Value> = result
        .events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap_or_default())
        .collect();
    let count = events.len();
    let partition_offsets: std::collections::BTreeMap<String, u64> = result
        .partition_offsets
        .into_iter()
        .map(|(pid, lsn)| (pid.to_string(), lsn))
        .collect();

    Json(PollResponse {
        events,
        partition_offsets,
        count,
    })
    .into_response()
}
