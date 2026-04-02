//! Prometheus remote write/read HTTP handlers.
//!
//! - POST `/obsv/api/v1/write`  — accept snappy-compressed protobuf `WriteRequest`
//! - POST `/obsv/api/v1/read`   — accept snappy-compressed protobuf `ReadRequest`

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::bridge::physical_plan::{PhysicalPlan, TimeseriesOp};
use crate::control::promql::remote_proto::{
    self, Label, MatchType, QueryResult, ReadRequest, ReadResponse, Sample, TimeSeries,
    WriteRequest,
};
use crate::control::promql::{self, types::DEFAULT_LOOKBACK_MS};
use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::server::http::auth::AppState;
use crate::types::{TenantId, VShardId};

/// POST `/obsv/api/v1/write` — Prometheus remote write endpoint.
///
/// Accepts: `Content-Encoding: snappy`, body = snappy-compressed protobuf `WriteRequest`.
/// Converts each `TimeSeries` to ILP lines and dispatches to the Data Plane.
pub async fn remote_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Decompress snappy if Content-Encoding indicates it (Prometheus always sends snappy).
    let decompressed = if is_snappy(&headers) {
        match snap::raw::Decoder::new().decompress_vec(&body) {
            Ok(d) => d,
            Err(e) => return (StatusCode::BAD_REQUEST, format!("snappy decode error: {e}")),
        }
    } else {
        body.to_vec()
    };

    // Decode protobuf.
    let write_req = match WriteRequest::decode(&decompressed[..]) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("protobuf decode error: {e}"),
            );
        }
    };

    // Convert each TimeSeries to ILP lines and batch-dispatch.
    let mut total_accepted = 0u64;
    let mut total_rejected = 0u64;

    for ts in &write_req.timeseries {
        let lines = ts.to_ilp_lines();
        if lines.is_empty() {
            total_rejected += ts.samples.len() as u64;
            continue;
        }

        let ilp_payload = lines.join("\n");
        let collection = ts.metric_name().to_string();
        if collection.is_empty() {
            total_rejected += ts.samples.len() as u64;
            continue;
        }

        let vshard = VShardId::from_collection(&collection);
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload: ilp_payload.into_bytes(),
            format: "ilp".into(),
            wal_lsn: None,
        });
        match dispatch_to_data_plane(&state.shared, TenantId::new(1), vshard, plan, 0).await {
            Ok(_) => total_accepted += ts.samples.len() as u64,
            Err(e) => {
                tracing::warn!(error = %e, collection = %ts.metric_name(), "remote write dispatch failed");
                total_rejected += ts.samples.len() as u64;
            }
        }
    }

    // Record exemplars (stored alongside samples for trace correlation).
    for ts in &write_req.timeseries {
        for exemplar in &ts.exemplars {
            store_exemplar(&state, ts, exemplar).await;
        }
    }

    // Prometheus expects 204 No Content on success.
    if total_rejected == 0 {
        (StatusCode::NO_CONTENT, String::new())
    } else {
        (
            StatusCode::OK,
            format!("{{\"accepted\":{total_accepted},\"rejected\":{total_rejected}}}"),
        )
    }
}

/// POST `/obsv/api/v1/read` — Prometheus remote read endpoint.
///
/// Accepts: snappy-compressed protobuf `ReadRequest`.
/// Returns: snappy-compressed protobuf `ReadResponse`.
pub async fn remote_read(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let decompressed = if is_snappy(&headers) {
        match snap::raw::Decoder::new().decompress_vec(&body) {
            Ok(d) => d,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("snappy decode error: {e}"))
                    .into_response();
            }
        }
    } else {
        body.to_vec()
    };

    let read_req = match ReadRequest::decode(&decompressed[..]) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("protobuf decode error: {e}"),
            )
                .into_response();
        }
    };

    // Execute each query.
    let mut results = Vec::with_capacity(read_req.queries.len());
    for query in &read_req.queries {
        let series = execute_read_query(&state, query).await;
        results.push(QueryResult { timeseries: series });
    }

    let response = ReadResponse { results };
    let mut response_buf = Vec::new();
    if let Err(e) = response.encode(&mut response_buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("protobuf encode error: {e}"),
        )
            .into_response();
    }

    // Compress response with snappy.
    let compressed = match snap::raw::Encoder::new().compress_vec(&response_buf) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "snappy compression failed for remote read response");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "compression error".to_string(),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        [
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "snappy"),
        ],
        compressed,
    )
        .into_response()
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Execute a single remote read query by fetching series from the evaluator.
async fn execute_read_query(state: &AppState, query: &remote_proto::Query) -> Vec<TimeSeries> {
    let start_ms = query.start_timestamp_ms;
    let end_ms = query.end_timestamp_ms;

    // Convert protobuf matchers to PromQL label matchers.
    let matchers: Vec<promql::LabelMatcher> = query
        .matchers
        .iter()
        .map(|m| {
            let op = match MatchType::try_from(m.match_type) {
                Ok(MatchType::Eq) => promql::LabelMatchOp::Equal,
                Ok(MatchType::Neq) => promql::LabelMatchOp::NotEqual,
                Ok(MatchType::Re) => promql::LabelMatchOp::RegexMatch,
                Ok(MatchType::Nre) => promql::LabelMatchOp::RegexNotMatch,
                Err(_) => promql::LabelMatchOp::Equal,
            };
            promql::LabelMatcher::new(m.name.clone(), op, m.value.clone())
        })
        .collect();

    // Fetch series from the built-in metrics source.
    let all_series =
        super::helpers::fetch_series_for_query(state, start_ms - DEFAULT_LOOKBACK_MS, end_ms).await;

    // Filter and convert to protobuf TimeSeries.
    all_series
        .iter()
        .filter(|s| promql::label::matches_all(&matchers, &s.labels))
        .map(|s| {
            let labels: Vec<Label> = s
                .labels
                .iter()
                .map(|(k, v)| Label {
                    name: k.clone(),
                    value: v.clone(),
                })
                .collect();
            let samples: Vec<Sample> = s
                .samples
                .iter()
                .filter(|sample| sample.timestamp_ms >= start_ms && sample.timestamp_ms <= end_ms)
                .map(|sample| Sample {
                    value: sample.value,
                    timestamp: sample.timestamp_ms,
                })
                .collect();
            TimeSeries {
                labels,
                samples,
                exemplars: vec![],
            }
        })
        .filter(|ts| !ts.samples.is_empty())
        .collect()
}

/// Store an exemplar for later trace correlation.
///
/// Exemplars are stored as key-value pairs in the sparse engine
/// alongside the metric they're attached to.
async fn store_exemplar(_state: &AppState, ts: &TimeSeries, exemplar: &remote_proto::Exemplar) {
    // Log exemplar receipt for trace correlation visibility.
    // Persistent exemplar storage requires a dedicated TTL cache (not yet implemented).
    let trace_id = exemplar
        .labels
        .iter()
        .find(|l| l.name == "traceID")
        .map(|l| l.value.as_str())
        .unwrap_or("");
    tracing::debug!(
        metric = %ts.metric_name(),
        trace_id,
        "exemplar received"
    );
}

fn is_snappy(headers: &HeaderMap) -> bool {
    headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("snappy"))
}
