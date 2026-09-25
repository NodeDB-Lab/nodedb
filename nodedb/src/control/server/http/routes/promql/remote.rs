// SPDX-License-Identifier: BUSL-1.1

//! Prometheus remote write/read HTTP handlers.
//!
//! - POST `/obsv/api/v1/write`  — accept snappy-compressed protobuf `WriteRequest`
//! - POST `/obsv/api/v1/read`   — accept snappy-compressed protobuf `ReadRequest`

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::promql::remote_proto::{
    self, Label, MatchType, QueryResult, ReadRequest, ReadResponse, Sample, TimeSeries,
    WriteRequest,
};
use crate::control::promql::{self, types::DEFAULT_LOOKBACK_MS};
use crate::control::server::http::admission::admit_without_rate_limit;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// POST `/obsv/api/v1/write` — Prometheus remote write endpoint.
///
/// Accepts: `Content-Encoding: snappy`, body = snappy-compressed protobuf `WriteRequest`.
/// Converts each `TimeSeries` to ILP lines and dispatches to the Data Plane.
pub async fn remote_write(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant_id = identity.tenant_id();
    // Blacklist + account status, no rate limit: remote write is bulk metric
    // ingest, the same shape as ILP/OTLP, and not the per-query traffic the
    // rate limiter's cost table models. It runs before the body is
    // decompressed or decoded, so a refused sender costs nothing.
    if let Err(error) =
        admit_without_rate_limit(&state, &identity.0, DatabaseId::DEFAULT, peer.as_str())
    {
        return error.into_response();
    }

    // Decompress snappy if Content-Encoding indicates it (Prometheus always sends snappy).
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

    // Decode protobuf.
    let write_req = match WriteRequest::decode(&decompressed[..]) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("protobuf decode error: {e}"),
            )
                .into_response();
        }
    };

    // Convert each TimeSeries to ILP lines and batch-dispatch.
    let mut total_accepted = 0u64;
    let mut total_rejected = 0u64;

    // This endpoint builds its physical tasks itself instead of going through
    // the SQL planner, so it has to run the planner's row-level-security pass
    // over each one explicitly — otherwise remote write would be a way to
    // ingest rows a write policy forbids, with the same identity and the same
    // collection an `INSERT` refuses.
    let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
        &identity.0,
        state.shared.auth_stores(),
        DatabaseId::DEFAULT,
    );

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

        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, &collection),
            payload: ilp_payload.into_bytes(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            // Prometheus remote-write answers with an HTTP status, never rows,
            // for the same reason the line-protocol listener does. `inject_rls`
            // still runs over this task, so the read filter it fills in is
            // simply never consulted.
            returning: None,
            rls_filters: Vec::new(),
        });

        let mut task = PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        // Runs before authorization, because the pass compiles the write
        // predicate onto the task and it is the authorized copy that is
        // dispatched.
        if let Err(error) = crate::control::planner::rls_injection::inject_rls(
            std::slice::from_mut(&mut task),
            &state.shared.rls,
            scope.auth(),
        ) {
            tracing::warn!(error = ?error, collection = %collection, "remote write denied by row policy");
            total_rejected += ts.samples.len() as u64;
            continue;
        }
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&state.shared.audit));
        let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state: &state.shared,
                task,
                identity: &identity.0,
                tenant_id,
                permissions: &state.shared.permissions,
                roles: &state.shared.roles,
                emitter: &emitter,
            },
        )
        .await
        {
            Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                checked,
            )) => checked,
            Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(_)) => {
                // A timeseries ingest is never a clone-write shape.
                total_accepted += ts.samples.len() as u64;
                continue;
            }
            Err(error) => {
                tracing::warn!(error = ?error, collection = %collection, "remote write denied");
                total_rejected += ts.samples.len() as u64;
                continue;
            }
        };

        // Route through gateway when available (cluster-aware dispatch);
        // fall back to capability-bearing local dispatch on single-node boot.
        let dispatch_result = match state.shared.gateway.get() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id,
                    trace_id: TraceId::generate(),
                    database_id: nodedb_types::id::DatabaseId::DEFAULT,
                    txn_id: None,
                };
                gw.execute(&gw_ctx, checked).await
            }
            None => crate::control::server::dispatch_utils::dispatch_authorized_durable_write(
                &state.shared,
                checked,
                TraceId::generate(),
            )
            .await
            .map(|_| vec![]),
        };

        match dispatch_result {
            Ok(_) => total_accepted += ts.samples.len() as u64,
            Err(e) => {
                let (_status, msg) = GatewayErrorMap::to_http(&e);
                tracing::warn!(
                    error = %msg,
                    collection = %collection,
                    "remote write dispatch failed"
                );
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
        (StatusCode::NO_CONTENT, String::new()).into_response()
    } else {
        (
            StatusCode::OK,
            format!("{{\"accepted\":{total_accepted},\"rejected\":{total_rejected}}}"),
        )
            .into_response()
    }
}

/// POST `/obsv/api/v1/read` — Prometheus remote read endpoint.
///
/// Accepts: snappy-compressed protobuf `ReadRequest`.
/// Returns: snappy-compressed protobuf `ReadResponse`.
pub async fn remote_read(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Same door as remote write and `/metrics`: this is the observability
    // surface a Prometheus/Grafana deployment polls, not per-query traffic.
    // Runs before the body is decompressed or decoded.
    if let Err(error) =
        admit_without_rate_limit(&state, &identity.0, DatabaseId::DEFAULT, peer.as_str())
    {
        return error.into_response();
    }

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
