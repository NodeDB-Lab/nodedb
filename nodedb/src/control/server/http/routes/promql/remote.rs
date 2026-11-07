// SPDX-License-Identifier: BUSL-1.1

//! Prometheus remote write HTTP handler.
//!
//! - POST `/obsv/api/v1/write` — accept snappy-compressed protobuf `WriteRequest`
//!
//! Every metric lands in the timeseries collection named after it, in the
//! database the `X-NodeDB-Database` header selects.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::promql::remote_proto::{self, TimeSeries, WriteRequest};
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::http::admission::admit_without_rate_limit;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;
use crate::control::server::ilp_listener::{IlpMeasurementBatch, spawn_schema_projection_merge};
use crate::types::TraceId;
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::metric_registration::ensure_metric_collection;

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
    // The `X-NodeDB-Database` header selects the database the series land in.
    let database_id = match crate::control::server::http::routes::query::resolve_database_id(
        &headers,
        &crate::control::server::http::routes::query::DatabaseQueryParam::default(),
        &state,
    ) {
        Ok(id) => id,
        Err(error) => return error.into_response(),
    };
    // Blacklist + account status, no rate limit: remote write is bulk metric
    // ingest, the same shape as ILP/OTLP, and not the per-query traffic the
    // rate limiter's cost table models. It runs before the body is
    // decompressed or decoded, so a refused sender costs nothing.
    if let Err(error) = admit_without_rate_limit(&state, &identity.0, database_id, peer.as_str()) {
        return error.into_response();
    }

    let decompressed = match decompress_body(&headers, &body) {
        Ok(d) => d,
        Err(response) => return *response,
    };

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

    let mut total_accepted = 0u64;
    let mut total_rejected = 0u64;

    // This endpoint builds its physical tasks itself instead of going through
    // the SQL planner, so it has to run the planner's row-level-security pass
    // over each one explicitly — otherwise remote write will be a way to
    // ingest rows a write policy forbids, with the same identity and the same
    // collection an `INSERT` refuses. The scope is resolved against the
    // sender's address, as the line-protocol listener resolves it, so an
    // IP-conditional grant reads the same on both transports.
    let scope = match ClientRequestScope::for_database(
        &identity.0,
        state.shared.auth_stores(),
        database_id,
        peer.as_str(),
    ) {
        Ok(request) => request.into_resolved_scope(),
        Err(error) => {
            return crate::control::server::http::auth::ApiError::from(error).into_response();
        }
    };
    let mut registered: HashSet<String> = HashSet::new();
    // The stored lines of each metric, for the catalog field projection.
    let mut stored_lines: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for ts in &write_req.timeseries {
        let series_samples = ts.samples.len() as u64;
        let encoded = match ts.to_ilp_lines() {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::warn!(error = %error, "remote write series refused");
                total_rejected += series_samples;
                continue;
            }
        };
        total_rejected += encoded.rejected;
        if encoded.lines.is_empty() {
            continue;
        }
        let line_count = encoded.lines.len() as u64;
        let collection = ts.metric_name().to_string();

        if !registered.contains(&collection) {
            if let Err(error) =
                ensure_metric_collection(&state.shared, &identity.0, database_id, &collection).await
            {
                tracing::warn!(error = %error, collection = %collection, "remote write metric registration refused");
                total_rejected += line_count;
                continue;
            }
            registered.insert(collection.clone());
        }

        let vshard = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            payload: encoded.lines.join("\n").into_bytes(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            // Prometheus remote-write answers with an HTTP status, never rows,
            // for the same reason the line-protocol listener does. `inject_rls`
            // still runs over this task, so the read filter it fills in is
            // never consulted.
            returning: None,
            rls_filters: Vec::new(),
        });

        let mut task = PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id,
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
            state.shared.credentials.catalog(),
            scope.auth(),
        ) {
            tracing::warn!(error = ?error, collection = %collection, "remote write denied by row policy");
            total_rejected += line_count;
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
                total_accepted += line_count;
                continue;
            }
            Err(error) => {
                tracing::warn!(error = ?error, collection = %collection, "remote write denied");
                total_rejected += line_count;
                continue;
            }
        };

        let gw_ctx = QueryContext {
            tenant_id,
            trace_id: TraceId::generate(),
            database_id,
            txn_id: None,
            linearizable: true,
        };
        let dispatch_result = match state.shared.installed_gateway() {
            Ok(gateway) => gateway.execute(&gw_ctx, checked).await,
            Err(error) => Err(error),
        };

        match dispatch_result {
            Ok(_) => {
                total_accepted += line_count;
                stored_lines
                    .entry(collection)
                    .or_default()
                    .extend(encoded.lines);
            }
            Err(e) => {
                let (_status, msg) = GatewayErrorMap::to_http(&e);
                tracing::warn!(
                    error = %msg,
                    collection = %collection,
                    "remote write dispatch failed"
                );
                total_rejected += line_count;
            }
        }
    }

    // Label columns join the catalog projection the way line-protocol
    // ingest adds them, so SQL resolves `WHERE job = ...` on a metric. The
    // merge runs off this task and is rebuilt by the next write.
    let mut groups = Vec::with_capacity(stored_lines.len());
    for (metric, lines) in stored_lines {
        match IlpMeasurementBatch::from_lines(metric, lines) {
            Ok(group) => groups.push(group),
            // The lines were already parsed and stored by the Data Plane,
            // so a parse error here is an encoder bug.
            Err(error) => {
                tracing::error!(error = %error, "remote write lines do not parse for the catalog projection");
            }
        }
    }
    if !groups.is_empty() {
        spawn_schema_projection_merge(&state.shared, database_id, tenant_id, groups);
    }

    // Record exemplars (stored alongside samples for trace correlation).
    for ts in &write_req.timeseries {
        for exemplar in &ts.exemplars {
            store_exemplar(&state, ts, exemplar).await;
        }
    }

    // A sync mirror's timeout maps to class 55, a 4xx. A remote-write client
    // does not retry it, and the samples stay stored.
    if total_accepted > 0
        && let Err(error) =
            crate::control::mirror::source::sync::await_database_sync(&state.shared, database_id)
                .await
    {
        return crate::control::server::http::auth::ApiError::from(error).into_response();
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

/// The request body, snappy-decompressed when `Content-Encoding` says so.
/// Prometheus always sends snappy.
pub(super) fn decompress_body(headers: &HeaderMap, body: &Bytes) -> Result<Vec<u8>, Box<Response>> {
    if !is_snappy(headers) {
        return Ok(body.to_vec());
    }
    snap::raw::Decoder::new().decompress_vec(body).map_err(|e| {
        Box::new((StatusCode::BAD_REQUEST, format!("snappy decode error: {e}")).into_response())
    })
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
