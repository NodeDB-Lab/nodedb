// SPDX-License-Identifier: BUSL-1.1

//! OTLP/HTTP receiver on dedicated port 4318.
//!
//! Endpoints:
//! - POST `/v1/metrics`  — OTLP metrics → timeseries engine
//! - POST `/v1/traces`   — OTLP traces → document engine (as structured spans)
//! - POST `/v1/logs`     — OTLP logs → document engine (with timestamp indexing)

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use prost::Message;
use tracing::info;

use super::proto;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::tls_policy::TransportSecurity;
use crate::control::server::session_auth;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Configuration for the OTLP receiver.
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Whether to enable OTLP ingest endpoints.
    pub enabled: bool,
    /// Listen address for OTLP/HTTP (default: 0.0.0.0:4318).
    pub listen: SocketAddr,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: SocketAddr::from(([0, 0, 0, 0], 4318)),
        }
    }
}

/// Start the OTLP/HTTP receiver on its dedicated port.
///
/// Spawns an independent axum server. Call from main after SharedState is created.
pub async fn run(config: OtelConfig, shared: Arc<SharedState>) -> std::io::Result<()> {
    if !config.enabled {
        return Ok(());
    }

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    info!(addr = %config.listen, "OTLP/HTTP receiver started");
    axum::serve(
        listener,
        router(shared).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(std::io::Error::other)
}

/// The OTLP/HTTP route table.
///
/// Split out of [`run`] so a caller can serve these handlers on a listener it
/// owns — the transport gate they run is only observable end to end, over a
/// real socket.
pub fn router(shared: Arc<SharedState>) -> Router {
    Router::new()
        .route("/v1/metrics", post(receive_metrics))
        .route("/v1/traces", post(receive_traces))
        .route("/v1/logs", post(receive_logs))
        .with_state(shared)
}

/// POST `/v1/metrics` — OTLP metrics receiver.
///
/// Accepts protobuf `ExportMetricsServiceRequest` (optionally gzip-compressed).
/// Maps OTLP metric types (gauge, sum, histogram) to ILP for timeseries ingest.
pub async fn receive_metrics(
    State(state): State<Arc<SharedState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let identity = match authenticate_otel(&headers, &state, &peer.to_string()).await {
        Ok(identity) => identity,
        Err(message) => return (StatusCode::UNAUTHORIZED, message),
    };
    let data = decompress_body(&headers, &body);
    let req = match proto::ExportMetricsServiceRequest::decode(&data[..]) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode error: {e}")),
    };

    let peer_addr = peer.to_string();
    let mut accepted = 0u64;
    let mut rejected = 0u64;

    for rm in &req.resource_metrics {
        let resource_tags = proto::resource_tags(&rm.resource);

        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let lines = metric_to_ilp(metric, &resource_tags);
                if lines.is_empty() {
                    continue;
                }
                let payload = lines.join("\n");

                match ingest_ilp(&state, &identity, &peer_addr, &payload).await {
                    Ok(n) => accepted += n,
                    Err(_) => rejected += lines.len() as u64,
                }
            }
        }
    }

    (
        StatusCode::OK,
        format!("{{\"accepted\":{accepted},\"rejected\":{rejected}}}"),
    )
}

/// POST `/v1/traces` — OTLP traces receiver.
///
/// Stores spans as timeseries rows with trace_id, span_id, timestamps,
/// attributes, and status. Enables distributed trace querying via SQL.
pub async fn receive_traces(
    State(state): State<Arc<SharedState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let identity = match authenticate_otel(&headers, &state, &peer.to_string()).await {
        Ok(identity) => identity,
        Err(message) => return (StatusCode::UNAUTHORIZED, message),
    };
    let data = decompress_body(&headers, &body);
    let req = match proto::ExportTraceServiceRequest::decode(&data[..]) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode error: {e}")),
    };

    match ingest_traces(&state, &identity, &peer.to_string(), &req).await {
        Ok(span_count) => (StatusCode::OK, format!("{{\"spans\":{span_count}}}")),
        Err(error) => (StatusCode::FORBIDDEN, error.to_string()),
    }
}

/// POST `/v1/logs` — OTLP logs receiver.
///
/// Stores log records as timeseries rows with severity, body, timestamp, and trace correlation.
pub async fn receive_logs(
    State(state): State<Arc<SharedState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let identity = match authenticate_otel(&headers, &state, &peer.to_string()).await {
        Ok(identity) => identity,
        Err(message) => return (StatusCode::UNAUTHORIZED, message),
    };
    let data = decompress_body(&headers, &body);
    let req = match proto::ExportLogsServiceRequest::decode(&data[..]) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode error: {e}")),
    };

    match ingest_logs(&state, &identity, &peer.to_string(), &req).await {
        Ok(log_count) => (StatusCode::OK, format!("{{\"logs\":{log_count}}}")),
        Err(error) => (StatusCode::FORBIDDEN, error.to_string()),
    }
}

// ── Core ingest functions (shared by HTTP + gRPC handlers) ───────────────

/// Ingest an OTLP metrics request into the timeseries engine.
pub async fn ingest_metrics(
    shared: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
    req: &proto::ExportMetricsServiceRequest,
) -> crate::Result<u64> {
    let mut lines = Vec::new();
    for rm in &req.resource_metrics {
        let resource_tags = proto::resource_tags(&rm.resource);
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                lines.extend(metric_to_ilp(metric, &resource_tags));
            }
        }
    }
    ingest_ilp(shared, identity, peer_addr, &lines.join("\n")).await
}

/// Ingest an OTLP traces request into the timeseries engine.
pub async fn ingest_traces(
    shared: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
    req: &proto::ExportTraceServiceRequest,
) -> crate::Result<u64> {
    let mut lines = Vec::new();
    for rs in &req.resource_spans {
        let resource_tags = proto::resource_tags(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                lines.push(span_to_ilp(span, &resource_tags));
            }
        }
    }
    ingest_ilp(shared, identity, peer_addr, &lines.join("\n")).await
}

/// Ingest an OTLP logs request into the timeseries engine.
pub async fn ingest_logs(
    shared: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
    req: &proto::ExportLogsServiceRequest,
) -> crate::Result<u64> {
    let mut lines = Vec::new();
    for rl in &req.resource_logs {
        let resource_tags = proto::resource_tags(&rl.resource);
        for sl in &rl.scope_logs {
            for record in &sl.log_records {
                lines.push(log_to_ilp(record, &resource_tags));
            }
        }
    }
    ingest_ilp(shared, identity, peer_addr, &lines.join("\n")).await
}

// ── Conversion helpers ───────────────────────────────────────────────────

/// Convert an OTLP metric to ILP lines.
fn metric_to_ilp(metric: &proto::Metric, resource_tags: &[(String, String)]) -> Vec<String> {
    let name = &metric.name;
    let mut lines = Vec::new();
    let base_tags = format_tags(resource_tags);

    match &metric.data {
        Some(proto::metric::Data::Gauge(g)) => {
            for dp in &g.data_points {
                let dp_tags = format_dp_tags(&dp.attributes);
                let all_tags = merge_tags(&base_tags, &dp_tags);
                let ts_ns = dp.time_unix_nano;
                lines.push(format!("{name}{all_tags} value={} {ts_ns}", dp.as_f64()));
            }
        }
        Some(proto::metric::Data::Sum(s)) => {
            for dp in &s.data_points {
                let dp_tags = format_dp_tags(&dp.attributes);
                let all_tags = merge_tags(&base_tags, &dp_tags);
                let ts_ns = dp.time_unix_nano;
                lines.push(format!("{name}{all_tags} value={} {ts_ns}", dp.as_f64()));
            }
        }
        Some(proto::metric::Data::Histogram(h)) => {
            for dp in &h.data_points {
                let dp_tags = format_dp_tags(&dp.attributes);
                let ts_ns = dp.time_unix_nano;
                // Emit one line per bucket + sum + count.
                for (i, &count) in dp.bucket_counts.iter().enumerate() {
                    let le = dp.explicit_bounds.get(i).copied().unwrap_or(f64::INFINITY);
                    let bucket_tags = merge_tags(&base_tags, &dp_tags);
                    lines.push(format!(
                        "{name}_bucket{bucket_tags},le={le} value={count} {ts_ns}"
                    ));
                }
                let sum_tags = merge_tags(&base_tags, &dp_tags);
                if let Some(sum) = dp.sum {
                    lines.push(format!("{name}_sum{sum_tags} value={sum} {ts_ns}"));
                }
                lines.push(format!("{name}_count{sum_tags} value={} {ts_ns}", dp.count));
            }
        }
        Some(proto::metric::Data::ExponentialHistogram(eh)) => {
            for dp in &eh.data_points {
                let dp_tags = format_dp_tags(&dp.attributes);
                let all_tags = merge_tags(&base_tags, &dp_tags);
                let ts_ns = dp.time_unix_nano;
                if let Some(sum) = dp.sum {
                    lines.push(format!("{name}_sum{all_tags} value={sum} {ts_ns}"));
                }
                lines.push(format!("{name}_count{all_tags} value={} {ts_ns}", dp.count));
            }
        }
        Some(proto::metric::Data::Summary(s)) => {
            for dp in &s.data_points {
                let dp_tags = format_dp_tags(&dp.attributes);
                let all_tags = merge_tags(&base_tags, &dp_tags);
                let ts_ns = dp.time_unix_nano;
                lines.push(format!("{name}_sum{all_tags} value={} {ts_ns}", dp.sum));
                lines.push(format!("{name}_count{all_tags} value={} {ts_ns}", dp.count));
            }
        }
        None => {}
    }

    lines
}

/// Convert an OTLP span to a single ILP line.
fn span_to_ilp(span: &proto::Span, resource_tags: &[(String, String)]) -> String {
    let base_tags = format_tags(resource_tags);
    let span_tags = format_dp_tags(&span.attributes);
    let all_tags = merge_tags(&base_tags, &span_tags);
    let trace_id = hex::encode(&span.trace_id);
    let span_id = hex::encode(&span.span_id);
    let duration_ns = span
        .end_time_unix_nano
        .saturating_sub(span.start_time_unix_nano);

    format!(
        "otel_traces{all_tags},trace_id={trace_id},span_id={span_id} \
         name=\"{}\",duration_ns={duration_ns}i,kind={}i {}",
        escape_ilp_string(&span.name),
        span.kind,
        span.start_time_unix_nano
    )
}

/// Convert an OTLP log record to a single ILP line.
fn log_to_ilp(record: &proto::LogRecord, resource_tags: &[(String, String)]) -> String {
    let base_tags = format_tags(resource_tags);
    let log_tags = format_dp_tags(&record.attributes);
    let all_tags = merge_tags(&base_tags, &log_tags);
    let trace_id = hex::encode(&record.trace_id);
    let body = record
        .body
        .as_ref()
        .and_then(|v| match &v.value {
            Some(proto::any_value::Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("");

    format!(
        "otel_logs{all_tags},trace_id={trace_id},severity={} \
         body=\"{}\",severity_number={}i {}",
        escape_ilp_string(&record.severity_text),
        escape_ilp_string(body),
        record.severity_number,
        record.time_unix_nano
    )
}

fn format_tags(tags: &[(String, String)]) -> String {
    if tags.is_empty() {
        return String::new();
    }
    let pairs: Vec<String> = tags
        .iter()
        .filter(|(k, v)| !k.is_empty() && !v.is_empty())
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!(",{}", pairs.join(","))
    }
}

fn format_dp_tags(attrs: &[proto::KeyValue]) -> String {
    let pairs: Vec<String> = attrs
        .iter()
        .filter(|kv| !kv.key.is_empty())
        .map(|kv| format!("{}={}", kv.key, kv.string_value()))
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!(",{}", pairs.join(","))
    }
}

fn merge_tags(a: &str, b: &str) -> String {
    format!("{a}{b}")
}

fn escape_ilp_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Neither OTLP receiver terminates TLS: [`run`] and the sibling gRPC
/// receiver both serve a bare `TcpListener`, so every request that reaches a
/// handler arrived in the clear. Stated once, here, rather than assumed at
/// each call site — if either receiver ever gains TLS termination, this
/// constant is what has to stop being a constant.
const OTLP_TRANSPORT: TransportSecurity = TransportSecurity::Cleartext;

/// Authenticate an OTLP request and check it against the TLS policy.
///
/// The transport check lives inside authentication rather than beside it so a
/// handler cannot get the bearer gate while silently skipping
/// `reject_cleartext`. That is the failure this fixes: both receivers bind
/// `0.0.0.0` by default and consulted no transport policy at all, so an
/// operator who demanded TLS still got plaintext ingest ports accepting
/// authenticated writes. Folding it in is also what surfaced the gRPC
/// receiver's three handlers — they were a compile error, not a second audit.
pub(super) async fn authenticate_otel(
    headers: &HeaderMap,
    shared: &SharedState,
    peer_addr: &str,
) -> Result<AuthenticatedIdentity, String> {
    let header = headers
        .get("authorization")
        .ok_or_else(|| "missing Authorization: Bearer <token> header".to_owned())?;
    let value = header
        .to_str()
        .map_err(|_| "invalid authorization header encoding".to_owned())?;
    let token = value
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| "invalid authorization header".to_owned())?;

    if token.matches('.').count() == 2
        && let Some(registry) = &shared.jwks_registry
        && let Ok((identity, verified)) = registry.validate_with_claims(token).await
    {
        crate::control::security::jwt_policy::enforce_stateful_jwt_policy(
            shared,
            verified.claims(),
            identity.tenant_id,
        )
        .map_err(|_| "invalid bearer token".to_owned())?;
        return admit_transport(shared, identity, peer_addr);
    }
    let identity = session_auth::verify_api_key_identity(shared, token, "otlp", "OTLP")
        .ok_or_else(|| "invalid bearer token".to_owned())?;
    admit_transport(shared, identity, peer_addr)
}

/// Refuse an authenticated OTLP request whose transport the TLS policy will
/// not accept.
///
/// Runs after authentication because the policy's superuser carve-out for
/// cleartext needs the identity — the same ordering `handle_ilp_connection`
/// uses for its own transport check.
fn admit_transport(
    shared: &SharedState,
    identity: AuthenticatedIdentity,
    peer_addr: &str,
) -> Result<AuthenticatedIdentity, String> {
    session_auth::check_transport_security(shared, &identity, OTLP_TRANSPORT, peer_addr)
        .map_err(|e| e.to_string())?;
    Ok(identity)
}

async fn ingest_ilp(
    shared: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
    payload: &str,
) -> Result<u64, crate::Error> {
    if payload.is_empty() {
        return Ok(0);
    }
    crate::control::server::ilp_listener::flush_authenticated_ilp_batch(
        shared,
        identity,
        DatabaseId::DEFAULT,
        peer_addr,
        payload,
    )
    .await
}

fn decompress_body(headers: &HeaderMap, body: &Bytes) -> Vec<u8> {
    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if encoding.contains("gzip") {
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(&body[..]);
        let mut decompressed = Vec::new();
        if decoder.read_to_end(&mut decompressed).is_ok() {
            return decompressed;
        }
    }

    body.to_vec()
}
