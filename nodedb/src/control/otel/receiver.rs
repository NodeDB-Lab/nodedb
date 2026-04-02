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
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use prost::Message;
use tracing::info;

use super::proto;
use crate::bridge::physical_plan::{PhysicalPlan, TimeseriesOp};
use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

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

    let router = Router::new()
        .route("/v1/metrics", post(receive_metrics))
        .route("/v1/traces", post(receive_traces))
        .route("/v1/logs", post(receive_logs))
        .with_state(shared);

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    info!(addr = %config.listen, "OTLP/HTTP receiver started");
    axum::serve(listener, router)
        .await
        .map_err(std::io::Error::other)
}

/// POST `/v1/metrics` — OTLP metrics receiver.
///
/// Accepts protobuf `ExportMetricsServiceRequest` (optionally gzip-compressed).
/// Maps OTLP metric types (gauge, sum, histogram) to ILP for timeseries ingest.
pub async fn receive_metrics(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let data = decompress_body(&headers, &body);
    let req = match proto::ExportMetricsServiceRequest::decode(&data[..]) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode error: {e}")),
    };

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
                let collection = metric.name.clone();
                let payload = lines.join("\n");

                match ingest_ilp(&state, &collection, payload.into_bytes()).await {
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
/// Stores spans as structured documents with trace_id, span_id, timestamps,
/// attributes, and status. Enables distributed trace querying via SQL.
pub async fn receive_traces(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let data = decompress_body(&headers, &body);
    let req = match proto::ExportTraceServiceRequest::decode(&data[..]) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode error: {e}")),
    };

    let mut span_count = 0u64;
    for rs in &req.resource_spans {
        let resource_tags = proto::resource_tags(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let ilp = span_to_ilp(span, &resource_tags);
                let _ = ingest_ilp(&state, "otel_traces", ilp.into_bytes()).await;
                span_count += 1;
            }
        }
    }

    (StatusCode::OK, format!("{{\"spans\":{span_count}}}"))
}

/// POST `/v1/logs` — OTLP logs receiver.
///
/// Stores log records with severity, body, timestamp, trace correlation.
pub async fn receive_logs(
    State(state): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let data = decompress_body(&headers, &body);
    let req = match proto::ExportLogsServiceRequest::decode(&data[..]) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode error: {e}")),
    };

    let mut log_count = 0u64;
    for rl in &req.resource_logs {
        let resource_tags = proto::resource_tags(&rl.resource);
        for sl in &rl.scope_logs {
            for record in &sl.log_records {
                let ilp = log_to_ilp(record, &resource_tags);
                let _ = ingest_ilp(&state, "otel_logs", ilp.into_bytes()).await;
                log_count += 1;
            }
        }
    }

    (StatusCode::OK, format!("{{\"logs\":{log_count}}}"))
}

// ── Core ingest functions (shared by HTTP + gRPC handlers) ───────────────

/// Ingest an OTLP metrics request into the timeseries engine.
pub async fn ingest_metrics(shared: &Arc<SharedState>, req: &proto::ExportMetricsServiceRequest) {
    for rm in &req.resource_metrics {
        let resource_tags = proto::resource_tags(&rm.resource);
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let lines = metric_to_ilp(metric, &resource_tags);
                if !lines.is_empty() {
                    let collection = metric.name.clone();
                    let payload = lines.join("\n");
                    let _ = ingest_ilp(shared, &collection, payload.into_bytes()).await;
                }
            }
        }
    }
}

/// Ingest an OTLP traces request into the document engine.
pub async fn ingest_traces(shared: &Arc<SharedState>, req: &proto::ExportTraceServiceRequest) {
    for rs in &req.resource_spans {
        let resource_tags = proto::resource_tags(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let ilp = span_to_ilp(span, &resource_tags);
                let _ = ingest_ilp(shared, "otel_traces", ilp.into_bytes()).await;
            }
        }
    }
}

/// Ingest an OTLP logs request into the document engine.
pub async fn ingest_logs(shared: &Arc<SharedState>, req: &proto::ExportLogsServiceRequest) {
    for rl in &req.resource_logs {
        let resource_tags = proto::resource_tags(&rl.resource);
        for sl in &rl.scope_logs {
            for record in &sl.log_records {
                let ilp = log_to_ilp(record, &resource_tags);
                let _ = ingest_ilp(shared, "otel_logs", ilp.into_bytes()).await;
            }
        }
    }
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

async fn ingest_ilp(
    shared: &Arc<SharedState>,
    collection: &str,
    payload: Vec<u8>,
) -> Result<u64, crate::Error> {
    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: collection.to_string(),
        payload,
        format: "ilp".into(),
        wal_lsn: None,
    });
    dispatch_to_data_plane(shared, TenantId::new(1), VShardId::new(0), plan, 0).await?;
    Ok(1)
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

/// Minimal hex encoding for trace/span IDs (avoids adding `hex` crate).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize]);
            s.push(HEX_CHARS[(b & 0xf) as usize]);
        }
        s
    }
    const HEX_CHARS: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
}
