// SPDX-License-Identifier: BUSL-1.1

//! OTLP exporter: pushes NodeDB's own traces and metrics to an OTLP collector.
//!
//! - Trace export: propagates existing trace_id spans (Control → Bridge → Data)
//!   as OTLP spans with parent-child relationships.
//! - Metrics export: periodically pushes SystemMetrics as OTLP gauge/sum metrics.
//! - Baggage propagation: tenant_id, vshard_id in trace attributes.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use super::proto::{
    self, ExportMetricsServiceRequest, ExportTraceServiceRequest, KeyValue, Metric,
    NumberDataPoint, Resource, ResourceMetrics, ResourceSpans, ScopeMetrics, ScopeSpans, Span,
    SpanKind, SpanStatus, StatusCode as OtelStatusCode,
};
use crate::control::metrics::SystemMetrics;
use crate::control::security::egress::EgressError;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;
use nodedb_types::TraceId;

/// Configuration for the OTLP exporter.
#[derive(Debug, Clone)]
pub struct ExporterConfig {
    /// OTLP collector endpoint (e.g., "http://localhost:4318").
    pub endpoint: String,
    /// How often to push metrics (default: 15 seconds).
    pub metrics_interval: Duration,
    /// Whether to export traces.
    pub export_traces: bool,
    /// Whether to export metrics.
    pub export_metrics: bool,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            metrics_interval: Duration::from_secs(15),
            export_traces: false,
            export_metrics: false,
        }
    }
}

impl ExporterConfig {
    pub fn is_enabled(&self) -> bool {
        !self.endpoint.is_empty() && (self.export_traces || self.export_metrics)
    }
}

/// Cap on one metrics POST.
const METRICS_POST_TIMEOUT: Duration = Duration::from_secs(10);

/// Spawn the background OTLP metrics exporter of `shared`.
///
/// Every `config.metrics_interval` it reads the node's `SystemMetrics` and
/// posts them to `<endpoint>/v1/metrics` through the outbound egress client.
/// The loop joins at the Control Plane drain. The endpoint is operator
/// config: one the `[egress]` policy refuses is an error, and nothing is
/// spawned. Returns whether the exporter started: it does not when metrics
/// export is off or the state carries no metrics registry.
pub fn spawn_metrics_exporter(
    shared: &Arc<SharedState>,
    config: ExporterConfig,
) -> Result<bool, EgressError> {
    if !config.is_enabled() || !config.export_metrics {
        return Ok(false);
    }
    let Some(metrics) = shared.system_metrics.clone() else {
        return Ok(false);
    };
    let endpoint = format!("{}/v1/metrics", config.endpoint.trim_end_matches('/'));
    shared.remote_sources.egress().check_url(&endpoint)?;
    let client = Arc::clone(shared.remote_sources.http());
    let interval = config.metrics_interval;
    let node_id = shared.node_id;

    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "otlp_metrics_export",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = ticker.tick() => {}
                }
                let request = build_metrics_request(&metrics, node_id);
                let mut buf = Vec::new();
                if prost::Message::encode(&request, &mut buf).is_err() {
                    continue;
                }
                match client
                    .post(&endpoint)
                    .header("content-type", "application/x-protobuf")
                    .timeout(METRICS_POST_TIMEOUT)
                    .body(buf)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        debug!("OTLP metrics export: OK");
                    }
                    Ok(resp) => {
                        warn!("OTLP metrics export: status {}", resp.status());
                    }
                    Err(e) => {
                        warn!("OTLP metrics export: {}", e.without_url());
                    }
                }
            }
        },
    );
    Ok(true)
}

/// Build an OTLP ExportMetricsServiceRequest from SystemMetrics.
fn build_metrics_request(metrics: &SystemMetrics, node_id: u64) -> ExportMetricsServiceRequest {
    use std::sync::atomic::Ordering;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let resource = Resource {
        attributes: vec![
            kv("service.name", "nodedb"),
            kv("service.instance.id", &node_id.to_string()),
        ],
    };

    let gauge_metrics = vec![
        gauge_metric(
            "nodedb_active_connections",
            metrics.active_connections.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
        gauge_metric(
            "nodedb_wal_fsync_mean_seconds",
            {
                let count = metrics.wal_fsync_seconds.count();
                if count > 0 {
                    metrics.wal_fsync_seconds.sum_us() as f64 / count as f64 / 1_000_000.0
                } else {
                    0.0
                }
            },
            now_ns,
        ),
        gauge_metric(
            "nodedb_bridge_utilization_ratio",
            metrics.bridge_utilization.load(Ordering::Relaxed) as f64 / 100.0,
            now_ns,
        ),
        gauge_metric(
            "nodedb_compaction_debt",
            metrics.compaction_debt.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
        gauge_metric(
            "nodedb_kv_memory_bytes",
            metrics.kv_memory_bytes.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
    ];

    let sum_metrics = vec![
        sum_metric(
            "nodedb_queries_total",
            metrics.queries_total.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
        sum_metric(
            "nodedb_query_errors_total",
            metrics.query_errors.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
        sum_metric(
            "nodedb_vector_searches_total",
            metrics.vector_searches.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
        sum_metric(
            "nodedb_graph_traversals_total",
            metrics.graph_traversals.load(Ordering::Relaxed) as f64,
            now_ns,
        ),
    ];

    let mut all_metrics = gauge_metrics;
    all_metrics.extend(sum_metrics);

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(proto::InstrumentationScope {
                    name: "nodedb".into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                }),
                metrics: all_metrics,
            }],
        }],
    }
}

/// Parameters for exporting a single trace span.
pub struct SpanExport<'a> {
    pub endpoint: &'a str,
    pub trace_id: TraceId,
    pub span_name: &'a str,
    pub start_ns: u64,
    pub end_ns: u64,
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub status_ok: bool,
}

/// Export a single trace span to an OTLP collector using the shared
/// `reqwest::Client` held on `SharedState`.
///
/// Call this from the request tracker when a bridge round-trip completes.
pub async fn export_span(
    client: &reqwest::Client,
    timeout: std::time::Duration,
    params: &SpanExport<'_>,
) {
    let SpanExport {
        endpoint,
        trace_id,
        span_name,
        start_ns,
        end_ns,
        tenant_id,
        vshard_id,
        status_ok,
    } = params;
    if endpoint.is_empty() {
        return;
    }

    let span_id_bytes = rand_span_id();

    let span = Span {
        trace_id: trace_id.0.to_vec(),
        span_id: span_id_bytes.to_vec(),
        name: (*span_name).into(),
        kind: SpanKind::Server as i32,
        start_time_unix_nano: *start_ns,
        end_time_unix_nano: *end_ns,
        attributes: vec![
            kv("nodedb.tenant_id", &tenant_id.to_string()),
            kv("nodedb.vshard_id", &vshard_id.to_string()),
        ],
        status: Some(SpanStatus {
            message: String::new(),
            code: if *status_ok {
                OtelStatusCode::Ok as i32
            } else {
                OtelStatusCode::Error as i32
            },
        }),
    };

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "nodedb")],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(proto::InstrumentationScope {
                    name: "nodedb".into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                }),
                spans: vec![span],
            }],
        }],
    };

    let mut buf = Vec::new();
    if prost::Message::encode(&request, &mut buf).is_err() {
        return;
    }

    let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
    export_trace_with_client(client, timeout, &url, &buf).await;
}

/// Public OTLP trace export using a caller-provided shared client.
///
/// The `SharedState` holds a single `Arc<reqwest::Client>` that alert,
/// SIEM, and OTEL emitters all reuse — no per-call client construction.
pub async fn export_trace_with_client(
    client: &reqwest::Client,
    timeout: std::time::Duration,
    url: &str,
    spans_proto_bytes: &[u8],
) {
    let _ = client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .timeout(timeout)
        .body(spans_proto_bytes.to_vec())
        .send()
        .await;
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(proto::AnyValue {
            value: Some(proto::any_value::Value::StringValue(value.into())),
        }),
    }
}

fn gauge_metric(name: &str, value: f64, time_ns: u64) -> Metric {
    Metric {
        name: name.into(),
        description: String::new(),
        unit: String::new(),
        data: Some(proto::metric::Data::Gauge(proto::Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                time_unix_nano: time_ns,
                value: Some(proto::number_data_point::Value::AsDouble(value)),
            }],
        })),
    }
}

fn sum_metric(name: &str, value: f64, time_ns: u64) -> Metric {
    Metric {
        name: name.into(),
        description: String::new(),
        unit: String::new(),
        data: Some(proto::metric::Data::Sum(proto::Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                time_unix_nano: time_ns,
                value: Some(proto::number_data_point::Value::AsDouble(value)),
            }],
            is_monotonic: true,
        })),
    }
}

fn rand_span_id() -> [u8; 8] {
    let mut id = [0u8; 8];
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    id.copy_from_slice(&ts.to_le_bytes());
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("otlp.wal")).expect("wal"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, dir)
    }

    fn metrics_config(endpoint: &str) -> ExporterConfig {
        ExporterConfig {
            endpoint: endpoint.into(),
            export_metrics: true,
            ..ExporterConfig::default()
        }
    }

    #[tokio::test]
    async fn a_metrics_endpoint_outside_the_egress_policy_is_refused() {
        let (state, _dir) = test_state();
        for (endpoint, expected) in [
            ("http://127.0.0.1:4318", EgressError::IpLiteral),
            ("http://otel-collector:4318", EgressError::NonHttps),
        ] {
            let refused = spawn_metrics_exporter(&state, metrics_config(endpoint));
            assert_eq!(refused.err(), Some(expected), "{endpoint}");
        }
    }

    #[tokio::test]
    async fn an_unconfigured_exporter_never_starts() {
        let (state, _dir) = test_state();
        let off = ExporterConfig {
            endpoint: "https://otel.example".into(),
            ..ExporterConfig::default()
        };
        assert_eq!(spawn_metrics_exporter(&state, off), Ok(false));
        assert_eq!(
            spawn_metrics_exporter(&state, ExporterConfig::default()),
            Ok(false)
        );
    }
}
