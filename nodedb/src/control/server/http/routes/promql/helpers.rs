//! Shared helpers for PromQL HTTP handlers.

use axum::http::StatusCode;

use crate::control::promql;
use crate::control::server::http::auth::AppState;

pub type PromResponse = (StatusCode, [(&'static str, &'static str); 1], String);

pub fn prom_success(value: promql::Value) -> PromResponse {
    let result = promql::PromResult::success(value);
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        result.to_json(),
    )
}

pub fn prom_error(err_type: &str, message: &str) -> PromResponse {
    let result = promql::PromResult::error(err_type, message.to_string());
    let status = match err_type {
        "bad_data" => StatusCode::BAD_REQUEST,
        "execution" => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        [("content-type", "application/json")],
        result.to_json(),
    )
}

pub fn parse_step(s: &str) -> Option<i64> {
    if let Some(d) = promql::ast::Duration::parse(s) {
        return Some(d.ms());
    }
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    None
}

pub fn parse_series_matcher(input: &str) -> Option<Vec<promql::LabelMatcher>> {
    let tokens = promql::lexer::tokenize(input).ok()?;
    let expr = promql::parse(&tokens).ok()?;
    match expr {
        promql::ast::Expr::VectorSelector { matchers, .. } => Some(matchers),
        _ => None,
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Fetch all timeseries data within [start_ms, end_ms] from the shared state.
pub async fn fetch_series_for_query(
    state: &AppState,
    _start_ms: i64,
    _end_ms: i64,
) -> Vec<promql::Series> {
    let mut series = Vec::new();

    if let Some(ref sys) = state.shared.system_metrics {
        use std::sync::atomic::Ordering;

        let ts = now_ms();
        let metrics: Vec<(&str, f64)> = vec![
            (
                "nodedb_queries_total",
                sys.queries_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_query_errors_total",
                sys.query_errors.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_active_connections",
                sys.active_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_wal_fsync_latency_us",
                sys.wal_fsync_latency_us.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_raft_apply_lag",
                sys.raft_apply_lag.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_bridge_utilization",
                sys.bridge_utilization.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_compaction_debt",
                sys.compaction_debt.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_vector_searches_total",
                sys.vector_searches.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_graph_traversals_total",
                sys.graph_traversals.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_text_searches_total",
                sys.text_searches.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_kv_gets_total",
                sys.kv_gets_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_kv_memory_bytes",
                sys.kv_memory_bytes.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_pgwire_connections",
                sys.pgwire_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_http_connections",
                sys.http_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_websocket_connections",
                sys.websocket_connections.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_slow_queries_total",
                sys.slow_queries_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_storage_l0_bytes",
                sys.storage_l0_bytes.load(Ordering::Relaxed) as f64,
            ),
            (
                "nodedb_storage_l1_bytes",
                sys.storage_l1_bytes.load(Ordering::Relaxed) as f64,
            ),
        ];

        for (name, value) in metrics {
            let mut labels = std::collections::BTreeMap::new();
            labels.insert("__name__".into(), name.into());
            series.push(promql::Series {
                labels,
                samples: vec![promql::Sample {
                    timestamp_ms: ts,
                    value,
                }],
            });
        }
    }

    series
}
