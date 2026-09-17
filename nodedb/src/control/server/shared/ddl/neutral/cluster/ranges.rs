// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW RANGES` — vshard distribution across the cluster.
//!
//! Ported from the pgwire `ddl::cluster::ranges` handler. The routing /
//! per-vshard-metrics reads are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral `DdlResult` over `ShapedRows`.
//!
//! `qps` and `p99_latency_ms` are `float8` columns. Unlike every other
//! migrated column (rendered as `JsonValue::String` decimal/text), these are
//! carried as `JsonValue::Number` so the shared pgwire `ddl_encode.rs` can
//! encode them through pgwire's native `f64` text path (ryu +
//! `extra_float_digits`) — the exact path the original handler's
//! `encoder.encode_field(&f64)` used. Pre-rendering via `f64::to_string()`
//! would diverge (e.g. `1.0` → pgwire `"1.0"` vs Rust `"1"`).
//!
//! Both source values are always finite: `qps` is an integer centihertz
//! counter divided by 100.0, and `p99_latency_ms` is an integer microsecond
//! percentile divided by 1_000.0 — neither can be NaN or infinite, so the
//! `Number::from_f64` conversion (which cannot represent NaN/inf) never
//! silently drops a value here.

use serde_json::{Map, Number, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

fn float_cell(v: f64) -> JsonValue {
    Number::from_f64(v)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

/// SHOW RANGES — list vshards with leaseholder, replicas, and the live
/// per-vshard load signals (QPS, p99 latency) the rebalancer uses for
/// move decisions.
///
/// Columns: vshard_id, group_id, leaseholder, replicas, qps,
/// p99_latency_ms, requests_total.
/// Superuser only.
pub fn show_ranges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view ranges",
        ));
    }

    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let columns = vec![
        "vshard_id".to_string(),
        "group_id".to_string(),
        "leaseholder".to_string(),
        "replicas".to_string(),
        "qps".to_string(),
        "p99_latency_ms".to_string(),
        "requests_total".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Float8,
        DdlColType::Float8,
        DdlColType::Int8,
    ];

    let mut rows = Vec::new();

    let rt = routing.read().unwrap_or_else(|p| p.into_inner());
    for vshard_id in 0..nodedb_cluster::routing::VSHARD_COUNT {
        let group_id = rt.group_for_vshard(vshard_id).unwrap_or(0);
        let (leader, replicas_str) = match rt.group_info(group_id) {
            Some(info) => {
                let replicas: String = info
                    .members
                    .iter()
                    .map(|m| m.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                (info.leader as i64, replicas)
            }
            None => (0i64, String::new()),
        };
        let (qps, p99_ms, req_total) = match state.per_vshard_metrics.snapshot(vshard_id) {
            Some(s) => (s.qps, s.p99_us as f64 / 1_000.0, s.requests_total as i64),
            None => (0.0_f64, 0.0_f64, 0_i64),
        };

        let mut row = Map::new();
        row.insert(
            "vshard_id".to_string(),
            JsonValue::String((vshard_id as i64).to_string()),
        );
        row.insert(
            "group_id".to_string(),
            JsonValue::String((group_id as i64).to_string()),
        );
        row.insert(
            "leaseholder".to_string(),
            JsonValue::String(leader.to_string()),
        );
        row.insert("replicas".to_string(), JsonValue::String(replicas_str));
        row.insert("qps".to_string(), float_cell(qps));
        row.insert("p99_latency_ms".to_string(), float_cell(p99_ms));
        row.insert(
            "requests_total".to_string(),
            JsonValue::String(req_total.to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
