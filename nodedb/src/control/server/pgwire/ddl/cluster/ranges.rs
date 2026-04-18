//! `SHOW RANGES` — vshard distribution across the cluster.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{float8_field, int8_field, sqlstate_error, text_field};

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
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view ranges",
        ));
    }

    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let schema = Arc::new(vec![
        int8_field("vshard_id"),
        int8_field("group_id"),
        int8_field("leaseholder"),
        text_field("replicas"),
        float8_field("qps"),
        float8_field("p99_latency_ms"),
        int8_field("requests_total"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

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
        encoder.encode_field(&(vshard_id as i64))?;
        encoder.encode_field(&(group_id as i64))?;
        encoder.encode_field(&leader)?;
        encoder.encode_field(&replicas_str)?;
        encoder.encode_field(&qps)?;
        encoder.encode_field(&p99_ms)?;
        encoder.encode_field(&req_total)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
