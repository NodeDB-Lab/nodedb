//! GRAPH ALGO handler and result-schema rendering.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;
use crate::data::executor::response_codec;

const MAX_ITERATIONS_CAP: usize = 1_000;
const MAX_SAMPLE_CAP: usize = 1_000_000;

#[allow(clippy::too_many_arguments)]
pub async fn algo(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    algorithm_name: &str,
    collection: String,
    damping: Option<f64>,
    tolerance: Option<f64>,
    resolution: Option<f64>,
    max_iterations: Option<usize>,
    sample_size: Option<usize>,
    source_node: Option<String>,
    direction: Option<String>,
    mode: Option<String>,
) -> PgWireResult<Vec<Response>> {
    let algorithm = match algorithm_name {
        "PAGERANK" => crate::engine::graph::algo::GraphAlgorithm::PageRank,
        "WCC" => crate::engine::graph::algo::GraphAlgorithm::Wcc,
        "COMMUNITY" => crate::engine::graph::algo::GraphAlgorithm::LabelPropagation,
        "LCC" => crate::engine::graph::algo::GraphAlgorithm::Lcc,
        "SSSP" => crate::engine::graph::algo::GraphAlgorithm::Sssp,
        "BETWEENNESS" => crate::engine::graph::algo::GraphAlgorithm::Betweenness,
        "CLOSENESS" => crate::engine::graph::algo::GraphAlgorithm::Closeness,
        "HARMONIC" => crate::engine::graph::algo::GraphAlgorithm::Harmonic,
        "DEGREE" => crate::engine::graph::algo::GraphAlgorithm::Degree,
        "LOUVAIN" => crate::engine::graph::algo::GraphAlgorithm::Louvain,
        "TRIANGLES" => crate::engine::graph::algo::GraphAlgorithm::Triangles,
        "DIAMETER" => crate::engine::graph::algo::GraphAlgorithm::Diameter,
        "KCORE" => crate::engine::graph::algo::GraphAlgorithm::KCore,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!("unknown graph algorithm '{other}'"),
            ));
        }
    };

    let max_iterations = clamp_opt(max_iterations, "ITERATIONS", MAX_ITERATIONS_CAP)?;
    let sample_size = clamp_opt(sample_size, "SAMPLE", MAX_SAMPLE_CAP)?;

    let params = crate::engine::graph::algo::AlgoParams {
        collection: collection.clone(),
        damping,
        max_iterations,
        tolerance,
        source_node,
        sample_size,
        direction,
        resolution,
        mode,
    };

    let tenant_id = identity.tenant_id;
    let plan = PhysicalPlan::Graph(GraphOp::Algo { algorithm, params });

    match dispatch_utils::broadcast_to_all_cores(state, tenant_id, plan, 0).await {
        Ok(resp) => algo_payload_to_query_response(&resp.payload, algorithm),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

fn clamp_opt(value: Option<usize>, field: &'static str, cap: usize) -> PgWireResult<Option<usize>> {
    match value {
        Some(v) if v > cap => Err(sqlstate_error(
            "22023",
            &format!("{field} {v} exceeds maximum allowed value {cap}"),
        )),
        other => Ok(other),
    }
}

fn algo_payload_to_query_response(
    payload: &crate::bridge::envelope::Payload,
    algorithm: crate::engine::graph::algo::GraphAlgorithm,
) -> PgWireResult<Vec<Response>> {
    use crate::engine::graph::algo::params::AlgoColumnType;

    let result_schema = algorithm.result_schema();
    let schema = Arc::new(
        result_schema
            .iter()
            .map(|&(name, _)| text_field(name))
            .collect::<Vec<_>>(),
    );

    if payload.is_empty() {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let rows: Vec<serde_json::Value> = sonic_rs::from_str(&json_text)
        .map_err(|e| sqlstate_error("XX000", &format!("invalid algorithm result JSON: {e}")))?;

    let mut pgwire_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for &(col_name, col_type) in result_schema {
            let field = row.get(col_name).unwrap_or(&serde_json::Value::Null);
            let val_str = match col_type {
                AlgoColumnType::Text => field.as_str().unwrap_or("").to_string(),
                AlgoColumnType::Float64 => match field.as_f64() {
                    Some(v) => format!("{v}"),
                    None => "Infinity".to_string(),
                },
                AlgoColumnType::Int64 => field.as_i64().map_or("0".into(), |v| v.to_string()),
            };
            encoder
                .encode_field(&val_str)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
        pgwire_rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(pgwire_rows),
    ))])
}
