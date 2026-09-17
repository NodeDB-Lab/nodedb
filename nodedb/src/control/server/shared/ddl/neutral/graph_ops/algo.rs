// SPDX-License-Identifier: BUSL-1.1

//! GRAPH ALGO handler and result-schema rendering.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::broadcast;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::engine::graph::algo::GraphAlgorithm;
use crate::types::TraceId;
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::support::ddl_err;

const MAX_ITERATIONS_CAP: usize = 1_000;
const MAX_SAMPLE_CAP: usize = 1_000_000;

/// Names the algorithm family in the refusal a read policy raises: an
/// algorithm's result is derived from every row of the collection, including
/// the ones the policy hides, and carries no row for a filter to apply to.
const ALGO_WHAT: &str =
    "a graph algorithm, which returns per-node scalars computed over every edge";

/// `GRAPH ALGO` request fields, bundled from the parsed `GraphStmt::GraphAlgo`
/// statement.
pub struct AlgoRequest<'a> {
    pub algorithm_name: &'a str,
    pub collection: String,
    pub edge_label: Option<String>,
    pub damping: Option<f64>,
    pub tolerance: Option<f64>,
    pub resolution: Option<f64>,
    pub max_iterations: Option<usize>,
    pub sample_size: Option<usize>,
    pub source_node: Option<String>,
    pub direction: Option<String>,
    pub mode: Option<String>,
    pub personalization: Option<String>,
}

pub async fn algo(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    request: AlgoRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let AlgoRequest {
        algorithm_name,
        collection,
        edge_label,
        damping,
        tolerance,
        resolution,
        max_iterations,
        sample_size,
        source_node,
        direction,
        mode,
        personalization,
    } = request;
    let algorithm = resolve_algorithm(algorithm_name)?;

    // Every dispatch shape below — the single-node broadcast and both cluster
    // coordinators — reaches the Data Plane without a single plan for the
    // planner's authorization and RLS passes to inspect, so both are resolved
    // here. The result is a rank / component id / count derived from every edge
    // of the collection, so a read policy refuses rather than filters: there is
    // no row in the payload for a filter to apply to, and the hidden rows have
    // already contributed to the number.
    RefusingReadGate::open(state, identity, database_id, &collection, ALGO_WHAT)?;

    let max_iterations = clamp_opt(max_iterations, "ITERATIONS", MAX_ITERATIONS_CAP)?;
    let sample_size = clamp_opt(sample_size, "SAMPLE", MAX_SAMPLE_CAP)?;
    let personalization_vector = parse_personalization(personalization.as_deref())?;

    let params = crate::engine::graph::algo::AlgoParams {
        collection: collection.clone(),
        edge_label,
        damping,
        max_iterations,
        tolerance,
        source_node,
        sample_size,
        direction,
        resolution,
        mode,
        personalization_vector,
    };

    let tenant_id = identity.tenant_id;

    // Cluster PageRank / WCC route through their distributed coordinators: graph
    // edges are Raft-homed on `from_key(src)` and each core's CSR is partitioned,
    // so a single-node `broadcast_to_all_cores` would only see the coordinator's
    // local partitions. Each coordinator runs its per-shard primitive
    // (`GraphOp::BspSuperstep` for PageRank, `GraphOp::WccSuperstep` for WCC)
    // across every shard and assembles the result into the SAME `AlgoResultBatch`
    // payload the single-node path produces, so `algo_payload_to_rows`
    // renders identical output.
    //
    // Single-node (`cluster_routing.is_none()`) and every other algorithm keep
    // the existing `broadcast_to_all_cores` path byte-identical — only
    // cluster-mode PageRank and WCC diverge here.
    if state.cluster_routing.is_some()
        && matches!(algorithm, GraphAlgorithm::PageRank | GraphAlgorithm::Wcc)
    {
        let deadline_ms = state.tuning.network.default_deadline_secs * 1_000;
        let result = match algorithm {
            GraphAlgorithm::PageRank => {
                crate::control::server::graph_dispatch::run_bsp_pagerank(
                    state,
                    tenant_id,
                    database_id,
                    params,
                    deadline_ms,
                )
                .await
            }
            _ => {
                // Wcc — the outer guard guarantees this.
                crate::control::server::graph_dispatch::run_bsp_wcc(
                    state,
                    tenant_id,
                    database_id,
                    params,
                    deadline_ms,
                )
                .await
            }
        };
        return match result {
            Ok(payload) => Ok(algo_payload_to_rows(&payload, algorithm)?),
            Err(e) => Err(ddl_err("XX000", e.to_string())),
        };
    }

    let plan = PhysicalPlan::Graph(GraphOp::Algo { algorithm, params });

    match broadcast::broadcast_to_all_cores(state, tenant_id, database_id, plan, TraceId::ZERO)
        .await
    {
        Ok(resp) => Ok(algo_payload_to_rows(&resp.payload, algorithm)?),
        Err(e) => Err(ddl_err("XX000", e.to_string())),
    }
}

/// Resolve a `GRAPH ALGO <name>` keyword to its [`GraphAlgorithm`] variant.
///
/// `COMMUNITY` and `LABEL_PROPAGATION` are accepted aliases that both map to
/// label propagation. Unknown names surface a structured `42601` error rather
/// than a catch-all default.
fn resolve_algorithm(algorithm_name: &str) -> Result<GraphAlgorithm, DdlError> {
    Ok(match algorithm_name {
        "PAGERANK" => GraphAlgorithm::PageRank,
        "WCC" => GraphAlgorithm::Wcc,
        "COMMUNITY" | "LABEL_PROPAGATION" => GraphAlgorithm::LabelPropagation,
        "LCC" => GraphAlgorithm::Lcc,
        "SSSP" => GraphAlgorithm::Sssp,
        "BETWEENNESS" => GraphAlgorithm::Betweenness,
        "CLOSENESS" => GraphAlgorithm::Closeness,
        "HARMONIC" => GraphAlgorithm::Harmonic,
        "DEGREE" => GraphAlgorithm::Degree,
        "LOUVAIN" => GraphAlgorithm::Louvain,
        "TRIANGLES" => GraphAlgorithm::Triangles,
        "DIAMETER" => GraphAlgorithm::Diameter,
        "KCORE" => GraphAlgorithm::KCore,
        other => {
            return Err(ddl_err(
                "42601",
                format!("unknown graph algorithm '{other}'"),
            ));
        }
    })
}

/// Parse the `PERSONALIZATION {…}` JSON object literal into a `node_id → weight`
/// seed map for Personalized PageRank. Returns `Ok(None)` when absent; a
/// malformed object surfaces a structured `22023` error rather than being
/// silently dropped.
fn parse_personalization(
    raw: Option<&str>,
) -> Result<Option<std::collections::HashMap<String, f64>>, DdlError> {
    let Some(text) = raw else {
        return Ok(None);
    };
    let map: std::collections::HashMap<String, f64> = sonic_rs::from_str(text).map_err(|e| {
        ddl_err(
            "22023",
            format!("invalid PERSONALIZATION object (expected JSON node→weight map): {e}"),
        )
    })?;
    if map.is_empty() {
        return Ok(None);
    }
    Ok(Some(map))
}

fn clamp_opt(
    value: Option<usize>,
    field: &'static str,
    cap: usize,
) -> Result<Option<usize>, DdlError> {
    match value {
        Some(v) if v > cap => Err(ddl_err(
            "22023",
            format!("{field} {v} exceeds maximum allowed value {cap}"),
        )),
        other => Ok(other),
    }
}

/// Render an algorithm result payload into a protocol-neutral row set.
///
/// Every column is emitted as `Text` with its cell pre-rendered to the exact
/// string the pgwire handler wrote (all algorithm result columns used
/// `text_field`): `Text` → the raw string, `Float64` → `format!("{v}")` or the
/// literal `Infinity` for a non-representable/non-finite score, `Int64` →
/// decimal or `0`. Pre-rendering keeps the wire bytes byte-identical (a native
/// float path would change both the column OID and the `Infinity` fallback).
fn algo_payload_to_rows(
    payload: &crate::bridge::envelope::Payload,
    algorithm: GraphAlgorithm,
) -> Result<Vec<DdlResult>, DdlError> {
    use crate::engine::graph::algo::params::AlgoColumnType;

    let result_schema = algorithm.result_schema();
    let columns: Vec<String> = result_schema
        .iter()
        .map(|&(name, _)| name.to_string())
        .collect();
    if payload.is_empty() {
        return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
            columns,
            Vec::new(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let rows: Vec<serde_json::Value> = sonic_rs::from_str(&json_text)
        .map_err(|e| ddl_err("XX000", format!("invalid algorithm result JSON: {e}")))?;

    let mut shaped_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut out = Map::new();
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
            out.insert(col_name.to_string(), JsonValue::String(val_str));
        }
        shaped_rows.push(out);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        shaped_rows,
    ))])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn community_resolves_to_label_propagation() {
        assert!(matches!(
            resolve_algorithm("COMMUNITY").unwrap(),
            GraphAlgorithm::LabelPropagation
        ));
    }

    #[test]
    fn label_propagation_alias_resolves_to_label_propagation() {
        assert!(matches!(
            resolve_algorithm("LABEL_PROPAGATION").unwrap(),
            GraphAlgorithm::LabelPropagation
        ));
    }

    #[test]
    fn unknown_algorithm_is_rejected() {
        assert!(resolve_algorithm("NOPE").is_err());
    }
}
