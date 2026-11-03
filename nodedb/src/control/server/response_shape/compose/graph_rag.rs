// SPDX-License-Identifier: BUSL-1.1

//! Shaping for a `GraphOp::RagFusion` response.

use nodedb_types::Value;

use crate::control::sequence::SequenceAccess;
use crate::data::executor::response_codec::decode_payload_value;

use super::super::redaction::RedactionCtx;
use super::super::schema::OutputSchema;
use super::super::types::{DdlColType, ShapedRows};
use super::kernel::{empty_shaped, shape_decoded_rows};

/// Shape a `GraphOp::RagFusion` response: one row per fused hit, with the
/// columns `node_id`, `rrf_score`, `vector_rank`, `vector_distance` and
/// `hop_distance`. The run's metadata becomes a notice when the run hit its
/// visit budget or reached nodes that carry no identity binding. A payload
/// without a `results` array shapes as the generic response does.
pub fn shape_graph_rag(
    payload: &[u8],
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
    sequences: Option<&dyn SequenceAccess>,
    crs_set: &nodedb_crs::CustomCrsSet,
) -> crate::Result<ShapedRows> {
    if payload.is_empty() {
        return Ok(empty_shaped());
    }
    let Ok(decoded) = decode_payload_value(payload) else {
        return Ok(empty_shaped());
    };
    let Value::Object(mut fields) = decoded else {
        return shape_decoded_rows(decoded, projection, redaction, sequences, crs_set);
    };
    let notice = fields.get("metadata").and_then(metadata_notice);
    let rows = fields
        .remove("results")
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let mut shaped = shape_decoded_rows(rows, projection, redaction, sequences, crs_set)?;
    if projection.is_none_or(|p| p.is_star || p.columns.is_empty()) {
        order_and_type_columns(&mut shaped);
    }
    shaped.notice = notice;
    Ok(shaped)
}

/// The columns of a fused hit, in the order a client reads them.
const HIT_COLUMNS: [(&str, DdlColType); 5] = [
    ("node_id", DdlColType::Text),
    ("rrf_score", DdlColType::Float8),
    ("vector_rank", DdlColType::Int8),
    ("vector_distance", DdlColType::Float8),
    ("hop_distance", DdlColType::Int8),
];

/// Put the hit columns first in their fixed order, each with its type. A
/// column outside the set follows as text.
fn order_and_type_columns(shaped: &mut ShapedRows) {
    let mut columns: Vec<String> = Vec::new();
    let mut types: Vec<DdlColType> = Vec::new();
    for (name, ty) in HIT_COLUMNS {
        if shaped.columns.iter().any(|c| c == name) {
            columns.push(name.to_string());
            types.push(ty);
        }
    }
    for name in &shaped.columns {
        if !columns.contains(name) {
            columns.push(name.clone());
            types.push(DdlColType::Text);
        }
    }
    shaped.columns = columns;
    shaped.column_types = types;
}

/// The notice a run's metadata earns, `None` for a complete run.
fn metadata_notice(metadata: &Value) -> Option<String> {
    let Value::Object(meta) = metadata else {
        return None;
    };
    let truncated = matches!(meta.get("truncated"), Some(Value::Bool(true)));
    let unaddressable = match meta.get("graph_unaddressable") {
        Some(Value::Integer(n)) => *n,
        _ => 0,
    };
    let mut parts = Vec::new();
    if truncated {
        parts
            .push("the expansion stopped at MAX_VISITED, so results may be incomplete".to_string());
    }
    if unaddressable > 0 {
        parts.push(format!(
            "{unaddressable} expanded nodes carry no identity binding and cannot intersect other engines"
        ));
    }
    (!parts.is_empty()).then(|| format!("GRAPH RAG FUSION: {}", parts.join("; ")))
}
