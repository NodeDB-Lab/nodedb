//! Graph operation plan builders.

use std::sync::Arc;

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;

use super::parse_direction;

pub(crate) fn build_rag_fusion(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::RagFusion {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        vector_top_k: fields.vector_top_k.unwrap_or(20) as usize,
        edge_label: fields.edge_label.clone(),
        direction: parse_direction(fields.direction.as_deref()),
        expansion_depth: fields.expansion_depth.unwrap_or(2) as usize,
        final_top_k: fields.final_top_k.unwrap_or(10) as usize,
        rrf_k: (
            fields.vector_k.unwrap_or(60.0),
            fields.graph_k.unwrap_or(10.0),
        ),
        options: Default::default(),
    }))
}

pub(crate) fn build_hop(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Hop {
        start_nodes: vec![start.clone()],
        depth: fields.depth.unwrap_or(2) as usize,
        edge_label: fields.edge_label.clone(),
        direction: parse_direction(fields.direction.as_deref()),
        options: Default::default(),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_neighbors(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Neighbors {
        node_id: start.clone(),
        edge_label: fields.edge_label.clone(),
        direction: parse_direction(fields.direction.as_deref()),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_path(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let from = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    let to = fields
        .end_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'end_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Path {
        src: from.clone(),
        dst: to.clone(),
        max_depth: fields.depth.unwrap_or(10) as usize,
        edge_label: fields.edge_label.clone(),
        options: Default::default(),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_subgraph(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Subgraph {
        start_nodes: vec![start.clone()],
        depth: fields.depth.unwrap_or(2) as usize,
        edge_label: fields.edge_label.clone(),
        options: Default::default(),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_edge_put(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let src = fields
        .from_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'from_node'".to_string(),
        })?;
    let dst = fields
        .to_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'to_node'".to_string(),
        })?;
    let label = fields
        .edge_type
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'edge_type'".to_string(),
        })?;
    let props = fields
        .properties
        .as_ref()
        .map(|v| serde_json::to_string(v).unwrap_or_default())
        .unwrap_or_default();
    Ok(PhysicalPlan::Graph(GraphOp::EdgePut {
        src_id: src.clone(),
        label: label.clone(),
        dst_id: dst.clone(),
        properties: props.into_bytes(),
    }))
}

pub(crate) fn build_edge_delete(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let src = fields
        .from_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'from_node'".to_string(),
        })?;
    let dst = fields
        .to_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'to_node'".to_string(),
        })?;
    let label = fields
        .edge_type
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'edge_type'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::EdgeDelete {
        src_id: src.clone(),
        label: label.clone(),
        dst_id: dst.clone(),
    }))
}

pub(crate) fn build_algo(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let algo_name = fields
        .algorithm
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'algorithm'".to_string(),
        })?;

    let algorithm = match algo_name.to_lowercase().as_str() {
        "pagerank" => crate::engine::graph::algo::params::GraphAlgorithm::PageRank,
        "wcc" => crate::engine::graph::algo::params::GraphAlgorithm::Wcc,
        "label_propagation" => crate::engine::graph::algo::params::GraphAlgorithm::LabelPropagation,
        "lcc" => crate::engine::graph::algo::params::GraphAlgorithm::Lcc,
        "sssp" => crate::engine::graph::algo::params::GraphAlgorithm::Sssp,
        "betweenness" => crate::engine::graph::algo::params::GraphAlgorithm::Betweenness,
        "closeness" => crate::engine::graph::algo::params::GraphAlgorithm::Closeness,
        "harmonic" => crate::engine::graph::algo::params::GraphAlgorithm::Harmonic,
        "degree" => crate::engine::graph::algo::params::GraphAlgorithm::Degree,
        "louvain" => crate::engine::graph::algo::params::GraphAlgorithm::Louvain,
        "triangles" => crate::engine::graph::algo::params::GraphAlgorithm::Triangles,
        "diameter" => crate::engine::graph::algo::params::GraphAlgorithm::Diameter,
        "kcore" => crate::engine::graph::algo::params::GraphAlgorithm::KCore,
        other => {
            return Err(crate::Error::BadRequest {
                detail: format!("unknown graph algorithm: {other}"),
            });
        }
    };

    let params = crate::engine::graph::algo::params::AlgoParams {
        collection: collection.to_string(),
        source_node: fields.start_node.clone(),
        max_iterations: fields.depth.map(|d| d as usize),
        tolerance: None,
        damping: None,
        sample_size: None,
        direction: fields.direction.clone(),
        resolution: None,
        mode: None,
    };

    Ok(PhysicalPlan::Graph(GraphOp::Algo { algorithm, params }))
}

pub(crate) fn build_match(fields: &TextFields, _collection: &str) -> crate::Result<PhysicalPlan> {
    let query_str = fields
        .match_query
        .as_ref()
        .or(fields.sql.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'match_query'".to_string(),
        })?;

    // Serialize the MATCH query string as MessagePack for the Data Plane.
    let query = rmp_serde::to_vec(query_str).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("match query serialization: {e}"),
    })?;

    Ok(PhysicalPlan::Graph(GraphOp::Match { query }))
}
