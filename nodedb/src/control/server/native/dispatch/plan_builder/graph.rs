// SPDX-License-Identifier: BUSL-1.1

//! Graph operation plan builders.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH;
use nodedb_physical::physical_plan::GraphOp;

use super::parse_direction;

/// Clamp a depth parameter coming in over the native protocol,
/// rejecting out-of-range values rather than forwarding them to the
/// engine. Mirrors the pgwire ingress so no entry point can saturate
/// traversal with an unbounded fan-out.
fn clamped_depth(value: Option<u32>, default: usize, field: &str) -> crate::Result<usize> {
    let v = value.map(|v| v as usize).unwrap_or(default);
    if v > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "{field} {v} exceeds maximum allowed value {MAX_GRAPH_TRAVERSAL_DEPTH}"
            ),
        });
    }
    Ok(v)
}

pub(crate) async fn build_rag_fusion(
    ctx: &DispatchCtx<'_>,
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
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        query_vector: query_vector.clone(),
        vector_top_k: fields.vector_top_k.unwrap_or(20) as usize,
        edge_label: rag_fusion_edge_label(fields)?,
        direction: parse_direction(fields.direction.as_deref())?,
        expansion_depth: clamped_depth(fields.expansion_depth, 2, "expansion_depth")?,
        final_top_k: fields.final_top_k.unwrap_or(10) as usize,
        rrf_k: (
            fields.vector_k.unwrap_or(60.0),
            fields.graph_k.unwrap_or(10.0),
        ),
        rrf_k_triple: None,
        vector_field: fields.vector_field.clone().unwrap_or_default(),
        options: Default::default(),
        bm25_query: None,
        bm25_field: None,
        stage: nodedb_physical::physical_plan::RagStage::Local,
    }))
}

/// The one edge label RAG fusion expands along.
///
/// RAG fusion follows one label or every label. A request with more than one
/// label is refused, never cut down to its first.
fn rag_fusion_edge_label(fields: &TextFields) -> crate::Result<Option<String>> {
    match fields.edge_labels.as_deref() {
        None | Some([]) => Ok(None),
        Some([label]) => Ok(Some(label.clone())),
        Some(labels) => Err(crate::Error::BadRequest {
            detail: format!(
                "RAG fusion takes one edge label, got {}: {labels:?}; send one label or none",
                labels.len()
            ),
        }),
    }
}

pub(crate) async fn build_hop(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Hop {
        collection: fields
            .collection
            .as_deref()
            .map(|c| QualifiedCollection::new(ctx.database_id(), &c.to_lowercase())),
        start_nodes: vec![start.clone()],
        depth: clamped_depth(fields.depth, 2, "depth")?,
        edge_labels: fields.edge_labels.clone().unwrap_or_default(),
        direction: parse_direction(fields.direction.as_deref())?,
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
    }))
}

pub(crate) async fn build_neighbors(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Neighbors {
        collection: fields
            .collection
            .as_deref()
            .map(|c| QualifiedCollection::new(ctx.database_id(), &c.to_lowercase())),
        node_id: start.clone(),
        edge_labels: fields.edge_labels.clone().unwrap_or_default(),
        direction: parse_direction(fields.direction.as_deref())?,
        rls_filters: Vec::new(),
    }))
}

pub(crate) async fn build_path(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
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
        collection: fields
            .collection
            .as_deref()
            .map(|c| QualifiedCollection::new(ctx.database_id(), &c.to_lowercase())),
        src: from.clone(),
        dst: to.clone(),
        max_depth: clamped_depth(fields.depth, 10, "depth")?,
        edge_labels: fields.edge_labels.clone().unwrap_or_default(),
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
    }))
}

pub(crate) async fn build_subgraph(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Subgraph {
        collection: fields
            .collection
            .as_deref()
            .map(|c| QualifiedCollection::new(ctx.database_id(), &c.to_lowercase())),
        start_nodes: vec![start.clone()],
        depth: clamped_depth(fields.depth, 2, "depth")?,
        edge_labels: fields.edge_labels.clone().unwrap_or_default(),
        options: Default::default(),
        rls_filters: Vec::new(),
    }))
}

pub(crate) async fn build_edge_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    if collection.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "edge PUT requires a non-empty collection".to_string(),
        });
    }
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
    let properties = edge_properties_msgpack(fields.properties.as_ref())?;
    // The endpoint surrogates come from the collection home, the one place a
    // key's surrogate is minted (`surrogate_exchange::authority`).
    let [src_surrogate, dst_surrogate] = endpoint_surrogates(ctx, collection, src, dst).await?;
    Ok(PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        src_id: src.clone(),
        label: label.clone(),
        dst_id: dst.clone(),
        properties,
        src_surrogate,
        dst_surrogate,
    }))
}

/// The plain-MessagePack property map an `EdgePut` stores. Absent or null
/// properties store nothing. Any other non-object value is refused.
fn edge_properties_msgpack(properties: Option<&serde_json::Value>) -> crate::Result<Vec<u8>> {
    match properties {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(object @ serde_json::Value::Object(_)) => {
            nodedb_types::json_msgpack::json_to_msgpack(object).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("edge properties: {e}"),
                }
            })
        }
        Some(other) => Err(crate::Error::BadRequest {
            detail: format!("edge properties must be a JSON object, got {other}"),
        }),
    }
}

pub(crate) async fn build_edge_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    if collection.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "edge DELETE requires a non-empty collection".to_string(),
        });
    }
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
    // The endpoint surrogates come from the collection home, as for
    // `build_edge_put`, so a cross-shard delete dual-homes and locks against a
    // concurrent insert of the same edge.
    let [src_surrogate, dst_surrogate] = endpoint_surrogates(ctx, collection, src, dst).await?;
    Ok(PhysicalPlan::Graph(GraphOp::EdgeDelete {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        src_id: src.clone(),
        label: label.clone(),
        dst_id: dst.clone(),
        src_surrogate,
        dst_surrogate,
        // Filled by the RLS injection pass that runs over this plan before
        // dispatch.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) async fn build_algo(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
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

    let personalization_vector = parse_algo_personalization(fields.algo_params.as_ref())?;

    let params = crate::engine::graph::algo::params::AlgoParams {
        collection: collection.to_string(),
        edge_label: None,
        source_node: fields.start_node.clone(),
        max_iterations: fields.depth.map(|d| d as usize),
        tolerance: None,
        damping: None,
        sample_size: None,
        direction: fields.direction.clone(),
        resolution: None,
        mode: None,
        personalization_vector,
    };

    // The native dispatch runs the algorithm over every partition
    // (`graph_owner`); the stage it builds from is chosen there.
    Ok(PhysicalPlan::Graph(GraphOp::Algo {
        algorithm,
        params,
        stage: nodedb_physical::physical_plan::AlgoStage::Local,
    }))
}

/// Extract the Personalized PageRank seed map from the raw-protocol
/// `algo_params` object (`{"personalization_vector": {"alice": 1.0, …}}`).
///
/// Returns `Ok(None)` when absent or empty. A present-but-malformed value
/// (not an object, or a non-numeric weight) surfaces a structured
/// `BadRequest` rather than being silently dropped. Parses the JSON object
/// directly (no runtime JSON de/serialization functions).
fn parse_algo_personalization(
    algo_params: Option<&serde_json::Value>,
) -> crate::Result<Option<std::collections::HashMap<String, f64>>> {
    let Some(pv) = algo_params.and_then(|p| p.get("personalization_vector")) else {
        return Ok(None);
    };
    if pv.is_null() {
        return Ok(None);
    }
    let obj = pv.as_object().ok_or_else(|| crate::Error::BadRequest {
        detail: "personalization_vector must be a JSON object of node_id → weight".to_string(),
    })?;
    let mut map = std::collections::HashMap::with_capacity(obj.len());
    for (node, weight) in obj {
        let w = weight.as_f64().ok_or_else(|| crate::Error::BadRequest {
            detail: format!("personalization_vector weight for '{node}' must be a number"),
        })?;
        map.insert(node.clone(), w);
    }
    if map.is_empty() {
        return Ok(None);
    }
    Ok(Some(map))
}

pub(crate) async fn build_match(
    fields: &TextFields,
    _collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_str = fields
        .match_query
        .as_ref()
        .or(fields.sql.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'match_query'".to_string(),
        })?;

    // Serialize the MATCH query string as MessagePack for the Data Plane.
    let query = zerompk::to_msgpack_vec(query_str).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("match query serialization: {e}"),
    })?;

    Ok(PhysicalPlan::Graph(GraphOp::Match {
        query,
        frontier_bitmap: None,
        // B1: native MATCH stays single-node; B2 wires cluster orchestration.
        cluster_mode: false,
    }))
}

/// Both endpoints' surrogates, in one batch at the collection's home.
async fn endpoint_surrogates(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    src: &str,
    dst: &str,
) -> crate::Result<[nodedb_types::Surrogate; 2]> {
    let bound =
        super::helpers::assign_surrogates(ctx, collection, &[src.as_bytes(), dst.as_bytes()])
            .await?;
    match bound.as_slice() {
        [src_surrogate, dst_surrogate] => Ok([*src_surrogate, *dst_surrogate]),
        _ => Err(crate::Error::Internal {
            detail: format!(
                "edge write in '{collection}': the home answered {} endpoint surrogates",
                bound.len()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn algo_fields(algo_params: Option<serde_json::Value>) -> TextFields {
        TextFields {
            algorithm: Some("pagerank".to_string()),
            algo_params,
            ..Default::default()
        }
    }

    fn params_of(plan: PhysicalPlan) -> crate::engine::graph::algo::params::AlgoParams {
        let PhysicalPlan::Graph(GraphOp::Algo { params, .. }) = plan else {
            panic!("expected GraphOp::Algo");
        };
        params
    }

    #[tokio::test]
    async fn build_algo_parses_personalization_from_algo_params() {
        let fields = algo_fields(Some(json!({
            "personalization_vector": { "alice": 1.0, "bob": 0.5 }
        })));
        let pv = params_of(build_algo(&fields, "social").await.unwrap())
            .personalization_vector
            .expect("personalization present");
        assert_eq!(pv.get("alice"), Some(&1.0));
        assert_eq!(pv.get("bob"), Some(&0.5));
    }

    #[tokio::test]
    async fn build_algo_without_personalization_is_none() {
        assert!(
            params_of(build_algo(&algo_fields(None), "social").await.unwrap())
                .personalization_vector
                .is_none()
        );
        // An algo_params object that omits the key is also None.
        let fields = algo_fields(Some(json!({ "other": 1 })));
        assert!(
            params_of(build_algo(&fields, "social").await.unwrap())
                .personalization_vector
                .is_none()
        );
    }

    #[tokio::test]
    async fn build_algo_rejects_non_numeric_weight() {
        let fields = algo_fields(Some(
            json!({ "personalization_vector": { "alice": "high" } }),
        ));
        assert!(build_algo(&fields, "social").await.is_err());
    }

    fn label_fields(labels: Option<Vec<&str>>) -> TextFields {
        TextFields {
            edge_labels: labels.map(|l| l.into_iter().map(str::to_string).collect()),
            ..Default::default()
        }
    }

    #[test]
    fn rag_fusion_edge_label_takes_none_or_one() {
        assert_eq!(rag_fusion_edge_label(&label_fields(None)).unwrap(), None);
        assert_eq!(
            rag_fusion_edge_label(&label_fields(Some(vec![]))).unwrap(),
            None
        );
        assert_eq!(
            rag_fusion_edge_label(&label_fields(Some(vec!["hop"]))).unwrap(),
            Some("hop".to_string())
        );
    }

    #[test]
    fn rag_fusion_edge_label_refuses_more_than_one() {
        let error = rag_fusion_edge_label(&label_fields(Some(vec!["a", "b"])))
            .expect_err("two labels must be refused");
        assert!(error.to_string().contains("one edge label"), "{error}");
    }

    #[test]
    fn edge_properties_store_plain_msgpack() {
        let bytes = edge_properties_msgpack(Some(&json!({"weight": 3, "kind": "road"})))
            .expect("object properties encode");
        assert_eq!(
            nodedb_graph::csr::weights::extract_weight_from_properties(&bytes),
            3.0
        );
        assert!(edge_properties_msgpack(None).expect("absent").is_empty());
        assert!(
            edge_properties_msgpack(Some(&serde_json::Value::Null))
                .expect("null")
                .is_empty()
        );
        assert!(edge_properties_msgpack(Some(&json!([1, 2]))).is_err());
    }

    #[tokio::test]
    async fn build_algo_rejects_non_object_personalization() {
        let fields = algo_fields(Some(json!({ "personalization_vector": [1, 2, 3] })));
        assert!(build_algo(&fields, "social").await.is_err());
    }
}
