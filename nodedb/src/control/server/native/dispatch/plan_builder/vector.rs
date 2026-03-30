//! Vector engine plan builders.

use std::sync::Arc;

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;

pub(crate) fn build_search(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;
    let field_name = fields.field_name.clone().unwrap_or_default();

    Ok(PhysicalPlan::Vector(VectorOp::Search {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        top_k,
        ef_search,
        filter_bitmap: None,
        field_name,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_batch_insert(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let batch_vectors = fields
        .vectors
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'vectors' array for batch insert".to_string(),
        })?;

    if batch_vectors.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "vectors array is empty".to_string(),
        });
    }

    let dim = batch_vectors[0].embedding.len();
    let vectors: Vec<Vec<f32>> = batch_vectors.iter().map(|v| v.embedding.clone()).collect();

    Ok(PhysicalPlan::Vector(VectorOp::BatchInsert {
        collection: collection.to_string(),
        vectors,
        dim,
    }))
}

pub(crate) fn build_insert(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let vector = fields
        .vector
        .as_ref()
        .or(fields.query_vector.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'vector'".to_string(),
        })?
        .clone();
    let dim = vector.len();
    let field_name = fields.field_name.clone().unwrap_or_default();
    let doc_id = fields.document_id.clone();

    Ok(PhysicalPlan::Vector(VectorOp::Insert {
        collection: collection.to_string(),
        vector,
        dim,
        field_name,
        doc_id,
    }))
}

pub(crate) fn build_multi_search(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;

    Ok(PhysicalPlan::Vector(VectorOp::MultiSearch {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        top_k,
        ef_search,
        filter_bitmap: None,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_delete(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let vector_id = fields.vector_id.ok_or_else(|| crate::Error::BadRequest {
        detail: "missing 'vector_id'".to_string(),
    })?;

    Ok(PhysicalPlan::Vector(VectorOp::Delete {
        collection: collection.to_string(),
        vector_id,
    }))
}

pub(crate) fn build_set_params(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let m = fields.m.unwrap_or(16) as usize;
    let ef_construction = fields.ef_construction.unwrap_or(200) as usize;
    let metric = fields
        .metric
        .clone()
        .unwrap_or_else(|| "cosine".to_string());
    let index_type = fields
        .index_type
        .clone()
        .unwrap_or_else(|| "hnsw".to_string());

    Ok(PhysicalPlan::Vector(VectorOp::SetParams {
        collection: collection.to_string(),
        m,
        ef_construction,
        metric,
        index_type,
        pq_m: 0,
        ivf_cells: 0,
        ivf_nprobe: 0,
    }))
}
