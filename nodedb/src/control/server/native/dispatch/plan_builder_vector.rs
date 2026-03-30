//! Vector engine plan builders for the native protocol.

use std::sync::Arc;

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;

pub(super) fn build_vector_insert(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
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

pub(super) fn build_vector_multi_search(
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

pub(super) fn build_vector_delete(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let vector_id = fields.vector_id.ok_or_else(|| crate::Error::BadRequest {
        detail: "missing 'vector_id'".to_string(),
    })?;

    Ok(PhysicalPlan::Vector(VectorOp::Delete {
        collection: collection.to_string(),
        vector_id,
    }))
}
