// SPDX-License-Identifier: BUSL-1.1

//! Vector engine plan builders.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;
use nodedb_types::vector_distance::DistanceMetric;

use super::super::DispatchCtx;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::VectorOp;

pub(crate) fn build_search(
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
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;
    let field_name = fields.field_name.clone().unwrap_or_default();

    Ok(PhysicalPlan::Vector(VectorOp::Search {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        query_vector: query_vector.clone(),
        top_k,
        ef_search,
        // The native protocol has no SQL operator context; use the collection
        // default metric (L2 as the wire sentinel — the Data Plane will apply
        // the collection-configured metric when these match).
        metric: DistanceMetric::L2,
        filter_bitmap: None,
        field_name,
        rls_filters: Vec::new(),
        inline_prefilter_plan: None,
        // The native protocol carries primitive top_k / ef_search fields
        // directly; advanced ANN tuning (quantization, oversample, target
        // recall, …) is exposed only through the SQL planner today. A
        // default-valued struct keeps the wire shape uniform until the
        // native fields gain matching options.
        ann_options: Default::default(),
        skip_payload_fetch: false,
        payload_filters: Vec::new(),
    }))
}

pub(crate) fn build_batch_insert(
    ctx: &DispatchCtx<'_>,
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

    // Headless batch — every vector gets a fresh anonymous surrogate.
    let assigner = &ctx.state.surrogate_assigner;
    let mut surrogates = Vec::with_capacity(vectors.len());
    for _ in &vectors {
        surrogates.push(assigner.assign_anonymous(
            ctx.database_id(),
            ctx.tenant_id(),
            collection,
        )?);
    }

    Ok(PhysicalPlan::Vector(VectorOp::BatchInsert {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        vectors,
        dim,
        surrogates,
    }))
}

pub(crate) fn build_insert(
    ctx: &DispatchCtx<'_>,
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

    let assigner = &ctx.state.surrogate_assigner;
    let (surrogate, pk_bytes) = match fields.document_id.as_deref() {
        Some(pk) if !pk.is_empty() => (
            assigner.assign(
                ctx.database_id(),
                ctx.tenant_id(),
                collection,
                pk.as_bytes(),
            )?,
            Some(pk.as_bytes().to_vec()),
        ),
        _ => (
            assigner.assign_anonymous(ctx.database_id(), ctx.tenant_id(), collection)?,
            None,
        ),
    };

    Ok(PhysicalPlan::Vector(VectorOp::Insert {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        vector,
        dim,
        field_name,
        surrogate,
        pk_bytes,
        provenance: None,
    }))
}

pub(crate) fn build_multi_search(
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
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;

    Ok(PhysicalPlan::Vector(VectorOp::MultiSearch {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        query_vector: query_vector.clone(),
        top_k,
        ef_search,
        filter_bitmap: None,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let vector_id_wire = fields.vector_id.ok_or_else(|| crate::Error::BadRequest {
        detail: "missing 'vector_id'".to_string(),
    })?;
    let vector_id: u32 = vector_id_wire
        .try_into()
        .map_err(|_| crate::Error::BadRequest {
            detail: "vector_id exceeds 32-bit surrogate space".to_string(),
        })?;

    Ok(PhysicalPlan::Vector(VectorOp::Delete {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        vector_id,
    }))
}
