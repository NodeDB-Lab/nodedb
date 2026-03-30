//! Columnar engine plan builders.

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ColumnarOp;

pub(crate) fn build_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let limit = fields.limit.unwrap_or(10_000) as usize;
    let filters = fields.filters.clone().unwrap_or_default();

    Ok(PhysicalPlan::Columnar(ColumnarOp::Scan {
        collection: collection.to_string(),
        projection: Vec::new(),
        limit,
        filters,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_insert(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let payload = fields
        .payload
        .as_ref()
        .or(fields.data.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'payload' or 'data'".to_string(),
        })?
        .clone();
    let format = fields.format.as_deref().unwrap_or("json").to_string();

    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: collection.to_string(),
        payload,
        format,
    }))
}
