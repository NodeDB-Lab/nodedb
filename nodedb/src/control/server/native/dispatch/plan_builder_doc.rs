//! Document engine advanced plan builders for native protocol opcodes.

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::DocumentOp;

pub(crate) fn build_doc_update(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let updates = fields
        .updates
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'updates'".to_string(),
        })?
        .clone();

    Ok(PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: collection.to_string(),
        document_id: doc_id,
        updates,
    }))
}

pub(crate) fn build_doc_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let limit = fields.limit.unwrap_or(1000) as usize;
    let filters = fields.filters.clone().unwrap_or_default();

    Ok(PhysicalPlan::Document(DocumentOp::Scan {
        collection: collection.to_string(),
        limit,
        offset: 0,
        sort_keys: Vec::new(),
        filters,
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
    }))
}

pub(crate) fn build_doc_upsert(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();

    Ok(PhysicalPlan::Document(DocumentOp::Upsert {
        collection: collection.to_string(),
        document_id: doc_id,
        value,
    }))
}

pub(crate) fn build_doc_bulk_update(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let filters = fields
        .filters
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'filters'".to_string(),
        })?
        .clone();
    let updates = fields
        .updates
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'updates'".to_string(),
        })?
        .clone();

    Ok(PhysicalPlan::Document(DocumentOp::BulkUpdate {
        collection: collection.to_string(),
        filters,
        updates,
    }))
}

pub(crate) fn build_doc_bulk_delete(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let filters = fields
        .filters
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'filters'".to_string(),
        })?
        .clone();

    Ok(PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: collection.to_string(),
        filters,
    }))
}

fn require_doc_id(fields: &TextFields) -> crate::Result<String> {
    fields
        .document_id
        .as_ref()
        .cloned()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'document_id'".to_string(),
        })
}
