//! Document engine plan builders.

use nodedb_types::CollectionType;
use nodedb_types::columnar::ColumnarProfile;
use nodedb_types::protocol::TextFields;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{DocumentOp, KvOp, TimeseriesOp};

use super::{DispatchCtx, collection_type, require_doc_id};

pub(crate) fn build_point_get(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    match collection_type(ctx, collection) {
        Some(CollectionType::KeyValue(_)) => Ok(PhysicalPlan::Kv(KvOp::Get {
            collection: collection.to_string(),
            key: doc_id.into_bytes(),
            rls_filters: Vec::new(),
        })),
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            Err(crate::Error::BadRequest {
                detail: "PointGet not supported on timeseries collections \
                         (use SQL SELECT with time range)"
                    .to_string(),
            })
        }
        Some(CollectionType::Columnar(_)) => Err(crate::Error::BadRequest {
            detail: "PointGet not supported on columnar collections \
                     (use SQL SELECT with filters)"
                .to_string(),
        }),
        Some(CollectionType::Document(_)) | None => {
            Ok(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: collection.to_string(),
                document_id: doc_id,
                rls_filters: Vec::new(),
            }))
        }
    }
}

pub(crate) fn build_point_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();
    match collection_type(ctx, collection) {
        Some(CollectionType::KeyValue(_)) => Ok(PhysicalPlan::Kv(KvOp::Put {
            collection: collection.to_string(),
            key: doc_id.into_bytes(),
            value,
            ttl_ms: 0,
        })),
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            let json_str = String::from_utf8_lossy(&value);
            let ilp_line = format!("{collection} value={json_str}\n");
            Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: collection.to_string(),
                payload: ilp_line.into_bytes(),
                format: "ilp".to_string(),
                wal_lsn: None,
            }))
        }
        Some(CollectionType::Columnar(_)) => Err(crate::Error::BadRequest {
            detail: "PointPut not supported on columnar collections \
                     (use SQL INSERT)"
                .to_string(),
        }),
        Some(CollectionType::Document(_)) | None => {
            Ok(PhysicalPlan::Document(DocumentOp::PointPut {
                collection: collection.to_string(),
                document_id: doc_id,
                value,
            }))
        }
    }
}

pub(crate) fn build_point_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    match collection_type(ctx, collection) {
        Some(CollectionType::KeyValue(_)) => Ok(PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.to_string(),
            keys: vec![doc_id.into_bytes()],
        })),
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            Err(crate::Error::BadRequest {
                detail: "PointDelete not supported on timeseries collections \
                         (append-only; use retention policies)"
                    .to_string(),
            })
        }
        Some(CollectionType::Columnar(_)) => Err(crate::Error::BadRequest {
            detail: "PointDelete not supported on columnar collections \
                     (append-only)"
                .to_string(),
        }),
        Some(CollectionType::Document(_)) | None => {
            Ok(PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: collection.to_string(),
                document_id: doc_id,
            }))
        }
    }
}

pub(crate) fn build_range_scan(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let field = fields
        .field
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'field'".to_string(),
        })?
        .clone();
    let limit = fields.limit.unwrap_or(100) as usize;
    Ok(PhysicalPlan::Document(DocumentOp::RangeScan {
        collection: collection.to_string(),
        field,
        lower: fields.lower_bound.clone(),
        upper: fields.upper_bound.clone(),
        limit,
    }))
}

pub(crate) fn build_batch_insert(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let batch_docs = fields
        .documents
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'documents' array for batch insert".to_string(),
        })?;
    if batch_docs.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "documents array is empty".to_string(),
        });
    }
    let documents: Vec<(String, Vec<u8>)> = batch_docs
        .iter()
        .map(|d| {
            let value_bytes =
                sonic_rs::to_vec(&d.fields).map_err(|e| crate::Error::Serialization {
                    format: "json".into(),
                    detail: format!("failed to serialize document '{}': {e}", d.id),
                })?;
            Ok((d.id.clone(), value_bytes))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection: collection.to_string(),
        documents,
    }))
}

pub(crate) fn build_update(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let updates: Vec<(String, crate::bridge::physical_plan::UpdateValue)> = fields
        .updates
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'updates'".to_string(),
        })?
        .iter()
        .map(|(f, b)| {
            (
                f.clone(),
                crate::bridge::physical_plan::UpdateValue::Literal(b.clone()),
            )
        })
        .collect();
    Ok(PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: collection.to_string(),
        document_id: doc_id,
        updates,
        returning: false,
    }))
}

pub(crate) fn build_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
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

pub(crate) fn build_upsert(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();
    Ok(PhysicalPlan::Document(DocumentOp::Upsert {
        collection: collection.to_string(),
        document_id: doc_id,
        value,
        // The native text protocol carries no ON CONFLICT clause; plain
        // merge semantics apply.
        on_conflict_updates: Vec::new(),
    }))
}

pub(crate) fn build_bulk_update(
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
    let updates: Vec<(String, crate::bridge::physical_plan::UpdateValue)> = fields
        .updates
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'updates'".to_string(),
        })?
        .iter()
        .map(|(f, b)| {
            (
                f.clone(),
                crate::bridge::physical_plan::UpdateValue::Literal(b.clone()),
            )
        })
        .collect();
    Ok(PhysicalPlan::Document(DocumentOp::BulkUpdate {
        collection: collection.to_string(),
        filters,
        updates,
        returning: false,
    }))
}

pub(crate) fn build_bulk_delete(
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

pub(crate) fn build_truncate(collection: &str) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Document(DocumentOp::Truncate {
        collection: collection.to_string(),
        restart_identity: false,
    }))
}

pub(crate) fn build_estimate_count(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let field = fields.field.as_deref().unwrap_or("id").to_string();

    Ok(PhysicalPlan::Document(DocumentOp::EstimateCount {
        collection: collection.to_string(),
        field,
    }))
}

pub(crate) fn build_insert_select(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let source = fields
        .source_collection
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'source_collection'".to_string(),
        })?
        .clone();
    let filters = fields.filters.clone().unwrap_or_default();
    let limit = fields.limit.unwrap_or(10_000) as usize;

    Ok(PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection: collection.to_string(),
        source_collection: source,
        source_filters: filters,
        source_limit: limit,
    }))
}

pub(crate) fn build_register(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let index_paths = fields.index_paths.clone().unwrap_or_default();

    Ok(PhysicalPlan::Document(DocumentOp::Register {
        collection: collection.to_string(),
        index_paths,
        crdt_enabled: false,
        storage_mode: crate::bridge::physical_plan::StorageMode::Schemaless,
        enforcement: Box::new(crate::bridge::physical_plan::EnforcementOptions::default()),
    }))
}

pub(crate) fn build_drop_index(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let field = fields
        .field
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'field'".to_string(),
        })?
        .clone();

    Ok(PhysicalPlan::Document(DocumentOp::DropIndex {
        collection: collection.to_string(),
        field,
    }))
}
