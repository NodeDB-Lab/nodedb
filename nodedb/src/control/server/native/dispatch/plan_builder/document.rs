// SPDX-License-Identifier: BUSL-1.1

//! Document engine plan builders.

use nodedb_types::CollectionType;
use nodedb_types::QualifiedCollection;
use nodedb_types::columnar::ColumnarProfile;
use nodedb_types::protocol::TextFields;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, TimeseriesOp};

use super::super::DispatchCtx;
use super::document_identity::{identified_body, identified_json_body, stores_schemaless_bodies};
use super::{collection_type, declared_primary_key, require_doc_id};

pub(crate) async fn build_point_get(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    match collection_type(ctx, collection)? {
        Some(CollectionType::KeyValue(_)) => Ok(PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(ctx.database_id(), collection),
            key: doc_id.into_bytes(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
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
            let pk_bytes = doc_id.as_bytes().to_vec();
            let surrogate = super::helpers::existing_surrogate(ctx, collection, &pk_bytes).await?;
            Ok(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                document_id: doc_id,
                surrogate,
                pk_bytes,
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }))
        }
    }
}

pub(crate) async fn build_point_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();
    let coll_type = collection_type(ctx, collection)?;
    let schemaless = stores_schemaless_bodies(coll_type.as_ref());
    match coll_type {
        Some(CollectionType::KeyValue(_)) => {
            let key = doc_id.into_bytes();
            let surrogate = super::helpers::assign_surrogate(ctx, collection, &key).await?;
            Ok(PhysicalPlan::Kv(KvOp::Put {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                key,
                value,
                ttl_ms: 0,
                surrogate,
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            }))
        }
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            let json_str = String::from_utf8_lossy(&value);
            let ilp_line = format!("{collection} value={json_str}\n");
            // The line's own surrogate keys its staged row, so a read later in
            // the same transaction observes it.
            let (surrogate, _identity) = ctx
                .state
                .surrogate_assigner
                .assign_fresh(
                    nodedb_types::CollectionKey::from_bare(ctx.database_id(), collection),
                    ctx.tenant_id(),
                )
                .await?;
            Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                payload: ilp_line.into_bytes(),
                format: "ilp".to_string(),
                wal_lsn: None,
                surrogates: vec![surrogate],
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
            }))
        }
        Some(CollectionType::Columnar(_)) => Err(crate::Error::BadRequest {
            detail: "PointPut not supported on columnar collections \
                     (use SQL INSERT)"
                .to_string(),
        }),
        Some(CollectionType::Document(_)) | None => {
            let value = if schemaless {
                identified_body(&value, &doc_id)?
            } else {
                value
            };
            let pk_bytes = doc_id.as_bytes().to_vec();
            let surrogate = super::helpers::assign_surrogate(ctx, collection, &pk_bytes).await?;
            Ok(PhysicalPlan::Document(DocumentOp::PointPut {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                document_id: doc_id,
                value,
                surrogate,
                pk_bytes,
                // The native protocol has no RETURNING clause, so this
                // write projects nothing and needs no read gate.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }))
        }
    }
}

pub(crate) async fn build_point_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    match collection_type(ctx, collection)? {
        Some(CollectionType::KeyValue(_)) => Ok(PhysicalPlan::Kv(KvOp::Delete {
            collection: QualifiedCollection::new(ctx.database_id(), collection),
            keys: vec![doc_id.into_bytes()],
            // Filled by the RLS injection pass this dispatch path runs.
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            // The native point-delete carries no RETURNING clause.
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
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
            // A row of an edge-bearing collection is also a graph node. Its
            // delete is a `BulkDelete` on its key, so the edge reconnaissance
            // gate commits the node's edge tombstones with it.
            if super::helpers::collection_is_edge_bearing(ctx, collection)? {
                return edge_bearing_key_delete(ctx, collection, doc_id);
            }
            let pk_bytes = doc_id.as_bytes().to_vec();
            let surrogate = super::helpers::existing_surrogate(ctx, collection, &pk_bytes).await?;
            Ok(PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                document_id: doc_id,
                surrogate,
                pk_bytes,
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                resolved_sum_targets: Vec::new(),
            }))
        }
    }
}

/// A `BulkDelete` of the one row whose identity column holds `doc_id`: the
/// declared primary key column, else `id`.
fn edge_bearing_key_delete(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    doc_id: String,
) -> crate::Result<PhysicalPlan> {
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    let declared_primary_key = declared_primary_key(ctx, collection)?;
    let filter = ScanFilter {
        field: declared_primary_key
            .clone()
            .unwrap_or_else(|| nodedb_types::DEFAULT_IDENTITY_COLUMN.to_string()),
        op: FilterOp::Eq,
        value: nodedb_types::Value::String(doc_id),
        clauses: Vec::new(),
        expr: None,
    };
    let key_filters: Vec<ScanFilter> = std::iter::once(filter).collect();
    let filters =
        zerompk::to_msgpack_vec(&key_filters).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("key delete filter encode: {e}"),
        })?;
    Ok(PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        filters,
        returning: None,
        ollp_predicted_surrogates: None,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // Filled in by the materialized-sum resolution pass.
        resolved_sum_targets: Vec::new(),
        declared_primary_key,
    }))
}

pub(crate) async fn build_range_scan(
    ctx: &DispatchCtx<'_>,
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
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        field,
        lower: fields.lower_bound.clone(),
        upper: fields.upper_bound.clone(),
        limit,
        rls_filters: Vec::new(),
    }))
}

pub(crate) async fn build_batch_insert(
    ctx: &DispatchCtx<'_>,
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
    // Every row's identity in one batch at the collection's home.
    let pks: Vec<&[u8]> = batch_docs.iter().map(|d| d.id.as_bytes()).collect();
    let surrogates = super::helpers::assign_surrogates(ctx, collection, &pks).await?;
    let schemaless = stores_schemaless_bodies(collection_type(ctx, collection)?.as_ref());
    let mut documents: Vec<(String, Vec<u8>)> = Vec::with_capacity(batch_docs.len());
    for d in batch_docs {
        let value_bytes = if schemaless {
            identified_json_body(d.fields.clone(), &d.id)?
        } else {
            sonic_rs::to_vec(&d.fields).map_err(|e| crate::Error::Serialization {
                format: "json".into(),
                detail: format!("failed to serialize document '{}': {e}", d.id),
            })?
        };
        documents.push((d.id.clone(), value_bytes));
    }
    Ok(PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        documents,
        surrogates,
        // See `build_point_put`.
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    }))
}

pub(crate) async fn build_update(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)> = fields
        .updates
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'updates'".to_string(),
        })?
        .iter()
        .map(|(f, b)| {
            (
                f.clone(),
                nodedb_physical::physical_plan::UpdateValue::Literal(b.clone()),
            )
        })
        .collect();
    let pk_bytes = doc_id.as_bytes().to_vec();
    let surrogate = super::helpers::existing_surrogate(ctx, collection, &pk_bytes).await?;
    Ok(PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id: doc_id,
        surrogate,
        pk_bytes,
        updates,
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        resolved_sum_targets: Vec::new(),
        // Read from the catalog so a declared PRIMARY KEY refuses a
        // NULL/omitted value the same way under this protocol as under SQL.
        declared_primary_key: declared_primary_key(ctx, collection)?,
    }))
}

/// A collection scan, routed by the collection's engine like the point ops
/// above: a document scan reads the sparse store only, so every other engine
/// takes its own scan builder. A spatial collection's plain scan is the
/// columnar scan; the geometry query is `SpatialScan`.
pub(crate) async fn build_scan(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    match collection_type(ctx, collection)? {
        Some(CollectionType::KeyValue(_)) => {
            return super::kv::build_scan(ctx, fields, collection).await;
        }
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            return super::timeseries::build_scan(ctx, fields, collection).await;
        }
        Some(CollectionType::Columnar(ColumnarProfile::Plain))
        | Some(CollectionType::Columnar(ColumnarProfile::Spatial { .. })) => {
            return super::columnar::build_scan(ctx, fields, collection).await;
        }
        Some(CollectionType::Document(_)) | None => {}
    }
    let limit = fields.limit.unwrap_or(1000) as usize;
    let filters = fields.filters.clone().unwrap_or_default();
    Ok(PhysicalPlan::Document(DocumentOp::Scan {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        limit,
        offset: 0,
        sort_keys: Vec::new(),
        filters,
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    }))
}

pub(crate) async fn build_upsert(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();
    let value = if stores_schemaless_bodies(collection_type(ctx, collection)?.as_ref()) {
        identified_body(&value, &doc_id)?
    } else {
        value
    };
    let surrogate = super::helpers::assign_surrogate(ctx, collection, doc_id.as_bytes()).await?;
    Ok(PhysicalPlan::Document(DocumentOp::Upsert {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id: doc_id,
        value,
        // The native text protocol carries no ON CONFLICT clause; plain
        // merge semantics apply.
        on_conflict_updates: Vec::new(),
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // See `build_point_put`.
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }))
}
