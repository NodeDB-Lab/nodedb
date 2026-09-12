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
use super::{collection_type, declared_primary_key, require_doc_id};

pub(crate) fn build_point_get(
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
            let surrogate = ctx
                .state
                .surrogate_assigner
                .lookup(ctx.database_id(), ctx.tenant_id(), collection, &pk_bytes)?
                .unwrap_or(nodedb_types::Surrogate::ZERO);
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

pub(crate) fn build_point_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();
    match collection_type(ctx, collection)? {
        Some(CollectionType::KeyValue(_)) => {
            let key = doc_id.into_bytes();
            let surrogate = ctx.state.surrogate_assigner.assign(
                ctx.database_id(),
                ctx.tenant_id(),
                collection,
                &key,
            )?;
            Ok(PhysicalPlan::Kv(KvOp::Put {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                key,
                value,
                ttl_ms: 0,
                surrogate,
                returning: None,
                rls_filters: Vec::new(),
            }))
        }
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            let json_str = String::from_utf8_lossy(&value);
            let ilp_line = format!("{collection} value={json_str}\n");
            Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(ctx.database_id(), collection),
                payload: ilp_line.into_bytes(),
                format: "ilp".to_string(),
                wal_lsn: None,
                surrogates: Vec::new(),
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
            let pk_bytes = doc_id.as_bytes().to_vec();
            let surrogate = ctx.state.surrogate_assigner.assign(
                ctx.database_id(),
                ctx.tenant_id(),
                collection,
                &pk_bytes,
            )?;
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

pub(crate) fn build_point_delete(
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
            let pk_bytes = doc_id.as_bytes().to_vec();
            let surrogate = ctx
                .state
                .surrogate_assigner
                .lookup(ctx.database_id(), ctx.tenant_id(), collection, &pk_bytes)?
                .unwrap_or(nodedb_types::Surrogate::ZERO);
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

pub(crate) fn build_range_scan(
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

pub(crate) fn build_batch_insert(
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
    let mut documents: Vec<(String, Vec<u8>)> = Vec::with_capacity(batch_docs.len());
    let mut surrogates: Vec<nodedb_types::Surrogate> = Vec::with_capacity(batch_docs.len());
    for d in batch_docs {
        let value_bytes = sonic_rs::to_vec(&d.fields).map_err(|e| crate::Error::Serialization {
            format: "json".into(),
            detail: format!("failed to serialize document '{}': {e}", d.id),
        })?;
        let surrogate = ctx.state.surrogate_assigner.assign(
            ctx.database_id(),
            ctx.tenant_id(),
            collection,
            d.id.as_bytes(),
        )?;
        documents.push((d.id.clone(), value_bytes));
        surrogates.push(surrogate);
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

pub(crate) fn build_update(
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
    let surrogate = ctx
        .state
        .surrogate_assigner
        .lookup(ctx.database_id(), ctx.tenant_id(), collection, &pk_bytes)?
        .unwrap_or(nodedb_types::Surrogate::ZERO);
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
pub(crate) fn build_scan(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    match collection_type(ctx, collection)? {
        Some(CollectionType::KeyValue(_)) => return super::kv::build_scan(ctx, fields, collection),
        Some(CollectionType::Columnar(ColumnarProfile::Timeseries { .. })) => {
            return super::timeseries::build_scan(ctx, fields, collection);
        }
        Some(CollectionType::Columnar(ColumnarProfile::Plain))
        | Some(CollectionType::Columnar(ColumnarProfile::Spatial { .. })) => {
            return super::columnar::build_scan(ctx, fields, collection);
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

pub(crate) fn build_upsert(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let value = fields.data.clone().unwrap_or_default();
    let surrogate = ctx.state.surrogate_assigner.assign(
        ctx.database_id(),
        ctx.tenant_id(),
        collection,
        doc_id.as_bytes(),
    )?;
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

pub(crate) fn build_bulk_update(
    ctx: &DispatchCtx<'_>,
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
    Ok(PhysicalPlan::Document(DocumentOp::BulkUpdate {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        filters,
        updates,
        returning: None,
        ollp_predicted_surrogates: None,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // Filled in by the materialized-sum resolution pass.
        resolved_sum_targets: Vec::new(),
        // See `build_update`: reads the declared PRIMARY KEY from the catalog.
        declared_primary_key: declared_primary_key(ctx, collection)?,
    }))
}

pub(crate) fn build_bulk_delete(
    ctx: &DispatchCtx<'_>,
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
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        filters,
        returning: None,
        ollp_predicted_surrogates: None,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // Filled in by the materialized-sum resolution pass.
        resolved_sum_targets: Vec::new(),
        // See `build_update`: reads the declared PRIMARY KEY from the catalog.
        declared_primary_key: declared_primary_key(ctx, collection)?,
    }))
}

pub(crate) fn build_truncate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Document(DocumentOp::Truncate {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        restart_identity: false,
        // Filled in by the materialized-sum resolution pass.
        resolved_sum_targets: Vec::new(),
    }))
}

pub(crate) fn build_estimate_count(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let field = fields.field.as_deref().unwrap_or("id").to_string();

    Ok(PhysicalPlan::Document(DocumentOp::EstimateCount {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        field,
    }))
}

pub(crate) fn build_insert_select(
    ctx: &DispatchCtx<'_>,
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
        target_collection: QualifiedCollection::new(ctx.database_id(), collection),
        source_collection: QualifiedCollection::new(ctx.database_id(), &source),
        source_filters: filters,
        source_limit: limit,
        // The native text-field form names no projection, so every source row
        // copies unchanged.
        column_map: Vec::new(),
    }))
}

pub(crate) fn build_register(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    // Native protocol exposes only a legacy `index_paths` text list — promote
    // each entry to a `Ready`, non-unique `RegisteredIndex` named after the
    // path. UNIQUE / COLLATE / build-state come from SQL DDL only.
    let indexes = fields
        .index_paths
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|path| nodedb_physical::physical_plan::RegisteredIndex {
            name: path.clone(),
            path,
            unique: false,
            case_insensitive: false,
            state: nodedb_physical::physical_plan::RegisteredIndexState::Ready,
            predicate: None,
        })
        .collect();

    Ok(PhysicalPlan::Document(DocumentOp::Register {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        indexes,
        crdt_enabled: false,
        storage_mode: nodedb_physical::physical_plan::StorageMode::Schemaless,
        enforcement: Box::new(nodedb_physical::physical_plan::EnforcementOptions::default()),
        bitemporal: false,
        conflict_policy: None,
        timeseries: None,
        // The native protocol's register frame carries only index paths; a
        // vector-primary collection is created through SQL DDL, which goes
        // through the catalog-sourced builder instead.
        vector_primary: None,
    }))
}

pub(crate) fn build_drop_index(
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

    Ok(PhysicalPlan::Document(DocumentOp::DropIndex {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        field,
    }))
}
