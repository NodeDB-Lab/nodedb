// SPDX-License-Identifier: BUSL-1.1

//! Document engine plan builders for set-level writes: predicate update and
//! delete, truncate, insert-select, and the row-count estimate.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{DocumentOp, DocumentPageSource, RowSource};

use super::super::DispatchCtx;
use super::declared_primary_key;

pub(crate) async fn build_bulk_update(
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

pub(crate) async fn build_bulk_delete(
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

pub(crate) async fn build_truncate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Document(DocumentOp::Truncate {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        restart_identity: false,
        // Filled in by the materialized-sum resolution pass.
        resolved_sum_targets: Vec::new(),
        // See `build_update`: reads the declared PRIMARY KEY from the catalog.
        declared_primary_key: declared_primary_key(ctx, collection)?,
    }))
}

pub(crate) async fn build_estimate_count(
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

pub(crate) async fn build_insert_select(
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
    // The op reads its source page by page as stored documents, so only a
    // document source has rows it can read. Any other engine is refused by
    // name, never copied as zero rows.
    let stored = ctx
        .state
        .credentials
        .catalog()
        .get_collection(ctx.database_id(), ctx.identity.tenant_id.as_u64(), &source)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: ctx.identity.tenant_id,
            collection: source.clone(),
        })?;
    if !matches!(
        stored.collection_type,
        nodedb_types::CollectionType::Document(_)
    ) {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "insert_select source '{source}' must be a document collection; use SQL \
                 INSERT ... SELECT to copy from any other engine"
            ),
        });
    }
    // No limit reads every matching row: the page source has no row bound.
    let limit = fields
        .limit
        .map(|limit| {
            usize::try_from(limit).map_err(|_| crate::Error::BadRequest {
                detail: format!("insert_select limit {limit} does not fit this platform"),
            })
        })
        .transpose()?;

    Ok(PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection: QualifiedCollection::new(ctx.database_id(), collection),
        // The native text-field form names no projection, so every source row
        // copies unchanged.
        source: RowSource::DocumentPages(DocumentPageSource {
            collection: QualifiedCollection::new(ctx.database_id(), &source),
            filters: fields.filters.clone().unwrap_or_default(),
            system_time: nodedb_types::SystemTimeScope::Current,
            offset: 0,
            limit,
            column_map: Vec::new(),
        }),
    }))
}
