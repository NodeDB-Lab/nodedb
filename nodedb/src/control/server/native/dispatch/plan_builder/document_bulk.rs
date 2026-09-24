// SPDX-License-Identifier: BUSL-1.1

//! Document engine plan builders for set-level writes: predicate update and
//! delete, truncate, insert-select, and the row-count estimate.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::DocumentOp;

use super::super::DispatchCtx;
use super::declared_primary_key;

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
        // See `build_update`: reads the declared PRIMARY KEY from the catalog.
        declared_primary_key: declared_primary_key(ctx, collection)?,
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
