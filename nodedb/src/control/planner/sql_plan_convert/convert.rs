//! Convert nodedb-sql SqlPlan IR to NodeDB PhysicalPlan + PhysicalTask.
//!
//! This is the Origin-specific mapping layer. It adds vShard routing,
//! serializes filters to msgpack, and handles broadcast join decisions.

use nodedb_sql::types::SqlPlan;

use std::sync::Arc;

use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::TenantId;

use super::super::physical::PhysicalTask;

/// Conversion context holding optional references needed during plan conversion.
pub struct ConvertContext {
    pub retention_registry: Option<Arc<RetentionPolicyRegistry>>,
}

/// Convert a list of SqlPlans to PhysicalTasks.
pub fn convert(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = Vec::new();
    for plan in plans {
        tasks.extend(convert_one(plan, tenant_id, ctx)?);
    }
    Ok(tasks)
}

pub(super) fn convert_one(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    match plan {
        SqlPlan::ConstantResult { columns, values } => {
            super::set_ops::convert_constant_result(columns, values, tenant_id)
        }

        SqlPlan::Scan {
            collection,
            alias: _,
            engine,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
        } => super::scan::convert_scan(super::scan_params::ScanParams {
            collection,
            engine,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            tenant_id,
        }),

        SqlPlan::PointGet {
            collection,
            alias: _,
            engine,
            key_column: _,
            key_value,
        } => super::scan::convert_point_get(collection, engine, key_value, tenant_id),

        SqlPlan::Insert {
            collection,
            engine,
            rows,
            column_defaults,
        } => super::dml::convert_insert(collection, engine, rows, column_defaults, tenant_id),

        SqlPlan::Upsert {
            collection,
            engine,
            rows,
            column_defaults,
            on_conflict_updates,
        } => super::dml::convert_upsert(
            collection,
            engine,
            rows,
            column_defaults,
            on_conflict_updates,
            tenant_id,
        ),

        SqlPlan::KvInsert {
            collection,
            entries,
            ttl_secs,
        } => super::dml::convert_kv_insert(collection, entries, *ttl_secs, tenant_id),

        SqlPlan::Update {
            collection,
            engine,
            assignments,
            filters,
            target_keys,
            returning,
        } => super::dml::convert_update(
            collection,
            engine,
            assignments,
            filters,
            target_keys,
            *returning,
            tenant_id,
        ),

        SqlPlan::Delete {
            collection,
            engine,
            filters,
            target_keys,
        } => super::dml::convert_delete(collection, engine, filters, target_keys, tenant_id),

        SqlPlan::Truncate {
            collection,
            restart_identity,
        } => super::set_ops::convert_truncate(collection, *restart_identity, tenant_id),

        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            limit,
            projection,
            filters,
            ..
        } => super::scan::convert_join(super::scan_params::JoinPlanParams {
            left,
            right,
            on,
            join_type,
            limit,
            projection,
            filters,
            tenant_id,
            ctx,
        }),

        SqlPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            limit,
        } => super::aggregate::convert_aggregate(
            input, group_by, aggregates, having, *limit, tenant_id, ctx,
        ),

        SqlPlan::TimeseriesScan {
            collection,
            time_range,
            bucket_interval_ms,
            group_by,
            aggregates,
            filters,
            projection,
            gap_fill,
            limit,
            tiered,
        } => super::scan::convert_timeseries_scan(super::scan_params::TimeseriesScanParams {
            collection,
            time_range,
            bucket_interval_ms,
            group_by,
            aggregates,
            filters,
            projection,
            gap_fill,
            limit,
            tiered,
            tenant_id,
            ctx,
        }),

        SqlPlan::TimeseriesIngest { collection, rows } => {
            super::scan::convert_timeseries_ingest(collection, rows, tenant_id)
        }

        SqlPlan::VectorSearch {
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            filters,
        } => super::scan::convert_vector_search(
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            filters,
            tenant_id,
        ),

        SqlPlan::TextSearch {
            collection,
            query,
            top_k,
            fuzzy,
            ..
        } => super::scan::convert_text_search(collection, query, top_k, fuzzy, tenant_id),

        SqlPlan::HybridSearch {
            collection,
            query_vector,
            query_text,
            top_k,
            ef_search,
            vector_weight,
            fuzzy,
        } => super::scan::convert_hybrid_search(super::scan_params::HybridSearchParams {
            collection,
            query_vector,
            query_text,
            top_k,
            ef_search,
            vector_weight,
            fuzzy,
            tenant_id,
        }),

        SqlPlan::SpatialScan {
            collection,
            field,
            predicate,
            query_geometry,
            distance_meters,
            attribute_filters,
            limit,
            projection,
        } => super::scan::convert_spatial_scan(super::scan_params::SpatialScanParams {
            collection,
            field,
            predicate,
            query_geometry,
            distance_meters,
            attribute_filters,
            limit,
            projection,
            tenant_id,
        }),

        SqlPlan::Union { inputs, distinct } => {
            super::set_ops::convert_union(inputs, *distinct, tenant_id, ctx)
        }

        SqlPlan::Intersect { left, right, all } => {
            super::set_ops::convert_intersect(left, right, *all, tenant_id, ctx)
        }

        SqlPlan::Except { left, right, all } => {
            super::set_ops::convert_except(left, right, *all, tenant_id, ctx)
        }

        SqlPlan::InsertSelect { target, source, .. } => {
            super::set_ops::convert_insert_select(target, source, tenant_id, ctx)
        }

        SqlPlan::RecursiveScan {
            collection,
            base_filters,
            recursive_filters,
            max_iterations,
            distinct,
            limit,
        } => super::scan::convert_recursive_scan(
            collection,
            base_filters,
            recursive_filters,
            max_iterations,
            distinct,
            limit,
            tenant_id,
        ),

        SqlPlan::Cte { definitions, outer } => {
            super::set_ops::convert_cte(definitions, outer, tenant_id, ctx)
        }

        SqlPlan::MultiVectorSearch { .. } | SqlPlan::RangeScan { .. } => {
            Err(crate::Error::PlanError {
                detail: format!("unsupported SqlPlan variant: {plan:?}"),
            })
        }
    }
}
