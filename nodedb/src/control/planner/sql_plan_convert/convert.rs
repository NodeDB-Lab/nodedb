//! Convert nodedb-sql SqlPlan IR to NodeDB PhysicalPlan + PhysicalTask.
//!
//! This is the Origin-specific mapping layer. It adds vShard routing,
//! serializes filters to msgpack, and handles broadcast join decisions.

use nodedb_sql::types::SqlPlan;

use std::sync::Arc;

use crate::control::array_catalog::ArrayCatalogHandle;
use crate::control::security::credential::CredentialStore;
use crate::control::surrogate::SurrogateAssigner;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::TenantId;
use crate::wal::WalManager;

use super::super::physical::PhysicalTask;

/// Conversion context holding optional references needed during plan conversion.
pub struct ConvertContext {
    pub retention_registry: Option<Arc<RetentionPolicyRegistry>>,
    /// Array DDL/DML targets — when `None`, array statements fail with a
    /// deterministic error so converters used by sub-planners (which do
    /// not own array state) cannot accidentally mutate the catalog.
    pub array_catalog: Option<ArrayCatalogHandle>,
    /// Used by `SqlPlan::CreateArray` / `DropArray` to persist or
    /// remove `_system.arrays` rows.
    pub credentials: Option<Arc<CredentialStore>>,
    /// LSN allocator for array Put/Delete dispatches.
    pub wal: Option<Arc<WalManager>>,
    /// CP-side surrogate assigner — bound to the same `Arc` held on
    /// `SharedState`. Threaded into INSERT/UPSERT/KV-INSERT converters
    /// to bind `(collection, pk_bytes)` → `Surrogate` before the op
    /// crosses the SPSC bridge. `None` only for converters used by
    /// sub-planners that never lower to the surrogate-bearing variants
    /// (e.g. CREATE/DROP/ARRAY paths).
    pub surrogate_assigner: Option<Arc<SurrogateAssigner>>,
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
            temporal,
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
            temporal,
        }),

        SqlPlan::PointGet {
            collection,
            alias: _,
            engine,
            key_column,
            key_value,
        } => super::scan::convert_point_get(
            collection, engine, key_column, key_value, tenant_id, ctx,
        ),

        SqlPlan::DocumentIndexLookup {
            collection,
            alias: _,
            engine: _,
            field,
            value,
            filters,
            projection,
            sort_keys: _,
            limit,
            offset,
            distinct: _,
            window_functions: _,
            case_insensitive: _,
            temporal: _,
        } => super::scan::convert_document_index_lookup(
            collection, field, value, filters, projection, *limit, *offset, tenant_id,
        ),

        SqlPlan::Insert {
            collection,
            engine,
            rows,
            column_defaults,
            if_absent,
        } => super::dml::convert_insert(
            collection,
            engine,
            rows,
            column_defaults,
            *if_absent,
            tenant_id,
            ctx,
        ),

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
            ctx,
        ),

        SqlPlan::KvInsert {
            collection,
            entries,
            ttl_secs,
            intent,
            on_conflict_updates,
        } => super::dml::convert_kv_insert(
            collection,
            entries,
            *ttl_secs,
            *intent,
            on_conflict_updates,
            tenant_id,
            ctx,
        ),

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
            ctx,
        ),

        SqlPlan::Delete {
            collection,
            engine,
            filters,
            target_keys,
        } => super::dml::convert_delete(collection, engine, filters, target_keys, tenant_id, ctx),

        SqlPlan::Truncate {
            collection,
            restart_identity,
        } => super::set_ops::convert_truncate(collection, *restart_identity, tenant_id),

        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection,
            filters,
        } => super::scan::convert_join(super::scan_params::JoinPlanParams {
            left,
            right,
            on,
            join_type,
            condition,
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
            temporal,
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
            temporal,
        }),

        SqlPlan::TimeseriesIngest { collection, rows } => {
            super::scan::convert_timeseries_ingest(collection, rows, tenant_id, ctx)
        }

        SqlPlan::VectorSearch {
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            filters,
            array_prefilter,
        } => super::scan::convert_vector_search(super::scan_params::VectorSearchParams {
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            filters,
            array_prefilter: array_prefilter.as_ref(),
            tenant_id,
            ctx,
        }),

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
            join_link,
            max_iterations,
            distinct,
            limit,
        } => super::scan::convert_recursive_scan(super::scan_params::RecursiveScanParams {
            collection,
            base_filters,
            recursive_filters,
            join_link,
            max_iterations,
            distinct,
            limit,
            tenant_id,
        }),

        SqlPlan::Cte { definitions, outer } => {
            super::set_ops::convert_cte(definitions, outer, tenant_id, ctx)
        }

        SqlPlan::CreateArray {
            name,
            dims,
            attrs,
            tile_extents,
            cell_order,
            tile_order,
        } => super::array_convert::convert_create_array(
            name,
            dims,
            attrs,
            tile_extents,
            *cell_order,
            *tile_order,
            tenant_id,
            ctx,
        ),

        SqlPlan::DropArray { name, if_exists } => {
            super::array_convert::convert_drop_array(name, *if_exists, tenant_id, ctx)
        }

        SqlPlan::InsertArray { name, rows } => {
            super::array_convert::convert_insert_array(name, rows, tenant_id, ctx)
        }

        SqlPlan::DeleteArray { name, coords } => {
            super::array_convert::convert_delete_array(name, coords, tenant_id, ctx)
        }

        SqlPlan::NdArraySlice {
            name,
            slice,
            attr_projection,
            limit,
        } => super::array_fn_convert::convert_slice(
            name,
            slice,
            attr_projection,
            *limit,
            tenant_id,
            ctx,
        ),

        SqlPlan::NdArrayProject {
            name,
            attr_projection,
        } => super::array_fn_convert::convert_project(name, attr_projection, tenant_id, ctx),

        SqlPlan::NdArrayAgg {
            name,
            attr,
            reducer,
            group_by_dim,
        } => super::array_fn_convert::convert_agg(
            name,
            attr,
            *reducer,
            group_by_dim.as_deref(),
            tenant_id,
            ctx,
        ),

        SqlPlan::NdArrayElementwise {
            left,
            right,
            op,
            attr,
        } => super::array_fn_convert::convert_elementwise(left, right, *op, attr, tenant_id, ctx),

        SqlPlan::NdArrayFlush { name } => {
            super::array_fn_convert::convert_flush(name, tenant_id, ctx)
        }

        SqlPlan::NdArrayCompact { name } => {
            super::array_fn_convert::convert_compact(name, tenant_id, ctx)
        }

        SqlPlan::MultiVectorSearch { .. } | SqlPlan::RangeScan { .. } => {
            Err(crate::Error::PlanError {
                detail: format!("unsupported SqlPlan variant: {plan:?}"),
            })
        }
    }
}
