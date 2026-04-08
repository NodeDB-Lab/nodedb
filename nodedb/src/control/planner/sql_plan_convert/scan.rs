//! Scan and search plan conversions (read-only query paths).

use nodedb_sql::types::{
    AggregateExpr, EngineType, Filter, Projection, SortKey, SqlPlan, SqlValue,
};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::aggregate::{agg_expr_to_pair, extract_collection_name, extract_projection_names};
use super::convert::{ConvertContext, convert_one};
use super::expr::convert_sort_keys;
use super::filter::serialize_filters;
use super::value::{
    extract_time_range, serialize_ingest_rows, sql_value_to_bytes, sql_value_to_string,
};

pub(super) fn convert_scan(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    projection: &[Projection],
    sort_keys: &[SortKey],
    limit: &Option<usize>,
    offset: &usize,
    distinct: &bool,
    window_functions: &[nodedb_sql::types::WindowSpec],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection);
    let sort = convert_sort_keys(sort_keys);
    let vshard = VShardId::from_collection(collection);
    let computed_bytes = super::aggregate::extract_computed_columns(projection)?;
    let window_bytes = super::aggregate::serialize_window_functions(window_functions)?;

    let physical = match engine {
        EngineType::Timeseries => {
            let time_range = extract_time_range(filters);
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: collection.into(),
                time_range,
                projection: proj_names,
                limit: limit.unwrap_or(10000),
                filters: filter_bytes,
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                rls_filters: Vec::new(),
            })
        }
        EngineType::Columnar | EngineType::Spatial => PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: collection.into(),
            projection: proj_names,
            limit: limit.unwrap_or(10000),
            filters: filter_bytes,
            rls_filters: Vec::new(),
        }),
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Scan {
            collection: collection.into(),
            cursor: Vec::new(),
            count: limit.unwrap_or(10000),
            filters: filter_bytes,
            match_pattern: None,
        }),
        _ => PhysicalPlan::Document(DocumentOp::Scan {
            collection: collection.into(),
            limit: limit.unwrap_or(10000),
            offset: *offset,
            sort_keys: sort,
            filters: filter_bytes,
            distinct: *distinct,
            projection: proj_names,
            computed_columns: computed_bytes,
            window_functions: window_bytes,
        }),
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: physical,
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_point_get(
    collection: &str,
    engine: &EngineType,
    key_value: &SqlValue,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let physical = match engine {
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Get {
            collection: collection.into(),
            key: sql_value_to_bytes(key_value),
            rls_filters: Vec::new(),
        }),
        _ => PhysicalPlan::Document(DocumentOp::PointGet {
            collection: collection.into(),
            document_id: sql_value_to_string(key_value),
            rls_filters: Vec::new(),
        }),
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: physical,
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_join(
    left: &SqlPlan,
    right: &SqlPlan,
    on: &[(String, String)],
    join_type: &nodedb_sql::types::JoinType,
    limit: &usize,
    projection: &[Projection],
    filters: &[Filter],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut left_collection = extract_collection_name(left);
    let mut right_collection = extract_collection_name(right);
    let proj_names = extract_projection_names(projection);
    let filter_bytes = serialize_filters(filters)?;

    // Check if the left side is a nested join (multi-way join).
    // If so, convert the inner join to a physical plan and pass it
    // as `inline_left` so the executor runs it first.
    let inline_left = if matches!(left, SqlPlan::Join { .. }) {
        let inner_tasks = convert_one(left, tenant_id, ctx)?;
        inner_tasks.into_iter().next().map(|t| Box::new(t.plan))
    } else {
        None
    };
    let inline_right = super::aggregate::inline_join_side(right, tenant_id, ctx)?;

    // RIGHT JOIN → swap sides and convert to LEFT JOIN.
    let mut on_keys = on.to_vec();
    let effective_join_type = if join_type.as_str() == "right" {
        std::mem::swap(&mut left_collection, &mut right_collection);
        on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
        "left".to_string()
    } else {
        join_type.as_str().to_string()
    };

    let vshard = VShardId::from_collection(&left_collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            on: on_keys,
            join_type: effective_join_type,
            limit: *limit,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: proj_names,
            post_filters: filter_bytes,
            inline_left,
            inline_right,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_timeseries_scan(
    collection: &str,
    time_range: &(i64, i64),
    bucket_interval_ms: &i64,
    group_by: &[String],
    aggregates: &[AggregateExpr],
    filters: &[Filter],
    projection: &[Projection],
    gap_fill: &str,
    limit: &usize,
    tiered: &bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let filter_bytes = serialize_filters(filters)?;
    let agg_pairs: Vec<(String, String)> = aggregates.iter().map(agg_expr_to_pair).collect();

    // AUTO_TIER: split query across retention tiers if enabled.
    if *tiered
        && let Some(registry) = &ctx.retention_registry
        && let Some(policy) = registry.get(tenant_id.as_u32(), collection)
        && policy.auto_tier
    {
        return Ok(super::super::auto_tier::plan_tiered_scan(
            &policy,
            tenant_id,
            *time_range,
            filter_bytes,
            group_by.to_vec(),
            agg_pairs,
            gap_fill.to_string(),
        ));
    }

    let proj_names = extract_projection_names(projection);
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: collection.into(),
            time_range: *time_range,
            projection: proj_names,
            limit: *limit,
            filters: filter_bytes,
            bucket_interval_ms: *bucket_interval_ms,
            group_by: group_by.to_vec(),
            aggregates: agg_pairs,
            gap_fill: gap_fill.to_string(),
            rls_filters: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_timeseries_ingest(
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let payload = serialize_ingest_rows(rows)?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.into(),
            payload,
            format: "json".into(),
            wal_lsn: None,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_vector_search(
    collection: &str,
    field: &str,
    query_vector: &[f32],
    top_k: &usize,
    ef_search: &usize,
    filters: &[Filter],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let filter_bytes = serialize_filters(filters)?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Vector(VectorOp::Search {
            collection: collection.into(),
            query_vector: query_vector.to_vec().into(),
            top_k: *top_k,
            ef_search: *ef_search,
            filter_bitmap: None,
            field_name: field.to_string(),
            rls_filters: filter_bytes,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_text_search(
    collection: &str,
    query: &str,
    top_k: &usize,
    fuzzy: &bool,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Text(TextOp::Search {
            collection: collection.into(),
            query: query.to_string(),
            top_k: *top_k,
            fuzzy: *fuzzy,
            rls_filters: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_hybrid_search(
    collection: &str,
    query_vector: &[f32],
    query_text: &str,
    top_k: &usize,
    ef_search: &usize,
    vector_weight: &f32,
    fuzzy: &bool,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Text(TextOp::HybridSearch {
            collection: collection.into(),
            query_vector: query_vector.to_vec().into(),
            query_text: query_text.to_string(),
            top_k: *top_k,
            ef_search: *ef_search,
            fuzzy: *fuzzy,
            vector_weight: *vector_weight,
            filter_bitmap: None,
            rls_filters: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_spatial_scan(
    collection: &str,
    field: &str,
    predicate: &nodedb_sql::types::SpatialPredicate,
    query_geometry: &[u8],
    distance_meters: &f64,
    attribute_filters: &[Filter],
    limit: &usize,
    projection: &[Projection],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let attr_bytes = serialize_filters(attribute_filters)?;
    let proj_names = extract_projection_names(projection);
    let sp = match predicate {
        nodedb_sql::types::SpatialPredicate::DWithin => SpatialPredicate::DWithin,
        nodedb_sql::types::SpatialPredicate::Contains => SpatialPredicate::Contains,
        nodedb_sql::types::SpatialPredicate::Intersects => SpatialPredicate::Intersects,
        nodedb_sql::types::SpatialPredicate::Within => SpatialPredicate::Within,
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: collection.into(),
            field: field.to_string(),
            predicate: sp,
            query_geometry: query_geometry.to_vec(),
            distance_meters: *distance_meters,
            attribute_filters: attr_bytes,
            limit: *limit,
            projection: proj_names,
            rls_filters: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_recursive_scan(
    collection: &str,
    base_filters: &[Filter],
    recursive_filters: &[Filter],
    max_iterations: &usize,
    distinct: &bool,
    limit: &usize,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
            collection: collection.into(),
            base_filters: serialize_filters(base_filters)?,
            recursive_filters: serialize_filters(recursive_filters)?,
            max_iterations: *max_iterations,
            distinct: *distinct,
            limit: *limit,
        }),
        post_set_op: PostSetOp::None,
    }])
}
