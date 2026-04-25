//! Scan and search plan conversions (read-only query paths).

use nodedb_sql::TemporalScope;
use nodedb_sql::planner::bitmap_emit::predicate::BitmapHint;
use nodedb_sql::types::{EngineType, Filter, SqlPlan, SqlValue};

/// Project `TemporalScope::valid_time` into the wire field `valid_at_ms`.
/// `ValidTime::Range` is currently not threaded on the wire — a range
/// form requires a wire-type widening, tracked as a separate batch.
fn valid_at_from_scope(t: &TemporalScope) -> Option<i64> {
    match t.valid_time {
        nodedb_sql::ValidTime::Any | nodedb_sql::ValidTime::Range(..) => None,
        nodedb_sql::ValidTime::At(ms) => Some(ms),
    }
}

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::aggregate::{
    agg_expr_to_pair, extract_collection_name, extract_join_projection_specs,
    extract_projection_names, extract_scan_alias,
};
use super::convert::convert_one;
use super::expr::convert_sort_keys;
use super::filter::{expr_filter_qualified, serialize_filters};
use super::scan_params::{
    HybridSearchParams, JoinPlanParams, RecursiveScanParams, ScanParams, SpatialScanParams,
    TimeseriesScanParams,
};
use super::value::{
    extract_time_range, row_to_msgpack, sql_value_to_bytes, sql_value_to_string,
    write_msgpack_array_header,
};

/// Serialize WHERE filters + non-equi join condition into a single `Vec<u8>`.
///
/// The non-equi condition (from the ON clause) is appended as a
/// `FilterOp::Expr` ScanFilter so the join executor evaluates it on
/// merged rows alongside any post-join WHERE filters.
fn serialize_join_filters(
    filters: &[Filter],
    condition: &Option<nodedb_sql::types::SqlExpr>,
) -> crate::Result<Vec<u8>> {
    match condition {
        None => serialize_filters(filters),
        Some(cond) => {
            // Deserialize existing filters (if any), append condition, re-serialize.
            let mut scan_filters: Vec<nodedb_query::scan_filter::ScanFilter> =
                if !filters.is_empty() {
                    let base = serialize_filters(filters)?;
                    if base.is_empty() {
                        Vec::new()
                    } else {
                        zerompk::from_msgpack(&base).unwrap_or_default()
                    }
                } else {
                    Vec::new()
                };
            scan_filters.push(expr_filter_qualified(cond));
            zerompk::to_msgpack_vec(&scan_filters).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("join filter serialization: {e}"),
            })
        }
    }
}

pub(super) fn convert_scan(p: ScanParams<'_>) -> crate::Result<Vec<PhysicalTask>> {
    let ScanParams {
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
    } = p;
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection, window_functions);
    let sort = convert_sort_keys(sort_keys);
    let vshard = VShardId::from_collection(collection);
    let computed_bytes = super::aggregate::extract_computed_columns(projection, window_functions)?;
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
                computed_columns: computed_bytes,
                rls_filters: Vec::new(),
                system_as_of_ms: None,
                valid_at_ms: None,
            })
        }
        EngineType::Columnar => PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: collection.into(),
            projection: proj_names,
            limit: limit.unwrap_or(10000),
            filters: filter_bytes,
            rls_filters: Vec::new(),
            sort_keys: sort.clone(),
            system_as_of_ms: temporal.system_as_of_ms,
            valid_at_ms: valid_at_from_scope(temporal),
            prefilter: None,
        }),
        EngineType::Spatial => PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: collection.into(),
            projection: proj_names,
            limit: limit.unwrap_or(10000),
            filters: filter_bytes,
            rls_filters: Vec::new(),
            sort_keys: sort.clone(),
            system_as_of_ms: None,
            valid_at_ms: None,
            prefilter: None,
        }),
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Scan {
            collection: collection.into(),
            cursor: Vec::new(),
            count: limit.unwrap_or(10000),
            filters: filter_bytes,
            match_pattern: None,
        }),
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: collection.into(),
                limit: limit.unwrap_or(10000),
                offset: *offset,
                sort_keys: sort,
                filters: filter_bytes,
                distinct: *distinct,
                projection: proj_names,
                computed_columns: computed_bytes,
                window_functions: window_bytes,
                system_as_of_ms: temporal.system_as_of_ms,
                valid_at_ms: valid_at_from_scope(temporal),
                prefilter: None,
            })
        }
        EngineType::Array => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "scan on '{collection}': array engine has no table-shaped scan; use NDARRAY_SLICE"
                ),
            });
        }
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: physical,
        post_set_op: PostSetOp::None,
    }])
}

/// Map `SqlPlan::DocumentIndexLookup` to a `DocumentOp::IndexedFetch` task.
///
/// The handler resolves doc IDs through the sparse index, fetches each
/// document, applies any remaining filters + projection, and emits rows
/// in the same wire format as a document scan.
#[allow(clippy::too_many_arguments)]
pub(super) fn convert_document_index_lookup(
    collection: &str,
    field: &str,
    value: &SqlValue,
    filters: &[Filter],
    projection: &[nodedb_sql::types::Projection],
    limit: Option<usize>,
    offset: usize,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection, &[]);
    let vshard = VShardId::from_collection(collection);
    let physical = PhysicalPlan::Document(DocumentOp::IndexedFetch {
        collection: collection.into(),
        path: field.into(),
        value: sql_value_to_string(value),
        filters: filter_bytes,
        projection: proj_names,
        limit: limit.unwrap_or(10_000),
        offset,
    });
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
    key_column: &str,
    key_value: &SqlValue,
    tenant_id: TenantId,
    ctx: &super::convert::ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let physical = match engine {
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Get {
            collection: collection.into(),
            key: sql_value_to_bytes(key_value),
            rls_filters: Vec::new(),
        }),
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            let pk_string = sql_value_to_string(key_value);
            let pk_bytes = pk_string.clone().into_bytes();
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(collection, &pk_bytes)? {
                    Some(s) => s,
                    None => {
                        // Row not bound — return zero tasks; the
                        // dispatcher emits an empty result set.
                        return Ok(Vec::new());
                    }
                },
                None => nodedb_types::Surrogate::ZERO,
            };
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: collection.into(),
                document_id: pk_string,
                surrogate,
                pk_bytes,
                rls_filters: Vec::new(),
                system_as_of_ms: None,
                valid_at_ms: None,
            })
        }
        // Columnar point get: emit a ColumnarOp::Scan with an `Eq` filter
        // on the PK column and limit=1. Columnar collections have no
        // document store, so routing to `DocumentOp::PointGet` silently
        // returns zero rows.
        EngineType::Columnar | EngineType::Spatial => {
            use nodedb_query::scan_filter::{FilterOp, ScanFilter};
            let scan_filter = ScanFilter {
                field: key_column.to_string(),
                op: FilterOp::Eq,
                value: super::value::sql_value_to_nodedb_value(key_value),
                clauses: Vec::new(),
                expr: None,
            };
            let filter_bytes = zerompk::to_msgpack_vec(&vec![scan_filter]).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("columnar point-get filter: {e}"),
                }
            })?;
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: collection.into(),
                projection: Vec::new(),
                limit: 1,
                filters: filter_bytes,
                rls_filters: Vec::new(),
                sort_keys: Vec::new(),
                system_as_of_ms: None,
                valid_at_ms: None,
                prefilter: None,
            })
        }
        // Timeseries should never reach here — nodedb-sql rejects point gets.
        EngineType::Timeseries => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "point get on '{collection}': timeseries does not support point lookups"
                ),
            });
        }
        // Array reads do not have a key column.
        EngineType::Array => {
            return Err(crate::Error::PlanError {
                detail: format!("point get on '{collection}': array engine has no primary key"),
            });
        }
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: physical,
        post_set_op: PostSetOp::None,
    }])
}

/// Build a `PhysicalPlan` bitmap-producer sub-plan from a `BitmapHint`.
///
/// Returns `None` for hint shapes that cannot be represented as an
/// `IndexedFetch` (e.g. non-string primary values that have no reasonable
/// index-path encoding). The caller treats `None` as "no bitmap pushdown".
fn bitmap_hint_to_plan(hint: &BitmapHint) -> Option<Box<PhysicalPlan>> {
    // Only equality hints translate to an `IndexedFetch` sub-plan.
    // IN-list hints with extra_values would require a multi-value fetch —
    // not currently supported by `DocumentOp::IndexedFetch`; skip them.
    if !hint.extra_values.is_empty() {
        return None;
    }
    let value_str = sql_value_to_string(&hint.primary_value);
    Some(Box::new(PhysicalPlan::Document(DocumentOp::IndexedFetch {
        collection: hint.collection.clone(),
        path: hint.field.clone(),
        value: value_str,
        filters: Vec::new(),
        projection: Vec::new(),
        limit: 10_000,
        offset: 0,
    })))
}

pub(super) fn convert_join(p: JoinPlanParams<'_>) -> crate::Result<Vec<PhysicalTask>> {
    let JoinPlanParams {
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
    } = p;
    let mut left_collection = extract_collection_name(left);
    let mut right_collection = extract_collection_name(right);
    let mut left_alias = extract_scan_alias(left);
    let mut right_alias = extract_scan_alias(right);
    let join_projection = extract_join_projection_specs(projection);
    let filter_bytes = serialize_join_filters(filters, condition)?;

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
    let mut inline_left = inline_left;
    let mut inline_right = inline_right;
    let effective_join_type = if join_type.as_str() == "right" {
        std::mem::swap(&mut left_collection, &mut right_collection);
        std::mem::swap(&mut left_alias, &mut right_alias);
        std::mem::swap(&mut inline_left, &mut inline_right);
        on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
        "left".to_string()
    } else {
        join_type.as_str().to_string()
    };

    // Analyze join children for selective-predicate bitmap pushdown.
    // The analysis runs on the *original* (pre-swap) children since it inspects
    // SqlPlan shape. After the RIGHT→LEFT swap, we swap the resulting hints too.
    let bitmap_hints = nodedb_sql::planner::bitmap_emit::hashjoin::analyze_join_sides(left, right);
    let (mut raw_left_bm, mut raw_right_bm) = (bitmap_hints.left, bitmap_hints.right);
    if join_type.as_str() == "right" {
        std::mem::swap(&mut raw_left_bm, &mut raw_right_bm);
    }
    let inline_left_bitmap = raw_left_bm.and_then(|h| bitmap_hint_to_plan(&h));
    let inline_right_bitmap = raw_right_bm.and_then(|h| bitmap_hint_to_plan(&h));

    let vshard = VShardId::from_collection(&left_collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on: on_keys,
            join_type: effective_join_type,
            limit: *limit,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: join_projection,
            post_filters: filter_bytes,
            inline_left,
            inline_right,
            inline_left_bitmap,
            inline_right_bitmap,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_timeseries_scan(
    p: TimeseriesScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let TimeseriesScanParams {
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
    } = p;
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

    let proj_names = extract_projection_names(projection, &[]);
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
            computed_columns: Vec::new(),
            rls_filters: Vec::new(),
            system_as_of_ms: temporal.system_as_of_ms,
            valid_at_ms: valid_at_from_scope(temporal),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_timeseries_ingest(
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    tenant_id: TenantId,
    ctx: &super::convert::ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let mut payload = Vec::with_capacity(rows.len() * 128);
    write_msgpack_array_header(&mut payload, rows.len());
    let mut surrogates: Vec<nodedb_types::Surrogate> = Vec::with_capacity(rows.len());
    for row in rows {
        let row_bytes = row_to_msgpack(row)?;
        payload.extend_from_slice(&row_bytes);
        // Timeseries PK is the (timestamp, tag-set) tuple, which is not
        // canonically named. Use the same (id|document_id|key) heuristic
        // as the columnar/document path; rows with no PK column receive
        // `Surrogate::ZERO` (downstream re-derivation owned by the
        // engine integration once the row's natural identity is known).
        let pk = row
            .iter()
            .find(|(k, _)| k == "id" || k == "document_id" || k == "key")
            .map(|(_, v)| sql_value_to_string(v))
            .unwrap_or_default();
        if pk.is_empty() {
            surrogates.push(nodedb_types::Surrogate::ZERO);
        } else {
            let s = match ctx.surrogate_assigner.as_ref() {
                Some(a) => a.assign(collection, pk.as_bytes())?,
                None => nodedb_types::Surrogate::ZERO,
            };
            surrogates.push(s);
        }
    }
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.into(),
            payload,
            format: "msgpack".into(),
            wal_lsn: None,
            surrogates,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_vector_search(
    p: super::scan_params::VectorSearchParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(p.collection);
    let filter_bytes = serialize_filters(p.filters)?;
    let inline_prefilter_plan = match p.array_prefilter {
        Some(pref) => Some(Box::new(build_array_prefilter_plan(
            pref,
            p.tenant_id,
            p.ctx,
        )?)),
        None => None,
    };
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Vector(VectorOp::Search {
            collection: p.collection.into(),
            query_vector: p.query_vector.to_vec(),
            top_k: *p.top_k,
            ef_search: *p.ef_search,
            filter_bitmap: None,
            field_name: p.field.to_string(),
            rls_filters: filter_bytes,
            inline_prefilter_plan,
        }),
        post_set_op: PostSetOp::None,
    }])
}

/// Lower an `NdArrayPrefilter` (array name + slice AST) into the
/// `ArrayOp::SurrogateBitmapScan` sub-plan that the vector search handler
/// runs as its `inline_prefilter_plan`.
fn build_array_prefilter_plan(
    prefilter: &nodedb_sql::types::NdArrayPrefilter,
    tenant_id: TenantId,
    ctx: &super::convert::ConvertContext,
) -> crate::Result<PhysicalPlan> {
    use nodedb_array::query::slice::{DimRange, Slice};
    use nodedb_array::schema::ArraySchema;
    use nodedb_array::types::ArrayId;

    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "array prefilter: no array catalog wired into convert context".into(),
        })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name(&prefilter.array_name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!(
                    "array prefilter: array '{}' not found",
                    prefilter.array_name
                ),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    let mut dim_ranges: Vec<Option<DimRange>> = vec![None; schema.dims.len()];
    for r in &prefilter.slice.dim_ranges {
        let idx = schema
            .dims
            .iter()
            .position(|d| d.name == r.dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!(
                    "array prefilter: array '{}' has no dim '{}'",
                    prefilter.array_name, r.dim
                ),
            })?;
        let dtype = schema.dims[idx].dtype;
        let lo = super::array_fn_convert::helpers::coerce_bound(&r.lo, dtype, &r.dim)?;
        let hi = super::array_fn_convert::helpers::coerce_bound(&r.hi, dtype, &r.dim)?;
        dim_ranges[idx] = Some(DimRange::new(lo, hi));
    }
    let slice = Slice::new(dim_ranges);
    let slice_msgpack =
        zerompk::to_msgpack_vec(&slice).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array slice encode: {e}"),
        })?;

    let aid = ArrayId::new(tenant_id, &prefilter.array_name);
    Ok(PhysicalPlan::Array(
        crate::bridge::physical_plan::ArrayOp::SurrogateBitmapScan {
            array_id: aid,
            slice_msgpack,
        },
    ))
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
            prefilter: None,
            rls_filters: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_hybrid_search(p: HybridSearchParams<'_>) -> crate::Result<Vec<PhysicalTask>> {
    let HybridSearchParams {
        collection,
        query_vector,
        query_text,
        top_k,
        ef_search,
        vector_weight,
        fuzzy,
        tenant_id,
    } = p;
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Text(TextOp::HybridSearch {
            collection: collection.into(),
            query_vector: query_vector.to_vec(),
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

pub(super) fn convert_spatial_scan(p: SpatialScanParams<'_>) -> crate::Result<Vec<PhysicalTask>> {
    let SpatialScanParams {
        collection,
        field,
        predicate,
        query_geometry,
        distance_meters,
        attribute_filters,
        limit,
        projection,
        tenant_id,
    } = p;
    let vshard = VShardId::from_collection(collection);
    let attr_bytes = serialize_filters(attribute_filters)?;
    let proj_names = extract_projection_names(projection, &[]);
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
            prefilter: None,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_recursive_scan(
    p: RecursiveScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(p.collection);
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
            collection: p.collection.into(),
            base_filters: serialize_filters(p.base_filters)?,
            recursive_filters: serialize_filters(p.recursive_filters)?,
            join_link: p.join_link.clone(),
            max_iterations: *p.max_iterations,
            distinct: *p.distinct,
            limit: *p.limit,
        }),
        post_set_op: PostSetOp::None,
    }])
}
