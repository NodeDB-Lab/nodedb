//! Convert nodedb-sql SqlPlan IR to NodeDB PhysicalPlan + PhysicalTask.
//!
//! This is the Origin-specific mapping layer. It adds vShard routing,
//! serializes filters to msgpack, and handles broadcast join decisions.

use nodedb_sql::types::{
    AggregateExpr, EngineType, Filter, FilterExpr, Projection, SortKey, SqlExpr, SqlPlan, SqlValue,
};

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::{TenantId, VShardId};

use super::physical::PhysicalTask;

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

fn convert_one(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    match plan {
        SqlPlan::ConstantResult { columns, values } => {
            // Build a single-row result as Value::Object → zerompk.
            let mut map = std::collections::HashMap::new();
            for (col, val) in columns.iter().zip(values.iter()) {
                map.insert(col.clone(), sql_value_to_nodedb_value(val));
            }
            let row = nodedb_types::Value::Object(map);
            let payload =
                nodedb_types::value_to_msgpack(&row).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("constant result: {e}"),
                })?;
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: VShardId::from_collection(""),
                plan: PhysicalPlan::Meta(MetaOp::RawResponse { payload }),
                post_dedup: false,
            }])
        }

        SqlPlan::Scan {
            collection,
            engine,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
        } => {
            let filter_bytes = serialize_filters(filters)?;
            let proj_names = extract_projection_names(projection);
            let sort = convert_sort_keys(sort_keys);
            let vshard = VShardId::from_collection(collection);
            let computed_bytes = extract_computed_columns(projection)?;
            let window_bytes = serialize_window_functions(window_functions)?;

            let physical = match engine {
                EngineType::Timeseries => PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection: collection.clone(),
                    time_range: (i64::MIN, i64::MAX),
                    projection: proj_names,
                    limit: limit.unwrap_or(10000),
                    filters: filter_bytes,
                    bucket_interval_ms: 0,
                    group_by: Vec::new(),
                    aggregates: Vec::new(),
                    gap_fill: String::new(),
                    rls_filters: Vec::new(),
                }),
                EngineType::Columnar => PhysicalPlan::Columnar(ColumnarOp::Scan {
                    collection: collection.clone(),
                    projection: proj_names,
                    limit: limit.unwrap_or(10000),
                    filters: filter_bytes,
                    rls_filters: Vec::new(),
                }),
                EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Scan {
                    collection: collection.clone(),
                    cursor: Vec::new(),
                    count: limit.unwrap_or(10000),
                    filters: filter_bytes,
                    match_pattern: None,
                }),
                _ => PhysicalPlan::Document(DocumentOp::Scan {
                    collection: collection.clone(),
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
                post_dedup: false,
            }])
        }

        SqlPlan::PointGet {
            collection,
            engine,
            key_column: _, // Column name is implicit (id/key) per engine type
            key_value,
        } => {
            let vshard = VShardId::from_collection(collection);
            let physical = match engine {
                EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Get {
                    collection: collection.clone(),
                    key: sql_value_to_bytes(key_value),
                    rls_filters: Vec::new(),
                }),
                _ => PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: collection.clone(),
                    document_id: sql_value_to_string(key_value),
                    rls_filters: Vec::new(),
                }),
            };
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: physical,
                post_dedup: false,
            }])
        }

        SqlPlan::Insert {
            collection,
            engine,
            rows,
        } => convert_insert(collection, engine, rows, tenant_id),

        SqlPlan::Update {
            collection,
            engine,
            assignments,
            filters,
            target_keys,
            returning,
        } => convert_update(
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
        } => convert_delete(collection, engine, filters, target_keys, tenant_id),

        SqlPlan::Truncate { collection } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::Truncate {
                    collection: collection.clone(),
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            limit,
            projection,
            filters,
            ..
        } => {
            let left_collection = extract_collection_name(left);
            let right_collection = extract_collection_name(right);
            let vshard = VShardId::from_collection(&left_collection);
            let proj_names = extract_projection_names(projection);
            let filter_bytes = serialize_filters(filters)?;

            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Query(QueryOp::HashJoin {
                    left_collection,
                    right_collection,
                    on: on.to_vec(),
                    join_type: join_type.as_str().to_string(),
                    limit: *limit,
                    post_group_by: Vec::new(),
                    post_aggregates: Vec::new(),
                    projection: proj_names,
                    post_filters: filter_bytes,
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            limit,
        } => convert_aggregate(input, group_by, aggregates, having, *limit, tenant_id),

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
        } => {
            let filter_bytes = serialize_filters(filters)?;
            let agg_pairs: Vec<(String, String)> =
                aggregates.iter().map(agg_expr_to_pair).collect();

            // AUTO_TIER: split query across retention tiers if enabled.
            if *tiered
                && let Some(registry) = &ctx.retention_registry
                && let Some(policy) = registry.get(tenant_id.as_u32(), collection)
                && policy.auto_tier
            {
                return Ok(super::auto_tier::plan_tiered_scan(
                    &policy,
                    tenant_id,
                    *time_range,
                    filter_bytes,
                    group_by.clone(),
                    agg_pairs,
                    gap_fill.clone(),
                ));
            }

            let proj_names = extract_projection_names(projection);
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection: collection.clone(),
                    time_range: *time_range,
                    projection: proj_names,
                    limit: *limit,
                    filters: filter_bytes,
                    bucket_interval_ms: *bucket_interval_ms,
                    group_by: group_by.clone(),
                    aggregates: agg_pairs,
                    gap_fill: gap_fill.clone(),
                    rls_filters: Vec::new(),
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::TimeseriesIngest { collection, rows } => {
            let vshard = VShardId::from_collection(collection);
            let payload = serialize_ingest_rows(rows)?;
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: collection.clone(),
                    payload,
                    format: "json".into(),
                    wal_lsn: None,
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::VectorSearch {
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            filters,
        } => {
            let vshard = VShardId::from_collection(collection);
            let filter_bytes = serialize_filters(filters)?;
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Vector(VectorOp::Search {
                    collection: collection.clone(),
                    query_vector: query_vector.clone().into(),
                    top_k: *top_k,
                    ef_search: *ef_search,
                    filter_bitmap: None,
                    field_name: field.clone(),
                    rls_filters: filter_bytes,
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::TextSearch {
            collection,
            query,
            top_k,
            fuzzy,
            ..
        } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::Search {
                    collection: collection.clone(),
                    query: query.clone(),
                    top_k: *top_k,
                    fuzzy: *fuzzy,
                    rls_filters: Vec::new(),
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::HybridSearch {
            collection,
            query_vector,
            query_text,
            top_k,
            ef_search,
            vector_weight,
            fuzzy,
        } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::HybridSearch {
                    collection: collection.clone(),
                    query_vector: query_vector.clone().into(),
                    query_text: query_text.clone(),
                    top_k: *top_k,
                    ef_search: *ef_search,
                    fuzzy: *fuzzy,
                    vector_weight: *vector_weight,
                    filter_bitmap: None,
                    rls_filters: Vec::new(),
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::SpatialScan {
            collection,
            field,
            predicate,
            query_geometry,
            distance_meters,
            attribute_filters,
            limit,
            projection,
        } => {
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
                    collection: collection.clone(),
                    field: field.clone(),
                    predicate: sp,
                    query_geometry: query_geometry.clone(),
                    distance_meters: *distance_meters,
                    attribute_filters: attr_bytes,
                    limit: *limit,
                    projection: proj_names,
                    rls_filters: Vec::new(),
                }),
                post_dedup: false,
            }])
        }

        SqlPlan::Union { inputs, distinct } => {
            let mut all_tasks = Vec::new();
            for input in inputs {
                all_tasks.extend(convert_one(input, tenant_id, ctx)?);
            }
            if *distinct {
                for task in &mut all_tasks {
                    task.post_dedup = true;
                }
            }
            Ok(all_tasks)
        }

        SqlPlan::RecursiveScan {
            collection,
            base_filters,
            recursive_filters,
            max_iterations,
            distinct,
            limit,
        } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
                    collection: collection.clone(),
                    base_filters: serialize_filters(base_filters)?,
                    recursive_filters: serialize_filters(recursive_filters)?,
                    max_iterations: *max_iterations,
                    distinct: *distinct,
                    limit: *limit,
                }),
                post_dedup: false,
            }])
        }

        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported SqlPlan variant: {plan:?}"),
        }),
    }
}

// ── DML conversions ──

fn convert_insert(
    collection: &str,
    engine: &EngineType,
    rows: &[Vec<(String, SqlValue)>],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let mut tasks = Vec::new();

    for row in rows {
        let doc_id = row
            .iter()
            .find(|(k, _)| k == "id" || k == "document_id" || k == "key")
            .map(|(_, v)| sql_value_to_string(v))
            .unwrap_or_default();

        let value_bytes = row_to_msgpack(row)?;

        match engine {
            EngineType::KeyValue => {
                let key = row
                    .iter()
                    .find(|(k, _)| k == "key")
                    .map(|(_, v)| sql_value_to_bytes(v))
                    .unwrap_or_else(|| doc_id.as_bytes().to_vec());
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Kv(KvOp::Put {
                        collection: collection.into(),
                        key,
                        value: value_bytes,
                        ttl_ms: 0,
                    }),
                    post_dedup: false,
                });
            }
            _ => {
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::PointPut {
                        collection: collection.into(),
                        document_id: doc_id,
                        value: value_bytes,
                    }),
                    post_dedup: false,
                });
            }
        }
    }
    Ok(tasks)
}

fn convert_update(
    collection: &str,
    engine: &EngineType,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
    returning: bool,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let filter_bytes = serialize_filters(filters)?;
    let updates = assignments_to_bytes(assignments)?;

    // KV engine: route to FieldSet for point updates.
    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let field_updates: Vec<(String, Vec<u8>)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let SqlExpr::Literal(val) = expr {
                        Some((field.clone(), sql_value_to_bytes(val)))
                    } else {
                        None
                    }
                })
                .collect();
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Kv(KvOp::FieldSet {
                    collection: collection.into(),
                    key: sql_value_to_bytes(key),
                    updates: field_updates,
                }),
                post_dedup: false,
            });
        }
        return Ok(tasks);
    }

    if !target_keys.is_empty() {
        // Point updates (document engine).
        let mut tasks = Vec::new();
        for key in target_keys {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: sql_value_to_string(key),
                    updates: updates.clone(),
                    returning,
                }),
                post_dedup: false,
            });
        }
        Ok(tasks)
    } else {
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: collection.into(),
                filters: filter_bytes,
                updates,
                returning,
            }),
            post_dedup: false,
        }])
    }
}

fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);

    // KV engine: route to KvOp::Delete.
    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let keys: Vec<Vec<u8>> = target_keys.iter().map(sql_value_to_bytes).collect();
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Kv(KvOp::Delete {
                collection: collection.into(),
                keys,
            }),
            post_dedup: false,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: collection.into(),
                    document_id: sql_value_to_string(key),
                }),
                post_dedup: false,
            });
        }
        Ok(tasks)
    } else {
        let filter_bytes = serialize_filters(filters)?;
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection.into(),
                filters: filter_bytes,
            }),
            post_dedup: false,
        }])
    }
}

fn convert_aggregate(
    input: &SqlPlan,
    group_by: &[SqlExpr],
    aggregates: &[AggregateExpr],
    having: &[Filter],
    limit: usize,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    // Check if aggregating over a join.
    if let SqlPlan::Join {
        left,
        right,
        on,
        join_type,
        limit: join_limit,
        ..
    } = input
    {
        let left_collection = extract_collection_name(left);
        let right_collection = extract_collection_name(right);
        let vshard = VShardId::from_collection(&left_collection);

        let group_strs = group_by_to_strings(group_by);
        let agg_pairs = aggregates.iter().map(agg_expr_to_pair).collect();

        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                on: on.to_vec(),
                join_type: join_type.as_str().to_string(),
                limit: *join_limit,
                post_group_by: group_strs,
                post_aggregates: agg_pairs,
                projection: Vec::new(),
                post_filters: Vec::new(),
            }),
            post_dedup: false,
        }]);
    }

    // Standard aggregate on a single collection.
    let collection = extract_collection_name(input);
    let vshard = VShardId::from_collection(&collection);
    let filter_bytes = match input {
        SqlPlan::Scan { filters, .. } => serialize_filters(filters)?,
        _ => Vec::new(),
    };
    let having_bytes = serialize_filters(having)?;

    let group_strs = group_by_to_strings(group_by);
    let agg_pairs = aggregates.iter().map(agg_expr_to_pair).collect();

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            group_by: group_strs,
            aggregates: agg_pairs,
            filters: filter_bytes,
            having: having_bytes,
            limit,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
        }),
        post_dedup: false,
    }])
}

// ── Helpers ──

fn extract_collection_name(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        SqlPlan::PointGet { collection, .. } => collection.clone(),
        SqlPlan::Join { left, .. } => extract_collection_name(left),
        SqlPlan::Aggregate { input, .. } => extract_collection_name(input),
        _ => String::new(),
    }
}

/// Convert an `AggregateExpr` to the `(op, field)` pair the executor expects.
///
/// The field is extracted from the first argument (e.g. `elapsed_ms` from
/// `AVG(elapsed_ms)`). Wildcard args produce `"*"`. Falls back to `"*"` when
/// there are no arguments (bare `COUNT`).
fn agg_expr_to_pair(a: &AggregateExpr) -> (String, String) {
    let field = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => name.clone(),
            SqlExpr::Wildcard => "*".into(),
            _ => format!("{arg:?}"),
        })
        .unwrap_or_else(|| "*".into());
    (a.function.clone(), field)
}

fn group_by_to_strings(exprs: &[SqlExpr]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|e| match e {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn extract_projection_names(proj: &[Projection]) -> Vec<String> {
    proj.iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn extract_computed_columns(proj: &[Projection]) -> crate::Result<Vec<u8>> {
    // Collect computed projections (alias + expression string).
    let computed: Vec<(String, String)> = proj
        .iter()
        .filter_map(|p| match p {
            Projection::Computed { expr, alias } => Some((alias.clone(), format!("{expr:?}"))),
            _ => None,
        })
        .collect();
    if computed.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&computed).map_err(|e| crate::Error::Internal {
        detail: format!("serialize computed columns: {e}"),
    })
}

fn serialize_window_functions(specs: &[nodedb_sql::types::WindowSpec]) -> crate::Result<Vec<u8>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let bridge_specs: Vec<crate::bridge::window_func::WindowFuncSpec> = specs
        .iter()
        .map(|s| crate::bridge::window_func::WindowFuncSpec {
            alias: s.alias.clone(),
            func_name: s.function.clone(),
            args: s.args.iter().map(sql_expr_to_bridge_expr).collect(),
            partition_by: s
                .partition_by
                .iter()
                .filter_map(|e| match e {
                    SqlExpr::Column { name, .. } => Some(name.clone()),
                    _ => None,
                })
                .collect(),
            order_by: s
                .order_by
                .iter()
                .filter_map(|k| match &k.expr {
                    SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
                    _ => None,
                })
                .collect(),
            frame: crate::bridge::window_func::WindowFrame::default(),
        })
        .collect();
    zerompk::to_msgpack_vec(&bridge_specs).map_err(|e| crate::Error::Internal {
        detail: format!("serialize window functions: {e}"),
    })
}

/// Convert a `nodedb_sql::types::SqlExpr` (parser AST) to a
/// `nodedb_query::expr::SqlExpr` (bridge evaluation type).
fn sql_expr_to_bridge_expr(expr: &SqlExpr) -> crate::bridge::expr_eval::SqlExpr {
    use crate::bridge::expr_eval::SqlExpr as BExpr;
    match expr {
        SqlExpr::Column { name, .. } => BExpr::Column(name.clone()),
        SqlExpr::Literal(v) => BExpr::Literal(sql_value_to_json(v)),
        SqlExpr::Wildcard => BExpr::Column("*".into()),
        _ => BExpr::Literal(serde_json::Value::Null),
    }
}

fn convert_sort_keys(keys: &[SortKey]) -> Vec<(String, bool)> {
    keys.iter()
        .filter_map(|k| match &k.expr {
            SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
            _ => None,
        })
        .collect()
}

/// Convert SqlPlan filters to ScanFilter msgpack bytes.
fn serialize_filters(filters: &[Filter]) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let scan_filters: Vec<nodedb_query::scan_filter::ScanFilter> = filters
        .iter()
        .flat_map(|f| filter_to_scan_filters(&f.expr))
        .collect();
    if scan_filters.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&scan_filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

fn filter_to_scan_filters(expr: &FilterExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        FilterExpr::Comparison { field, op, value } => {
            let filter_op = match op {
                nodedb_sql::types::CompareOp::Eq => FilterOp::Eq,
                nodedb_sql::types::CompareOp::Ne => FilterOp::Ne,
                nodedb_sql::types::CompareOp::Gt => FilterOp::Gt,
                nodedb_sql::types::CompareOp::Ge => FilterOp::Gte,
                nodedb_sql::types::CompareOp::Lt => FilterOp::Lt,
                nodedb_sql::types::CompareOp::Le => FilterOp::Lte,
            };
            vec![ScanFilter {
                field: field.clone(),
                op: filter_op,
                value: sql_value_to_nodedb_value(value),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::Like { field, pattern } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::Like,
                value: nodedb_types::Value::String(pattern.clone()),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::InList { field, values } => {
            let arr = values.iter().map(sql_value_to_nodedb_value).collect();
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::In,
                value: nodedb_types::Value::Array(arr),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::IsNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        FilterExpr::IsNotNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNotNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        FilterExpr::And(filters) => filters
            .iter()
            .flat_map(|f| filter_to_scan_filters(&f.expr))
            .collect(),
        FilterExpr::Or(filters) => {
            let clauses: Vec<Vec<ScanFilter>> = filters
                .iter()
                .map(|f| filter_to_scan_filters(&f.expr))
                .collect();
            vec![ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: nodedb_types::Value::Null,
                clauses,
            }]
        }
        FilterExpr::Expr(sql_expr) => {
            // Convert SqlExpr to ScanFilter via pattern matching.
            sql_expr_to_scan_filters(sql_expr)
        }
        _ => vec![ScanFilter {
            field: String::new(),
            op: FilterOp::MatchAll,
            value: nodedb_types::Value::Null,
            clauses: Vec::new(),
        }],
    }
}

/// Convert a raw SqlExpr (from WHERE clause) to ScanFilter list.
fn sql_expr_to_scan_filters(expr: &SqlExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        SqlExpr::BinaryOp {
            left,
            op: nodedb_sql::types::BinaryOp::And,
            right,
        } => {
            let mut filters = sql_expr_to_scan_filters(left);
            filters.extend(sql_expr_to_scan_filters(right));
            filters
        }
        SqlExpr::BinaryOp {
            left,
            op: nodedb_sql::types::BinaryOp::Or,
            right,
        } => {
            let left_filters = sql_expr_to_scan_filters(left);
            let right_filters = sql_expr_to_scan_filters(right);
            vec![ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: nodedb_types::Value::Null,
                clauses: vec![left_filters, right_filters],
            }]
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let field = match left.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let value = match right.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            let filter_op = match op {
                nodedb_sql::types::BinaryOp::Eq => FilterOp::Eq,
                nodedb_sql::types::BinaryOp::Ne => FilterOp::Ne,
                nodedb_sql::types::BinaryOp::Gt => FilterOp::Gt,
                nodedb_sql::types::BinaryOp::Ge => FilterOp::Gte,
                nodedb_sql::types::BinaryOp::Lt => FilterOp::Lt,
                nodedb_sql::types::BinaryOp::Le => FilterOp::Lte,
                _ => return vec![match_all()],
            };
            vec![ScanFilter {
                field,
                op: filter_op,
                value,
                clauses: Vec::new(),
            }]
        }
        SqlExpr::IsNull { expr, negated } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let op = if *negated {
                FilterOp::IsNotNull
            } else {
                FilterOp::IsNull
            };
            vec![ScanFilter {
                field,
                op,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let values: Vec<nodedb_types::Value> = list
                .iter()
                .filter_map(|e| match e {
                    SqlExpr::Literal(v) => Some(sql_value_to_nodedb_value(v)),
                    _ => None,
                })
                .collect();
            vec![ScanFilter {
                field,
                op: if *negated {
                    FilterOp::NotIn
                } else {
                    FilterOp::In
                },
                value: nodedb_types::Value::Array(values),
                clauses: Vec::new(),
            }]
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let pat = match pattern.as_ref() {
                SqlExpr::Literal(SqlValue::String(s)) => s.clone(),
                _ => return vec![match_all()],
            };
            vec![ScanFilter {
                field,
                op: if *negated {
                    FilterOp::NotLike
                } else {
                    FilterOp::Like
                },
                value: nodedb_types::Value::String(pat),
                clauses: Vec::new(),
            }]
        }
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let low_val = match low.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            let high_val = match high.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            if *negated {
                // NOT BETWEEN → lt OR gt (outside the range)
                // clauses is Vec<Vec<ScanFilter>> — each inner vec is an AND-group,
                // outer is OR. So: [[field < low], [field > high]].
                vec![ScanFilter {
                    field: String::new(),
                    op: FilterOp::Or,
                    value: nodedb_types::Value::Null,
                    clauses: vec![
                        vec![ScanFilter {
                            field: field.clone(),
                            op: FilterOp::Lt,
                            value: low_val,
                            clauses: Vec::new(),
                        }],
                        vec![ScanFilter {
                            field,
                            op: FilterOp::Gt,
                            value: high_val,
                            clauses: Vec::new(),
                        }],
                    ],
                }]
            } else {
                // BETWEEN → gte AND lte
                vec![
                    ScanFilter {
                        field: field.clone(),
                        op: FilterOp::Gte,
                        value: low_val,
                        clauses: Vec::new(),
                    },
                    ScanFilter {
                        field,
                        op: FilterOp::Lte,
                        value: high_val,
                        clauses: Vec::new(),
                    },
                ]
            }
        }
        _ => vec![match_all()],
    }
}

fn match_all() -> nodedb_query::scan_filter::ScanFilter {
    nodedb_query::scan_filter::ScanFilter {
        field: String::new(),
        op: nodedb_query::scan_filter::FilterOp::MatchAll,
        value: nodedb_types::Value::Null,
        clauses: Vec::new(),
    }
}

fn sql_value_to_nodedb_value(v: &SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Int(i) => nodedb_types::Value::Integer(*i),
        SqlValue::Float(f) => nodedb_types::Value::Float(*f),
        SqlValue::String(s) => nodedb_types::Value::String(s.clone()),
        SqlValue::Bool(b) => nodedb_types::Value::Bool(*b),
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(sql_value_to_nodedb_value).collect())
        }
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b.clone()),
    }
}

fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn sql_value_to_bytes(v: &SqlValue) -> Vec<u8> {
    match v {
        SqlValue::String(s) => s.as_bytes().to_vec(),
        SqlValue::Bytes(b) => b.clone(),
        SqlValue::Int(i) => i.to_string().as_bytes().to_vec(),
        _ => sql_value_to_string(v).into_bytes(),
    }
}

fn row_to_msgpack(row: &[(String, SqlValue)]) -> crate::Result<Vec<u8>> {
    let mut map = std::collections::HashMap::new();
    for (key, val) in row {
        map.insert(key.clone(), sql_value_to_nodedb_value(val));
    }
    let obj = nodedb_types::Value::Object(map);
    zerompk::to_msgpack_vec(&obj).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("row serialization: {e}"),
    })
}

fn assignments_to_bytes(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let mut result = Vec::new();
    for (field, expr) in assignments {
        let value = match expr {
            SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
            _ => nodedb_types::Value::String(format!("{expr:?}")),
        };
        let bytes = zerompk::to_msgpack_vec(&value).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("assignment serialization: {e}"),
        })?;
        result.push((field.clone(), bytes));
    }
    Ok(result)
}

fn serialize_ingest_rows(rows: &[Vec<(String, SqlValue)>]) -> crate::Result<Vec<u8>> {
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (key, val) in row {
                map.insert(key.clone(), sql_value_to_json(val));
            }
            serde_json::Value::Object(map)
        })
        .collect();
    sonic_rs::to_vec(&json_rows).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: format!("ingest serialization: {e}"),
    })
}

fn sql_value_to_json(v: &SqlValue) -> serde_json::Value {
    match v {
        SqlValue::Int(i) => serde_json::Value::Number((*i).into()),
        SqlValue::Float(f) => serde_json::json!(*f),
        SqlValue::String(s) => serde_json::Value::String(s.clone()),
        SqlValue::Bool(b) => serde_json::Value::Bool(*b),
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sql_value_to_json).collect())
        }
        SqlValue::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
    }
}
