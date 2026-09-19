// SPDX-License-Identifier: BUSL-1.1

//! Set operations and miscellaneous plan conversions (UNION, INTERSECT, EXCEPT, CTE, etc.).

use nodedb_sql::types::{EngineType, Projection, SortKey, SqlExpr, SqlPlan, SqlValue, WindowSpec};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::body::convert_body_to_single_plan;
use super::convert::{ConvertContext, convert_one};
use super::expr::inline_cte;
use super::value::{sql_value_to_nodedb_value, sql_value_to_string};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::Value;

pub(super) fn convert_constant_result(
    columns: &[String],
    values: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    // A constant row is one object, which cannot hold two cells under one
    // key. `SELECT nextval('s'), nextval('s')` legally repeats an output name;
    // keying both cells by the name would collapse them to the last value. Use
    // the same unique per-column keys every response encoder derives, so each
    // column keeps its own cell.
    let cell_keys = crate::control::server::response_shape::project::cell_keys(columns);
    let mut obj = std::collections::HashMap::with_capacity(columns.len());
    for ((_col, val), key) in columns.iter().zip(values.iter()).zip(cell_keys) {
        let cell = match val {
            SqlValue::Int(_)
            | SqlValue::Float(_)
            | SqlValue::Bool(_)
            | SqlValue::Null
            | SqlValue::String(_) => sql_value_to_nodedb_value(val),
            // The shaper has no typed renderer that reproduces PostgreSQL's
            // text form for these — `\x..` for bytes, `{1,2}` for arrays, the
            // ISO string for timestamps — from a typed value, so they keep
            // that text form under a `Text` column instead.
            SqlValue::Decimal(_)
            | SqlValue::Bytes(_)
            | SqlValue::Array(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Timestamptz(_) => Value::String(sql_value_to_string(val)),
        };
        obj.insert(key, cell);
    }
    let arr = Value::Array(vec![Value::Object(obj)]);
    let payload =
        nodedb_types::value_to_msgpack(&arr).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("constant result: {e}"),
        })?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: VShardId::from_collection_in_database(ctx.database_id, ""),
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: None,
            rows: payload,
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Lower `SqlPlan::Truncate` to the engine that stores the rows. The match
/// is exhaustive over `EngineType`: a document-family collection clears its
/// document store, a KV collection clears its hash index, and an engine with
/// no truncate op yet refuses with a typed error rather than clearing the
/// empty document store and reporting success.
pub(super) fn convert_truncate(
    collection: &str,
    engine: EngineType,
    restart_identity: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let plan = match engine {
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection: qualified_collection,
                restart_identity,
                // Filled in by the materialized-sum resolution pass, which recon-
                // scans the rows this TRUNCATE will remove.
                resolved_sum_targets: Vec::new(),
                // Names the column each removed row's identity is read from.
                declared_primary_key: super::dml::declared_primary_key_name(ctx, collection)?,
            })
        }
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Truncate {
            collection: qualified_collection,
            restart_identity,
        }),
        EngineType::Columnar | EngineType::Timeseries | EngineType::Spatial => {
            return Err(crate::Error::FeatureNotSupported {
                detail: format!(
                    "TRUNCATE is not yet routed for engine '{}'",
                    engine_name(engine)
                ),
            });
        }
        // `ArrayRules::plan_truncate` refuses before a plan is built.
        EngineType::Array => {
            return Err(crate::Error::Internal {
                detail: format!(
                    "TRUNCATE reached plan conversion for array collection '{collection}'"
                ),
            });
        }
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// The `WITH (engine='<name>')` spelling of an engine, for error text.
fn engine_name(engine: EngineType) -> &'static str {
    match engine {
        EngineType::DocumentSchemaless => "document_schemaless",
        EngineType::DocumentStrict => "document_strict",
        EngineType::KeyValue => "kv",
        EngineType::Columnar => "columnar",
        EngineType::Timeseries => "timeseries",
        EngineType::Spatial => "spatial",
        EngineType::Array => "array",
    }
}

pub(super) fn convert_union(
    inputs: &[SqlPlan],
    distinct: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut all_tasks = Vec::new();
    for input in inputs {
        all_tasks.extend(convert_one(input, tenant_id, ctx)?);
    }
    if distinct {
        for task in &mut all_tasks {
            task.post_set_op = PostSetOp::UnionDistinct;
        }
    }
    Ok(all_tasks)
}

pub(super) fn convert_intersect(
    left: &SqlPlan,
    right: &SqlPlan,
    all: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut left_tasks = convert_one(left, tenant_id, ctx)?;
    let mut right_tasks = convert_one(right, tenant_id, ctx)?;
    let op = if all {
        PostSetOp::IntersectAll
    } else {
        PostSetOp::Intersect
    };
    for task in &mut left_tasks {
        task.post_set_op = op;
    }
    for task in &mut right_tasks {
        task.post_set_op = op;
    }
    left_tasks.extend(right_tasks);
    Ok(left_tasks)
}

pub(super) fn convert_except(
    left: &SqlPlan,
    right: &SqlPlan,
    all: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut left_tasks = convert_one(left, tenant_id, ctx)?;
    let mut right_tasks = convert_one(right, tenant_id, ctx)?;
    let op = if all {
        PostSetOp::ExceptAll
    } else {
        PostSetOp::Except
    };
    for task in &mut left_tasks {
        task.post_set_op = op;
    }
    for task in &mut right_tasks {
        task.post_set_op = op;
    }
    left_tasks.extend(right_tasks);
    Ok(left_tasks)
}

pub(super) fn convert_insert_select(
    target: &str,
    source: &SqlPlan,
    column_map: &[(String, SqlExpr)],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let target_qualified = super::convert::db_qualified(ctx.database_id, target);
    let qualified_target = nodedb_types::QualifiedCollection::new(ctx.database_id, target);
    let target = target_qualified.as_str();

    // A declared PRIMARY KEY implies NOT NULL. A literal `SELECT NULL` into
    // the pk column is knowable at plan time, before any row is scanned.
    if let Some(declared) = super::dml::declared_primary_key_name(ctx, target)?
        && column_map.iter().any(|(field, expr)| {
            field == &declared && matches!(expr, SqlExpr::Literal(SqlValue::Null))
        })
    {
        return Err(crate::Error::RejectedConstraint {
            collection: target.to_string(),
            constraint: "not_null".to_string(),
            detail: format!("primary key '{declared}' cannot be set to NULL"),
        });
    }

    let SqlPlan::Scan {
        collection,
        filters,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        ..
    } = source
    else {
        return Err(crate::Error::PlanError {
            detail: "INSERT ... SELECT currently requires a direct source scan".into(),
        });
    };

    // Ordering, offset, distinct, and window functions each need an ordered
    // materialization the page-at-a-time copy does not provide.
    if !sort_keys.is_empty() || *offset != 0 || *distinct || !window_functions.is_empty() {
        return Err(crate::Error::PlanError {
            detail: "INSERT ... SELECT currently supports only SELECT * with optional WHERE/LIMIT"
                .into(),
        });
    }

    let filter_bytes = super::filter::serialize_filters(filters)?;
    let column_map_bytes = super::aggregate::serialize_column_map(column_map)?;
    let vshard = VShardId::from_collection_in_database(ctx.database_id, target);
    let qualified_source = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: qualified_target,
            source_collection: qualified_source,
            source_filters: filter_bytes,
            source_limit: limit.unwrap_or(10_000),
            column_map: column_map_bytes,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(super) fn convert_cte(
    definitions: &[(String, SqlPlan)],
    outer: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    // Inline CTE definitions: replace scans on CTE names with the
    // CTE's actual subquery plan.
    let mut resolved = outer.clone();
    for (name, cte_plan) in definitions {
        resolved = inline_cte(&resolved, name, cte_plan);
    }
    convert_one(&resolved, tenant_id, ctx)
}

/// Lower `SqlPlan::Subquery` — relational post-processing over a subquery body
/// whose leaf could not absorb the outer constraints — into a coordinator-
/// resolved `QueryOp::PostProcess`.
///
/// The body lowers to ONE physical relation through
/// `convert_body_to_single_plan`: a set-operation body becomes a
/// coordinator-resolved `SetOp`, and a sharded body is wrapped in
/// `Exchange{Gather}` so the sort/distinct/offset/limit tail runs exactly
/// once over the full union at resolve time.
pub(super) fn convert_subquery(
    args: nodedb_sql::SubqueryVisitArgs<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let nodedb_sql::SubqueryVisitArgs {
        input,
        filters,
        projection,
        window_functions,
        sort_keys,
        offset,
        distinct,
        limit,
    } = args;

    // The body is ONE relation, already gathered when sharded.
    let child = convert_body_to_single_plan(input, tenant_id, ctx)?;

    // A join / lateral body emits ONE merged document per output row whose
    // columns keep their table prefix (`a.attnum`), which is why the response
    // shaper looks those rows up by the qualified name. The tail's sort keys,
    // computed columns, and window specs must address the same shape — an
    // unqualified key resolves to NULL on every merged row, and a sort where
    // every key is NULL is a no-op that silently answers an ordered query in
    // the body's own order. The body may sit under the `Exchange{Gather}`
    // wrapper, so the detection looks through it.
    let merged_doc_body = is_merged_doc_body(&child);

    Ok(vec![PhysicalTask {
        tenant_id,
        // Coordinator-local: resolved to a `ProviderScan` over the gathered
        // rows (empty collection, like a constant result), dispatched once.
        vshard_id: VShardId::from_collection_in_database(ctx.database_id, ""),
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(child),
            filters: super::filter::serialize_filters(filters)?,
            projection: lower_subquery_projection(projection, window_functions)?,
            computed_columns: super::aggregate::extract_computed_columns(
                projection,
                window_functions,
                merged_doc_body,
            )?,
            window_functions: super::aggregate::serialize_window_functions(
                window_functions,
                merged_doc_body,
            )?,
            sort_keys: lower_subquery_sort_keys(sort_keys, merged_doc_body),
            limit,
            offset,
            distinct,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Whether a body plan is a join / lateral whose rows keep their table
/// prefix on every column, looking through the converter's
/// `Exchange{Gather}` wrapper.
fn is_merged_doc_body(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. })) => {
            is_merged_doc_body(child)
        }
        PhysicalPlan::Query(
            QueryOp::HashJoin { .. }
            | QueryOp::NestedLoopJoin { .. }
            | QueryOp::SortMergeJoin { .. }
            | QueryOp::LateralTopK { .. }
            | QueryOp::LateralLoop { .. },
        ) => true,
        PhysicalPlan::Query(
            QueryOp::ProviderScan { .. }
            | QueryOp::PostProcess { .. }
            | QueryOp::SetOp { .. }
            | QueryOp::Aggregate { .. }
            | QueryOp::PartialAggregate { .. }
            | QueryOp::PartialAggregateState { .. }
            | QueryOp::ShuffleJoinConsume { .. }
            | QueryOp::ShuffleAggregateConsume { .. }
            | QueryOp::FacetCounts { .. }
            | QueryOp::RecursiveScan { .. }
            | QueryOp::RecursiveValue { .. },
        )
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

/// Lower outer projection items to the row keys the relational tail matches.
///
/// A bare column keeps its unqualified name (the flattened row's column key); a
/// star selects every column, so no column pruning is applied (empty = all).
///
/// A computed item is projected under its alias: the tail evaluates the
/// expression over the materialized rows and emits the value under that name,
/// which is the same key the response shaper reads it back by. Every window
/// alias is kept too, so a window output the SELECT list does not repeat as a
/// computed entry survives the column pruning.
///
/// A Control-Plane-computed item keeps the base columns its expression reads,
/// not its alias: the Control Plane evaluates it once the tail returns, then
/// drops those columns.
fn lower_subquery_projection(
    projection: &[Projection],
    window_functions: &[WindowSpec],
) -> crate::Result<Vec<String>> {
    let mut names = Vec::with_capacity(projection.len() + window_functions.len());
    for p in projection {
        match p {
            Projection::Column(qname) => {
                names.push(qname.rsplit('.').next().unwrap_or(qname).to_string());
            }
            Projection::Star | Projection::QualifiedStar(_) => return Ok(Vec::new()),
            Projection::Computed { alias, .. } => names.push(alias.clone()),
            Projection::CpComputed { expr, .. } => {
                for column in nodedb_sql::types::plan::referenced_columns(expr) {
                    let bare = column.rsplit('.').next().unwrap_or(&column).to_string();
                    if !names.contains(&bare) {
                        names.push(bare);
                    }
                }
            }
        }
    }
    for spec in window_functions {
        if !names.contains(&spec.alias) {
            names.push(spec.alias.clone());
        }
    }
    Ok(names)
}

/// Lower outer ORDER BY keys for the row-post-processing tail.
///
/// The tail evaluates each key against the gathered rows, so a computed key
/// (`ORDER BY 100 / weight`) sorts by its value rather than having to be
/// projected in the subquery first.
///
/// `merged_doc_body` selects the column-naming convention of the rows the tail
/// will see: a join / lateral body prefixes every column with its table alias,
/// so its keys must be qualified to resolve. Column references that carry no
/// table qualifier lower identically either way.
fn lower_subquery_sort_keys(keys: &[SortKey], merged_doc_body: bool) -> Vec<SortKeySpec> {
    if merged_doc_body {
        return keys
            .iter()
            .map(|k| SortKeySpec {
                expr: super::expr::sql_expr_to_bridge_expr_qualified(&k.expr),
                ascending: k.ascending,
                nulls_first: k.nulls_first,
            })
            .collect();
    }
    super::expr::convert_sort_keys(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bare_ctx() -> ConvertContext {
        ConvertContext {
            purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    #[test]
    fn convert_truncate_routes_kv_to_kv_op_with_restart_flag() {
        let tasks = convert_truncate(
            "kvc",
            EngineType::KeyValue,
            true,
            TenantId::new(1),
            &bare_ctx(),
        )
        .expect("kv truncate converts");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Kv(KvOp::Truncate {
                collection,
                restart_identity,
            }) => {
                assert_eq!(collection.as_str(), "kvc");
                assert!(*restart_identity);
            }
            other => panic!("expected KvOp::Truncate, got {other:?}"),
        }
    }

    #[test]
    fn convert_truncate_routes_document_engines_to_document_op() {
        for engine in [EngineType::DocumentSchemaless, EngineType::DocumentStrict] {
            let tasks = convert_truncate("docs", engine, false, TenantId::new(1), &bare_ctx())
                .expect("document truncate converts");
            assert!(
                matches!(
                    &tasks[0].plan,
                    PhysicalPlan::Document(DocumentOp::Truncate { .. })
                ),
                "{engine:?} must lower to DocumentOp::Truncate, got {:?}",
                tasks[0].plan
            );
        }
    }

    #[test]
    fn convert_truncate_refuses_unrouted_engines_with_a_typed_error() {
        for (engine, name) in [
            (EngineType::Columnar, "columnar"),
            (EngineType::Timeseries, "timeseries"),
            (EngineType::Spatial, "spatial"),
        ] {
            let err = convert_truncate("c", engine, false, TenantId::new(1), &bare_ctx())
                .expect_err("unrouted engine must refuse");
            match err {
                crate::Error::FeatureNotSupported { detail } => assert_eq!(
                    detail,
                    format!("TRUNCATE is not yet routed for engine '{name}'")
                ),
                other => panic!("expected FeatureNotSupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn convert_truncate_on_array_is_an_internal_error() {
        let err = convert_truncate(
            "arr",
            EngineType::Array,
            false,
            TenantId::new(1),
            &bare_ctx(),
        )
        .expect_err("array never reaches conversion");
        assert!(matches!(err, crate::Error::Internal { .. }), "got {err:?}");
    }

    #[test]
    fn convert_insert_select_builds_document_op() {
        let source = SqlPlan::Scan {
            collection: "batch_test".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: Some(50),
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::TemporalScope::default(),
        };

        let tasks = convert_insert_select(
            "batch_copy",
            &source,
            &[],
            TenantId::new(1),
            &ConvertContext {
                purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
                retention_registry: None,
                array_catalog: None,
                credentials: None,
                wal: None,
                surrogate_assigner: None,
                cluster_enabled: false,
                bitemporal_retention_registry: None,
                max_vector_dim: 0,
                force_shuffle_join: false,
                shuffle_num_parts: 0,
                force_shuffle_agg: false,
                shuffle_agg_num_parts: 0,
                broadcast_threshold_bytes: 8 * 1024 * 1024,
                shuffle_agg_threshold: 10_000,
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: crate::types::TenantId::new(0),
            },
        )
        .expect("convert insert-select");

        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_limit,
                ..
            }) => {
                assert_eq!(target_collection.as_str(), "batch_copy");
                assert_eq!(source_collection.as_str(), "batch_test");
                assert_eq!(*source_limit, 50);
            }
            other => panic!("expected DocumentOp::InsertSelect, got {other:?}"),
        }
    }

    #[test]
    fn convert_insert_select_allows_star_projection() {
        let source = SqlPlan::Scan {
            collection: "batch_test".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: vec![Projection::Star],
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::TemporalScope::default(),
        };

        let tasks = convert_insert_select(
            "batch_copy",
            &source,
            &[],
            TenantId::new(1),
            &ConvertContext {
                purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
                retention_registry: None,
                array_catalog: None,
                credentials: None,
                wal: None,
                surrogate_assigner: None,
                cluster_enabled: false,
                bitemporal_retention_registry: None,
                max_vector_dim: 0,
                force_shuffle_join: false,
                shuffle_num_parts: 0,
                force_shuffle_agg: false,
                shuffle_agg_num_parts: 0,
                broadcast_threshold_bytes: 8 * 1024 * 1024,
                shuffle_agg_threshold: 10_000,
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: crate::types::TenantId::new(0),
            },
        )
        .expect("convert insert-select with star");

        assert_eq!(tasks.len(), 1);
    }
}
