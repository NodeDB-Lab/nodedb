// SPDX-License-Identifier: BUSL-1.1

//! Set operations and miscellaneous plan conversions (UNION, INTERSECT, EXCEPT, CTE, etc.).

use nodedb_sql::types::{Projection, SortKey, SqlExpr, SqlPlan, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::convert::{ConvertContext, convert_one};
use super::expr::inline_cte;
use super::value::sql_value_to_string;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(super) fn convert_constant_result(
    columns: &[String],
    values: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut obj = serde_json::Map::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        let json_val = match val {
            SqlValue::Null => serde_json::Value::Null,
            other => serde_json::Value::String(sql_value_to_string(other)),
        };
        obj.insert(col.clone(), json_val);
    }
    let arr = serde_json::Value::Array(vec![serde_json::Value::Object(obj)]);
    let payload = nodedb_types::json_to_msgpack(&arr).map_err(|e| crate::Error::Serialization {
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
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(super) fn convert_truncate(
    collection: &str,
    restart_identity: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::Truncate {
            collection: qualified_collection,
            restart_identity,
            // Filled in by the materialized-sum resolution pass, which recon-
            // scans the rows this TRUNCATE will remove.
            resolved_sum_targets: Vec::new(),
            // Names the column each removed row's identity is read from.
            declared_primary_key: super::dml::declared_primary_key_name(ctx, collection)?,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
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
/// The body is converted to a single physical plan and, when it is a sharded
/// source, wrapped in `Exchange{Gather}` so the sort/distinct/offset/limit tail
/// runs exactly once over the full union at resolve time.
pub(super) fn convert_subquery(
    args: nodedb_sql::SubqueryVisitArgs<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let nodedb_sql::SubqueryVisitArgs {
        input,
        filters,
        projection,
        sort_keys,
        offset,
        distinct,
        limit,
    } = args;

    // Materialize the body as a single physical plan. A subquery/derived-table
    // body is one relation; a body that lowers to multiple tasks (e.g. a set
    // operation) has no single row stream to post-process here.
    let mut body_tasks = convert_one(input, tenant_id, ctx)?;
    if body_tasks.len() != 1 {
        return Err(crate::Error::PlanError {
            detail: format!(
                "ORDER BY / OFFSET / DISTINCT over a subquery whose body lowers to {} physical \
                 tasks is not supported; the body must produce a single relation",
                body_tasks.len()
            ),
        });
    }
    let mut child = body_tasks.pop().expect("checked len == 1").plan;

    // A join / lateral body emits ONE merged document per output row whose
    // columns keep their table prefix (`a.attnum`), which is why the response
    // shaper looks those rows up by the qualified name. The tail's sort keys
    // must address the same shape — an unqualified key resolves to NULL on
    // every merged row, and a sort where every key is NULL is a no-op that
    // silently answers an ordered query in the body's own order.
    let merged_doc_body = matches!(
        child,
        PhysicalPlan::Query(
            QueryOp::HashJoin { .. }
                | QueryOp::NestedLoopJoin { .. }
                | QueryOp::SortMergeJoin { .. }
                | QueryOp::LateralTopK { .. }
                | QueryOp::LateralLoop { .. }
        )
    );

    // A sharded body must be gathered before the relational tail runs, so the
    // sort/distinct/offset/limit observe the FULL union exactly once.
    // PostProcess is itself coordinator-local (`is_sharded_source() == false`),
    // so the top-level `convert()` wrap loop will not gather the child for us.
    if child.is_sharded_source() {
        let as_aggregate = matches!(
            &child,
            PhysicalPlan::Query(QueryOp::Aggregate { .. })
                | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
        );
        child = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(child),
            mode: ExchangeMode::Gather { as_aggregate },
        }));
    }

    Ok(vec![PhysicalTask {
        tenant_id,
        // Coordinator-local: resolved to a `ProviderScan` over the gathered
        // rows (empty collection, like a constant result), dispatched once.
        vshard_id: VShardId::from_collection_in_database(ctx.database_id, ""),
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(child),
            filters: super::filter::serialize_filters(filters)?,
            projection: lower_subquery_projection(projection)?,
            sort_keys: lower_subquery_sort_keys(sort_keys, merged_doc_body),
            limit,
            offset,
            distinct,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Lower outer projection items to the row keys the relational tail matches.
///
/// A bare column keeps its unqualified name (the flattened row's column key); a
/// star selects every column, so no column pruning is applied (empty = all).
///
/// A computed item is projected under its alias: the body evaluates the
/// expression and emits the value under that name before the tail runs, which
/// is the same key the response shaper reads it back by. Erroring here instead
/// would reject `SELECT a, f(b) … ORDER BY c` outright, since the wrapper
/// carries the original SELECT list whenever the body had to be widened to keep
/// the sort column.
fn lower_subquery_projection(projection: &[Projection]) -> crate::Result<Vec<String>> {
    let mut names = Vec::with_capacity(projection.len());
    for p in projection {
        match p {
            Projection::Column(qname) => {
                names.push(qname.rsplit('.').next().unwrap_or(qname).to_string());
            }
            Projection::Star | Projection::QualifiedStar(_) => return Ok(Vec::new()),
            Projection::Computed { alias, .. } => names.push(alias.clone()),
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
    use nodedb_sql::types::EngineType;

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
                sql_catalog: None,
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
                sql_catalog: None,
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: crate::types::TenantId::new(0),
            },
        )
        .expect("convert insert-select with star");

        assert_eq!(tasks.len(), 1);
    }
}
