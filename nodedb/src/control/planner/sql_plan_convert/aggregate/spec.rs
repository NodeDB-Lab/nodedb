// SPDX-License-Identifier: BUSL-1.1

//! Aggregate-spec construction, collection/alias extraction, and join-side
//! embedding helpers shared by `convert_aggregate` and the scan converters.

use nodedb_sql::types::{AggregateExpr, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::convert::{ConvertContext, convert_one, db_qualified};
use super::super::expr::sql_expr_to_bridge_expr;

/// Convert a join-side sub-plan to an optional embedded `PhysicalPlan`.
///
/// Plain `Scan` and `PointGet` sides are handled via `left_collection` /
/// `right_collection` on the parent `HashJoin`, so they return `None` here.
///
/// For any other sub-plan (a nested join, an aggregate, etc.) we convert it
/// to a physical plan. If the result is a sharded source it is wrapped in
/// `Exchange{Broadcast}` so the coordinator gathers it to the coordinator
/// and embeds it as the inline build-side input.  Constant / catalog
/// `ProviderScan` inputs are embedded directly without Exchange.
pub(in crate::control::planner::sql_plan_convert) fn inline_join_side(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    // Catalog scan sides are coordinator-materialized: lower them to a
    // `ProviderScan` embedded as the join input. This keeps a catalog⋈catalog
    // join coordinator-local (see `is_sharded_source`) so it runs once instead
    // of being broadcast to every core (which would duplicate every row N
    // times). The converter (`convert_scan`) already emits the right
    // `ProviderScan{provider: Some(name)}` shape for catalog scans, and that
    // node is not a sharded source, so it is embedded directly without
    // Exchange.
    if matches!(plan, SqlPlan::PointGet { .. }) {
        return Ok(None);
    }
    // Plain user-collection scan side: handled via the parent HashJoin's
    // `left_collection` / `right_collection` by name. Catalog scans fall
    // through to conversion so we embed their ProviderScan.
    if let SqlPlan::Scan { collection, .. } = plan
        && !scan_is_catalog(collection)
    {
        return Ok(None);
    }

    let mut tasks = convert_one(plan, tenant_id, ctx)?;
    if tasks.len() > 1 {
        return Err(crate::Error::PlanError {
            detail: format!(
                "inline join side must produce exactly 1 task, got {}",
                tasks.len()
            ),
        });
    }
    Ok(tasks.pop().map(|t| {
        let p = t.plan;
        if p.is_sharded_source() {
            Box::new(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                child: Box::new(p),
                mode: ExchangeMode::Broadcast,
            })))
        } else {
            Box::new(p)
        }
    }))
}

/// Whether a (raw, non-db-qualified) collection name resolves to a system
/// catalog table (pg_class, information_schema.*, _system.*, etc.). Catalog
/// tables are coordinator-materialized, not per-shard.
fn scan_is_catalog(collection: &str) -> bool {
    crate::control::server::pgwire::catalog::schema::catalog_collection_info(collection).is_some()
}

/// Resolve the `left_collection` / `right_collection` name for a `HashJoin`
/// side. Catalog sides return an EMPTY string: their rows come from the
/// embedded `ProviderScan` input (see `inline_join_side`), and an empty
/// collection name is what `is_sharded_source` and the executor use to
/// recognize a non-per-shard side. Plain user-collection sides return the
/// db-qualified name as before.
pub(in crate::control::planner::sql_plan_convert) fn join_side_collection(
    plan: &SqlPlan,
    database_id: crate::types::DatabaseId,
) -> String {
    // An input-sourced join side carries no routing collection; its rows come
    // from `left_input` / `right_input`.
    match extract_collection_name(plan) {
        Some(raw) if !scan_is_catalog(&raw) => db_qualified(database_id, &raw),
        Some(_) | None => String::new(),
    }
}

/// Routing collection for a scan-shaped input. `None` for an input-sourced
/// (materialized) body.
///
/// A `Join` yields its left side's collection as the routing hint for a join
/// over scans. An `Aggregate` input is a materialized relation, not a scan.
pub(in crate::control::planner::sql_plan_convert) fn extract_collection_name(
    plan: &SqlPlan,
) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. } => Some(collection.clone()),
        SqlPlan::PointGet { collection, .. } => Some(collection.clone()),
        SqlPlan::Join { left, .. } => extract_collection_name(left),
        _ => None,
    }
}

/// Scan alias for a scan-shaped input. `None` for an input-sourced body.
pub(in crate::control::planner::sql_plan_convert) fn extract_scan_alias(
    plan: &SqlPlan,
) -> Option<String> {
    match plan {
        SqlPlan::Scan { alias, .. } => alias.clone(),
        SqlPlan::PointGet { alias, .. } => alias.clone(),
        SqlPlan::Join { left, .. } => extract_scan_alias(left),
        _ => None,
    }
}

/// Convert an `AggregateExpr` to the Data Plane aggregate spec.
pub(super) fn agg_expr_to_spec(a: &AggregateExpr) -> AggregateSpec {
    // GROUPING(col) pseudo-aggregate: encode the canonical key index in the
    // `field` so the Data Plane executor can read the grouping-set bitmask.
    if a.function == "grouping" {
        let idx = a.grouping_col_index.unwrap_or(0);
        let field = idx.to_string();
        let canonical = format!("grouping({field})");
        let user_alias = if a.alias.eq_ignore_ascii_case(&canonical) {
            None
        } else {
            Some(a.alias.clone())
        };
        return AggregateSpec {
            function: "grouping".into(),
            alias: canonical,
            user_alias,
            field,
            expr: None,
        };
    }

    let (field, expr) = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => (name.clone(), None),
            SqlExpr::Wildcard => ("*".into(), None),
            _ => ("*".into(), Some(sql_expr_to_bridge_expr(arg))),
        })
        .unwrap_or_else(|| ("*".into(), None));

    let function = nodedb_sql::planner::agg_naming::aggregate_function_name(a);
    let canonical = nodedb_query::agg_key::canonical_agg_key(&function, &field);
    let user_alias = if a.alias.eq_ignore_ascii_case(&canonical) {
        None
    } else {
        Some(a.alias.clone())
    };

    AggregateSpec {
        function,
        alias: canonical,
        user_alias,
        field,
        expr,
    }
}

/// Convert an `AggregateExpr` to the legacy `(op, field)` pair used by
/// non-QueryOp aggregate paths (timeseries, post-join aggregation).
pub(in crate::control::planner::sql_plan_convert) fn agg_expr_to_pair(
    a: &AggregateExpr,
) -> (String, String) {
    let field = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => name.clone(),
            SqlExpr::Wildcard => "*".into(),
            _ => format!("{arg:?}"),
        })
        .unwrap_or_else(|| "*".into());
    (
        nodedb_sql::planner::agg_naming::aggregate_function_name(a),
        field,
    )
}

pub(super) fn group_by_to_strings(exprs: &[SqlExpr]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|e| match e {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Build the coordinator-local `Aggregate{input: Some(child)}` task shared by
/// the catalog and derived-table-body lowerings: an aggregate that runs once
/// over an already-materialized `child` plan instead of scanning a per-shard
/// collection. `raw_collection` is the RAW catalog source name for a catalog
/// body, or `String::new()` for a body with no routing collection (derived
/// table, constant result, union) — either way the task's vshard is the
/// coordinator's empty-collection vshard, so it is never broadcast.
/// Inputs to [`build_input_sourced_aggregate_task`].
pub(in crate::control::planner::sql_plan_convert) struct InputSourcedTaskParams<'a> {
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
    pub raw_collection: String,
    pub child: PhysicalPlan,
    pub group_by: &'a [SqlExpr],
    pub aggregates: &'a [AggregateExpr],
    pub having_bytes: Vec<u8>,
    pub limit: usize,
    pub sort_keys: Vec<SortKeySpec>,
}

pub(in crate::control::planner::sql_plan_convert) fn build_input_sourced_aggregate_task(
    p: InputSourcedTaskParams<'_>,
) -> PhysicalTask {
    let InputSourcedTaskParams {
        tenant_id,
        ctx,
        raw_collection,
        child,
        group_by,
        aggregates,
        having_bytes,
        limit,
        sort_keys,
    } = p;
    let group_specs = group_by_to_specs(group_by);
    let agg_specs: Vec<AggregateSpec> = aggregates.iter().map(agg_expr_to_spec).collect();
    PhysicalTask {
        tenant_id,
        // Coordinator-local: empty collection keeps the task on the
        // coordinator vshard (the child's rows are not per-shard).
        vshard_id: VShardId::from_collection_in_database(ctx.database_id, ""),
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::from_stored(raw_collection),
            input: Some(Box::new(child)),
            group_by: group_specs,
            aggregates: agg_specs,
            // The child carries its own WHERE / ProviderScan filters; the
            // aggregate node applies none of its own.
            filters: Vec::new(),
            having: having_bytes,
            limit,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            // Guarded by the caller: input-sourced aggregates never carry
            // grouping sets.
            grouping_sets: Vec::new(),
            sort_keys,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

/// Lower GROUP BY expressions to Data-Plane group-key specs.
///
/// A bare `Column` key extracts from, and is emitted under, its own column
/// name, so its output stays byte-identical to the string-keyed form. A
/// computed-expression key carries the bridge-evaluated expression (`field:
/// None, expr: Some(..)`) and is emitted under the shared index-based
/// [`group_key_output_name`] name (`group_{index}`) — a purely internal
/// executor↔shaper handshake, matching the response shaper's `lookup_key`. The
/// SELECT alias never enters here; it reaches only the shaper's `display_name`.
pub(super) fn group_by_to_specs(exprs: &[SqlExpr]) -> Vec<GroupKeySpec> {
    use super::super::group_key_name::group_key_output_name;
    exprs
        .iter()
        .enumerate()
        .map(|(index, e)| match e {
            SqlExpr::Column { name, .. } => GroupKeySpec::column(name.clone()),
            _ => GroupKeySpec {
                output_name: group_key_output_name(e, index),
                field: None,
                expr: Some(sql_expr_to_bridge_expr(e)),
            },
        })
        .collect()
}
