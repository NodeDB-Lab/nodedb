// SPDX-License-Identifier: BUSL-1.1

//! The `convert_aggregate` entry point: join-sourced, catalog (input-sourced),
//! timeseries, and standard single-collection aggregate lowering.

use nodedb_sql::types::{EngineType, Filter, SortKey, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::convert::{ConvertContext, db_qualified};
use super::super::filter::serialize_filters;
use super::super::value::extract_time_range;
use super::spec::{
    agg_expr_to_pair, agg_expr_to_spec, extract_collection_name, extract_scan_alias,
    group_by_to_specs, group_by_to_strings, inline_join_side, join_side_collection,
};
use nodedb_sql::types::AggregateExpr;

pub(in crate::control::planner::sql_plan_convert) struct ConvertAggregateParams<'a> {
    pub input: &'a SqlPlan,
    pub group_by: &'a [SqlExpr],
    pub aggregates: &'a [AggregateExpr],
    pub having: &'a [Filter],
    pub limit: usize,
    pub grouping_sets: Option<&'a [Vec<usize>]>,
    pub sort_keys: &'a [SortKey],
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in crate::control::planner::sql_plan_convert) fn convert_aggregate(
    p: ConvertAggregateParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertAggregateParams {
        input,
        group_by,
        aggregates,
        having,
        limit,
        grouping_sets,
        sort_keys,
        tenant_id,
        ctx,
    } = p;
    // Encode SortKey expressions into the wire-friendly
    // `(column_name, ascending)` shape. The post-aggregate sorter
    // only supports bare column references — non-column sort
    // expressions (e.g. `ORDER BY a + b`, `ORDER BY COUNT(*)`) need
    // a dedicated post-aggregate projection step that is not yet
    // wired through this path. Returning a typed error here surfaces
    // the limitation up to the client; silently dropping such keys
    // would yield unordered output that looks correct, which is the
    // exact silent-narrowing class the audit guidance forbids.
    let mut bridge_sort_keys: Vec<(String, bool)> = Vec::with_capacity(sort_keys.len());
    for k in sort_keys {
        match &k.expr {
            SqlExpr::Column { name, .. } => bridge_sort_keys.push((name.clone(), k.ascending)),
            other => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "ORDER BY after GROUP BY currently supports bare column references only; \
                         expression {other:?} requires a post-aggregate projection step that is \
                         not yet implemented"
                    ),
                });
            }
        }
    }
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
        let mut left_collection = join_side_collection(left, ctx.database_id);
        let mut right_collection = join_side_collection(right, ctx.database_id);
        let mut left_alias = extract_scan_alias(left);
        let mut right_alias = extract_scan_alias(right);

        let group_strs = group_by_to_strings(group_by);
        let agg_pairs = aggregates.iter().map(agg_expr_to_pair).collect();
        let left_input = inline_join_side(left, tenant_id, ctx)?;
        let right_input = inline_join_side(right, tenant_id, ctx)?;

        // RIGHT JOIN → swap sides and convert to LEFT JOIN.
        let mut on_keys = on.to_vec();
        let mut left_input = left_input;
        let mut right_input = right_input;
        let effective_join_type = if join_type.as_str() == "right" {
            std::mem::swap(&mut left_collection, &mut right_collection);
            std::mem::swap(&mut left_alias, &mut right_alias);
            std::mem::swap(&mut left_input, &mut right_input);
            on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
            "left".to_string()
        } else {
            join_type.as_str().to_string()
        };

        let vshard = VShardId::from_collection_in_database(ctx.database_id, &left_collection);

        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on: on_keys,
                join_type: effective_join_type,
                // `Option<usize>` → `usize` sentinel: `usize::MAX` = no SQL
                // LIMIT (handler bounds output by the byte budget); `Some(n)` =
                // explicit `LIMIT n`. Mirrors the plain-join converter.
                limit: join_limit.unwrap_or(usize::MAX),
                post_group_by: group_strs,
                post_aggregates: agg_pairs,
                projection: Vec::new(),
                computed_projection: Vec::new(),
                join_filters: Vec::new(),
                post_filters: Vec::new(),
                left_input,
                right_input,
                left_bitmap: None,
                right_bitmap: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // Standard aggregate on a single collection.
    let raw_collection = extract_collection_name(input);
    let (filters_ref, engine) = match input {
        SqlPlan::Scan {
            filters, engine, ..
        } => (filters.as_slice(), Some(*engine)),
        _ => (&[][..], None),
    };
    let filter_bytes = serialize_filters(filters_ref)?;
    let having_bytes = serialize_filters(having)?;

    // Catalog aggregate: the rows are coordinator-materialized, not per-shard.
    // Lower the catalog source to a `ProviderScan` carried in the aggregate's
    // `input` so the executor aggregates over those rows instead of scanning a
    // (non-existent) per-shard collection. `is_sharded_source` sees the
    // `ProviderScan` input and keeps the aggregate coordinator-local (run once,
    // never broadcast — broadcasting a catalog COUNT(*) would N×-overcount).
    // The catalog provider name is the RAW (non-db-qualified) collection name,
    // matching how plain catalog scans are lowered in `scan/core.rs`.
    if crate::control::server::pgwire::catalog::schema::catalog_collection_info(&raw_collection)
        .is_some()
    {
        // The input-sourced (catalog) aggregate executor does not expand
        // ROLLUP / CUBE / GROUPING SETS. Surface the limitation as a typed
        // error rather than silently returning only the base grouping (which
        // would be the silent-narrowing class the audit guidance forbids).
        if grouping_sets.is_some_and(|sets| !sets.is_empty()) {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "ROLLUP / CUBE / GROUPING SETS over catalog table '{raw_collection}' is not \
                     supported"
                ),
            });
        }
        let group_specs = group_by_to_specs(group_by);
        let agg_specs: Vec<AggregateSpec> = aggregates.iter().map(agg_expr_to_spec).collect();
        let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(raw_collection.clone()),
            rows: Vec::new(),
            // WHERE predicates on the catalog are applied by the ProviderScan
            // before the rows reach the aggregate.
            filters: filter_bytes.clone(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        return Ok(vec![PhysicalTask {
            tenant_id,
            // Coordinator-local: empty collection keeps the task on the
            // coordinator vshard (catalog rows are not per-shard).
            vshard_id: VShardId::from_collection_in_database(ctx.database_id, ""),
            database_id: ctx.database_id,
            plan: PhysicalPlan::Query(QueryOp::Aggregate {
                collection: raw_collection,
                input: Some(Box::new(provider_scan)),
                group_by: group_specs,
                aggregates: agg_specs,
                // Filters live on the ProviderScan input; the aggregate node
                // applies none of its own over the already-filtered rows.
                filters: Vec::new(),
                having: having_bytes,
                limit,
                sub_group_by: Vec::new(),
                sub_aggregates: Vec::new(),
                // Guarded above: catalog aggregates never carry grouping sets.
                grouping_sets: Vec::new(),
                sort_keys: bridge_sort_keys,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    let collection = db_qualified(ctx.database_id, &raw_collection);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, &collection);

    let group_strs = group_by_to_strings(group_by);
    let agg_specs: Vec<AggregateSpec> = aggregates.iter().map(agg_expr_to_spec).collect();
    let agg_pairs: Vec<(String, String)> = aggregates.iter().map(agg_expr_to_pair).collect();

    // Timeseries aggregates: route through TimeseriesOp::Scan with time_range + aggregates.
    if engine == Some(EngineType::Timeseries) {
        let time_range = extract_time_range(filters_ref);
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection,
                time_range,
                projection: Vec::new(),
                limit,
                filters: filter_bytes,
                bucket_interval_ms: 0,
                group_by: group_strs,
                aggregates: agg_pairs,
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // Convert grouping_sets from usize indices to u32 for wire transport.
    let bridge_grouping_sets: Vec<Vec<u32>> = grouping_sets
        .unwrap_or(&[])
        .iter()
        .map(|set| set.iter().map(|&i| i as u32).collect())
        .collect();

    // Distributed shuffle-aggregate eligibility. A whole-aggregate shuffle is
    // only *correct* when there is NO global ORDER BY and NO explicit LIMIT: each
    // part finalizes its disjoint groups independently and the coordinator
    // concatenates them, so a global sort or take-N (which spans groups across
    // parts) would be applied per-part and yield a wrong answer. Such queries
    // MUST keep the default plan — `convert.rs` wraps the bare Aggregate in
    // `Gather{as_aggregate}`, where the single owning node applies the global
    // sort/limit correctly. HAVING is per-group (disjoint across parts) so it is
    // allowed.
    //
    // The no-LIMIT sentinel for an aggregate is `10000` (the planner default
    // applied when no `LIMIT` clause is present; an explicit `LIMIT n` overwrites
    // it — see `nodedb-sql` `apply_limit`), so `limit == 10000` means "no explicit
    // LIMIT". `grouping_sets` (ROLLUP / CUBE) cannot be shuffled either: the
    // partial-state producer keys on the base GROUP BY columns only. Only honored
    // in cluster mode (single-node has no peers to shuffle across).
    //
    // These structural gates are correctness gates and are checked FIRST. The
    // shuffle is taken when EITHER the operator forces it
    // (`nodedb.force_shuffle_agg`) OR the ANALYZE-driven cost model picks it from
    // the GROUP BY's estimated group cardinality. The cost model is consulted
    // only after the structural gates pass, so it can never override correctness;
    // force still wins (and short-circuits the stats lookup).
    let shuffle_agg_eligible = ctx.cluster_enabled
        && !group_strs.is_empty()
        && bridge_sort_keys.is_empty()
        && limit == 10000
        && bridge_grouping_sets.is_empty()
        && (ctx.force_shuffle_agg
            || super::cost::cost_model_picks_aggregate_shuffle(ctx, &collection, &group_strs));

    // Clone the group keys for the Exchange.keys field only when the shuffle
    // path is actually taken; in the non-eligible branch no copy is needed.
    let exchange_keys = if shuffle_agg_eligible {
        group_strs.clone()
    } else {
        Vec::new()
    };
    // The Data-Plane aggregate carries group-key specs; the shuffle exchange
    // keys and the cost model above still key on the plain column-name strings.
    let group_specs = group_by_to_specs(group_by);
    let aggregate = PhysicalPlan::Query(QueryOp::Aggregate {
        collection,
        input: None,
        group_by: group_specs,
        aggregates: agg_specs,
        filters: filter_bytes,
        having: having_bytes,
        limit,
        sub_group_by: Vec::new(),
        sub_aggregates: Vec::new(),
        grouping_sets: bridge_grouping_sets,
        sort_keys: bridge_sort_keys,
    });

    let plan = if shuffle_agg_eligible {
        // `num_parts == 0` is the "unset" sentinel: the operator left
        // `nodedb.shuffle_agg_num_parts` unset, so the coordinator resolver
        // defaults it to the cluster data-node count (the convert layer has no
        // view of the routing table). A non-zero value is used verbatim.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(aggregate),
            mode: ExchangeMode::ShuffleAggregate {
                keys: exchange_keys,
                num_parts: ctx.shuffle_agg_num_parts,
            },
        }))
    } else {
        aggregate
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

#[cfg(test)]
mod tests {
    use super::super::projection::{extract_computed_columns, extract_projection_names};
    use super::super::spec::agg_expr_to_spec;
    use nodedb_sql::types::{AggregateExpr, BinaryOp, Projection, SqlExpr, SqlValue, WindowSpec};

    #[test]
    fn aggregate_spec_preserves_alias_and_case_expression() {
        let agg = AggregateExpr {
            function: "sum".into(),
            args: vec![SqlExpr::Case {
                operand: None,
                when_then: vec![(
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column {
                            table: None,
                            name: "category".into(),
                        }),
                        op: BinaryOp::Eq,
                        right: Box::new(SqlExpr::Literal(SqlValue::String("tools".into()))),
                    },
                    SqlExpr::Literal(SqlValue::Int(1)),
                )],
                else_expr: Some(Box::new(SqlExpr::Literal(SqlValue::Int(0)))),
            }],
            alias: "tools_count".into(),
            distinct: false,
            grouping_col_index: None,
        };

        let spec = agg_expr_to_spec(&agg);

        assert_eq!(spec.function, "sum");
        assert_eq!(spec.alias, "sum(*)");
        assert_eq!(spec.user_alias.as_deref(), Some("tools_count"));
        assert_eq!(spec.field, "*");
        assert!(matches!(
            spec.expr,
            Some(crate::bridge::expr_eval::SqlExpr::Case { .. })
        ));
    }

    #[test]
    fn window_aliases_stay_in_projection_and_out_of_computed_columns() {
        let projection = vec![
            Projection::Column("name".into()),
            Projection::Computed {
                expr: SqlExpr::Function {
                    name: "row_number".into(),
                    args: Vec::new(),
                    distinct: false,
                },
                alias: "rn".into(),
            },
            Projection::Computed {
                expr: SqlExpr::Column {
                    table: None,
                    name: "age".into(),
                },
                alias: "age_copy".into(),
            },
        ];
        let window_functions = vec![WindowSpec {
            function: "row_number".into(),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            alias: "rn".into(),
            frame: Default::default(),
        }];

        assert_eq!(
            extract_projection_names(&projection, &window_functions),
            vec!["name".to_string(), "rn".to_string()]
        );

        let computed_bytes =
            extract_computed_columns(&projection, &window_functions).expect("serialize computed");
        let computed: Vec<crate::bridge::expr_eval::ComputedColumn> =
            zerompk::from_msgpack(&computed_bytes).expect("deserialize computed");

        assert_eq!(computed.len(), 1);
        assert_eq!(computed[0].alias, "age_copy");
    }
}
