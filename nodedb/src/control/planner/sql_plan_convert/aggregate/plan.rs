// SPDX-License-Identifier: BUSL-1.1

//! The `convert_aggregate` entry point. Three lowering shapes:
//!
//! - input-sourced: a join, a derived table, a constant result, a union, a
//!   raw timeseries scan, or a scan under a `FOR SYSTEM_TIME` /
//!   `FOR VALID_TIME` scope. The input lowers to one relation, the
//!   coordinator materializes it, and the aggregate runs once over its rows
//!   (`input_sourced.rs`).
//! - catalog: `ProviderScan` input, coordinator-local
//! - single collection: per-shard `Aggregate`

use nodedb_sql::types::{Filter, SortKey, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::convert::{ConvertContext, db_qualified};
use super::super::expr::convert_sort_keys;
use super::super::filter::serialize_filters;
use super::spec::{
    InputFieldNames, InputSourcedTaskParams, agg_expr_to_spec, build_input_sourced_aggregate_task,
    group_by_to_specs, group_by_to_strings,
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
    /// SELECT-list output alias of each GROUP BY key, parallel to `group_by`.
    pub group_by_aliases: &'a [Option<String>],
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
        group_by_aliases,
        tenant_id,
        ctx,
    } = p;
    // Post-aggregate sort keys are expressions over the finalized group row.
    // The planner has already bound any aggregate call in them to the output
    // column it lands in, so the executor evaluates them like any other row
    // expression.
    let bridge_sort_keys: Vec<SortKeySpec> = convert_sort_keys(sort_keys);

    // Only a plain current-state scan of one collection aggregates per
    // shard. Every other input — a join, a derived table, a constant result,
    // a union, a raw timeseries scan, a temporal scan — lowers to one
    // relation the coordinator materializes, so the aggregate runs once over
    // exactly the rows that input produces.
    let (raw_collection, engine, filters_ref) = match input {
        SqlPlan::Scan {
            collection,
            engine,
            filters,
            temporal,
            ..
        } if !temporal.is_temporal() => (collection.clone(), engine, filters.as_slice()),
        _ => {
            return super::input_sourced::convert_input_sourced_aggregate(
                super::input_sourced::InputSourcedAggregateParams {
                    input,
                    group_by,
                    aggregates,
                    having,
                    limit,
                    grouping_sets,
                    sort_keys: bridge_sort_keys,
                    group_by_aliases,
                    tenant_id,
                    ctx,
                },
            );
        }
    };
    let filter_bytes = serialize_filters(filters_ref)?;
    let having_bytes = serialize_filters(having)?;
    let bridge_grouping_sets: Vec<Vec<u32>> = grouping_sets
        .unwrap_or(&[])
        .iter()
        .map(|set| set.iter().map(|&i| i as u32).collect())
        .collect();

    // Catalog or continuous-aggregate source: the rows are
    // coordinator-materialized, not per-shard. Lower the source to a
    // `ProviderScan` carried in the aggregate's `input` so the executor
    // aggregates over those rows instead of scanning a per-shard collection.
    // `is_sharded_source` sees the `ProviderScan` input and keeps the
    // aggregate coordinator-local (run once, never broadcast — broadcasting a
    // catalog COUNT(*) would N×-overcount). The provider name is the RAW
    // (non-db-qualified) collection name, matching how provider scans are
    // lowered in `scan/core.rs`.
    if super::super::scan::is_provider_relation(&raw_collection, engine) {
        let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(raw_collection.clone()),
            rows: Vec::new(),
            sources: Vec::new(),
            // WHERE predicates on the catalog are applied by the ProviderScan
            // before the rows reach the aggregate.
            filters: filter_bytes,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            // A temporal scan lowers through `convert_input_sourced_aggregate`
            // above. A transaction block pins this cut to its snapshot.
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        return Ok(vec![build_input_sourced_aggregate_task(
            InputSourcedTaskParams {
                tenant_id,
                ctx,
                raw_collection,
                child: provider_scan,
                group_by,
                aggregates,
                having_bytes,
                limit,
                grouping_sets: bridge_grouping_sets,
                sort_keys: bridge_sort_keys,
                group_by_aliases,
                names: InputFieldNames::Bare,
            },
        )?]);
    }

    let collection = db_qualified(ctx.database_id, &raw_collection);
    let qualified_collection =
        nodedb_types::QualifiedCollection::new(ctx.database_id, &raw_collection);
    let vshard = ctx.collection_key(&raw_collection).vshard();

    let group_strs = group_by_to_strings(group_by);
    let agg_specs = aggregates
        .iter()
        .map(agg_expr_to_spec)
        .collect::<crate::Result<Vec<AggregateSpec>>>()?;

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
    // The no-LIMIT value for an aggregate is `AGGREGATE_NO_LIMIT` (every
    // group; an explicit `LIMIT n` overwrites it — see `nodedb-sql`
    // `apply_limit`), so `limit == AGGREGATE_NO_LIMIT` means "no explicit
    // LIMIT". `grouping_sets` (ROLLUP / CUBE) cannot be shuffled either: the
    // partial-state producer keys on the base GROUP BY columns only.
    //
    // These structural gates are correctness gates and are checked FIRST. The
    // shuffle is taken when EITHER the operator forces it
    // (`nodedb.force_shuffle_agg`) OR the ANALYZE-driven cost model picks it from
    // the GROUP BY's estimated group cardinality. The cost model is consulted
    // only after the structural gates pass, so it can never override correctness;
    // force still wins (and short-circuits the stats lookup).
    let shuffle_agg_eligible = !group_strs.is_empty()
        && bridge_sort_keys.is_empty()
        && limit == nodedb_sql::planner::aggregate::AGGREGATE_NO_LIMIT
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
    let group_specs = group_by_to_specs(group_by, InputFieldNames::Bare);
    let aggregate = PhysicalPlan::Query(QueryOp::Aggregate {
        collection: qualified_collection,
        input: None,
        group_by: group_specs,
        aggregates: agg_specs,
        filters: filter_bytes,
        system_time: nodedb_types::SystemTimeScope::Current,
        having: having_bytes,
        limit,
        sub_group_by: Vec::new(),
        sub_aggregates: Vec::new(),
        grouping_sets: bridge_grouping_sets,
        sort_keys: bridge_sort_keys,
        group_key_aliases: group_by_aliases.to_vec(),
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
            filter_text: None,
            arg_text: None,
        };

        let spec = agg_expr_to_spec(&agg).expect("sum is a generic aggregate");

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

        let computed_bytes = extract_computed_columns(&projection, &window_functions, false)
            .expect("serialize computed");
        let computed: Vec<crate::bridge::expr_eval::ComputedColumn> =
            zerompk::from_msgpack(&computed_bytes).expect("deserialize computed");

        assert_eq!(computed.len(), 1);
        assert_eq!(computed[0].alias, "age_copy");
    }

    fn plain_agg(function: &str) -> AggregateExpr {
        AggregateExpr {
            function: function.into(),
            args: vec![SqlExpr::Column {
                table: None,
                name: "v".into(),
            }],
            alias: format!("{function}(v)"),
            distinct: false,
            grouping_col_index: None,
            filter_text: None,
            arg_text: None,
        }
    }

    /// A function with no generic accumulator is refused, never counted.
    #[test]
    fn function_without_generic_accumulator_is_refused() {
        for function in ["ts_percentile", "ts_correlate", "approx_count"] {
            assert!(
                agg_expr_to_spec(&plain_agg(function)).is_err(),
                "{function} must be refused"
            );
        }
        assert!(agg_expr_to_spec(&plain_agg("ts_stddev")).is_ok());
    }

    /// GROUPING() the planner did not resolve to a key is refused.
    #[test]
    fn unresolved_grouping_is_refused() {
        let mut agg = plain_agg("grouping");
        assert!(agg_expr_to_spec(&agg).is_err());
        agg.grouping_col_index = Some(1);
        let spec = agg_expr_to_spec(&agg).expect("resolved grouping");
        assert_eq!(spec.field, "1");
    }

    /// A parameterized aggregate carries its constant to the Data Plane in
    /// the typed `param`, beside the plain field: `approx_percentile(v, 0.9)`
    /// once shipped only `v`, and the Data Plane answered the median.
    #[test]
    fn a_parameterized_aggregate_carries_its_constant() {
        use nodedb_physical::physical_plan::AggregateParam;
        let column = SqlExpr::Column {
            table: None,
            name: "v".into(),
        };
        let with_param = |function: &str, param: SqlExpr| {
            let mut agg = plain_agg(function);
            agg.args = vec![column.clone(), param];
            agg_expr_to_spec(&agg)
        };

        let spec = with_param("approx_percentile", SqlExpr::Literal(SqlValue::Float(0.9)))
            .expect("percentile");
        assert_eq!(spec.field, "v");
        assert_eq!(
            spec.param,
            Some(AggregateParam::Percentile { fraction: 0.9 })
        );

        let spec = with_param("approx_topk", SqlExpr::Literal(SqlValue::Int(3))).expect("topk");
        assert_eq!(spec.field, "v");
        assert_eq!(spec.param, Some(AggregateParam::TopK { k: 3 }));

        assert_eq!(
            agg_expr_to_spec(&plain_agg("sum")).expect("sum").param,
            None
        );

        for (function, param) in [
            ("approx_topk", column.clone()),
            ("approx_topk", SqlExpr::Literal(SqlValue::Int(0))),
            ("approx_topk", SqlExpr::Literal(SqlValue::Float(2.5))),
            ("approx_percentile", SqlExpr::Literal(SqlValue::Float(1.5))),
        ] {
            assert!(
                with_param(function, param.clone()).is_err(),
                "{function} with {param:?} must be refused"
            );
        }
    }
}
