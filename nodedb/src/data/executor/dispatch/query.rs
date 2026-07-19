// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for QueryOp variants (aggregates, joins, recursive scans, facets).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::{GroupKeySpec, QueryOp};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::join::{
    HashJoinParams, JoinParams, NestedLoopJoinParams, ShuffleJoinInputs, SortMergeJoinParams,
    lateral::{LateralLoopParams, LateralTopKParams},
};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_query(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &QueryOp,
    ) -> Response {
        match op {
            QueryOp::Aggregate {
                collection,
                input,
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
                grouping_sets,
                sort_keys,
            } => self.execute_aggregate(
                crate::data::executor::handlers::aggregate::exec::AggregateExecInputs {
                    task,
                    tid,
                    collection,
                    input: input.as_deref(),
                    group_by,
                    aggregates,
                    filters,
                    having,
                    limit: *limit,
                    sub_group_by,
                    sub_aggregates,
                    grouping_sets,
                    sort_keys,
                },
            ),

            QueryOp::Exchange(_) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: "Exchange must be resolved by the coordinator before dispatch"
                        .to_string(),
                },
            ),

            QueryOp::ProviderScan {
                rows,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct,
                ..
            } => self.execute_provider_scan(
                task,
                crate::data::executor::handlers::provider_scan::ProviderScanParams {
                    rows_bytes: rows,
                    filters_bytes: filters,
                    projection,
                    sort_keys,
                    limit: *limit,
                    offset: *offset,
                    distinct: *distinct,
                },
            ),

            QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                projection,
                computed_projection,
                join_filters,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                ..
            } => self.execute_hash_join(HashJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    computed_projection_bytes: computed_projection,
                    join_filter_bytes: join_filters,
                    post_filter_bytes: post_filters,
                },
                tid,
                left_collection,
                right_collection,
                left_alias: left_alias.as_deref(),
                right_alias: right_alias.as_deref(),
                left_input: left_input.as_deref(),
                right_input: right_input.as_deref(),
                left_bitmap: left_bitmap.as_deref(),
                right_bitmap: right_bitmap.as_deref(),
            }),

            QueryOp::ShuffleJoinConsume {
                build_path,
                probe_path,
                on,
                join_type,
                limit,
                probe_qualifier,
                index_qualifier,
            } => {
                // Reconstruct the borrowed `JoinParams` from the owned plan
                // fields. `projection` / `post_filter_bytes` are empty: a
                // shuffle-join consumer runs the bare grace join over the two
                // staged sides and emits the joined rows; any post-projection /
                // post-filter is applied by the coordinator on the gathered
                // union, not per-part. `on` / `join_type` borrow straight from
                // the plan.
                let join = JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection: &[],
                    computed_projection_bytes: &[],
                    join_filter_bytes: &[],
                    post_filter_bytes: &[],
                };
                let inputs = ShuffleJoinInputs {
                    build_path: std::path::PathBuf::from(build_path),
                    probe_path: std::path::PathBuf::from(probe_path),
                    probe_qualifier: probe_qualifier.clone(),
                    index_qualifier: index_qualifier.clone(),
                };
                // Same memory budget source the local hash-join path uses
                // (`execute_hash_join`): the per-query scan-result byte budget.
                let budget = self.query_tuning.max_scan_result_bytes;
                self.execute_shuffle_join(&join, inputs, budget)
            }

            QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type,
                limit,
            } => self.execute_nested_loop_join(NestedLoopJoinParams {
                task,
                tid,
                left_collection,
                right_collection,
                condition,
                join_type,
                limit: *limit,
            }),

            QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
                pre_sorted,
            } => self.execute_sort_merge_join(SortMergeJoinParams {
                task,
                tid,
                left_collection,
                right_collection,
                on,
                join_type,
                limit: *limit,
                pre_sorted: *pre_sorted,
            }),

            QueryOp::RecursiveScan {
                collection,
                base_filters,
                recursive_filters,
                join_link,
                max_iterations,
                distinct,
                limit,
            } => self.execute_recursive_scan(
                task,
                crate::data::executor::handlers::recursive::RecursiveScanParams {
                    tid,
                    collection,
                    base_filters,
                    recursive_filters,
                    join_link: join_link.as_ref(),
                    max_iterations: *max_iterations,
                    distinct: *distinct,
                    limit: *limit,
                },
            ),

            QueryOp::RecursiveValue {
                cte_name,
                columns,
                init_exprs,
                step_exprs,
                condition,
                max_depth,
                distinct,
            } => self.execute_recursive_value(
                task,
                crate::data::executor::handlers::recursive_value::RecursiveValueParams {
                    cte_name,
                    columns,
                    init_exprs,
                    step_exprs,
                    condition: condition.as_deref(),
                    max_depth: *max_depth,
                    distinct: *distinct,
                },
            ),

            QueryOp::FacetCounts {
                collection,
                filters,
                fields,
                limit_per_facet,
            } => {
                self.execute_facet_counts(task, tid, collection, filters, fields, *limit_per_facet)
            }

            QueryOp::PartialAggregate {
                collection,
                group_by,
                aggregates,
                filters,
            } => {
                // This variant still carries plain column-name keys; lift them
                // to bare-column specs for the shared executor entry point.
                let group_specs: Vec<GroupKeySpec> = group_by
                    .iter()
                    .map(|s| GroupKeySpec::column(s.as_str()))
                    .collect();
                self.execute_aggregate(
                    crate::data::executor::handlers::aggregate::exec::AggregateExecInputs {
                        task,
                        tid,
                        collection,
                        input: None,
                        group_by: &group_specs,
                        aggregates,
                        filters,
                        having: &[],
                        limit: usize::MAX,
                        sub_group_by: &[],
                        sub_aggregates: &[],
                        grouping_sets: &[],
                        sort_keys: &[],
                    },
                )
            }

            QueryOp::PartialAggregateState {
                collection,
                input,
                group_by,
                aggregates,
                filters,
            } => self.execute_partial_aggregate_state(
                crate::data::executor::handlers::aggregate::state_emit::PartialAggregateStateParams {
                    task,
                    tid,
                    collection,
                    input: input.as_deref(),
                    group_by,
                    aggregates,
                    filters,
                },
            ),

            QueryOp::ShuffleAggregateConsume {
                state_path,
                group_by,
                aggregates,
                having,
                limit,
                sort_keys,
            } => {
                // This variant still carries plain column-name keys; lift them
                // to bare-column specs for the consumer's merge + finalize.
                let group_specs: Vec<GroupKeySpec> = group_by
                    .iter()
                    .map(|s| GroupKeySpec::column(s.as_str()))
                    .collect();
                self.execute_shuffle_aggregate(
                    crate::data::executor::handlers::aggregate::shuffle_merge::ShuffleAggregateParams {
                        task,
                        state_path,
                        group_by: &group_specs,
                        aggregates,
                        having,
                        limit: *limit,
                        sort_keys,
                    },
                )
            }

            QueryOp::LateralTopK {
                outer_plan,
                outer_alias,
                inner_collection,
                inner_filters,
                inner_order_by,
                inner_limit,
                correlation_keys,
                lateral_alias,
                projection,
                left_join,
            } => self.execute_lateral_top_k(LateralTopKParams {
                task,
                tid,
                outer_plan,
                outer_alias,
                inner_collection,
                inner_filters,
                inner_order_by,
                inner_limit: *inner_limit,
                correlation_keys,
                lateral_alias,
                projection,
                left_join: *left_join,
            }),

            QueryOp::LateralLoop {
                outer_plan,
                outer_alias,
                inner_collection,
                inner_filters,
                correlation_predicates,
                lateral_alias,
                projection,
                left_join,
                outer_row_cap,
            } => self.execute_lateral_loop(LateralLoopParams {
                task,
                tid,
                outer_plan,
                outer_alias,
                inner_collection,
                inner_filters,
                correlation_predicates,
                lateral_alias,
                projection,
                left_join: *left_join,
                outer_row_cap: *outer_row_cap,
            }),
        }
    }
}
