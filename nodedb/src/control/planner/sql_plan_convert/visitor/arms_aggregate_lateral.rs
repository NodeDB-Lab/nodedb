// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for aggregate and lateral join variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_aggregate_lateral_arms_for_convert_visitor {
    () => {
        fn aggregate(
            &mut self,
            args: nodedb_sql::AggregateVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::AggregateVisitArgs {
                input,
                group_by,
                aggregates,
                having,
                limit,
                grouping_sets,
                sort_keys,
                group_by_aliases,
            } = args;
            super::super::aggregate::convert_aggregate(
                super::super::aggregate::ConvertAggregateParams {
                    input,
                    group_by,
                    aggregates,
                    having,
                    limit,
                    grouping_sets,
                    sort_keys,
                    group_by_aliases,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }

        fn lateral_top_k(
            &mut self,
            args: nodedb_sql::LateralTopKVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::LateralTopKVisitArgs {
                outer,
                outer_alias,
                inner_collection,
                inner_filters,
                inner_order_by,
                inner_limit,
                correlation_keys,
                lateral_alias,
                projection,
                left_join,
                temporal,
            } = args;
            super::super::lateral::convert_lateral_top_k(
                super::super::lateral::ConvertLateralTopKParams {
                    outer,
                    outer_alias,
                    inner_collection,
                    inner_filters,
                    inner_order_by,
                    inner_limit,
                    correlation_keys,
                    lateral_alias,
                    projection,
                    left_join,
                    temporal,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }

        fn lateral_loop(
            &mut self,
            args: nodedb_sql::LateralLoopVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::LateralLoopVisitArgs {
                outer,
                outer_alias,
                inner,
                correlation_predicates,
                lateral_alias,
                projection,
                outer_row_cap,
                left_join,
            } = args;
            super::super::lateral::convert_lateral_loop(
                super::super::lateral::ConvertLateralLoopParams {
                    outer,
                    outer_alias,
                    inner,
                    correlation_predicates,
                    lateral_alias,
                    projection,
                    outer_row_cap,
                    left_join,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }
    };
}

pub(super) use impl_aggregate_lateral_arms_for_convert_visitor;
