// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for scan/read/join/recursive variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_scan_read_arms_for_convert_visitor {
    () => {
        fn scan(
            &mut self,
            args: nodedb_sql::ScanVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::ScanVisitArgs {
                collection,
                alias: _alias,
                engine,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct,
                window_functions,
                temporal,
            } = args;
            super::super::scan::convert_scan(super::super::scan_params::ScanParams {
                collection,
                engine: &engine,
                filters,
                projection,
                sort_keys,
                limit: &limit,
                offset: &offset,
                distinct: &distinct,
                window_functions,
                tenant_id: self.tenant_id,
                temporal,
                database_id: self.ctx.database_id,
            })
        }

        fn point_get(
            &mut self,
            collection: &str,
            _alias: Option<&str>,
            engine: nodedb_sql::types::query::EngineType,
            key_column: &str,
            key_value: &nodedb_sql::types_expr::SqlValue,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::scan::convert_point_get(
                collection,
                &engine,
                key_column,
                key_value,
                self.tenant_id,
                self.ctx,
            )
        }

        fn document_index_lookup(
            &mut self,
            args: nodedb_sql::DocumentIndexLookupVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::DocumentIndexLookupVisitArgs {
                collection,
                alias: _alias,
                engine: _engine,
                field,
                value,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct: _distinct,
                window_functions: _window_functions,
                case_insensitive: _case_insensitive,
                temporal,
            } = args;
            super::super::scan::convert_document_index_lookup(
                super::super::scan::DocumentIndexLookupArgs {
                    collection,
                    field,
                    value,
                    filters,
                    projection,
                    sort_keys,
                    limit,
                    offset,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                    temporal,
                },
            )
        }

        fn join(
            &mut self,
            args: nodedb_sql::JoinVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::JoinVisitArgs {
                left,
                right,
                on,
                join_type,
                condition,
                limit,
                projection,
                filters,
            } = args;
            let condition_owned: Option<nodedb_sql::types_expr::SqlExpr> = condition.cloned();
            super::super::scan::convert_join(super::super::scan_params::JoinPlanParams {
                left,
                right,
                on,
                join_type: &join_type,
                condition: &condition_owned,
                limit: &limit,
                projection,
                filters,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn recursive_scan(
            &mut self,
            args: nodedb_sql::RecursiveScanVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::RecursiveScanVisitArgs {
                collection,
                base_filters,
                recursive_filters,
                join_link,
                max_iterations,
                distinct,
                limit,
            } = args;
            let join_link_owned: Option<(String, String)> = join_link.cloned();
            super::super::scan::convert_recursive_scan(
                super::super::scan_params::RecursiveScanParams {
                    collection,
                    base_filters,
                    recursive_filters,
                    join_link: &join_link_owned,
                    max_iterations: &max_iterations,
                    distinct: &distinct,
                    limit: &limit,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }

        fn recursive_value(
            &mut self,
            args: nodedb_sql::RecursiveValueVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::RecursiveValueVisitArgs {
                cte_name,
                columns,
                init_exprs,
                step_exprs,
                condition,
                max_depth,
                distinct,
            } = args;
            let condition_owned: Option<String> = condition.map(str::to_owned);
            super::super::scan::convert_recursive_value(
                super::super::scan_params::RecursiveValueParams {
                    cte_name,
                    columns,
                    init_exprs,
                    step_exprs,
                    condition: &condition_owned,
                    max_depth: &max_depth,
                    distinct: &distinct,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }
    };
}

pub(super) use impl_scan_read_arms_for_convert_visitor;
