// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for set-operation and CTE variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_set_ops_arms_for_convert_visitor {
    () => {
        fn constant_result(
            &mut self,
            columns: &[String],
            values: &[nodedb_sql::types_expr::SqlValue],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_constant_result(
                columns,
                values,
                self.tenant_id,
                self.ctx,
            )
        }

        fn truncate(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::EngineType,
            restart_identity: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_truncate(
                collection,
                engine,
                restart_identity,
                self.tenant_id,
                self.ctx,
            )
        }

        fn union(
            &mut self,
            inputs: &[nodedb_sql::types::SqlPlan],
            distinct: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_union(inputs, distinct, self.tenant_id, self.ctx)
        }

        fn intersect(
            &mut self,
            left: &nodedb_sql::types::SqlPlan,
            right: &nodedb_sql::types::SqlPlan,
            all: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_intersect(left, right, all, self.tenant_id, self.ctx)
        }

        fn except(
            &mut self,
            left: &nodedb_sql::types::SqlPlan,
            right: &nodedb_sql::types::SqlPlan,
            all: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_except(left, right, all, self.tenant_id, self.ctx)
        }

        fn insert_select(
            &mut self,
            target: &str,
            source: &nodedb_sql::types::SqlPlan,
            _limit: usize,
            column_map: &[(String, nodedb_sql::types::SqlExpr)],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_insert_select(
                target,
                source,
                column_map,
                self.tenant_id,
                self.ctx,
            )
        }

        fn cte(
            &mut self,
            definitions: &[(String, nodedb_sql::types::SqlPlan)],
            outer: &nodedb_sql::types::SqlPlan,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_cte(definitions, outer, self.tenant_id, self.ctx)
        }

        fn subquery(
            &mut self,
            args: nodedb_sql::SubqueryVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::set_ops::convert_subquery(args, self.tenant_id, self.ctx)
        }
    };
}

pub(super) use impl_set_ops_arms_for_convert_visitor;
