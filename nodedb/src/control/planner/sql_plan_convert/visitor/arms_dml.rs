// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for DML variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_dml_arms_for_convert_visitor {
    () => {
        fn insert(
            &mut self,
            args: nodedb_sql::InsertVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::InsertVisitArgs {
                collection,
                engine: _engine,
                route,
                rows,
                column_defaults,
                if_absent,
                column_schema,
                primary_key,
            } = args;
            super::super::dml::convert_insert(super::super::dml::ConvertInsertArgs {
                collection,
                route,
                rows,
                column_defaults,
                column_schema,
                if_absent,
                primary_key,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn upsert(
            &mut self,
            args: nodedb_sql::UpsertVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::UpsertVisitArgs {
                collection,
                engine: _engine,
                route,
                rows,
                column_defaults,
                on_conflict_updates,
                column_schema,
                primary_key,
            } = args;
            super::super::dml::convert_upsert(super::super::dml::ConvertUpsertArgs {
                collection,
                route,
                rows,
                column_defaults,
                column_schema,
                on_conflict_updates,
                primary_key,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn kv_insert(
            &mut self,
            collection: &str,
            entries: &[(
                nodedb_sql::types_expr::SqlValue,
                Vec<(String, nodedb_sql::types_expr::SqlValue)>,
            )],
            ttl_secs: u64,
            intent: nodedb_sql::types::plan::KvInsertIntent,
            on_conflict_updates: &[(String, nodedb_sql::types_expr::SqlExpr)],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_kv_insert(
                collection,
                entries,
                ttl_secs,
                intent,
                on_conflict_updates,
                self.tenant_id,
                self.ctx,
            )
        }

        fn update(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::query::EngineType,
            assignments: &[(String, nodedb_sql::types_expr::SqlExpr)],
            filters: &[nodedb_sql::types::filter::Filter],
            target_keys: &[nodedb_sql::types_expr::SqlValue],
            returning: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_update(super::super::dml::UpdateParams {
                collection,
                engine: &engine,
                assignments,
                filters,
                target_keys,
                returning,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn update_from(
            &mut self,
            args: nodedb_sql::UpdateFromVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::UpdateFromVisitArgs {
                collection,
                engine: _engine,
                source,
                target_join_col,
                source_join_col,
                assignments,
                target_filters,
                returning,
            } = args;
            super::super::dml::convert_update_from(super::super::dml::UpdateFromParams {
                collection,
                source,
                target_join_col,
                source_join_col,
                assignments,
                target_filters,
                returning,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn delete(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::query::EngineType,
            filters: &[nodedb_sql::types::filter::Filter],
            target_keys: &[nodedb_sql::types_expr::SqlValue],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_delete(
                collection,
                &engine,
                filters,
                target_keys,
                self.tenant_id,
                self.ctx,
            )
        }

        fn vector_primary_insert(
            &mut self,
            collection: &str,
            field: &str,
            quantization: &nodedb_types::VectorQuantization,
            storage_dtype: &nodedb_types::VectorStorageDtype,
            payload_indexes: &[(String, nodedb_types::PayloadIndexKind)],
            rows: &[nodedb_sql::types::plan::VectorPrimaryRow],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_vector_primary_insert(
                collection,
                &super::super::dml::VectorPrimaryInsertCfg {
                    field,
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes,
                },
                rows,
                self.tenant_id,
                self.ctx,
            )
        }

        fn merge(
            &mut self,
            args: nodedb_sql::MergeVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::MergeVisitArgs {
                target,
                engine: _engine,
                source,
                target_join_col,
                source_join_col,
                source_alias,
                clauses,
                returning,
            } = args;
            super::super::dml::convert_merge(super::super::dml::ConvertMergeArgs {
                target,
                source,
                target_join_col,
                source_join_col,
                source_alias,
                clauses,
                returning,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }
    };
}

pub(super) use impl_dml_arms_for_convert_visitor;
