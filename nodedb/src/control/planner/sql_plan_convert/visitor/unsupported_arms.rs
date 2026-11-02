// SPDX-License-Identifier: BUSL-1.1
//! Macro that expands to unsupported `PlanVisitor` method stubs.
//! Invoked once from `adapter.rs` inside `impl PlanVisitor for ConvertVisitor`.

macro_rules! impl_unsupported_convert_visitor_methods {
    () => {
        fn multi_vector_search(
            &mut self,
            _collection: &str,
            _query_vector: &[f32],
            _top_k: usize,
            _ef_search: usize,
            _temporal: &nodedb_sql::TemporalScope,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            Err(crate::Error::PlanError {
                detail: "unsupported SqlPlan variant: MultiVectorSearch".to_string(),
            })
        }

        fn range_scan(
            &mut self,
            _collection: &str,
            _field: &str,
            _lower: Option<&nodedb_sql::types_expr::SqlValue>,
            _upper: Option<&nodedb_sql::types_expr::SqlValue>,
            _limit: usize,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            Err(crate::Error::PlanError {
                detail: "unsupported SqlPlan variant: RangeScan".to_string(),
            })
        }

        fn create_index(
            &mut self,
            _index_name: Option<&str>,
            _collection: &str,
            _field: &str,
            _unique: bool,
            _if_not_exists: bool,
            _case_insensitive: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            Err(crate::Error::PlanError {
                detail: "unsupported SqlPlan variant: CreateIndex".to_string(),
            })
        }

        fn drop_index(
            &mut self,
            _index_name: &str,
            _collection: Option<&str>,
            _if_exists: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            Err(crate::Error::PlanError {
                detail: "unsupported SqlPlan variant: DropIndex".to_string(),
            })
        }
    };
}

pub(super) use impl_unsupported_convert_visitor_methods;
