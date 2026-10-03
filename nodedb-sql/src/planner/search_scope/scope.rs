// SPDX-License-Identifier: Apache-2.0

//! The pass that walks a plan and refuses a search function in a
//! row-evaluated position.

use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::types::{
    CtePlan, DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan, KvInsertPlan,
    LateralLoopPlan, LateralTopKPlan, MergePlan, RangeScanPlan, RecursiveScanPlan, TextSearchPlan,
    TimeseriesScanPlan, UpsertPlan, VectorPrimaryDeletePlan, VectorPrimaryInsertPlan,
    VectorPrimaryUpdatePlan,
};
use crate::types::{MergePlanAction, SqlPlan};

use super::lookup::has_search_plan;

/// Refuse `plan` when an index-owned search function sits where the row
/// evaluator runs it.
pub fn refuse_row_scoped_search_functions(
    plan: &SqlPlan,
    functions: &FunctionRegistry,
) -> Result<()> {
    Scope { functions }.plan(plan)
}

pub(super) struct Scope<'a> {
    pub(super) functions: &'a FunctionRegistry,
}

impl Scope<'_> {
    fn plan(&self, plan: &SqlPlan) -> Result<()> {
        match plan {
            SqlPlan::Scan {
                filters,
                projection,
                sort_keys,
                window_functions,
                ..
            }
            | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
                filters,
                projection,
                sort_keys,
                window_functions,
                ..
            }) => {
                self.filters(filters)?;
                self.projection(projection)?;
                self.sort_keys(sort_keys)?;
                self.windows(window_functions)
            }
            SqlPlan::PointGet { projection, .. }
            | SqlPlan::RangeScan(RangeScanPlan { projection, .. }) => self.projection(projection),
            SqlPlan::KvInsert(KvInsertPlan {
                on_conflict_updates,
                ..
            })
            | SqlPlan::Upsert(UpsertPlan {
                on_conflict_updates,
                ..
            })
            | SqlPlan::VectorPrimaryInsert(VectorPrimaryInsertPlan {
                on_conflict_updates,
                ..
            }) => self.assignments(on_conflict_updates),
            SqlPlan::InsertSelect {
                source, column_map, ..
            } => {
                self.plan(source)?;
                self.assignments(column_map)
            }
            SqlPlan::Update {
                assignments,
                filters,
                ..
            }
            | SqlPlan::VectorPrimaryUpdate(VectorPrimaryUpdatePlan {
                assignments,
                filters,
                ..
            }) => {
                self.assignments(assignments)?;
                self.filters(filters)
            }
            SqlPlan::UpdateFrom {
                source,
                assignments,
                target_filters,
                ..
            } => {
                self.plan(source)?;
                self.assignments(assignments)?;
                self.filters(target_filters)
            }
            SqlPlan::Delete { filters, .. }
            | SqlPlan::VectorPrimaryDelete(VectorPrimaryDeletePlan { filters, .. }) => {
                self.filters(filters)
            }
            SqlPlan::Join {
                left,
                right,
                condition,
                projection,
                filters,
                ..
            } => {
                self.plan(left)?;
                self.plan(right)?;
                if has_search_plan(left) || has_search_plan(right) {
                    return Ok(());
                }
                if let Some(condition) = condition {
                    self.expr(condition)?;
                }
                self.projection(projection)?;
                self.filters(filters)
            }
            SqlPlan::Aggregate {
                input,
                group_by,
                aggregates,
                having,
                sort_keys,
                ..
            } => {
                self.plan(input)?;
                if has_search_plan(input) {
                    return Ok(());
                }
                self.exprs(group_by)?;
                self.aggregates(aggregates)?;
                self.filters(having)?;
                self.sort_keys(sort_keys)
            }
            SqlPlan::TimeseriesScan(TimeseriesScanPlan {
                aggregates,
                filters,
                projection,
                sort_keys,
                ..
            }) => {
                self.aggregates(aggregates)?;
                self.filters(filters)?;
                self.projection(projection)?;
                self.sort_keys(sort_keys)
            }
            // A search plan serves its own score call as a column; only its
            // residual filters run on the row evaluator.
            SqlPlan::VectorSearch { filters, .. }
            | SqlPlan::TextSearch(TextSearchPlan { filters, .. })
            | SqlPlan::HybridSearch(HybridSearchPlan { filters, .. })
            | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan { filters, .. }) => {
                self.filters(filters)
            }
            SqlPlan::SpatialScan {
                attribute_filters, ..
            } => self.filters(attribute_filters),
            SqlPlan::RecursiveScan(RecursiveScanPlan {
                base_filters,
                recursive_filters,
                ..
            }) => {
                self.filters(base_filters)?;
                self.filters(recursive_filters)
            }
            SqlPlan::Union { inputs, .. } => inputs.iter().try_for_each(|input| self.plan(input)),
            SqlPlan::Intersect { left, right, .. } | SqlPlan::Except { left, right, .. } => {
                self.plan(left)?;
                self.plan(right)
            }
            SqlPlan::Cte(CtePlan { definitions, outer }) => {
                for (_, definition) in definitions {
                    self.plan(definition)?;
                }
                self.plan(outer)
            }
            SqlPlan::Subquery {
                input,
                filters,
                projection,
                window_functions,
                sort_keys,
                ..
            } => {
                self.plan(input)?;
                if has_search_plan(input) {
                    return Ok(());
                }
                self.filters(filters)?;
                self.projection(projection)?;
                self.windows(window_functions)?;
                self.sort_keys(sort_keys)
            }
            SqlPlan::Merge(MergePlan {
                source, clauses, ..
            }) => {
                self.plan(source)?;
                for clause in clauses {
                    self.filters(&clause.extra_predicate)?;
                    match &clause.action {
                        MergePlanAction::Update { assignments } => self.assignments(assignments)?,
                        MergePlanAction::Insert { values, .. } => self.exprs(values)?,
                        MergePlanAction::Delete | MergePlanAction::DoNothing => {}
                    }
                }
                Ok(())
            }
            SqlPlan::LateralTopK(LateralTopKPlan {
                outer,
                inner_filters,
                inner_order_by,
                projection,
                ..
            }) => {
                self.plan(outer)?;
                self.filters(inner_filters)?;
                self.sort_keys(inner_order_by)?;
                if has_search_plan(outer) {
                    return Ok(());
                }
                self.projection(projection)
            }
            SqlPlan::LateralLoop(LateralLoopPlan {
                outer,
                inner,
                projection,
                ..
            }) => {
                self.plan(outer)?;
                self.plan(inner)?;
                if has_search_plan(outer) || has_search_plan(inner) {
                    return Ok(());
                }
                self.projection(projection)
            }
            // No row-evaluated expression: constants, literal-row writes,
            // search plans that carry no residual filter, array statements,
            // and DDL.
            SqlPlan::ConstantResult { .. }
            | SqlPlan::Insert { .. }
            | SqlPlan::Truncate { .. }
            | SqlPlan::TimeseriesIngest { .. }
            | SqlPlan::MultiVectorSearch { .. }
            | SqlPlan::SparseSearch { .. }
            | SqlPlan::RecursiveValue { .. }
            | SqlPlan::CreateArray { .. }
            | SqlPlan::DropArray { .. }
            | SqlPlan::AlterArray { .. }
            | SqlPlan::InsertArray { .. }
            | SqlPlan::DeleteArray { .. }
            | SqlPlan::ArraySlice { .. }
            | SqlPlan::ArrayProject { .. }
            | SqlPlan::ArrayAgg { .. }
            | SqlPlan::ArrayElementwise { .. }
            | SqlPlan::ArrayFlush { .. }
            | SqlPlan::ArrayCompact { .. }
            | SqlPlan::VectorPrimaryTruncate { .. }
            | SqlPlan::CreateIndex { .. }
            | SqlPlan::DropIndex { .. } => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::refuse_row_scoped_search_functions;
    use crate::error::{Result, SqlError};
    use crate::functions::registry::FunctionRegistry;
    use crate::types::query::EngineType;
    use crate::types::query::Projection;
    use crate::types::{Filter, FilterExpr, SqlPlan};
    use crate::types_expr::SqlExpr;
    use crate::types_expr::SqlValue;

    fn call(name: &str) -> SqlExpr {
        SqlExpr::Function {
            name: name.into(),
            args: vec![
                SqlExpr::Column {
                    table: None,
                    name: "body".into(),
                },
                SqlExpr::Literal(SqlValue::String("rust".into())),
            ],
            distinct: false,
        }
    }

    fn scan(projection: Vec<Projection>, filters: Vec<Filter>) -> SqlPlan {
        SqlPlan::Scan {
            collection: "docs".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters,
            projection,
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: Default::default(),
        }
    }

    fn check(plan: &SqlPlan) -> Result<()> {
        refuse_row_scoped_search_functions(plan, &FunctionRegistry::new())
    }

    #[test]
    fn a_score_in_a_scan_projection_is_refused() {
        let plan = scan(
            vec![Projection::Computed {
                expr: call("bm25_score"),
                alias: "s".into(),
            }],
            Vec::new(),
        );
        assert_eq!(
            check(&plan),
            Err(SqlError::SearchFunctionOutsideSearch {
                name: "bm25_score".into()
            })
        );
    }

    #[test]
    fn a_match_nested_in_a_scan_filter_is_refused() {
        let nested = SqlExpr::BinaryOp {
            left: Box::new(call("text_match")),
            op: crate::types_expr::BinaryOp::Or,
            right: Box::new(SqlExpr::Literal(SqlValue::Bool(false))),
        };
        let plan = scan(
            Vec::new(),
            vec![Filter {
                expr: FilterExpr::Expr(nested),
            }],
        );
        assert!(matches!(
            check(&plan),
            Err(SqlError::SearchFunctionOutsideSearch { .. })
        ));
    }

    #[test]
    fn row_scalars_in_a_scan_pass() {
        let plan = scan(
            vec![
                Projection::Computed {
                    expr: call("vector_distance"),
                    alias: "d".into(),
                },
                Projection::Computed {
                    expr: call("doc_get"),
                    alias: "g".into(),
                },
            ],
            Vec::new(),
        );
        assert_eq!(check(&plan), Ok(()));
    }

    /// A text search that scores `rust` in every document under alias `s`.
    fn text_search(projection: Vec<Projection>) -> SqlPlan {
        SqlPlan::TextSearch(crate::types::TextSearchPlan {
            collection: "docs".into(),
            shape: crate::types::TextSearchShape::Match {
                field: None,
                query: crate::fts_types::FtsQuery::Plain {
                    text: "rust".into(),
                    fuzzy: true,
                },
                mode: nodedb_types::text_search::QueryMode::And,
                top_k: Some(10),
            },
            filters: Vec::new(),
            scores: vec![crate::types::TextScoreColumn {
                field: None,
                query: "rust".into(),
                mode: nodedb_types::text_search::QueryMode::And,
                fuzzy: true,
                alias: "s".into(),
            }],
            projection,
        })
    }

    #[test]
    fn a_search_plan_projection_serves_its_score() {
        let plan = text_search(vec![Projection::Computed {
            expr: call("bm25_score"),
            alias: "s".into(),
        }]);
        assert_eq!(check(&plan), Ok(()));
    }

    #[test]
    fn a_match_in_a_text_search_filter_is_refused() {
        let SqlPlan::TextSearch(mut search) = text_search(Vec::new()) else {
            unreachable!("text_search builds a TextSearch plan");
        };
        search.filters.push(Filter {
            expr: FilterExpr::Expr(call("text_match")),
        });
        assert!(matches!(
            check(&SqlPlan::TextSearch(search)),
            Err(SqlError::SearchFunctionOutsideSearch { .. })
        ));
    }

    #[test]
    fn a_subquery_tail_over_a_search_plan_is_not_checked() {
        let search = text_search(Vec::new());
        let plan = SqlPlan::Subquery {
            input: Box::new(search),
            filters: Vec::new(),
            projection: vec![Projection::Computed {
                expr: call("bm25_score"),
                alias: "s".into(),
            }],
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        };
        assert_eq!(check(&plan), Ok(()));
    }
}
