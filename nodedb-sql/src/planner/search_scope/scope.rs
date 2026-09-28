// SPDX-License-Identifier: Apache-2.0

//! The pass that walks a plan and refuses a search function in a
//! row-evaluated position.

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::types::query::{AggregateExpr, Projection, SortKey, WindowSpec};
use crate::types::{
    CtePlan, DocumentIndexLookupPlan, KvInsertPlan, LateralLoopPlan, LateralTopKPlan, MergePlan,
    RangeScanPlan, RecursiveScanPlan, TimeseriesScanPlan, UpsertPlan, VectorPrimaryDeletePlan,
    VectorPrimaryInsertPlan, VectorPrimaryUpdatePlan,
};
use crate::types::{Filter, FilterExpr, MergePlanAction, SqlPlan};
use crate::types_expr::SqlExpr;

use super::lookup::{first_search_function, has_search_plan};

/// Refuse `plan` when an index-owned search function sits where the row
/// evaluator runs it.
pub fn refuse_row_scoped_search_functions(
    plan: &SqlPlan,
    functions: &FunctionRegistry,
) -> Result<()> {
    Scope { functions }.plan(plan)
}

struct Scope<'a> {
    functions: &'a FunctionRegistry,
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
            SqlPlan::VectorSearch { filters, .. } | SqlPlan::TextSearch { filters, .. } => {
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
            | SqlPlan::HybridSearch { .. }
            | SqlPlan::HybridSearchTriple { .. }
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

    fn filters(&self, filters: &[Filter]) -> Result<()> {
        filters
            .iter()
            .try_for_each(|filter| self.filter(&filter.expr))
    }

    fn filter(&self, expr: &FilterExpr) -> Result<()> {
        match expr {
            FilterExpr::Expr(expr) => self.expr(expr),
            FilterExpr::And(children) | FilterExpr::Or(children) => self.filters(children),
            FilterExpr::Not(child) => self.filter(&child.expr),
            FilterExpr::Comparison { .. }
            | FilterExpr::InList { .. }
            | FilterExpr::Between { .. }
            | FilterExpr::IsNull { .. }
            | FilterExpr::IsNotNull { .. } => Ok(()),
        }
    }

    fn projection(&self, projection: &[Projection]) -> Result<()> {
        for item in projection {
            match item {
                Projection::Computed { expr, .. } | Projection::CpComputed { expr, .. } => {
                    self.expr(expr)?
                }
                Projection::Column(_) | Projection::Star | Projection::QualifiedStar(_) => {}
            }
        }
        Ok(())
    }

    fn sort_keys(&self, sort_keys: &[SortKey]) -> Result<()> {
        sort_keys.iter().try_for_each(|key| self.expr(&key.expr))
    }

    fn windows(&self, windows: &[WindowSpec]) -> Result<()> {
        for window in windows {
            self.exprs(&window.args)?;
            self.exprs(&window.partition_by)?;
            self.sort_keys(&window.order_by)?;
        }
        Ok(())
    }

    fn aggregates(&self, aggregates: &[AggregateExpr]) -> Result<()> {
        aggregates
            .iter()
            .try_for_each(|aggregate| self.exprs(&aggregate.args))
    }

    fn assignments(&self, assignments: &[(String, SqlExpr)]) -> Result<()> {
        assignments.iter().try_for_each(|(_, expr)| self.expr(expr))
    }

    fn exprs(&self, exprs: &[SqlExpr]) -> Result<()> {
        exprs.iter().try_for_each(|expr| self.expr(expr))
    }

    fn expr(&self, expr: &SqlExpr) -> Result<()> {
        match first_search_function(expr, self.functions) {
            Some(name) => Err(SqlError::SearchFunctionOutsideSearch {
                name: name.to_owned(),
            }),
            None => Ok(()),
        }
    }
}
