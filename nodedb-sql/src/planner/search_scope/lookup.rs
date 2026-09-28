// SPDX-License-Identifier: Apache-2.0

//! Which functions are index-owned, and which plans serve a score column.

use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::types::SqlPlan;
use crate::types::{CtePlan, LateralLoopPlan, LateralTopKPlan};
use crate::types_expr::SqlExpr;

/// Whether the search trigger `trigger` names a function that reads an
/// index and has no per-row value.
pub(super) fn is_index_owned(trigger: SearchTrigger) -> bool {
    match trigger {
        SearchTrigger::MultiVectorSearch
        | SearchTrigger::SparseSearch
        | SearchTrigger::TextSearch
        | SearchTrigger::HybridSearch
        | SearchTrigger::TextMatch
        | SearchTrigger::GraphSearch => true,
        // The vector distances and the spatial predicates evaluate per row;
        // the time bucket is a scalar; the array functions are table-valued
        // and planned from FROM.
        SearchTrigger::None
        | SearchTrigger::VectorSearch
        | SearchTrigger::SpatialDWithin
        | SearchTrigger::SpatialContains
        | SearchTrigger::SpatialIntersects
        | SearchTrigger::SpatialWithin
        | SearchTrigger::TimeBucket
        | SearchTrigger::ArraySlice
        | SearchTrigger::ArrayProject
        | SearchTrigger::ArrayAgg
        | SearchTrigger::ArrayElementwise
        | SearchTrigger::ArrayFlush
        | SearchTrigger::ArrayCompact => false,
    }
}

/// The first index-owned search function `expr` calls outside a subquery.
pub(super) fn first_search_function<'e>(
    expr: &'e SqlExpr,
    functions: &FunctionRegistry,
) -> Option<&'e str> {
    let find = |e: &'e SqlExpr| first_search_function(e, functions);
    match expr {
        SqlExpr::Function { name, args, .. } => {
            if is_index_owned(functions.search_trigger(name)) {
                return Some(name.as_str());
            }
            args.iter().find_map(find)
        }
        SqlExpr::BinaryOp { left, right, .. } => find(left).or_else(|| find(right)),
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull { expr, .. } => find(expr),
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => operand
            .as_deref()
            .and_then(find)
            .or_else(|| {
                when_then
                    .iter()
                    .find_map(|(when, then)| find(when).or_else(|| find(then)))
            })
            .or_else(|| else_expr.as_deref().and_then(find)),
        SqlExpr::InList { expr, list, .. } => find(expr).or_else(|| list.iter().find_map(find)),
        SqlExpr::Between {
            expr, low, high, ..
        } => find(expr).or_else(|| find(low)).or_else(|| find(high)),
        SqlExpr::Like { expr, pattern, .. } => find(expr).or_else(|| find(pattern)),
        SqlExpr::ArrayLiteral(items) => items.iter().find_map(find),
        SqlExpr::Column { .. } | SqlExpr::Literal(_) | SqlExpr::Subquery(_) | SqlExpr::Wildcard => {
            None
        }
    }
}

/// Whether `plan` is, or wraps, a search plan that serves a score column.
pub(super) fn has_search_plan(plan: &SqlPlan) -> bool {
    match plan {
        SqlPlan::VectorSearch { .. }
        | SqlPlan::MultiVectorSearch { .. }
        | SqlPlan::SparseSearch { .. }
        | SqlPlan::TextSearch { .. }
        | SqlPlan::HybridSearch { .. }
        | SqlPlan::HybridSearchTriple { .. } => true,
        SqlPlan::Subquery { input, .. } | SqlPlan::Aggregate { input, .. } => {
            has_search_plan(input)
        }
        SqlPlan::Join { left, right, .. } => has_search_plan(left) || has_search_plan(right),
        SqlPlan::LateralTopK(LateralTopKPlan { outer, .. }) => has_search_plan(outer),
        SqlPlan::LateralLoop(LateralLoopPlan { outer, inner, .. }) => {
            has_search_plan(outer) || has_search_plan(inner)
        }
        SqlPlan::Union { inputs, .. } => inputs.iter().any(has_search_plan),
        SqlPlan::Intersect { left, right, .. } | SqlPlan::Except { left, right, .. } => {
            has_search_plan(left) || has_search_plan(right)
        }
        SqlPlan::Cte(CtePlan { outer, .. }) => has_search_plan(outer),
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Scan { .. }
        | SqlPlan::PointGet { .. }
        | SqlPlan::DocumentIndexLookup { .. }
        | SqlPlan::RangeScan { .. }
        | SqlPlan::Insert { .. }
        | SqlPlan::KvInsert { .. }
        | SqlPlan::Upsert { .. }
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::TimeseriesScan { .. }
        | SqlPlan::TimeseriesIngest { .. }
        | SqlPlan::SpatialScan { .. }
        | SqlPlan::RecursiveScan { .. }
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
        | SqlPlan::Merge { .. }
        | SqlPlan::VectorPrimaryInsert { .. }
        | SqlPlan::VectorPrimaryDelete { .. }
        | SqlPlan::VectorPrimaryTruncate { .. }
        | SqlPlan::VectorPrimaryUpdate { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. } => false,
    }
}
