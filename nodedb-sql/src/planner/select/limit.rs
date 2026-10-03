// SPDX-License-Identifier: Apache-2.0

//! LIMIT / OFFSET application.
//!
//! Each plan variant owns a different slot for the row bound — a scan's
//! `limit`/`offset`, a search plan's `top_k`, an array slice's `u32` cap. A
//! variant with no slot at all gets a post-processing tail rather than
//! silently losing the clause.

use super::post_process::post_process;
use super::query_tail::QueryTail;
use crate::error::{Result, SqlError};
use crate::types::SqlPlan;
use crate::types::{
    ArraySlicePlan, CtePlan, DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan,
    TextSearchPlan, TextSearchShape, TimeseriesScanPlan,
};

/// Default `ef_search` multiplier applied when LIMIT is the only signal
/// available for sizing the HNSW beam (e.g. on a fused VectorSearch that
/// inherited LIMIT after `apply_order_by`). Wider beams trade extra distance
/// computations for higher recall; `2 * top_k` is a standard heuristic.
const DEFAULT_EF_SEARCH_MULTIPLIER: usize = 2;

/// Apply LIMIT and OFFSET to a plan.
///
/// A variant with no slot for the bound gets a post-processing tail rather
/// than passing through unbounded: answering `LIMIT 1` with every row is a
/// wrong answer, not a missed optimization.
pub(in crate::planner::select) fn apply_limit(
    mut plan: SqlPlan,
    tail: &QueryTail<'_>,
) -> Result<SqlPlan> {
    let (limit_val, offset_val) = tail.limit_offset()?;

    // The LIMIT belongs to the query reading the CTE, not to the CTE body, so
    // it lands on the outer plan — without this a derived table like
    // `FROM (...) s LIMIT n` comes back unbounded.
    if let SqlPlan::Cte(CtePlan { definitions, outer }) = plan {
        return Ok(SqlPlan::Cte(CtePlan {
            definitions,
            outer: Box::new(apply_limit(*outer, tail)?),
        }));
    }

    // Only these three variants carry an OFFSET of their own. For the rest the
    // whole clause moves to a post-processing tail — including the LIMIT,
    // which must not be pushed into the body: truncating before the OFFSET
    // skips would drop the very rows the query asked for.
    let absorbs_offset = matches!(
        plan,
        SqlPlan::Scan { .. } | SqlPlan::DocumentIndexLookup { .. } | SqlPlan::Subquery { .. }
    );
    if offset_val != 0 && !absorbs_offset {
        return post_process(plan, Vec::new(), limit_val, offset_val);
    }

    // Single-row plans: a `LIMIT >= 1` is already satisfied and needs no slot.
    // `LIMIT 0` must return nothing, which neither variant can express itself.
    if matches!(
        plan,
        SqlPlan::ConstantResult { .. } | SqlPlan::PointGet { .. }
    ) {
        if limit_val == Some(0) {
            return post_process(plan, Vec::new(), Some(0), 0);
        }
        return Ok(plan);
    }

    match plan {
        SqlPlan::Scan {
            ref mut limit,
            ref mut offset,
            ..
        } => {
            *limit = limit_val;
            *offset = offset_val;
        }
        // The index-lookup rewrite of a document scan. It carries the same
        // row bound as the scan it replaced — without this the converter
        // substitutes its own default and `LIMIT 1` returns 10,000 rows.
        SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            ref mut limit,
            ref mut offset,
            ..
        }) => {
            *limit = limit_val;
            *offset = offset_val;
        }
        // A subquery/derived-table post-processing node applies its own
        // filter → offset → sort → distinct → project → limit pipeline.
        SqlPlan::Subquery {
            ref mut limit,
            ref mut offset,
            ..
        } => {
            *limit = limit_val;
            *offset = offset_val;
        }
        SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            limit: ref mut l, ..
        }) => {
            if let Some(lv) = limit_val {
                *l = lv;
            }
        }
        SqlPlan::SpatialScan {
            limit: ref mut l, ..
        } => {
            if let Some(lv) = limit_val {
                *l = lv;
            }
        }
        SqlPlan::ArraySlice(ArraySlicePlan {
            limit: ref mut l, ..
        }) => {
            if let Some(lv) = limit_val {
                // The slice bound is a `u32` (0 = unlimited); a LIMIT that
                // does not fit would wrap into a smaller bound.
                *l = u32::try_from(lv).map_err(|_| SqlError::Unsupported {
                    detail: format!("LIMIT {lv} exceeds the array slice row bound"),
                })?;
            }
        }
        // Search plans rank rows and return the best `top_k`, so the query's
        // `LIMIT N` *is* the top-k bound. `ef_search` is deliberately left
        // alone on the fused-search variants: a wider beam than the final N
        // costs distance computations, never correctness.
        // A text match ranks its hits, so `LIMIT N` is its top-k bound. With
        // no LIMIT it returns every match.
        SqlPlan::TextSearch(TextSearchPlan {
            shape: TextSearchShape::Match {
                top_k: ref mut k, ..
            },
            ..
        }) => {
            *k = limit_val;
        }
        // A score scan has no ranked cut: its rows are bounded by a tail.
        SqlPlan::TextSearch(TextSearchPlan {
            shape: TextSearchShape::ScoreScan,
            ..
        }) => {
            if limit_val.is_some() {
                return post_process(plan, Vec::new(), limit_val, 0);
            }
        }
        SqlPlan::MultiVectorSearch {
            top_k: ref mut k, ..
        }
        | SqlPlan::HybridSearch(HybridSearchPlan {
            top_k: ref mut k, ..
        })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan {
            top_k: ref mut k, ..
        }) => {
            if let Some(lv) = limit_val {
                *k = lv;
            }
        }
        SqlPlan::Aggregate {
            limit: ref mut l, ..
        } => {
            if let Some(lv) = limit_val {
                *l = lv;
            }
        }
        SqlPlan::VectorSearch {
            top_k: ref mut k,
            ef_search: ref mut ef,
            ann_options: ref opts,
            ..
        } => {
            // Fused VectorSearch (e.g. ORDER BY vector_distance + JOIN
            // ARRAY_SLICE) inherits its outer LIMIT here. Without this,
            // a join-derived VectorSearch carries the join's default
            // 10000 limit instead of the user's `LIMIT N`.
            if let Some(lv) = limit_val {
                *k = lv;
                *ef = opts
                    .ef_search_override
                    .unwrap_or(lv * DEFAULT_EF_SEARCH_MULTIPLIER);
            }
        }
        SqlPlan::Join {
            limit: ref mut l, ..
        } => {
            // Record the LIMIT clause verbatim: `Some(n)` for an explicit
            // `LIMIT n`, `None` for no clause. A no-LIMIT join is bounded
            // downstream by the memory byte budget rather than silently
            // truncated, so we must NOT substitute a default row cap here.
            *l = limit_val;
        }
        SqlPlan::SparseSearch {
            top_k: ref mut k, ..
        } => {
            // `SparseSearch` returns the highest-scoring `top_k` documents, so
            // the query's `LIMIT N` is the top-k bound. Without this the plan
            // carries the trigger's fallback default instead of the user's N.
            if let Some(lv) = limit_val {
                *k = lv;
            }
        }
        ref other => {
            // With no LIMIT there is nothing to apply and the plan is left
            // untouched. With one, this variant has no slot to hold it, and
            // answering a bounded query with every row is a wrong answer —
            // bound the rows in a post-processing tail instead.
            if limit_val.is_some() {
                return post_process(other.clone(), Vec::new(), limit_val, offset_val);
            }
        }
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use sqlparser::ast;

    use super::*;
    use crate::temporal::TemporalScope;
    use crate::types::LateralLoopPlan;
    use crate::types::query::{EngineType, JoinType};

    fn minimal_scan() -> SqlPlan {
        SqlPlan::Scan {
            collection: "t".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: vec![],
            projection: vec![],
            sort_keys: vec![],
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: vec![],
            temporal: TemporalScope::default(),
        }
    }

    fn join_plan_with_limit(limit: Option<usize>) -> SqlPlan {
        SqlPlan::Join {
            left: Box::new(minimal_scan()),
            right: Box::new(minimal_scan()),
            on: vec![],
            join_type: JoinType::Inner,
            condition: None,
            limit,
            projection: vec![],
            filters: vec![],
        }
    }

    fn limit_clause(n: usize) -> Option<ast::LimitClause> {
        Some(ast::LimitClause::LimitOffset {
            limit: Some(ast::Expr::Value(
                ast::Value::Number(n.to_string(), false).into(),
            )),
            offset: None,
            limit_by: vec![],
        })
    }

    fn tail(limit_clause: &Option<ast::LimitClause>) -> QueryTail<'_> {
        QueryTail {
            order_by: None,
            limit_clause,
            fetch: None,
        }
    }

    #[test]
    fn apply_limit_sets_join_limit() {
        // An explicit `LIMIT 5` clause is recorded as `Some(5)`.
        let plan = join_plan_with_limit(None);
        let clause = limit_clause(5);
        let result = apply_limit(plan, &tail(&clause)).expect("join accepts a LIMIT");
        match result {
            SqlPlan::Join { limit, .. } => assert_eq!(limit, Some(5)),
            other => panic!("expected SqlPlan::Join, got {other:?}"),
        }
    }

    #[test]
    fn apply_limit_none_leaves_join_limit_none() {
        // No LIMIT clause stays `None` — the join is bounded by the memory
        // budget downstream, never by a default row cap.
        let plan = join_plan_with_limit(None);
        let result = apply_limit(plan, &tail(&None)).expect("no LIMIT is a no-op");
        match result {
            SqlPlan::Join { limit, .. } => assert_eq!(limit, None),
            other => panic!("expected SqlPlan::Join, got {other:?}"),
        }
    }

    fn lateral_loop_plan() -> SqlPlan {
        SqlPlan::LateralLoop(LateralLoopPlan {
            outer: Box::new(minimal_scan()),
            outer_alias: None,
            inner: Box::new(minimal_scan()),
            correlation_predicates: vec![],
            lateral_alias: "x".into(),
            projection: vec![],
            outer_row_cap: 10,
            left_join: false,
        })
    }

    #[test]
    fn apply_limit_post_processes_a_plan_that_cannot_hold_it() {
        // A LIMIT with nowhere to live bounds the rows in a post-processing
        // tail rather than being dropped.
        let clause = limit_clause(5);
        let result = apply_limit(lateral_loop_plan(), &tail(&clause)).expect("LIMIT is applied");
        match result {
            SqlPlan::Subquery { limit, offset, .. } => {
                assert_eq!(limit, Some(5));
                assert_eq!(offset, 0);
            }
            other => panic!("expected SqlPlan::Subquery, got {other:?}"),
        }
    }

    #[test]
    fn apply_limit_post_processes_offset_a_plan_cannot_hold() {
        // A join carries a LIMIT but no OFFSET, so both move to the tail —
        // pushing the LIMIT into the join would truncate before the skip.
        let clause = Some(ast::LimitClause::LimitOffset {
            limit: Some(ast::Expr::Value(
                ast::Value::Number("5".into(), false).into(),
            )),
            offset: Some(ast::Offset {
                value: ast::Expr::Value(ast::Value::Number("3".into(), false).into()),
                rows: ast::OffsetRows::None,
            }),
            limit_by: vec![],
        });
        let result = apply_limit(join_plan_with_limit(None), &tail(&clause))
            .expect("OFFSET is applied in the tail");
        match result {
            SqlPlan::Subquery {
                input,
                limit,
                offset,
                ..
            } => {
                assert_eq!(limit, Some(5));
                assert_eq!(offset, 3);
                assert!(
                    matches!(*input, SqlPlan::Join { limit: None, .. }),
                    "the body keeps no row bound of its own"
                );
            }
            other => panic!("expected SqlPlan::Subquery, got {other:?}"),
        }
    }
}
