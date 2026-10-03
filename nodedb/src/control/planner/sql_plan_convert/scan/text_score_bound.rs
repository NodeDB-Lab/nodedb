// SPDX-License-Identifier: BUSL-1.1

//! LIMIT pushdown into a score scan.
//!
//! A `bm25_score(...)` scan with no `text_match` emits every admitted row,
//! and the relational tail above it applies the query's ORDER BY and LIMIT.
//! When the tail only cuts rows (no filter, DISTINCT, or window) the scan can
//! return fewer rows and the tail's answer is unchanged:
//!
//! - no ORDER BY: any `limit + offset` admitted rows.
//! - ORDER BY one score column: the first `limit + offset` rows in that
//!   order, kept in a bounded top-k by the Data Plane.
//!
//! Any other ORDER BY leaves the scan unbounded. The tail still applies its
//! own ORDER BY and LIMIT over the rows returned.

use nodedb_physical::physical_plan::{QueryOp, ScoreScanBound, ScoreScanOrder, TextOp};
use nodedb_sql::types::{SortKey, SqlExpr};

use crate::bridge::envelope::PhysicalPlan;

/// The tail clauses a pushdown must respect.
pub(in crate::control::planner::sql_plan_convert) struct ScoreScanTail<'a> {
    pub sort_keys: &'a [SortKey],
    pub limit: Option<usize>,
    pub offset: usize,
    /// Whether the tail filters, deduplicates, or computes windows: each
    /// reads rows past the cut, so no bound is pushed.
    pub reads_past_cut: bool,
}

/// Bound the score scan `plan` is (or gathers) by the tail's LIMIT.
pub(in crate::control::planner::sql_plan_convert) fn bound_score_scan(
    plan: &mut PhysicalPlan,
    tail: &ScoreScanTail<'_>,
) {
    if tail.reads_past_cut {
        return;
    }
    let Some(limit) = tail.limit else {
        return;
    };
    if let PhysicalPlan::Query(QueryOp::Exchange(exchange)) = plan {
        // Each shard returns its own first rows: their union holds the
        // first rows of the whole collection.
        bound_score_scan(&mut exchange.child, tail);
        return;
    }
    let PhysicalPlan::Text(TextOp::BM25ScoreScan { scores, bound, .. }) = plan else {
        return;
    };
    let order = match tail.sort_keys {
        [] => None,
        [key] => match &key.expr {
            SqlExpr::Column { name, .. } if scores.iter().any(|s| &s.alias == name) => {
                Some(ScoreScanOrder {
                    alias: name.clone(),
                    ascending: key.ascending,
                    nulls_first: key.nulls_first,
                })
            }
            _ => return,
        },
        _ => return,
    };
    *bound = Some(ScoreScanBound {
        rows: limit.saturating_add(tail.offset),
        order,
    });
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TextScoreSpec;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use super::*;

    fn scan() -> PhysicalPlan {
        PhysicalPlan::Text(TextOp::BM25ScoreScan {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: vec![TextScoreSpec {
                field: None,
                query: "q".into(),
                mode: nodedb_types::text_search::QueryMode::And, fuzzy: false,
                alias: "s".into(),
            }],
            bound: None,
        })
    }

    fn key(name: &str) -> SortKey {
        SortKey {
            expr: SqlExpr::Column {
                table: None,
                name: name.into(),
            },
            ascending: false,
            nulls_first: false,
        }
    }

    fn bound_of(plan: &PhysicalPlan) -> Option<ScoreScanBound> {
        match plan {
            PhysicalPlan::Text(TextOp::BM25ScoreScan { bound, .. }) => bound.clone(),
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    fn tail(sort_keys: &[SortKey], reads_past_cut: bool) -> ScoreScanTail<'_> {
        ScoreScanTail {
            sort_keys,
            limit: Some(10),
            offset: 5,
            reads_past_cut,
        }
    }

    #[test]
    fn an_unordered_limit_bounds_the_scan() {
        let mut plan = scan();
        bound_score_scan(&mut plan, &tail(&[], false));
        assert_eq!(
            bound_of(&plan),
            Some(ScoreScanBound {
                rows: 15,
                order: None
            })
        );
    }

    #[test]
    fn a_score_order_is_kept_in_the_bound() {
        let mut plan = scan();
        bound_score_scan(&mut plan, &tail(&[key("s")], false));
        let bound = bound_of(&plan).expect("bounded");
        assert_eq!(bound.rows, 15);
        assert_eq!(
            bound.order,
            Some(ScoreScanOrder {
                alias: "s".into(),
                ascending: false,
                nulls_first: false,
            })
        );
    }

    #[test]
    fn a_non_score_order_or_a_filtering_tail_leaves_the_scan_unbounded() {
        let mut plan = scan();
        bound_score_scan(&mut plan, &tail(&[key("id")], false));
        assert_eq!(bound_of(&plan), None);
        bound_score_scan(&mut plan, &tail(&[key("s"), key("id")], false));
        assert_eq!(bound_of(&plan), None);
        bound_score_scan(&mut plan, &tail(&[], true));
        assert_eq!(bound_of(&plan), None);
    }
}
