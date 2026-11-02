// SPDX-License-Identifier: BUSL-1.1

//! LIMIT pushdown into a text body under a relational tail.
//!
//! The tail above a text body applies the query's ORDER BY and LIMIT. When
//! the tail only cuts rows (no filter, DISTINCT, or window) the body can
//! return fewer rows and the tail's answer is unchanged.
//!
//! A `bm25_score(...)` scan with no `text_match` emits every admitted row:
//!
//! - no ORDER BY: any `limit + offset` admitted rows.
//! - ORDER BY one score column: the first `limit + offset` rows in that
//!   order, kept in a bounded top-k by the Data Plane.
//!
//! A `text_match` search ranks its hits by BM25 score descending, then by
//! surrogate ascending. ORDER BY one score column descending, whose spec
//! (field, query, mode, fuzzy) equals the search's, sorts by the same score.
//! The tail's sort is stable, so its first `limit + offset` rows are the
//! search's first `limit + offset` hits, and `top_k` takes that bound. A
//! sharded search keeps the answer too: each shard's first rows hold every
//! row of the gathered first rows. A phrase search ranks by match position,
//! not by BM25 score, so it takes no bound.
//!
//! Any other ORDER BY leaves the body unbounded. The tail still applies its
//! own ORDER BY and LIMIT over the rows returned.

use nodedb_physical::physical_plan::{
    QueryOp, ScoreScanBound, ScoreScanOrder, TextOp, TextScoreSpec,
};
use nodedb_sql::types::{SortKey, SqlExpr};
use nodedb_types::text_search::QueryMode;

use crate::bridge::envelope::PhysicalPlan;

/// The tail clauses a pushdown must respect.
pub(in crate::control::planner::sql_plan_convert) struct TextBodyTail<'a> {
    pub sort_keys: &'a [SortKey],
    pub limit: Option<usize>,
    pub offset: usize,
    /// Whether the tail filters, deduplicates, or computes windows: each
    /// reads rows past the cut, so no bound is pushed.
    pub reads_past_cut: bool,
}

/// Bound the text body `plan` is (or gathers) by the tail's LIMIT.
pub(in crate::control::planner::sql_plan_convert) fn bound_text_body(
    plan: &mut PhysicalPlan,
    tail: &TextBodyTail<'_>,
) {
    if tail.reads_past_cut {
        return;
    }
    let Some(limit) = tail.limit else {
        return;
    };
    let rows = limit.saturating_add(tail.offset);
    if let PhysicalPlan::Query(QueryOp::Exchange(exchange)) = plan {
        // Each shard returns its own first rows: their union holds the
        // first rows of the whole collection.
        bound_text_body(&mut exchange.child, tail);
        return;
    }
    let PhysicalPlan::Text(op) = plan else {
        return;
    };
    if let TextOp::BM25ScoreScan { scores, bound, .. } = op {
        if let Some(order) = score_scan_order(tail.sort_keys, scores) {
            *bound = Some(ScoreScanBound { rows, order });
        }
        return;
    }
    if let TextOp::Search {
        field,
        query,
        top_k,
        mode,
        fuzzy,
        scores,
        ..
    } = op
    {
        let search = SearchSpec {
            field: field.as_deref(),
            query,
            mode: *mode,
            fuzzy: *fuzzy,
        };
        if sorts_by_search_rank(tail.sort_keys, scores, &search) {
            *top_k = (*top_k).min(rows);
        }
    }
}

/// The query a `TextOp::Search` ranks by.
struct SearchSpec<'a> {
    field: Option<&'a str>,
    query: &'a str,
    mode: QueryMode,
    fuzzy: bool,
}

/// Whether `sort_keys` order rows the way the search ranks them: one key,
/// descending, naming a score column whose spec equals the search's.
fn sorts_by_search_rank(
    sort_keys: &[SortKey],
    scores: &[TextScoreSpec],
    search: &SearchSpec<'_>,
) -> bool {
    let [key] = sort_keys else {
        return false;
    };
    if key.ascending {
        return false;
    }
    let SqlExpr::Column { name, .. } = &key.expr else {
        return false;
    };
    // The last column of an alias is the value a row carries under it.
    scores
        .iter()
        .rev()
        .find(|s| &s.alias == name)
        .is_some_and(|s| {
            s.field.as_deref() == search.field
                && s.query == search.query
                && s.mode == search.mode
                && s.fuzzy == search.fuzzy
        })
}

/// The bounded order of a score scan under `sort_keys`. `None` when the
/// keys leave the scan unbounded. `Some(None)` bounds it with no order.
fn score_scan_order(
    sort_keys: &[SortKey],
    scores: &[TextScoreSpec],
) -> Option<Option<ScoreScanOrder>> {
    match sort_keys {
        [] => Some(None),
        [key] => match &key.expr {
            SqlExpr::Column { name, .. } if scores.iter().any(|s| &s.alias == name) => {
                Some(Some(ScoreScanOrder {
                    alias: name.clone(),
                    ascending: key.ascending,
                    nulls_first: key.nulls_first,
                }))
            }
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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
                mode: nodedb_types::text_search::QueryMode::And,
                fuzzy: false,
                alias: "s".into(),
            }],
            bound: None,
            system_time: nodedb_types::SystemTimeScope::Current,
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

    fn tail(sort_keys: &[SortKey], reads_past_cut: bool) -> TextBodyTail<'_> {
        TextBodyTail {
            sort_keys,
            limit: Some(10),
            offset: 5,
            reads_past_cut,
        }
    }

    #[test]
    fn an_unordered_limit_bounds_the_scan() {
        let mut plan = scan();
        bound_text_body(&mut plan, &tail(&[], false));
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
        bound_text_body(&mut plan, &tail(&[key("s")], false));
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
        bound_text_body(&mut plan, &tail(&[key("id")], false));
        assert_eq!(bound_of(&plan), None);
        bound_text_body(&mut plan, &tail(&[key("s"), key("id")], false));
        assert_eq!(bound_of(&plan), None);
        bound_text_body(&mut plan, &tail(&[], true));
        assert_eq!(bound_of(&plan), None);
    }

    fn spec(query: &str, mode: QueryMode, fuzzy: bool) -> TextScoreSpec {
        TextScoreSpec {
            field: Some("body".into()),
            query: query.into(),
            mode,
            fuzzy,
            alias: "score".into(),
        }
    }

    fn search(score: TextScoreSpec) -> PhysicalPlan {
        PhysicalPlan::Text(TextOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            field: Some("body".into()),
            query: "rust db".into(),
            top_k: usize::MAX,
            mode: QueryMode::And,
            fuzzy: false,
            prefilter: None,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            scores: vec![score],
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    fn top_k_of(plan: &PhysicalPlan) -> usize {
        match plan {
            PhysicalPlan::Text(TextOp::Search { top_k, .. }) => *top_k,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    #[test]
    fn a_descending_sort_by_the_search_score_bounds_the_search() {
        let mut plan = search(spec("rust db", QueryMode::And, false));
        bound_text_body(&mut plan, &tail(&[key("score")], false));
        assert_eq!(top_k_of(&plan), 15);
    }

    #[test]
    fn a_sort_the_search_does_not_rank_by_leaves_it_unbounded() {
        let ascending = SortKey {
            ascending: true,
            ..key("score")
        };
        let cases: Vec<(PhysicalPlan, Vec<SortKey>, bool)> = vec![
            // Ascending order.
            (
                search(spec("rust db", QueryMode::And, false)),
                vec![ascending],
                false,
            ),
            // A score of another query, mode, or fuzzy setting.
            (
                search(spec("rust", QueryMode::And, false)),
                vec![key("score")],
                false,
            ),
            (
                search(spec("rust db", QueryMode::Or, false)),
                vec![key("score")],
                false,
            ),
            (
                search(spec("rust db", QueryMode::And, true)),
                vec![key("score")],
                false,
            ),
            // A non-score key, a second key, no key, a filtering tail.
            (
                search(spec("rust db", QueryMode::And, false)),
                vec![key("id")],
                false,
            ),
            (
                search(spec("rust db", QueryMode::And, false)),
                vec![key("score"), key("id")],
                false,
            ),
            (
                search(spec("rust db", QueryMode::And, false)),
                vec![],
                false,
            ),
            (
                search(spec("rust db", QueryMode::And, false)),
                vec![key("score")],
                true,
            ),
        ];
        for (mut plan, keys, reads_past_cut) in cases {
            bound_text_body(&mut plan, &tail(&keys, reads_past_cut));
            assert_eq!(top_k_of(&plan), usize::MAX, "keys {keys:?}");
        }
    }

    #[test]
    fn a_sharded_search_bounds_each_shard() {
        let child = search(spec("rust db", QueryMode::And, false));
        let mut plan = PhysicalPlan::Query(QueryOp::Exchange(
            nodedb_physical::physical_plan::ExchangeOp {
                child: Box::new(child),
                mode: nodedb_physical::physical_plan::ExchangeMode::Gather {
                    as_aggregate: false,
                },
            },
        ));
        bound_text_body(&mut plan, &tail(&[key("score")], false));
        let PhysicalPlan::Query(QueryOp::Exchange(exchange)) = &plan else {
            panic!("plan shape changed: {plan:?}");
        };
        assert_eq!(top_k_of(&exchange.child), 15);
    }
}
