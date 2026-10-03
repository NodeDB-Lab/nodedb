// SPDX-License-Identifier: Apache-2.0

//! Attach `bm25_score(...)` columns to a plan.
//!
//! A `TextSearch` plan takes the columns as they are. A `Scan` becomes a
//! score scan over every row its filters admit. No other plan takes a score
//! column: it has no per-row BM25 value.

use crate::error::{Result, SqlError};
use crate::planner::select::post_process::post_process;
use crate::types::{SqlPlan, TextScoreColumn, TextSearchPlan, TextSearchShape};

/// Whether the scan's ORDER BY moves onto the score scan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanSort {
    /// The scan's sort keys come from an ORDER BY the caller does not
    /// consume: the score scan keeps them in a post-processing tail.
    Keep,
    /// The caller consumes the ORDER BY: the scan's sort keys are dropped.
    Consumed,
}

/// `plan` with `scores` attached. `None` when `plan` takes no score column.
pub(super) fn attach_scores(
    plan: &SqlPlan,
    scores: Vec<TextScoreColumn>,
    sort: ScanSort,
) -> Result<Option<SqlPlan>> {
    match plan {
        SqlPlan::TextSearch(search) => {
            let mut search = search.clone();
            for column in scores {
                search.add_score(column);
            }
            Ok(Some(SqlPlan::TextSearch(search)))
        }
        SqlPlan::Scan {
            collection,
            filters,
            projection,
            sort_keys,
            ..
        } => {
            refuse_scan_clauses(plan, "a bm25_score() scan")?;
            let mut search = TextSearchPlan {
                collection: collection.clone(),
                shape: TextSearchShape::ScoreScan,
                filters: filters.clone(),
                scores: Vec::new(),
                projection: projection.clone(),
            };
            for column in scores {
                search.add_score(column);
            }
            let search = SqlPlan::TextSearch(search);
            if sort == ScanSort::Keep && !sort_keys.is_empty() {
                return post_process(search, sort_keys.clone(), None, 0).map(Some);
            }
            Ok(Some(search))
        }
        _ => Ok(None),
    }
}

/// Refuse rewriting a `Scan` into `target` when the scan carries a clause the
/// rewritten plan has no slot for. Rewriting would answer without it.
pub(super) fn refuse_scan_clauses(plan: &SqlPlan, target: &str) -> Result<()> {
    let SqlPlan::Scan {
        distinct,
        window_functions,
        temporal,
        ..
    } = plan
    else {
        return Ok(());
    };
    let dropped = if *distinct {
        Some("DISTINCT")
    } else if !window_functions.is_empty() {
        Some("a window function")
    } else if temporal.is_temporal() {
        Some("AS OF")
    } else {
        None
    };
    match dropped {
        Some(clause) => Err(SqlError::Unsupported {
            detail: format!(
                "{clause} with {target} is not supported; wrap the search in a subquery"
            ),
        }),
        None => Ok(()),
    }
}
