// SPDX-License-Identifier: BUSL-1.1

//! The row sink of a score scan.
//!
//! Rows stream in one at a time and are scored in batches, so memory holds
//! one batch plus the rows the scan returns. With a LIMIT and no score order
//! the sink stops the scan once it holds the limit. With a LIMIT ordered by
//! a score column it keeps the best rows in a bounded heap and never holds
//! more than the limit. With no LIMIT it returns every row.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ops::ControlFlow;

use nodedb_physical::physical_plan::{ScoreScanBound, ScoreScanOrder};
use nodedb_types::Surrogate;

use crate::data::executor::handlers::text_score_columns::ScoreColumns;
use crate::data::executor::response_codec::DocumentRow;

/// Rows scored per batch: one doc-length read transaction per column.
const SCORE_BATCH_ROWS: usize = 256;

/// The direction of an ordered bounded scan.
#[derive(Clone, Copy)]
struct Direction {
    ascending: bool,
    nulls_first: bool,
}

/// One kept row of an ordered bounded scan.
struct Ranked {
    score: Option<f64>,
    /// Arrival order. A later row ranks after an earlier one with the same
    /// score, so the heap keeps the earliest of tied rows.
    seq: u64,
    direction: Direction,
    row: DocumentRow,
}

impl Ranked {
    /// `Less` when `self` comes before `other` in the output.
    fn output_cmp(&self, other: &Self) -> Ordering {
        let Direction {
            ascending,
            nulls_first,
        } = self.direction;
        let by_score = match (self.score, other.score) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) if nulls_first => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) if nulls_first => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) if ascending => a.total_cmp(&b),
            (Some(a), Some(b)) => b.total_cmp(&a),
        };
        by_score.then(self.seq.cmp(&other.seq))
    }
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.output_cmp(other) == Ordering::Equal
    }
}

impl Eq for Ranked {}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ranked {
    /// The heap's maximum is the row that comes last in the output.
    fn cmp(&self, other: &Self) -> Ordering {
        self.output_cmp(other)
    }
}

/// Where scored rows go.
enum Target {
    /// Every row, in arrival order. `limit` stops the scan once reached.
    All {
        rows: Vec<DocumentRow>,
        limit: Option<usize>,
    },
    /// The first `limit` rows in `order`.
    Top {
        heap: BinaryHeap<Ranked>,
        limit: usize,
        order: ScoreScanOrder,
        seq: u64,
    },
}

/// Streams admitted rows of a score scan into its result.
pub(in crate::data::executor) struct ScoreScanSink<'a> {
    columns: &'a ScoreColumns<'a>,
    pending_keys: Vec<Surrogate>,
    pending_rows: Vec<DocumentRow>,
    target: Target,
}

impl<'a> ScoreScanSink<'a> {
    pub(in crate::data::executor) fn new(
        columns: &'a ScoreColumns<'a>,
        bound: Option<&ScoreScanBound>,
    ) -> Self {
        let target = match bound {
            None => Target::All {
                rows: Vec::new(),
                limit: None,
            },
            Some(ScoreScanBound { rows, order: None }) => Target::All {
                rows: Vec::with_capacity((*rows).min(SCORE_BATCH_ROWS)),
                limit: Some(*rows),
            },
            Some(ScoreScanBound {
                rows,
                order: Some(order),
            }) => Target::Top {
                heap: BinaryHeap::with_capacity((*rows).min(SCORE_BATCH_ROWS)),
                limit: *rows,
                order: order.clone(),
                seq: 0,
            },
        };
        Self {
            columns,
            pending_keys: Vec::new(),
            pending_rows: Vec::new(),
            target,
        }
    }

    /// Whether the sink takes no more rows.
    pub(in crate::data::executor) fn is_full(&self) -> bool {
        match &self.target {
            Target::All {
                rows,
                limit: Some(limit),
            } => rows.len() + self.pending_rows.len() >= *limit,
            Target::All { limit: None, .. } => false,
            Target::Top { limit, .. } => *limit == 0,
        }
    }

    /// Take one admitted row. `Break` once the sink is full.
    pub(in crate::data::executor) fn push(
        &mut self,
        surrogate: Surrogate,
        row: DocumentRow,
    ) -> crate::Result<ControlFlow<()>> {
        if self.is_full() {
            return Ok(ControlFlow::Break(()));
        }
        self.pending_keys.push(surrogate);
        self.pending_rows.push(row);
        if self.pending_rows.len() >= SCORE_BATCH_ROWS {
            self.flush()?;
        }
        Ok(if self.is_full() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    }

    /// Score the pending batch and move it into the target.
    fn flush(&mut self) -> crate::Result<()> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        self.columns
            .inject(&self.pending_keys, &mut self.pending_rows)?;
        self.pending_keys.clear();
        let batch = std::mem::take(&mut self.pending_rows);
        match &mut self.target {
            Target::All { rows, .. } => rows.extend(batch),
            Target::Top {
                heap,
                limit,
                order,
                seq,
            } => {
                for row in batch {
                    let score = match &row.data {
                        serde_json::Value::Object(map) => {
                            map.get(&order.alias).and_then(serde_json::Value::as_f64)
                        }
                        _ => None,
                    };
                    let ranked = Ranked {
                        score,
                        seq: *seq,
                        direction: Direction {
                            ascending: order.ascending,
                            nulls_first: order.nulls_first,
                        },
                        row,
                    };
                    *seq += 1;
                    if heap.len() < *limit {
                        heap.push(ranked);
                    } else if heap.peek().is_some_and(|worst| ranked < *worst) {
                        heap.pop();
                        heap.push(ranked);
                    }
                }
            }
        }
        Ok(())
    }

    /// The scan's rows: in arrival order, or in score order for an ordered
    /// bound.
    pub(in crate::data::executor) fn finish(mut self) -> crate::Result<Vec<DocumentRow>> {
        self.flush()?;
        Ok(match self.target {
            Target::All { rows, .. } => rows,
            Target::Top { heap, .. } => heap
                .into_sorted_vec()
                .into_iter()
                .map(|ranked| ranked.row)
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranked(score: Option<f64>, seq: u64, ascending: bool, nulls_first: bool) -> Ranked {
        Ranked {
            score,
            seq,
            direction: Direction {
                ascending,
                nulls_first,
            },
            row: DocumentRow {
                id: seq.to_string(),
                data: serde_json::Value::Null,
            },
        }
    }

    #[test]
    fn descending_puts_higher_scores_first_and_nulls_where_asked() {
        let high = ranked(Some(2.0), 0, false, true);
        let low = ranked(Some(1.0), 1, false, true);
        let null = ranked(None, 2, false, true);
        assert_eq!(high.output_cmp(&low), Ordering::Less);
        assert_eq!(null.output_cmp(&high), Ordering::Less, "NULLS FIRST");

        let null_last = ranked(None, 2, false, false);
        let high_last = ranked(Some(2.0), 0, false, false);
        assert_eq!(null_last.output_cmp(&high_last), Ordering::Greater);
    }

    #[test]
    fn ascending_puts_lower_scores_first() {
        let low = ranked(Some(1.0), 1, true, false);
        let high = ranked(Some(2.0), 0, true, false);
        assert_eq!(low.output_cmp(&high), Ordering::Less);
    }

    #[test]
    fn ties_keep_arrival_order() {
        let first = ranked(Some(1.0), 0, false, false);
        let second = ranked(Some(1.0), 1, false, false);
        assert_eq!(first.output_cmp(&second), Ordering::Less);
    }
}
