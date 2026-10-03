// SPDX-License-Identifier: BUSL-1.1

//! Per-row `bm25_score(field, query)` columns.
//!
//! Each column is a per-document scorer: its query resolves once, then each
//! emitted row is scored by point lookups of its postings and of its
//! recorded length, read in one transaction per batch of rows. No column
//! builds a corpus-wide score map, so a `LIMIT 10` read scores ten rows.
//!
//! A row scores the BM25 value of its match. A row the index holds that the
//! query does not match scores `0.0`. A row the index does not hold (no text
//! in the field, or no text at all for the whole-document index) scores
//! `null`. The issuing transaction's staged rows are scored from their
//! staged text. The AND-mode fallback of a column is decided over the rows
//! the reading query admits, the same rows its search ranks.

use nodedb_fts::posting::QueryMode;
use nodedb_fts::{DocScore, TextQuery};
use nodedb_physical::physical_plan::TextScoreSpec;
use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::inverted::TextDocScorer;
use crate::types::TenantId;

/// One score column: its output alias and its scorer. `None` scorer: the
/// collection holds no text, so every row scores `null`.
struct ScoreColumn<'a> {
    alias: &'a str,
    scorer: Option<TextDocScorer<'a>>,
}

/// The score columns of one read.
pub(in crate::data::executor) struct ScoreColumns<'a> {
    columns: Vec<ScoreColumn<'a>>,
}

impl ScoreColumns<'_> {
    /// Write every column into each of `rows`. `surrogates` is parallel to
    /// `rows`. A row that is not an object takes none.
    pub(in crate::data::executor) fn inject(
        &self,
        surrogates: &[Surrogate],
        rows: &mut [DocumentRow],
    ) -> crate::Result<()> {
        for column in &self.columns {
            let scores = match &column.scorer {
                Some(scorer) => scorer.score(surrogates)?,
                None => vec![DocScore::Absent; surrogates.len()],
            };
            for (row, score) in rows.iter_mut().zip(scores) {
                if let serde_json::Value::Object(map) = &mut row.data {
                    map.insert(column.alias.to_string(), score_value(score));
                }
            }
        }
        Ok(())
    }
}

/// The JSON value of a score: its number, `0.0` for a held miss, `null` for
/// a row the index does not hold.
pub(in crate::data::executor) fn score_value(score: DocScore) -> serde_json::Value {
    let number = match score {
        DocScore::Match(value) => Some(value),
        DocScore::Miss => Some(0.0),
        DocScore::Absent => None,
    };
    number
        .and_then(|value| serde_json::Number::from_f64(f64::from(value)))
        .map_or(serde_json::Value::Null, serde_json::Value::Number)
}

impl CoreLoop {
    /// The score columns of `specs`, in order. `eligible` is the set of rows
    /// the reading query admits.
    pub(in crate::data::executor) fn text_score_columns<'a>(
        &'a self,
        task: &ExecutionTask,
        tid: u64,
        collection: &'a str,
        specs: &'a [TextScoreSpec],
        eligible: Option<&SurrogateBitmap>,
    ) -> crate::Result<ScoreColumns<'a>> {
        let database_id = task.request.database_id.as_u64();
        let tenant = TenantId::new(tid);
        let mut columns = Vec::with_capacity(specs.len());
        for spec in specs {
            let scorer = match self.text_index(task, tid, collection, spec.field.as_deref())? {
                None => None,
                Some(index) => {
                    let staged = self.text_staged_view(task, tid, index)?;
                    Some(self.inverted.doc_scorer(
                        database_id,
                        tenant,
                        index,
                        TextQuery {
                            query: &spec.query,
                            fuzzy_enabled: spec.fuzzy,
                            mode: QueryMode::from(spec.mode),
                        },
                        eligible,
                        staged,
                    )?)
                }
            };
            columns.push(ScoreColumn {
                alias: &spec.alias,
                scorer,
            });
        }
        Ok(ScoreColumns { columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_match_scores_a_held_miss_scores_zero_and_an_unheld_row_is_null() {
        assert_eq!(score_value(DocScore::Match(1.5)), serde_json::json!(1.5));
        assert_eq!(score_value(DocScore::Miss), serde_json::json!(0.0));
        assert_eq!(score_value(DocScore::Absent), serde_json::Value::Null);
    }
}
