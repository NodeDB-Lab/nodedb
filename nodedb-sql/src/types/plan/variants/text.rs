// SPDX-License-Identifier: Apache-2.0

//! Full-text search plan payload.

use nodedb_types::text_search::QueryMode;

use crate::fts_types::FtsQuery;
use crate::types::filter::Filter;
use crate::types::query::Projection;

/// One `bm25_score(column, query)` the SELECT list or ORDER BY reads.
#[derive(Debug, Clone, PartialEq)]
pub struct TextScoreColumn {
    /// `None` for `bm25_score(*, q)`: the whole-document index.
    pub field: Option<String>,
    pub query: String,
    /// `mode => 'and' | 'or'` of the call.
    pub mode: QueryMode,
    /// `fuzzy => true | false` of the call.
    pub fuzzy: bool,
    /// Output column the score lands in.
    pub alias: String,
}

/// What a text search returns.
#[derive(Debug, Clone)]
pub enum TextSearchShape {
    /// `WHERE text_match(column, q)`: matching rows, best first.
    /// `top_k: None` returns every match.
    Match {
        /// `None` for `text_match(*, q)`: the whole-document index.
        field: Option<String>,
        /// The query. A `Plain` query carries the call's `fuzzy` option.
        query: FtsQuery,
        /// `mode => 'and' | 'or'` of the call.
        mode: QueryMode,
        top_k: Option<usize>,
    },
    /// `bm25_score(...)` with no `text_match`: every row the filters admit.
    ScoreScan,
}

/// Payload of [`SqlPlan::TextSearch`](crate::types::SqlPlan::TextSearch).
#[derive(Debug, Clone)]
pub struct TextSearchPlan {
    pub collection: String,
    pub shape: TextSearchShape,
    /// Residual WHERE predicates. They restrict candidates before ranking.
    pub filters: Vec<Filter>,
    /// Score columns. Empty when no `bm25_score` is read.
    pub scores: Vec<TextScoreColumn>,
    /// Resolved SELECT target list, for output-schema derivation.
    pub projection: Vec<Projection>,
}

impl TextSearchPlan {
    /// Add `column` unless a score column of the same alias exists.
    pub fn add_score(&mut self, column: TextScoreColumn) {
        if !self.scores.iter().any(|s| s.alias == column.alias) {
            self.scores.push(column);
        }
    }
}
