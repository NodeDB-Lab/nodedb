// SPDX-License-Identifier: Apache-2.0

//! Text search parameter types shared across the NodeDb trait boundary.
//!
//! These are the user-facing knobs for full-text search queries. The
//! implementation (BM25 scoring, BMW pruning, fuzzy matching) lives in
//! `nodedb-fts`. These types are in `nodedb-types` so both `nodedb-client`
//! (trait definition) and all implementations can use them without pulling
//! in the full FTS engine as a dependency.

use serde::{Deserialize, Serialize};

/// Boolean query mode for full-text search.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum QueryMode {
    /// Any query term can match (union). Most permissive — best recall.
    #[default]
    Or,
    /// All query terms must match (intersection). More precise — best precision.
    And,
}

impl QueryMode {
    /// The SQL spelling of the mode: `'or'` or `'and'`, as
    /// `text_match(col, 'q', mode => 'or')` takes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Or => "or",
            Self::And => "and",
        }
    }

    /// Parse the SQL spelling, ignoring ASCII case. `None` names no mode.
    pub fn parse(text: &str) -> Option<Self> {
        if text.eq_ignore_ascii_case("or") {
            Some(Self::Or)
        } else if text.eq_ignore_ascii_case("and") {
            Some(Self::And)
        } else {
            None
        }
    }
}

/// BM25 ranking parameters.
///
/// Controls how term frequency and document length affect scoring.
/// The defaults (`k1 = 1.2`, `b = 0.75`) are standard Okapi BM25 values
/// that work well across most corpora.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bm25Params {
    /// Term frequency saturation factor.
    /// Higher values give more weight to repeated terms. Range: 0.5–3.0.
    /// Default: 1.2.
    pub k1: f32,
    /// Length normalization factor.
    /// `0.0` = no length normalization, `1.0` = full normalization.
    /// Default: 0.75.
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Per-query parameters for full-text search.
///
/// These are the knobs that vary per-query. BM25 scoring parameters (`k1`, `b`)
/// are corpus-level settings configured at collection creation time — they depend
/// on document characteristics (length, vocabulary), not on individual queries.
///
/// Pass [`TextSearchParams::default()`] for standard OR-mode non-fuzzy search.
/// SQL `text_match` / `bm25_score` with no options run the same defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextSearchParams {
    /// Boolean query mode: `Or` (any term) or `And` (all terms).
    /// Default: `Or`.
    pub mode: QueryMode,
    /// Enable fuzzy (Levenshtein distance) matching for approximate lookup.
    /// Fuzzy hits are scored with a discount relative to exact matches.
    /// Default: `false`.
    pub fuzzy: bool,
}

impl Default for TextSearchParams {
    fn default() -> Self {
        Self {
            mode: QueryMode::Or,
            fuzzy: false,
        }
    }
}

/// Why a column cannot serve a full-text search.
///
/// Carried by the planner error, the Data-Plane error code, and the server
/// error, so each surface renders the same SQLSTATE through [`Self::sqlstate`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum TextColumnFault {
    /// The strict schema declares no column of that name.
    #[error("is not a column of the collection")]
    Undeclared,
    /// The column is declared with a non-text type.
    #[error("has type {data_type}, not text")]
    NotText { data_type: String },
    /// The argument is an expression or literal, not a column reference.
    #[error("is not a column reference; name a text column or use *")]
    NotAColumn,
    /// No document of the collection holds text under that field, while
    /// other fields hold text.
    #[error("holds no indexed text in any document")]
    NotIndexed,
}

impl TextColumnFault {
    /// SQLSTATE of the fault: `42703` (undefined_column) for a column that
    /// does not exist as text, `42804` (datatype_mismatch) for an argument
    /// that is not a text column.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Undeclared | Self::NotIndexed => crate::error::sqlstate::UNDEFINED_COLUMN,
            Self::NotText { .. } | Self::NotAColumn => crate::error::sqlstate::DATATYPE_MISMATCH,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_mode_sql_spelling_round_trips() {
        for mode in [QueryMode::Or, QueryMode::And] {
            assert_eq!(QueryMode::parse(mode.as_str()), Some(mode));
        }
        assert_eq!(QueryMode::parse("AND"), Some(QueryMode::And));
        assert_eq!(QueryMode::parse("xor"), None);
    }

    #[test]
    fn query_mode_round_trips_through_msgpack() {
        for mode in [QueryMode::Or, QueryMode::And] {
            let bytes = zerompk::to_msgpack_vec(&mode).expect("encode");
            let back: QueryMode = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(back, mode);
        }
    }

    #[test]
    fn faults_map_to_their_sqlstate() {
        assert_eq!(TextColumnFault::Undeclared.sqlstate(), "42703");
        assert_eq!(TextColumnFault::NotIndexed.sqlstate(), "42703");
        assert_eq!(TextColumnFault::NotAColumn.sqlstate(), "42804");
        let not_text = TextColumnFault::NotText {
            data_type: "INT".into(),
        };
        assert_eq!(not_text.sqlstate(), "42804");
        assert_eq!(not_text.to_string(), "has type INT, not text");
    }
}
