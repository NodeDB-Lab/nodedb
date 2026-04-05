//! Core types for the full-text search engine.
//!
//! These types are shared between Origin (redb-backed) and Lite (in-memory)
//! deployments, ensuring identical scoring semantics across all tiers.

/// A single posting entry for a term in a document.
///
/// Records the document ID, how many times the term appears, and the
/// token positions (for phrase matching and proximity boost).
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Clone,
    Debug,
)]
pub struct Posting {
    pub doc_id: String,
    pub term_freq: u32,
    pub positions: Vec<u32>,
}

/// Boolean query mode for multi-term searches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryMode {
    /// All query terms must match (intersection). Default.
    #[default]
    And,
    /// Any query term can match (union).
    Or,
}

/// A scored search result from the inverted index.
#[derive(Debug, Clone)]
pub struct TextSearchResult {
    pub doc_id: String,
    pub score: f32,
    /// Whether any result came from fuzzy matching.
    pub fuzzy: bool,
}

/// A character-level offset of a matched term in the original text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchOffset {
    pub start: usize,
    pub end: usize,
    pub term: String,
}

/// BM25 parameters.
#[derive(Debug, Clone, Copy)]
pub struct Bm25Params {
    /// Term frequency saturation. Default: 1.2.
    pub k1: f32,
    /// Length normalization. Default: 0.75.
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_query_mode_is_and() {
        assert_eq!(QueryMode::default(), QueryMode::And);
    }

    #[test]
    fn default_bm25_params() {
        let p = Bm25Params::default();
        assert!((p.k1 - 1.2).abs() < f32::EPSILON);
        assert!((p.b - 0.75).abs() < f32::EPSILON);
    }

    #[test]
    fn posting_fields() {
        let posting = Posting {
            doc_id: "doc1".into(),
            term_freq: 3,
            positions: vec![0, 5, 12],
        };
        assert_eq!(posting.doc_id, "doc1");
        assert_eq!(posting.term_freq, 3);
        assert_eq!(posting.positions, vec![0, 5, 12]);
    }
}
