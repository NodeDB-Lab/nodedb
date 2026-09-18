// SPDX-License-Identifier: Apache-2.0

//! Shared parameter extraction for graph-vector fusion SQL surfaces.
//!
//! Two syntaxes reach the same `GraphOp::RagFusion` executor today:
//!
//! - `GRAPH RAG FUSION ON <col> QUERY ARRAY[...] ...` (DSL form)
//! - `SEARCH <col> USING FUSION(ARRAY[...] ...)` (wrapped form)
//!
//! They use different keyword aliases for the same parameters
//! (`EXPANSION_DEPTH` vs `DEPTH`, `EDGE_LABEL` vs `LABEL`, `FINAL_TOP_K`
//! vs `TOP`). Both must extract the same typed bag so future fusion
//! variants (hybrid text+vector, multi-vector, etc.) can share this
//! code and cannot silently drop parameters the way substring-find
//! parsing did.

use super::super::statement::GraphDirection;
use super::cursor::Cursor;
use super::tokenizer::{Tok, tokenize};
use crate::error::SqlError;

/// Keyword aliases for the shared fusion parameters.
///
/// Each fusion SQL surface picks one of the `*_KEYWORDS` constants below.
/// New fusion variants add their own constant rather than editing the
/// extractor.
pub struct FusionKeywords {
    pub vector_top_k: &'static str,
    pub expansion_depth: &'static str,
    pub edge_label: &'static str,
    pub final_top_k: &'static str,
    pub rrf_k: &'static str,
    pub vector_field: &'static str,
    pub direction: &'static str,
    pub max_visited: &'static str,
    /// Keyword that precedes `ARRAY[...]` in raw SQL (e.g. `QUERY` or
    /// `ARRAY` itself when there is no leading keyword).
    pub query_anchor: &'static str,
    /// Keyword that precedes the BM25 query string for three-source fusion.
    /// Empty string disables BM25 parsing for surfaces that do not support it.
    pub bm25_query: &'static str,
    /// Keyword that precedes the BM25 field name in three-source fusion.
    pub bm25_field: &'static str,
}

/// Keywords used by `GRAPH RAG FUSION ON ...`.
pub const RAG_FUSION_KEYWORDS: FusionKeywords = FusionKeywords {
    vector_top_k: "VECTOR_TOP_K",
    expansion_depth: "EXPANSION_DEPTH",
    edge_label: "EDGE_LABEL",
    final_top_k: "FINAL_TOP_K",
    rrf_k: "RRF_K",
    vector_field: "VECTOR_FIELD",
    direction: "DIRECTION",
    max_visited: "MAX_VISITED",
    query_anchor: "QUERY",
    bm25_query: "BM25",
    bm25_field: "ON",
};

/// Keywords used by `SEARCH ... USING FUSION(...)`.
pub const SEARCH_FUSION_KEYWORDS: FusionKeywords = FusionKeywords {
    vector_top_k: "VECTOR_TOP_K",
    expansion_depth: "DEPTH",
    edge_label: "LABEL",
    final_top_k: "TOP",
    rrf_k: "RRF_K",
    vector_field: "VECTOR_FIELD",
    direction: "DIRECTION",
    max_visited: "MAX_VISITED",
    query_anchor: "ARRAY",
    bm25_query: "BM25",
    bm25_field: "ON",
};

/// Typed parameter bag for every graph-vector fusion SQL surface.
///
/// All fields are optional at parse time — bounds, caps, and
/// "absent but required" errors are enforced at the pgwire boundary.
///
/// Three-source fusion (vector + text + graph) is enabled by populating
/// `bm25_query` and `bm25_field` together with `rrf_k_triple`. When only
/// `rrf_k` is set (two values), behaviour is unchanged from the two-source
/// form. When `rrf_k_triple` is set it takes precedence and the BM25 leg
/// participates in the fusion.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FusionParams {
    pub query_vector: Option<Vec<f32>>,
    pub vector_top_k: Option<usize>,
    pub expansion_depth: Option<usize>,
    pub edge_label: Option<String>,
    pub final_top_k: Option<usize>,
    /// Two-source RRF k constants: `(vector_k, graph_k)`. Used when no
    /// `bm25_query` is present (backwards-compatible two-source form).
    pub rrf_k: Option<(f64, f64)>,
    /// Three-source RRF k constants: `(vector_k, text_k, graph_k)`. Set
    /// when `RRF_K (kv, kt, kg)` is parsed and three values are found.
    pub rrf_k_triple: Option<(f64, f64, f64)>,
    pub vector_field: Option<String>,
    pub direction: Option<GraphDirection>,
    pub max_visited: Option<usize>,
    /// BM25 query string for the text leg of three-source fusion. Parsed
    /// from `BM25 'query string' ON 'field_name'` in the FUSION DSL.
    pub bm25_query: Option<String>,
    /// Document field on which BM25 scoring is applied in three-source fusion.
    pub bm25_field: Option<String>,
}

impl FusionParams {
    /// Read every fusion parameter through the cursor, so each keyword and value
    /// is claimed and a token no clause owns is left for `Cursor::finish` to
    /// refuse. A clause that is present but unreadable is an error, never a
    /// silent default.
    pub(super) fn extract(cursor: &mut Cursor<'_>, kw: &FusionKeywords) -> Result<Self, SqlError> {
        let direction = match cursor.word_after(kw.direction) {
            None => None,
            Some(word) => match word.to_ascii_uppercase().as_str() {
                "IN" => Some(GraphDirection::In),
                "BOTH" => Some(GraphDirection::Both),
                "OUT" => Some(GraphDirection::Out),
                _ => {
                    return Err(SqlError::Parse {
                        detail: format!(
                            "{} must be one of in, out, both — found '{word}'",
                            kw.direction
                        ),
                    });
                }
            },
        };

        // Three values form the RRF_K triple; two form the legacy pair. Any
        // other count is a typo, not a default.
        let rrf = cursor.floats_after_max(kw.rrf_k, 3)?;
        let (rrf_k, rrf_k_triple) = match rrf.as_deref() {
            None => (None, None),
            Some([a, b]) => (Some((*a, *b)), None),
            Some([a, b, c]) => (None, Some((*a, *b, *c))),
            Some(other) => {
                return Err(SqlError::Parse {
                    detail: format!(
                        "{} expects two or three numbers — found {}",
                        kw.rrf_k,
                        other.len()
                    ),
                });
            }
        };

        // BM25 text leg — only parsed when the keyword is non-empty. The field
        // keyword (`ON`) is read after the BM25 anchor: `ON` also introduces
        // the fusion collection, and the first match is not the field's.
        let (bm25_query, bm25_field) = if !kw.bm25_query.is_empty() {
            (
                cursor.quoted_after(kw.bm25_query),
                cursor.quoted_after_from(kw.bm25_query, kw.bm25_field),
            )
        } else {
            (None, None)
        };

        let query_vector = cursor
            .floats_array_after(kw.query_anchor)
            .map(|floats| floats.into_iter().map(|f| f as f32).collect());

        Ok(Self {
            query_vector,
            vector_top_k: cursor.usize_after_checked(kw.vector_top_k)?,
            expansion_depth: cursor.usize_after_checked(kw.expansion_depth)?,
            edge_label: cursor.quoted_after(kw.edge_label),
            final_top_k: cursor.usize_after_checked(kw.final_top_k)?,
            rrf_k,
            rrf_k_triple,
            vector_field: cursor.quoted_after(kw.vector_field),
            direction,
            max_visited: cursor.usize_after_checked(kw.max_visited)?,
            bm25_query,
            bm25_field,
        })
    }
}

/// Parse `SEARCH <collection> USING FUSION(...)` into its collection name
/// and a typed [`FusionParams`]. Returns `Ok(None)` when the SQL does not
/// match the expected shape; a matched shape whose options are unreadable is
/// an error, never a silent default.
///
/// Body extraction uses the same quote- and bracket-aware tokenizer as
/// the `GRAPH RAG FUSION` path, so a keyword-shaped string literal (e.g.
/// a label value `'TOP'`) cannot shadow a real parameter keyword.
pub fn parse_search_using_fusion(sql: &str) -> Result<Option<(String, FusionParams)>, SqlError> {
    let toks = tokenize(sql);
    let collection = match toks.as_slice() {
        [Tok::Word(s), Tok::Word(c), Tok::Word(u), Tok::Word(f), ..]
            if s.eq_ignore_ascii_case("SEARCH")
                && u.eq_ignore_ascii_case("USING")
                && f.eq_ignore_ascii_case("FUSION") =>
        {
            (*c).to_string()
        }
        _ => return Ok(None),
    };
    let mut cursor = Cursor::new(toks, 4);
    let params = FusionParams::extract(&mut cursor, &SEARCH_FUSION_KEYWORDS)?;
    cursor.finish("SEARCH ... USING FUSION")?;
    Ok(Some((collection, params)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_fusion_full_surface_parses() {
        let (col, p) = parse_search_using_fusion(
            "SEARCH mycol USING FUSION(ARRAY[0.1, 0.2] VECTOR_TOP_K 5 DEPTH 2 \
             LABEL 'related' TOP 10 RRF_K (60.0, 35.0))",
        )
        .unwrap()
        .unwrap();
        assert_eq!(col, "mycol");
        assert_eq!(p.query_vector.as_deref().map(<[f32]>::len), Some(2));
        assert_eq!(p.vector_top_k, Some(5));
        assert_eq!(p.expansion_depth, Some(2));
        assert_eq!(p.edge_label.as_deref(), Some("related"));
        assert_eq!(p.final_top_k, Some(10));
        assert_eq!(p.rrf_k, Some((60.0, 35.0)));
        assert_eq!(p.rrf_k_triple, None);
    }

    #[test]
    fn search_fusion_three_source_parses() {
        let (col, p) = parse_search_using_fusion(
            "SEARCH entities USING FUSION(ARRAY[0.1, 0.3] VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 50 BM25 'transformer attention' ON 'body' \
             DEPTH 2 LABEL 'related_to' TOP 10 RRF_K (60.0, 35.0, 50.0))",
        )
        .unwrap()
        .unwrap();
        assert_eq!(col, "entities");
        assert_eq!(p.rrf_k, None);
        assert_eq!(p.rrf_k_triple, Some((60.0, 35.0, 50.0)));
        assert_eq!(p.bm25_query.as_deref(), Some("transformer attention"));
        assert_eq!(p.bm25_field.as_deref(), Some("body"));
        assert_eq!(p.expansion_depth, Some(2));
        assert_eq!(p.edge_label.as_deref(), Some("related_to"));
        assert_eq!(p.final_top_k, Some(10));
    }

    #[test]
    fn search_fusion_label_literal_that_shadows_top_keyword() {
        // A quoted label value containing the `TOP` keyword must not be
        // misread as the `TOP` numeric parameter — the tokenizer keeps
        // quoted strings whole, so `TOP 10` is the real parameter.
        let (_, p) =
            parse_search_using_fusion("SEARCH c USING FUSION(ARRAY[0.5] LABEL 'TOP_SECRET' TOP 7)")
                .unwrap()
                .unwrap();
        assert_eq!(p.edge_label.as_deref(), Some("TOP_SECRET"));
        assert_eq!(p.final_top_k, Some(7));
    }

    #[test]
    fn search_fusion_rejects_wrong_prefix() {
        assert!(
            parse_search_using_fusion("INSERT INTO x VALUES (1)")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_search_using_fusion("SEARCH x USING VECTOR(ARRAY[1.0])")
                .unwrap()
                .is_none()
        );
    }

    /// A mistyped option keyword belongs to no clause; the cursor refuses it by
    /// name instead of running the statement with a default.
    #[test]
    fn search_fusion_refuses_a_mistyped_option_keyword() {
        let err = parse_search_using_fusion("SEARCH c USING FUSION(ARRAY[0.5] VECTOR_TOPK 5)")
            .unwrap_err();
        assert!(
            err.to_string().contains("VECTOR_TOPK"),
            "the error must name the unclaimed token: {err}"
        );
    }

    /// A value the clause cannot read is an error, never a silent default.
    #[test]
    fn search_fusion_refuses_an_unreadable_option_value() {
        let err = parse_search_using_fusion("SEARCH c USING FUSION(ARRAY[0.5] VECTOR_TOP_K abc)")
            .unwrap_err();
        assert!(
            err.to_string().contains("VECTOR_TOP_K"),
            "the error must name the clause: {err}"
        );
    }
}
