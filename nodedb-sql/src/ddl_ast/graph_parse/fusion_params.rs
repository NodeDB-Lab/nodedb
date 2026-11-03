// SPDX-License-Identifier: Apache-2.0

//! Shared parameter extraction for graph-vector fusion SQL surfaces.
//!
//! Two syntaxes reach the same `GraphOp::RagFusion` executor:
//!
//! - `GRAPH RAG FUSION ON <col> QUERY ARRAY[...] ...` (DSL form)
//! - `SEARCH <col> USING FUSION(ARRAY[...] ...)` (wrapped form)
//!
//! They use different keyword aliases for the same parameters
//! (`EXPANSION_DEPTH` vs `DEPTH`, `EDGE_LABEL` vs `LABEL`, `FINAL_TOP_K`
//! vs `TOP`). Both read into the same typed bag through one left-to-right
//! clause reader, so an unknown or misspelled parameter is refused by name.

use super::super::statement::GraphDirection;
use super::cursor::{Cursor, set_once};
use super::tokenizer::{Tok, blank_comments, tokenize};
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
    /// Read the fusion clauses left to right until the end of the statement.
    ///
    /// A keyword outside `kw` is refused by name, a clause given twice is
    /// refused, and a value that does not read as its type is refused.
    pub(super) fn parse(c: &mut Cursor<'_, '_>, kw: &FusionKeywords) -> Result<Self, SqlError> {
        let stmt = c.statement();
        let mut p = Self::default();
        while let Some(clause) = c.next_clause()? {
            let k = clause.as_str();
            if k == kw.query_anchor {
                if kw.query_anchor != "ARRAY" && !c.eat_keyword("ARRAY") {
                    return Err(SqlError::Parse {
                        detail: format!("{k} needs ARRAY[...] in {stmt}"),
                    });
                }
                let vector = parse_vector(c.list(k)?, k)?;
                set_once(&mut p.query_vector, vector, k, stmt)?;
            } else if k == kw.vector_top_k {
                set_once(&mut p.vector_top_k, c.usize(k)?, k, stmt)?;
            } else if k == kw.expansion_depth {
                set_once(&mut p.expansion_depth, c.usize(k)?, k, stmt)?;
            } else if k == kw.edge_label {
                set_once(&mut p.edge_label, c.value(k)?, k, stmt)?;
            } else if k == kw.final_top_k {
                set_once(&mut p.final_top_k, c.usize(k)?, k, stmt)?;
            } else if k == kw.rrf_k {
                if p.rrf_k.is_some() || p.rrf_k_triple.is_some() {
                    return Err(SqlError::Parse {
                        detail: format!("{k} given more than once in {stmt}"),
                    });
                }
                match c.number_list(k)?.as_slice() {
                    [vector_k, graph_k] => p.rrf_k = Some((*vector_k, *graph_k)),
                    [vector_k, text_k, graph_k] => {
                        p.rrf_k_triple = Some((*vector_k, *text_k, *graph_k));
                    }
                    _ => {
                        return Err(SqlError::Parse {
                            detail: format!("{k} needs two or three numbers, as {k} (60.0, 35.0)"),
                        });
                    }
                }
            } else if k == kw.vector_field {
                set_once(&mut p.vector_field, c.value(k)?, k, stmt)?;
            } else if k == kw.direction {
                set_once(&mut p.direction, c.direction()?, k, stmt)?;
            } else if k == kw.max_visited {
                set_once(&mut p.max_visited, c.usize(k)?, k, stmt)?;
            } else if !kw.bm25_query.is_empty() && k == kw.bm25_query {
                set_once(&mut p.bm25_query, c.value(k)?, k, stmt)?;
                // The field clause belongs to the BM25 clause only when it
                // follows it directly.
                if c.eat_keyword(kw.bm25_field) {
                    set_once(
                        &mut p.bm25_field,
                        c.value(kw.bm25_field)?,
                        kw.bm25_field,
                        stmt,
                    )?;
                }
            } else {
                return Err(c.unknown_clause());
            }
        }
        Ok(p)
    }
}

/// Read the comma-separated numbers of an `ARRAY[...]` literal.
fn parse_vector(inner: &str, clause: &str) -> Result<Vec<f32>, SqlError> {
    if inner.trim().is_empty() {
        return Err(SqlError::Parse {
            detail: format!("{clause} ARRAY[...] needs one or more numbers"),
        });
    }
    inner
        .split(',')
        .map(|part| {
            let text = part.trim();
            text.parse::<f32>().map_err(|_| SqlError::Parse {
                detail: format!("{clause} ARRAY element must be a number — found '{text}'"),
            })
        })
        .collect()
}

/// Parse `SEARCH <collection> USING FUSION(...)` into its collection name
/// and a typed [`FusionParams`].
///
/// `None` means the SQL does not start with `SEARCH <collection> USING
/// FUSION`. `Some(Err)` means it does and a clause is malformed. The body
/// goes through the same quote- and bracket-aware tokenizer and the same
/// clause reader as `GRAPH RAG FUSION`, so a keyword-shaped string literal
/// (e.g. a label value `'TOP'`) cannot shadow a real parameter keyword.
pub fn parse_search_using_fusion(sql: &str) -> Option<Result<(String, FusionParams), SqlError>> {
    let sql = blank_comments(sql);
    let toks = tokenize(&sql);
    let collection = match toks.as_slice() {
        [Tok::Word(s), Tok::Word(c), Tok::Word(u), Tok::Word(f), ..]
            if s.eq_ignore_ascii_case("SEARCH")
                && u.eq_ignore_ascii_case("USING")
                && f.eq_ignore_ascii_case("FUSION") =>
        {
            (*c).to_string()
        }
        _ => return None,
    };
    let mut cursor = Cursor::new(&toks, "SEARCH USING FUSION", 4);
    Some(
        FusionParams::parse(&mut cursor, &SEARCH_FUSION_KEYWORDS)
            .map(|params| (collection, params)),
    )
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
        .expect("SEARCH USING FUSION form")
        .expect("well-formed fusion clauses");
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
        .expect("SEARCH USING FUSION form")
        .expect("well-formed fusion clauses");
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
                .expect("SEARCH USING FUSION form")
                .expect("well-formed fusion clauses");
        assert_eq!(p.edge_label.as_deref(), Some("TOP_SECRET"));
        assert_eq!(p.final_top_k, Some(7));
    }

    #[test]
    fn search_fusion_rejects_wrong_prefix() {
        assert!(parse_search_using_fusion("INSERT INTO x VALUES (1)").is_none());
        assert!(parse_search_using_fusion("SEARCH x USING VECTOR(ARRAY[1.0])").is_none());
    }

    fn search_error(sql: &str) -> String {
        parse_search_using_fusion(sql)
            .expect("SEARCH USING FUSION form")
            .expect_err("malformed fusion clauses must be refused")
            .to_string()
    }

    #[test]
    fn search_fusion_refuses_misspelled_parameter() {
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.5] TOPP 7)");
        assert!(
            error.contains("unexpected token 'TOPP' in SEARCH USING FUSION"),
            "{error}"
        );
    }

    #[test]
    fn search_fusion_refuses_non_numeric_vector_element() {
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.1, x] TOP 3)");
        assert!(error.contains("'x'"), "{error}");
    }

    #[test]
    fn search_fusion_refuses_unknown_direction() {
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.1] DIRECTION sideways)");
        assert!(error.contains("sideways"), "{error}");
    }

    #[test]
    fn search_fusion_refuses_repeated_parameter() {
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.1] TOP 3 TOP 4)");
        assert!(error.contains("TOP given more than once"), "{error}");
    }

    /// `RRF_K` numbers need commas between them, as label lists do.
    #[test]
    fn search_fusion_refuses_rrf_k_numbers_without_a_comma() {
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.1] RRF_K (60.0 35.0))");
        assert!(
            error.contains("RRF_K list items must be separated by commas: 60.0 35.0"),
            "{error}"
        );
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.1] RRF_K (60.0, 35.0,) TOP 3)");
        assert!(error.contains("RRF_K list ends with a comma"), "{error}");
        let error = search_error("SEARCH c USING FUSION(ARRAY[0.1] RRF_K (60.0) TOP 3)");
        assert!(
            error.contains("RRF_K needs two or three numbers"),
            "{error}"
        );
    }
}
