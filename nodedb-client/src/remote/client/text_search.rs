// SPDX-License-Identifier: Apache-2.0

//! Remote full-text search over pgwire.
//!
//! The search is one statement:
//! `SELECT id, bm25_score(f, q[, opts]) AS score FROM c
//! WHERE text_match(f, q[, opts]) [AND id IN (...)] ORDER BY score DESC LIMIT k`.
//!
//! `opts` are the named options `mode => 'and'` and `fuzzy => true` for each
//! `params` value that differs from [`TextSearchParams::default`]. The server
//! runs that default for an omitted option. A phrase query (`"..."`) takes no
//! option on the server, so a default-params phrase search renders none.

use std::collections::HashSet;

use nodedb_types::error::NodeDbResult;
use nodedb_types::result::SearchResult;
use nodedb_types::text_search::TextSearchParams;

use crate::row_decode::search_hit::ID_COLUMN;
use crate::row_decode::{HitSource, decode_search_hits};
use crate::sql_escape::{quote_identifier, quote_string_literal};

use super::core::NodeDbRemote;

/// Output column holding the BM25 score.
const SCORE_COLUMN: &str = "score";

impl NodeDbRemote {
    pub(super) async fn text_search_impl(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        if allowed_ids.is_some_and(HashSet::is_empty) || top_k == 0 {
            return Ok(Vec::new());
        }
        let sql = text_search_sql(collection, field, query, top_k, &params, allowed_ids);
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        decode_search_hits(&text_hit_source(collection), &columns, &rows)
    }
}

/// The hit rows of a text search of `collection`.
fn text_hit_source(collection: &str) -> HitSource<'_> {
    HitSource {
        op: "text_search",
        collection,
        score_column: SCORE_COLUMN,
    }
}

/// The named options of `params` that differ from the default, each led by
/// `, `. Empty when `params` is the default.
fn text_search_options(params: &TextSearchParams) -> String {
    let defaults = TextSearchParams::default();
    let mut options = String::new();
    if params.mode != defaults.mode {
        options.push_str(&format!(", mode => '{}'", params.mode.as_str()));
    }
    if params.fuzzy != defaults.fuzzy {
        options.push_str(&format!(", fuzzy => {}", params.fuzzy));
    }
    options
}

/// The search statement. Every user string is a quoted literal or identifier.
fn text_search_sql(
    collection: &str,
    field: &str,
    query: &str,
    top_k: usize,
    params: &TextSearchParams,
    allowed_ids: Option<&HashSet<String>>,
) -> String {
    let target = text_search_target(field);
    let q = quote_string_literal(query);
    let opts = text_search_options(params);
    let allowed = match allowed_ids {
        Some(ids) => {
            let mut ids: Vec<&String> = ids.iter().collect();
            ids.sort();
            let list: Vec<String> = ids
                .iter()
                .map(|id| quote_string_literal(id.as_str()))
                .collect();
            format!(" AND {ID_COLUMN} IN ({})", list.join(", "))
        }
        None => String::new(),
    };
    format!(
        "SELECT {ID_COLUMN}, bm25_score({target}, {q}{opts}) AS {SCORE_COLUMN} \
         FROM {} WHERE text_match({target}, {q}{opts}){allowed} \
         ORDER BY {SCORE_COLUMN} DESC LIMIT {top_k}",
        quote_identifier(collection)
    )
}

/// The first argument of `text_match` / `bm25_score`: the quoted column, or
/// `*` (the whole-document index) when `field` is empty.
fn text_search_target(field: &str) -> String {
    if field.is_empty() {
        "*".to_string()
    } else {
        quote_identifier(field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::text_search::QueryMode;
    use nodedb_types::value::Value;

    #[test]
    fn an_empty_field_searches_the_whole_document() {
        assert_eq!(text_search_target(""), "*");
        assert_eq!(text_search_target("body"), "\"body\"");
        assert_eq!(text_search_target("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn the_statement_ranks_best_first_before_the_limit() {
        let sql = text_search_sql(
            "docs",
            "body",
            "it's",
            5,
            &TextSearchParams::default(),
            None,
        );
        assert_eq!(
            sql,
            "SELECT id, bm25_score(\"body\", 'it''s') AS score FROM \"docs\" \
             WHERE text_match(\"body\", 'it''s') ORDER BY score DESC LIMIT 5"
        );
    }

    #[test]
    fn allowed_ids_restrict_the_candidates() {
        let ids: HashSet<String> = ["b", "a'"].iter().map(|s| s.to_string()).collect();
        let sql = text_search_sql(
            "docs",
            "body",
            "q",
            3,
            &TextSearchParams::default(),
            Some(&ids),
        );
        assert!(
            sql.contains("WHERE text_match(\"body\", 'q') AND id IN ('a''', 'b') ORDER BY"),
            "{sql}"
        );
    }

    #[test]
    fn default_params_render_no_option() {
        assert_eq!(text_search_options(&TextSearchParams::default()), "");
    }

    #[test]
    fn non_default_params_render_on_both_calls() {
        let params = TextSearchParams {
            mode: QueryMode::And,
            fuzzy: true,
        };
        assert_eq!(
            text_search_options(&params),
            ", mode => 'and', fuzzy => true"
        );
        let sql = text_search_sql("docs", "body", "q", 3, &params, None);
        assert!(
            sql.contains("bm25_score(\"body\", 'q', mode => 'and', fuzzy => true) AS score"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE text_match(\"body\", 'q', mode => 'and', fuzzy => true) ORDER"),
            "{sql}"
        );
        let fuzzy_only = TextSearchParams {
            mode: QueryMode::Or,
            fuzzy: true,
        };
        assert_eq!(text_search_options(&fuzzy_only), ", fuzzy => true");
    }

    #[test]
    fn hits_decode_from_the_score_column() {
        let rows = vec![vec![
            Value::String("0.5".into()),
            Value::String("d1".into()),
        ]];
        let names = vec!["score".to_string(), "id".to_string()];
        let hits = decode_search_hits(&text_hit_source("c"), &names, &rows).expect("decode");
        assert_eq!(hits[0].id, "d1");
        assert_eq!(hits[0].distance, 0.5);
        let err = decode_search_hits(&text_hit_source("c"), &names[1..], &rows)
            .expect_err("no score column");
        assert!(err.to_string().contains("'score'"), "{err}");
    }
}
