// SPDX-License-Identifier: Apache-2.0

//! `GRAPH ALGO PAGERANK` statement builder and result decoder.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use crate::sql_escape::quote_string_literal;

/// Build the `GRAPH ALGO PAGERANK …` DSL statement.
///
/// Emits optional `ITERATIONS`, `DAMPING`, and `PERSONALIZATION {…}` clauses.
/// The personalization map is serialized as a JSON `node_id → weight` object
/// literal, which the server tokenizer captures as a single object token.
pub(crate) fn build_pagerank_sql(
    collection: &str,
    personalization: Option<&HashMap<String, f64>>,
    damping: Option<f64>,
    max_iterations: Option<u32>,
) -> NodeDbResult<String> {
    let mut sql = format!(
        "GRAPH ALGO PAGERANK ON {}",
        quote_string_literal(collection)
    );
    if let Some(iters) = max_iterations {
        sql.push_str(&format!(" ITERATIONS {iters}"));
    }
    if let Some(d) = damping {
        sql.push_str(&format!(" DAMPING {d}"));
    }
    if let Some(p) = personalization.filter(|p| !p.is_empty()) {
        let json = sonic_rs::to_string(p)
            .map_err(|e| NodeDbError::storage(format!("personalization serialization: {e}")))?;
        sql.push_str(&format!(" PERSONALIZATION {json}"));
    }
    Ok(sql)
}

/// Parse a `(columns, rows)` PageRank result into `(node_id, rank)` pairs.
///
/// The result schema is `node_id` (text) + `rank` (float). `rank` may arrive
/// as a float (native protocol) or as a text-encoded number (pgwire), so both
/// are accepted. An empty result with no columns (empty graph) yields an empty
/// vec; any other column-shape mismatch is a structured error — no silent
/// fallback to wrong data.
pub(crate) fn parse_pagerank_rows(
    columns: &[String],
    rows: &[Vec<Value>],
) -> NodeDbResult<Vec<(String, f64)>> {
    let node_idx = columns.iter().position(|c| c == "node_id");
    let rank_idx = columns.iter().position(|c| c == "rank");
    let (Some(ni), Some(ri)) = (node_idx, rank_idx) else {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        return Err(NodeDbError::storage(format!(
            "unexpected pagerank columns: {columns:?} (expected [node_id, rank])"
        )));
    };

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let node = row
            .get(ni)
            .and_then(|v| v.as_str())
            .ok_or_else(|| NodeDbError::storage("pagerank row missing node_id"))?
            .to_string();
        let rank = row
            .get(ri)
            .and_then(value_as_f64)
            .ok_or_else(|| NodeDbError::storage("pagerank row missing or non-numeric rank"))?;
        out.push((node, rank));
    }
    Ok(out)
}

/// Coerce a result cell to `f64`, accepting either a native numeric value or a
/// text-encoded number (the pgwire transport renders floats as text).
fn value_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_sql_minimal() {
        let sql = build_pagerank_sql("social", None, None, None).unwrap();
        assert_eq!(sql, "GRAPH ALGO PAGERANK ON 'social'");
    }

    #[test]
    fn build_sql_with_iterations_and_damping() {
        let sql = build_pagerank_sql("social", None, Some(0.85), Some(30)).unwrap();
        assert_eq!(
            sql,
            "GRAPH ALGO PAGERANK ON 'social' ITERATIONS 30 DAMPING 0.85"
        );
    }

    #[test]
    fn build_sql_escapes_collection() {
        let sql = build_pagerank_sql("it's", None, None, None).unwrap();
        assert_eq!(sql, "GRAPH ALGO PAGERANK ON 'it''s'");
    }

    #[test]
    fn build_sql_with_personalization() {
        let mut seed = HashMap::new();
        seed.insert("alice".to_string(), 1.0);
        let sql = build_pagerank_sql("social", Some(&seed), None, None).unwrap();
        assert_eq!(
            sql,
            r#"GRAPH ALGO PAGERANK ON 'social' PERSONALIZATION {"alice":1.0}"#
        );
    }

    #[test]
    fn empty_personalization_omits_clause() {
        let seed = HashMap::new();
        let sql = build_pagerank_sql("social", Some(&seed), None, None).unwrap();
        assert_eq!(sql, "GRAPH ALGO PAGERANK ON 'social'");
    }

    #[test]
    fn parse_rows_numeric_and_text_rank() {
        let columns = vec!["node_id".to_string(), "rank".to_string()];
        let rows = vec![
            vec![Value::String("a".into()), Value::Float(0.5)],
            vec![Value::String("b".into()), Value::String("0.25".into())],
        ];
        let parsed = parse_pagerank_rows(&columns, &rows).unwrap();
        assert_eq!(parsed[0], ("a".to_string(), 0.5));
        assert_eq!(parsed[1], ("b".to_string(), 0.25));
    }

    #[test]
    fn parse_empty_is_ok() {
        assert!(parse_pagerank_rows(&[], &[]).unwrap().is_empty());
    }

    #[test]
    fn parse_wrong_columns_errors() {
        let columns = vec!["foo".to_string()];
        let rows = vec![vec![Value::String("a".into())]];
        assert!(parse_pagerank_rows(&columns, &rows).is_err());
    }
}
