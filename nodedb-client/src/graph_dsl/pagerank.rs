// SPDX-License-Identifier: Apache-2.0

//! `GRAPH ALGO PAGERANK` statement builder and result decoder.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::id::NodeId;
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

/// Largest number of edges one batch statement may carry.
///
/// Read from the protocol bound, which the statement parser also reads, so the
/// client and the server cannot hold different numbers. Checked here before
/// building the statement so an oversized batch fails with the limit named
/// instead of a round trip and a parse error.
pub(crate) use nodedb_types::protocol::{MAX_BATCH_TEXT_BYTES, MAX_EDGES_PER_BATCH_STATEMENT};

/// Build one `GRAPH INSERT EDGES` statement carrying every edge.
///
/// The batched statement is one round trip for the whole list, which is what
/// `batch_graph_insert_edges` promises. Its server-side form is property-less:
/// `BatchEdge` carries no property object, so a caller that needs per-edge
/// properties stays on the singular statement.
///
/// A batch over [`MAX_EDGES_PER_BATCH_STATEMENT`] is refused here rather than
/// sent to be rejected.
pub(crate) fn build_insert_edges_sql(
    collection: &str,
    edges: &[(&str, &str, &str)],
) -> NodeDbResult<String> {
    build_edges_sql("INSERT", "batch_graph_insert_edges", collection, edges)
}

/// Build one `GRAPH DELETE EDGES` statement for a list of `(src, dst, label)`
/// triples. The bounds are the ones of [`build_insert_edges_sql`].
pub(crate) fn build_delete_edges_sql(
    collection: &str,
    edges: &[(&str, &str, &str)],
) -> NodeDbResult<String> {
    build_edges_sql("DELETE", "batch_graph_delete_edges", collection, edges)
}

fn build_edges_sql(
    verb: &str,
    caller: &str,
    collection: &str,
    edges: &[(&str, &str, &str)],
) -> NodeDbResult<String> {
    if edges.is_empty() {
        return Err(NodeDbError::storage(format!(
            "{caller} requires at least one edge"
        )));
    }
    if edges.len() > MAX_EDGES_PER_BATCH_STATEMENT {
        return Err(NodeDbError::storage(format!(
            "{caller} carries at most {MAX_EDGES_PER_BATCH_STATEMENT} edges per statement, got {}",
            edges.len()
        )));
    }
    let mut sql = format!(
        "GRAPH {verb} EDGES IN {} VALUES ",
        quote_string_literal(collection)
    );
    for (i, (from, to, label)) in edges.iter().enumerate() {
        for node in [from, to] {
            NodeId::try_new(*node)
                .map_err(|e| NodeDbError::storage(format!("invalid node id: {e}")))?;
        }
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({},{},{})",
            quote_string_literal(from),
            quote_string_literal(to),
            quote_string_literal(label)
        ));
    }
    // The count is not the only bound: a few edges with long node ids exceed
    // the text cap while staying far below it. Check the statement that was
    // actually built, so the client refuses the same statements the parser does.
    if sql.len() > MAX_BATCH_TEXT_BYTES {
        return Err(NodeDbError::storage(format!(
            "{caller} builds at most {MAX_BATCH_TEXT_BYTES} bytes of SQL text per statement, got {}",
            sql.len()
        )));
    }
    Ok(sql)
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

    #[test]
    fn insert_edges_sql_carries_every_triple_in_one_statement() {
        let sql = build_insert_edges_sql("c", &[("a", "b", "L"), ("b", "c", "L")]).unwrap();
        assert_eq!(
            sql,
            "GRAPH INSERT EDGES IN 'c' VALUES ('a','b','L'),('b','c','L')"
        );
        // One statement, one VALUES clause: a per-edge loop would build two.
        assert_eq!(sql.matches("VALUES").count(), 1);
        assert_eq!(sql.matches("GRAPH INSERT EDGES").count(), 1);
    }

    #[test]
    fn delete_edges_sql_carries_every_triple_in_one_statement() {
        let sql = build_delete_edges_sql("c", &[("a", "b", "L"), ("b", "c", "L")]).unwrap();
        assert_eq!(
            sql,
            "GRAPH DELETE EDGES IN 'c' VALUES ('a','b','L'),('b','c','L')"
        );
    }

    #[test]
    fn delete_edges_sql_refuses_an_empty_batch() {
        let error = build_delete_edges_sql("c", &[]).expect_err("an empty batch has nothing to do");
        assert!(
            error.to_string().contains("batch_graph_delete_edges"),
            "the error must name the caller: {error}"
        );
    }

    #[test]
    fn insert_edges_sql_escapes_every_field() {
        // A quote in any field must not break out of its literal.
        let sql = build_insert_edges_sql("c'x", &[("a'1", "b'2", "L'3")]).unwrap();
        assert!(
            sql.contains("'a''1'") && sql.contains("'b''2'") && sql.contains("'L''3'"),
            "every field must be escaped: {sql}"
        );
        assert!(
            sql.contains("'c''x'"),
            "the collection must be escaped: {sql}"
        );
    }

    #[test]
    fn insert_edges_sql_refuses_an_empty_batch() {
        let error = build_insert_edges_sql("c", &[]).expect_err("an empty batch has nothing to do");
        assert!(
            error.to_string().contains("at least one edge"),
            "the error must say why: {error}"
        );
    }

    #[test]
    fn insert_edges_sql_refuses_a_batch_over_the_cap() {
        let owned: Vec<(String, String, String)> = (0..=MAX_EDGES_PER_BATCH_STATEMENT)
            .map(|i| (format!("s{i}"), format!("d{i}"), "L".to_string()))
            .collect();
        let edges: Vec<(&str, &str, &str)> = owned
            .iter()
            .map(|(s, d, l)| (s.as_str(), d.as_str(), l.as_str()))
            .collect();
        let error = build_insert_edges_sql("c", &edges)
            .expect_err("a batch over the cap must not build a statement");
        assert!(
            error.to_string().contains("at most 1000 edges"),
            "the client must name the limit before sending: {error}"
        );
    }

    #[test]
    fn insert_edges_sql_accepts_a_batch_exactly_at_the_cap() {
        let owned: Vec<(String, String, String)> = (0..MAX_EDGES_PER_BATCH_STATEMENT)
            .map(|i| (format!("s{i}"), format!("d{i}"), "L".to_string()))
            .collect();
        let edges: Vec<(&str, &str, &str)> = owned
            .iter()
            .map(|(s, d, l)| (s.as_str(), d.as_str(), l.as_str()))
            .collect();
        let sql = build_insert_edges_sql("c", &edges).expect("the cap is a maximum");
        assert_eq!(sql.matches("VALUES").count(), 1);
    }

    #[test]
    fn insert_edges_sql_refuses_a_batch_over_the_text_cap() {
        // 1000 edges of 600-byte ids: exactly at the count cap and every id is
        // legal, but the built statement exceeds 1 MiB. The count alone would
        // let this through.
        let long = "x".repeat(600);
        let owned: Vec<(&str, &str, &str)> = (0..MAX_EDGES_PER_BATCH_STATEMENT)
            .map(|_| (long.as_str(), long.as_str(), "L"))
            .collect();
        let error = build_insert_edges_sql("c", &owned)
            .expect_err("a batch over the text cap must not build a statement");
        assert!(
            error.to_string().contains("bytes of SQL text"),
            "the client must name the text limit before sending: {error}"
        );
    }

    #[test]
    fn insert_edges_sql_refuses_an_invalid_node_id() {
        // The in-process fallback path validates node ids; the wire builder must
        // reject the same input instead of sending a statement the parser may
        // not diagnose.
        let error = build_insert_edges_sql("c", &[("a", "", "L")])
            .expect_err("an empty node id must not reach the wire");
        assert!(
            error.to_string().contains("invalid node id"),
            "the client must say which field is bad: {error}"
        );
    }
}
