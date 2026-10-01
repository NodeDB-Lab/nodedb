// SPDX-License-Identifier: Apache-2.0

//! Graph operation implementations for `NodeDbRemote`.

use std::collections::HashMap;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::EdgeFilter;
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{SubGraph, SubGraphEdge, SubGraphNode};
use nodedb_types::value::Value;

use super::super::parse::parse_graph_traverse_json;
use super::core::NodeDbRemote;
use crate::sql_escape::quote_string_literal;

impl NodeDbRemote {
    pub(super) async fn graph_traverse_impl(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        direction: nodedb_types::graph::Direction,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let sql = build_graph_traverse_sql(collection, start, depth, direction, edge_filter);

        let (columns, rows) = self.simple_query_raw(&sql).await?;

        if columns.len() == 1 && columns[0] == "result" {
            if let Some(row) = rows.first()
                && let Some(Value::String(json_text)) = row.first()
            {
                return parse_graph_traverse_json(json_text);
            }
            return Ok(SubGraph::empty());
        }

        // Structured: node_id, depth, edge_src, edge_dst, edge_label columns.
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut seen_nodes = std::collections::HashSet::new();

        for row in &rows {
            let node_id_str = row.first().and_then(|v| v.as_str()).unwrap_or("");
            let d = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u8;

            if seen_nodes.insert(node_id_str.to_string()) {
                nodes.push(SubGraphNode {
                    id: NodeId::from_validated(node_id_str.to_owned()),
                    depth: d,
                    properties: HashMap::new(),
                });
            }

            if let (Some(src), Some(dst), Some(label)) = (
                row.get(2).and_then(|v| v.as_str()),
                row.get(3).and_then(|v| v.as_str()),
                row.get(4).and_then(|v| v.as_str()),
            ) {
                edges.push(SubGraphEdge {
                    id: EdgeId::try_first(
                        NodeId::from_validated(src.to_owned()),
                        NodeId::from_validated(dst.to_owned()),
                        label,
                    )
                    .expect("server wire label already validated"),
                    from: NodeId::from_validated(src.to_owned()),
                    to: NodeId::from_validated(dst.to_owned()),
                    label: label.to_string(),
                    properties: HashMap::new(),
                });
            }
        }

        Ok(SubGraph { nodes, edges })
    }

    pub(super) async fn graph_insert_edge_impl(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        // `GRAPH INSERT EDGE IN '<collection>' FROM '<src>' TO '<dst>'
        // TYPE '<label>' [PROPERTIES '<json>']`. The DSL handler lives
        // in `pgwire/ddl/graph_ops/edge.rs` and registers the edge in
        // the collection's graph overlay. simple_query is required
        // because the DSL doesn't fit the extended-query row-description
        // shape (it returns CommandComplete only).
        let props_clause = match properties {
            Some(d) => {
                let json = sonic_rs::to_string(&d)
                    .map_err(|e| NodeDbError::storage(format!("properties serialization: {e}")))?;
                format!(" PROPERTIES {}", quote_string_literal(&json))
            }
            None => String::new(),
        };
        let coll = quote_string_literal(collection);
        let from_s = quote_string_literal(from.as_str());
        let to_s = quote_string_literal(to.as_str());
        let label_s = quote_string_literal(edge_type);
        let sql = format!(
            "GRAPH INSERT EDGE IN {coll} FROM {from_s} TO {to_s} TYPE {label_s}{props_clause}"
        );

        self.simple_query_raw(&sql).await?;

        EdgeId::try_first(from.clone(), to.clone(), edge_type)
            .map_err(|e| NodeDbError::storage(format!("invalid edge label: {e}")))
    }

    pub(super) async fn graph_delete_edge_impl(
        &self,
        collection: &str,
        edge_id: &EdgeId,
    ) -> NodeDbResult<()> {
        // `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>'
        // TYPE '<label>'`. The (src, dst, label) tuple identifies a
        // unique edge in the overlay — `seq` is part of `EdgeId` for
        // multi-edge disambiguation, but the server DSL keys on the
        // tuple alone today, so we drop seq here.
        let coll = quote_string_literal(collection);
        let src = quote_string_literal(edge_id.src.as_str());
        let dst = quote_string_literal(edge_id.dst.as_str());
        let label = quote_string_literal(&edge_id.label);
        let sql = format!("GRAPH DELETE EDGE IN {coll} FROM {src} TO {dst} TYPE {label}");
        self.simple_query_raw(&sql).await?;
        Ok(())
    }

    pub(super) async fn graph_stats_impl(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>> {
        let sql = build_graph_stats_sql(collection, as_of);
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        parse_graph_stats_response(&columns, &rows)
    }

    pub(super) async fn graph_pagerank_impl(
        &self,
        collection: &str,
        personalization: Option<&std::collections::HashMap<String, f64>>,
        damping: Option<f64>,
        max_iterations: Option<u32>,
    ) -> NodeDbResult<Vec<(String, f64)>> {
        let sql = crate::graph_dsl::build_pagerank_sql(
            collection,
            personalization,
            damping,
            max_iterations,
        )?;
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        crate::graph_dsl::parse_pagerank_rows(&columns, &rows)
    }

    pub(super) async fn graph_shortest_path_impl(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        // Use the server's `GRAPH PATH` operator instead of the trait
        // default's per-hop BFS — one round-trip vs O(path_length).
        // Like `graph_traverse`, the path is scoped to `collection`: the server
        // authorizes against it and walks only that collection's edges.
        let label_clause = edge_filter
            .and_then(|f| f.labels.first())
            .map(|l| format!(" LABEL {}", quote_string_literal(l)))
            .unwrap_or_default();
        let collection_lit = quote_string_literal(collection);
        let from_s = quote_string_literal(from.as_str());
        let to_s = quote_string_literal(to.as_str());
        let sql = format!(
            "GRAPH PATH IN {collection_lit} FROM {from_s} TO {to_s} \
             MAX_DEPTH {max_depth}{label_clause}"
        );

        let (_columns, rows) = self.simple_query_raw(&sql).await?;
        // Server emits a single `result` column carrying a JSON array
        // of node ids — empty array means unreachable.
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let Some(Value::String(json_text)) = row.first() else {
            return Ok(None);
        };
        let parsed: Vec<String> = sonic_rs::from_str(json_text)
            .map_err(|e| NodeDbError::storage(format!("graph shortest path response: {e}")))?;
        if parsed.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            parsed
                .into_iter()
                .map(NodeId::from_validated)
                .collect::<Vec<_>>(),
        ))
    }
}

/// Build a collection-scoped traversal with escaped string literals.
pub(crate) fn build_graph_traverse_sql(
    collection: &str,
    start: &NodeId,
    depth: u8,
    direction: nodedb_types::graph::Direction,
    edge_filter: Option<&EdgeFilter>,
) -> String {
    let label_clause = edge_filter
        .and_then(|filter| filter.labels.first())
        .map(|label| format!(" LABEL {}", quote_string_literal(label)))
        .unwrap_or_default();
    let collection_lit = quote_string_literal(collection);
    let start_lit = quote_string_literal(start.as_str());
    let direction = direction.as_str();
    format!(
        "GRAPH TRAVERSE IN {collection_lit} FROM {start_lit} DEPTH {depth} DIRECTION {direction}{label_clause}"
    )
}

/// Build the SQL for `SHOW GRAPH STATS`. Collection and `as_of` are both
/// optional; when absent the corresponding clause is omitted entirely.
pub(crate) fn build_graph_stats_sql(collection: Option<&str>, as_of: Option<i64>) -> String {
    let mut sql = "SHOW GRAPH STATS".to_string();
    if let Some(c) = collection {
        sql.push(' ');
        sql.push_str(&quote_string_literal(c));
    }
    if let Some(ms) = as_of {
        sql.push_str(&format!(" AS OF SYSTEM TIME {ms}"));
    }
    sql
}

/// Parse a multi-row response from `SHOW GRAPH STATS` into a vec of
/// `GraphStats`. Empty rows produce an empty vec. Column-shape mismatches
/// are surfaced as errors — no silent fallbacks.
pub(crate) fn parse_graph_stats_response(
    columns: &[String],
    rows: &[Vec<nodedb_types::value::Value>],
) -> NodeDbResult<Vec<GraphStats>> {
    GraphStats::parse_show_stats_response(columns, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::value::Value;

    #[test]
    fn traverse_sql_preserves_direction_and_escapes_literals() {
        let start = NodeId::try_new("seed'one").unwrap();
        let filter = EdgeFilter::labels(["it's"]);
        for direction in [
            nodedb_types::graph::Direction::Out,
            nodedb_types::graph::Direction::In,
            nodedb_types::graph::Direction::Both,
        ] {
            let sql = build_graph_traverse_sql("it's", &start, 3, direction, Some(&filter));
            assert_eq!(
                sql,
                format!(
                    "GRAPH TRAVERSE IN 'it''s' FROM 'seed''one' DEPTH 3 DIRECTION {} LABEL 'it''s'",
                    direction.as_str()
                )
            );
        }
    }

    fn stat_columns() -> Vec<String> {
        GraphStats::EXPECTED_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn parse_multi_row_returns_all_entries() {
        let columns = stat_columns();
        let labels_a = r#"[{"label":"KNOWS","count":3}]"#;
        let labels_b = r#"[{"label":"OWNS","count":7}]"#;
        let rows = vec![
            vec![
                Value::String("alpha".into()),
                Value::Integer(5),
                Value::Integer(3),
                Value::Integer(1),
                Value::String(labels_a.into()),
            ],
            vec![
                Value::String("beta".into()),
                Value::Integer(9),
                Value::Integer(7),
                Value::Integer(1),
                Value::String(labels_b.into()),
            ],
        ];
        let result = parse_graph_stats_response(&columns, &rows).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].collection, "alpha");
        assert_eq!(result[0].edge_count, 3);
        assert_eq!(result[1].collection, "beta");
        assert_eq!(result[1].edge_count, 7);
    }

    #[test]
    fn parse_empty_rows_returns_empty_vec() {
        let columns = stat_columns();
        let result = parse_graph_stats_response(&columns, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_wrong_columns_errors() {
        let columns = vec!["id".to_string(), "count".to_string()];
        let err = parse_graph_stats_response(&columns, &[]).unwrap_err();
        assert!(err.to_string().contains("unexpected columns"));
    }

    #[test]
    fn parse_no_columns_no_rows_returns_empty_vec() {
        let result = parse_graph_stats_response(&[], &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn build_sql_collection_only() {
        let sql = build_graph_stats_sql(Some("social"), None);
        assert_eq!(sql, "SHOW GRAPH STATS 'social'");
    }

    #[test]
    fn build_sql_tenant_wide() {
        let sql = build_graph_stats_sql(None, None);
        assert_eq!(sql, "SHOW GRAPH STATS");
    }

    #[test]
    fn build_sql_as_of() {
        let sql = build_graph_stats_sql(Some("social"), Some(1_700_000_000_000));
        assert_eq!(
            sql,
            "SHOW GRAPH STATS 'social' AS OF SYSTEM TIME 1700000000000"
        );
    }

    #[test]
    fn build_sql_tenant_wide_as_of() {
        let sql = build_graph_stats_sql(None, Some(42));
        assert_eq!(sql, "SHOW GRAPH STATS AS OF SYSTEM TIME 42");
    }

    #[test]
    fn build_sql_escapes_single_quotes() {
        let sql = build_graph_stats_sql(Some("it's"), None);
        assert_eq!(sql, "SHOW GRAPH STATS 'it''s'");
    }
}
