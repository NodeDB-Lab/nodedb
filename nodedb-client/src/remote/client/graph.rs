// SPDX-License-Identifier: Apache-2.0

//! Graph operation implementations for `NodeDbRemote`.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::EdgeFilter;
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::SubGraph;
use nodedb_types::value::Value;

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
        let sql = crate::graph_dsl::build_graph_traverse_sql(
            collection,
            start,
            depth,
            direction,
            edge_filter,
        )?;
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        crate::graph_dsl::decode_traverse_result(&columns, &rows)
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
        // The property object is the document's fields, as the native client
        // sends them. The document id is not an edge property.
        let props_clause = match properties {
            Some(d) => {
                let object = serde_json::Value::from(Value::Object(d.fields));
                let json = sonic_rs::to_string(&object)
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
        // The server's `GRAPH PATH` answers in one round trip. The path is
        // scoped to `collection`: the server authorizes against it and walks
        // only that collection's edges.
        let sql =
            crate::graph_dsl::build_graph_path_sql(collection, from, to, max_depth, edge_filter)?;
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        crate::graph_dsl::decode_path_result(&columns, &rows)
    }
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
