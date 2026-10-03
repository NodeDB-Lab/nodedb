// SPDX-License-Identifier: Apache-2.0

//! Graph operation implementations for `NativeClient`.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::EdgeFilter;
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::protocol::{OpCode, TextFields};
use nodedb_types::result::SubGraph;

use super::core::NativeClient;
use crate::native::connection::check_error;
use crate::sql_escape::quote_string_literal;

impl NativeClient {
    pub(super) async fn graph_traverse_impl(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        direction: nodedb_types::graph::Direction,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        // `GRAPH TRAVERSE` answers with nodes, their depths and the crossed
        // edges with their properties, the same result the remote client
        // decodes. `OpCode::GraphHop` answers only the reachable node set.
        let sql = crate::graph_dsl::build_graph_traverse_sql(
            collection,
            start,
            depth,
            direction,
            edge_filter,
        )?;
        let result = self.query(&sql).await?;
        crate::graph_dsl::decode_traverse_result(&result.columns, &result.rows)
    }

    pub(super) async fn graph_shortest_path_impl(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        let sql =
            crate::graph_dsl::build_graph_path_sql(collection, from, to, max_depth, edge_filter)?;
        let result = self.query(&sql).await?;
        crate::graph_dsl::decode_path_result(&result.columns, &result.rows)
    }

    pub(super) async fn graph_insert_edge_impl(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        // Convert the document fields to a `serde_json::Value` via the
        // type-level `From<nodedb_types::Value>` conversion — no runtime JSON
        // serialization, and infallible, so properties are never silently
        // dropped on a serializer error.
        let props_json = properties.map(|d| {
            serde_json::Value::Object(
                d.fields
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            )
        });
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::EdgePut,
                TextFields {
                    collection: Some(collection.to_string()),
                    from_node: Some(from.as_str().to_string()),
                    to_node: Some(to.as_str().to_string()),
                    edge_type: Some(edge_type.to_string()),
                    properties: props_json,
                    ..Default::default()
                },
            )
            .await?;
        // A refused write must not be reported as an inserted edge: the id
        // below is derived from the caller's own arguments and would look
        // identical whether or not the server stored anything.
        check_error(&resp)?;
        EdgeId::try_first(from.clone(), to.clone(), edge_type)
            .map_err(|e| NodeDbError::storage(format!("invalid edge label: {e}")))
    }

    pub(super) async fn graph_delete_edge_impl(
        &self,
        collection: &str,
        edge_id: &EdgeId,
    ) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::EdgeDelete,
                TextFields {
                    collection: Some(collection.to_string()),
                    from_node: Some(edge_id.src.as_str().to_string()),
                    to_node: Some(edge_id.dst.as_str().to_string()),
                    edge_type: Some(edge_id.label.clone()),
                    ..Default::default()
                },
            )
            .await?;
        check_error(&resp)
    }

    pub(super) async fn graph_stats_impl(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>> {
        let sql = build_native_graph_stats_sql(collection, as_of);
        let result = self.query(&sql).await?;
        parse_native_graph_stats(&result.columns, &result.rows)
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
        let result = self.query(&sql).await?;
        crate::graph_dsl::parse_pagerank_rows(&result.columns, &result.rows)
    }
}

/// Build the SQL for `SHOW GRAPH STATS` for the native protocol. Collection
/// and `as_of` are both optional; when absent the corresponding clause is
/// omitted entirely.
pub(crate) fn build_native_graph_stats_sql(collection: Option<&str>, as_of: Option<i64>) -> String {
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
pub(crate) fn parse_native_graph_stats(
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
        let labels_a = r#"[{"label":"FOLLOWS","count":4}]"#;
        let labels_b = r#"[{"label":"LIKES","count":2}]"#;
        let rows = vec![
            vec![
                Value::String("graph_a".into()),
                Value::Integer(8),
                Value::Integer(4),
                Value::Integer(1),
                Value::String(labels_a.into()),
            ],
            vec![
                Value::String("graph_b".into()),
                Value::Integer(4),
                Value::Integer(2),
                Value::Integer(1),
                Value::String(labels_b.into()),
            ],
        ];
        let result = parse_native_graph_stats(&columns, &rows).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].collection, "graph_a");
        assert_eq!(result[0].edge_count, 4);
        assert_eq!(result[1].collection, "graph_b");
        assert_eq!(result[1].edge_count, 2);
    }

    #[test]
    fn parse_empty_rows_returns_empty_vec() {
        let columns = stat_columns();
        let result = parse_native_graph_stats(&columns, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_wrong_columns_errors() {
        let columns = vec!["bad".to_string()];
        let err = parse_native_graph_stats(&columns, &[]).unwrap_err();
        assert!(err.to_string().contains("unexpected columns"));
    }

    #[test]
    fn parse_no_columns_no_rows_returns_empty_vec() {
        let result = parse_native_graph_stats(&[], &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn build_sql_collection_only() {
        let sql = build_native_graph_stats_sql(Some("social"), None);
        assert_eq!(sql, "SHOW GRAPH STATS 'social'");
    }

    #[test]
    fn build_sql_tenant_wide() {
        let sql = build_native_graph_stats_sql(None, None);
        assert_eq!(sql, "SHOW GRAPH STATS");
    }

    #[test]
    fn build_sql_as_of() {
        let sql = build_native_graph_stats_sql(Some("g"), Some(999));
        assert_eq!(sql, "SHOW GRAPH STATS 'g' AS OF SYSTEM TIME 999");
    }
}
