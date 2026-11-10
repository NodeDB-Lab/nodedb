// SPDX-License-Identifier: Apache-2.0

//! Edge batches for `NodeDbRemote`: one `GRAPH INSERT EDGE` or
//! `GRAPH DELETE EDGE` statement per call, carrying every edge.

use nodedb_types::error::NodeDbResult;
use nodedb_types::id::EdgeId;
use nodedb_types::protocol::EdgeWrite;

use super::core::NodeDbRemote;
use super::document_bind::TypedParam;
use super::props_bind::properties_literal;
use crate::sql_escape::quote_string_literal;

impl NodeDbRemote {
    /// One `GRAPH INSERT EDGE` statement. An empty slice writes nothing and
    /// sends nothing.
    pub(super) async fn graph_insert_edges_impl(
        &self,
        collection: &str,
        edges: &[EdgeWrite],
    ) -> NodeDbResult<u64> {
        if edges.is_empty() {
            return Ok(0);
        }
        let (sql, params) = build_insert_edges_sql(collection, edges)?;
        if params.is_empty() {
            self.simple_command_count(&sql).await
        } else {
            self.execute_typed(&sql, &params).await
        }
    }

    /// One `GRAPH DELETE EDGE` statement. An empty slice deletes nothing and
    /// sends nothing.
    pub(super) async fn graph_delete_edges_impl(
        &self,
        collection: &str,
        edges: &[EdgeId],
    ) -> NodeDbResult<u64> {
        if edges.is_empty() {
            return Ok(0);
        }
        let sql = build_delete_edges_sql(collection, edges);
        self.simple_command_count(&sql).await
    }
}

/// `GRAPH INSERT EDGE IN '<collection>'` followed by one clause run per edge:
/// `FROM .. TO .. TYPE ..`, then `PROPERTIES { key: $n, ... }` when the edge
/// has properties, then its valid time when set. Every property value is a
/// typed parameter, returned in placeholder order.
pub(super) fn build_insert_edges_sql(
    collection: &str,
    edges: &[EdgeWrite],
) -> NodeDbResult<(String, Vec<TypedParam>)> {
    let mut sql = format!("GRAPH INSERT EDGE IN {}", quote_string_literal(collection));
    let mut params = Vec::new();
    for edge in edges {
        push_endpoints(&mut sql, edge.src.as_str(), edge.dst.as_str(), &edge.label);
        if !edge.properties.is_empty() {
            sql.push_str(" PROPERTIES ");
            sql.push_str(&properties_literal(
                "graph_insert_edges",
                &edge.properties,
                &mut params,
            )?);
        }
        if let Some(from) = edge.valid_from_ms {
            sql.push_str(&format!(" VALID_TIME {from}"));
        }
        if let Some(until) = edge.valid_until_ms {
            sql.push_str(&format!(" UNTIL {until}"));
        }
    }
    Ok((sql, params))
}

/// `GRAPH DELETE EDGE IN '<collection>'` followed by one
/// `FROM .. TO .. TYPE ..` run per edge.
pub(crate) fn build_delete_edges_sql(collection: &str, edges: &[EdgeId]) -> String {
    let mut sql = format!("GRAPH DELETE EDGE IN {}", quote_string_literal(collection));
    for edge in edges {
        push_endpoints(&mut sql, edge.src.as_str(), edge.dst.as_str(), &edge.label);
    }
    sql
}

fn push_endpoints(sql: &mut String, src: &str, dst: &str, label: &str) {
    sql.push_str(" FROM ");
    sql.push_str(&quote_string_literal(src));
    sql.push_str(" TO ");
    sql.push_str(&quote_string_literal(dst));
    sql.push_str(" TYPE ");
    sql.push_str(&quote_string_literal(label));
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::id::NodeId;
    use nodedb_types::value::Value;

    fn node(id: &str) -> NodeId {
        NodeId::try_new(id).expect("valid node id")
    }

    #[test]
    fn insert_sql_carries_every_edge_and_its_own_clauses() {
        let mut second = EdgeWrite::new(node("c"), node("it's"), "L");
        second.properties.insert("w".to_string(), Value::Integer(2));
        second.valid_from_ms = Some(5);
        second.valid_until_ms = Some(9);
        let edges = [EdgeWrite::new(node("a"), node("b"), "L"), second];
        let (sql, params) = build_insert_edges_sql("g", &edges).expect("sql");
        assert_eq!(
            sql,
            "GRAPH INSERT EDGE IN 'g' FROM 'a' TO 'b' TYPE 'L' \
             FROM 'c' TO 'it''s' TYPE 'L' PROPERTIES { w: $1 } VALID_TIME 5 UNTIL 9"
        );
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn delete_sql_carries_every_edge() {
        let edges = [
            EdgeId::try_first(node("a"), node("b"), "L").expect("edge id"),
            EdgeId::try_first(node("c"), node("d"), "M").expect("edge id"),
        ];
        assert_eq!(
            build_delete_edges_sql("g", &edges),
            "GRAPH DELETE EDGE IN 'g' FROM 'a' TO 'b' TYPE 'L' FROM 'c' TO 'd' TYPE 'M'"
        );
    }
}
