// SPDX-License-Identifier: Apache-2.0

//! `GRAPH TRAVERSE` and `GRAPH PATH` statement builders. Both clients send
//! these statements: the remote client over pgwire, the native client over
//! its SQL query path.

use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::EdgeFilter;
use nodedb_types::graph::Direction;
use nodedb_types::id::NodeId;

use super::edge_predicate::edge_where_clause;
use crate::sql_escape::quote_string_literal;

/// `GRAPH TRAVERSE IN '<collection>' FROM '<start>' DEPTH <n> DIRECTION <d>
/// [LABEL '<a>', …] [EDGE WHERE …]`.
pub(crate) fn build_graph_traverse_sql(
    collection: &str,
    start: &NodeId,
    depth: u8,
    direction: Direction,
    edge_filter: Option<&EdgeFilter>,
) -> NodeDbResult<String> {
    Ok(format!(
        "GRAPH TRAVERSE IN {} FROM {} DEPTH {depth} DIRECTION {}{}{}",
        quote_string_literal(collection),
        quote_string_literal(start.as_str()),
        direction.as_str(),
        label_clause(edge_filter),
        filter_clause(edge_filter)?,
    ))
}

/// `GRAPH PATH IN '<collection>' FROM '<from>' TO '<to>' MAX_DEPTH <n>
/// [LABEL '<a>', …] [EDGE WHERE …]`.
pub(crate) fn build_graph_path_sql(
    collection: &str,
    from: &NodeId,
    to: &NodeId,
    max_depth: u8,
    edge_filter: Option<&EdgeFilter>,
) -> NodeDbResult<String> {
    Ok(format!(
        "GRAPH PATH IN {} FROM {} TO {} MAX_DEPTH {max_depth}{}{}",
        quote_string_literal(collection),
        quote_string_literal(from.as_str()),
        quote_string_literal(to.as_str()),
        label_clause(edge_filter),
        filter_clause(edge_filter)?,
    ))
}

/// ` LABEL 'a', 'b'` for every label of `edge_filter`. No labels render
/// nothing, which keeps every edge.
fn label_clause(edge_filter: Option<&EdgeFilter>) -> String {
    let labels = edge_filter.map_or(&[][..], |filter| filter.labels.as_slice());
    if labels.is_empty() {
        return String::new();
    }
    let quoted: Vec<String> = labels
        .iter()
        .map(|label| quote_string_literal(label))
        .collect();
    format!(" LABEL {}", quoted.join(", "))
}

/// ` EDGE WHERE …` for the property filters of `edge_filter`.
fn filter_clause(edge_filter: Option<&EdgeFilter>) -> NodeDbResult<String> {
    edge_where_clause(edge_filter.map_or(&[][..], |filter| filter.property_filters.as_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::filter::MetadataFilter;
    use nodedb_types::value::Value;

    fn node(id: &str) -> NodeId {
        NodeId::try_new(id).expect("valid node id")
    }

    #[test]
    fn traverse_renders_direction_depth_and_escaped_literals() {
        for direction in [Direction::Out, Direction::In, Direction::Both] {
            let sql = build_graph_traverse_sql("it's", &node("seed'one"), 3, direction, None)
                .expect("builds");
            assert_eq!(
                sql,
                format!(
                    "GRAPH TRAVERSE IN 'it''s' FROM 'seed''one' DEPTH 3 DIRECTION {}",
                    direction.as_str()
                )
            );
        }
    }

    #[test]
    fn every_label_renders_escaped() {
        let filter = EdgeFilter::labels(["next", "it's"]);
        let sql = build_graph_traverse_sql("g", &node("a"), 1, Direction::Out, Some(&filter))
            .expect("builds");
        assert_eq!(
            sql,
            "GRAPH TRAVERSE IN 'g' FROM 'a' DEPTH 1 DIRECTION out LABEL 'next', 'it''s'"
        );
    }

    #[test]
    fn labels_precede_the_edge_predicate() {
        let filter = EdgeFilter {
            labels: vec!["road".into()],
            property_filters: vec![
                MetadataFilter::Gt {
                    field: "score".into(),
                    value: Value::Integer(5),
                },
                MetadataFilter::eq("owner", "O'Reilly"),
            ],
        };
        let sql = build_graph_traverse_sql("g", &node("a"), 2, Direction::Both, Some(&filter))
            .expect("builds");
        assert_eq!(
            sql,
            r#"GRAPH TRAVERSE IN 'g' FROM 'a' DEPTH 2 DIRECTION both LABEL 'road' EDGE WHERE ("score" > 5) AND ("owner" = 'O''Reilly')"#
        );
        let path =
            build_graph_path_sql("g", &node("a"), &node("z"), 6, Some(&filter)).expect("builds");
        assert_eq!(
            path,
            r#"GRAPH PATH IN 'g' FROM 'a' TO 'z' MAX_DEPTH 6 LABEL 'road' EDGE WHERE ("score" > 5) AND ("owner" = 'O''Reilly')"#
        );
    }

    #[test]
    fn path_without_a_filter_has_no_clauses() {
        let sql = build_graph_path_sql("g", &node("a"), &node("b'c"), 4, Some(&EdgeFilter::all()))
            .expect("builds");
        assert_eq!(sql, "GRAPH PATH IN 'g' FROM 'a' TO 'b''c' MAX_DEPTH 4");
    }

    #[test]
    fn an_unrenderable_filter_value_is_an_error() {
        let filter = EdgeFilter {
            labels: Vec::new(),
            property_filters: vec![MetadataFilter::eq("b", Value::Bytes(vec![0]))],
        };
        assert!(
            build_graph_traverse_sql("g", &node("a"), 1, Direction::Out, Some(&filter)).is_err()
        );
    }
}
