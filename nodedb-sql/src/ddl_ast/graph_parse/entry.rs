// SPDX-License-Identifier: Apache-2.0

//! Graph DSL entry point.

use super::super::statement::{GraphStmt, NodedbStatement};
use super::{edge_predicate, tokenizer, variants};
use crate::error::SqlError;

/// Parse a graph DSL statement.
///
/// `None` means the input is not graph DSL and the caller should keep trying
/// other statement families. `Some(Err(..))` means it *is* graph DSL and is
/// malformed — the two must stay apart, because returning `None` for a
/// malformed statement sends it to the general SQL parser, which can only
/// report that `GRAPH` is not an SQL statement and says nothing about the
/// clause actually at fault.
pub fn try_parse(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("MATCH ") || upper.starts_with("OPTIONAL MATCH ") {
        return Some(Ok(NodedbStatement::Graph(GraphStmt::MatchQuery {
            body: trimmed.to_string(),
        })));
    }

    if !upper.starts_with("GRAPH ") {
        return None;
    }

    Some(parse_graph(trimmed, &upper))
}

/// Parse a statement known to start with `GRAPH `. A trailing `EDGE WHERE`
/// predicate splits off first, so the clause text before it tokenizes alone.
fn parse_graph(trimmed: &str, upper: &str) -> Result<NodedbStatement, SqlError> {
    let (head, predicate_text) = edge_predicate::split_edge_where(trimmed)?;
    let predicate = match predicate_text {
        Some(text) => edge_predicate::parse_edge_predicate(text)?,
        None => Vec::new(),
    };
    let toks = tokenizer::tokenize(head);

    if upper.starts_with("GRAPH TRAVERSE ") {
        return variants::parse_traverse(&toks, predicate);
    }
    if upper.starts_with("GRAPH PATH ") {
        return variants::parse_path(&toks, predicate);
    }
    if predicate_text.is_some() {
        return Err(SqlError::Parse {
            detail: format!(
                "{} does not accept EDGE WHERE: only GRAPH TRAVERSE and GRAPH PATH filter on \
                 edge properties",
                graph_command(upper)
            ),
        });
    }

    if upper.starts_with("GRAPH INSERT EDGE ") {
        variants::parse_insert_edge(&toks)
    } else if upper.starts_with("GRAPH DELETE EDGE ") {
        variants::parse_delete_edge(&toks)
    } else if upper.starts_with("GRAPH LABEL ") {
        variants::parse_set_labels(&toks, false)
    } else if upper.starts_with("GRAPH UNLABEL ") {
        variants::parse_set_labels(&toks, true)
    } else if upper.starts_with("GRAPH NEIGHBORS ") {
        variants::parse_neighbors(&toks)
    } else if upper.starts_with("GRAPH ALGO ") {
        variants::parse_algo(&toks)
    } else if upper.starts_with("GRAPH RAG FUSION ") {
        variants::parse_rag_fusion(&toks, trimmed)
    } else {
        // Starts with `GRAPH ` but names no known command. Still graph DSL,
        // so report it here rather than letting the SQL parser guess.
        Err(SqlError::Parse {
            detail: "unrecognised GRAPH command".to_owned(),
        })
    }
}

/// The command words of a `GRAPH` statement, for an error message:
/// `GRAPH NEIGHBORS`, `GRAPH INSERT EDGE`, `GRAPH RAG FUSION`.
fn graph_command(upper: &str) -> String {
    const MULTI_WORD: [&str; 3] = ["GRAPH INSERT EDGE", "GRAPH DELETE EDGE", "GRAPH RAG FUSION"];
    if let Some(command) = MULTI_WORD.iter().find(|c| upper.starts_with(*c)) {
        return (*command).to_owned();
    }
    upper
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::statement::{GraphDirection, GraphProperties};

    /// Parse a statement that must be both graph DSL and well-formed.
    fn parsed(sql: &str) -> NodedbStatement {
        try_parse(sql)
            .expect("input is graph DSL")
            .expect("statement is well-formed")
    }

    #[test]
    fn parse_graph_insert_edge_keyword_shaped_ids() {
        let stmt = parsed("GRAPH INSERT EDGE IN 'myedges' FROM 'TO' TO 'FROM' TYPE 'LABEL'");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
                collection,
                src,
                dst,
                label,
                properties,
            }) => {
                assert_eq!(collection, "myedges");
                assert_eq!(src, "TO");
                assert_eq!(dst, "FROM");
                assert_eq!(label, "LABEL");
                assert_eq!(properties, GraphProperties::None);
            }
            other => panic!("expected GraphInsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_delete_edge_with_collection() {
        let stmt = parsed("GRAPH DELETE EDGE IN 'myedges' FROM 'a' TO 'b' TYPE 'l'");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
                collection,
                src,
                dst,
                label,
            }) => {
                assert_eq!(collection, "myedges");
                assert_eq!(src, "a");
                assert_eq!(dst, "b");
                assert_eq!(label, "l");
            }
            other => panic!("expected GraphDeleteEdge, got {other:?}"),
        }
    }

    /// A missing required clause is a malformed graph statement, not a
    /// non-graph one. `None` here would send it to the SQL parser, which
    /// reports only that `GRAPH` is not SQL.
    #[test]
    fn parse_graph_insert_edge_missing_collection_names_the_clause() {
        let error = try_parse("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
            .expect("input is graph DSL")
            .expect_err("missing IN <collection> must not produce a statement");
        assert!(
            error.to_string().contains("IN <collection>"),
            "the error must name the missing clause: {error}"
        );
    }

    #[test]
    fn parse_graph_insert_edge_with_object_properties() {
        let stmt = parsed(
            "GRAPH INSERT EDGE IN 'edges' FROM 'a' TO 'b' TYPE 'l' PROPERTIES { note: '} DEPTH 999' }",
        );
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
                collection,
                properties,
                ..
            }) => {
                assert_eq!(collection, "edges");
                match properties {
                    GraphProperties::Object(s) => assert!(s.contains("} DEPTH 999")),
                    other => panic!("expected Object properties, got {other:?}"),
                }
            }
            other => panic!("expected GraphInsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_traverse_keyword_substring_id() {
        let stmt =
            parsed("GRAPH TRAVERSE IN 'kw' FROM 'node_with_DEPTH_in_name' DEPTH 2 LABEL 'l'");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphTraverse {
                collection,
                start,
                depth,
                ..
            }) => {
                assert_eq!(collection, "kw");
                assert_eq!(start, "node_with_DEPTH_in_name");
                assert_eq!(depth, 2);
            }
            other => panic!("expected GraphTraverse, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_path() {
        let stmt = parsed("GRAPH PATH IN 'docs' FROM 'a' TO 'b' MAX_DEPTH 5 LABEL 'l'");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphPath {
                collection,
                src,
                dst,
                max_depth,
                edge_labels,
                edge_predicate,
            }) => {
                assert!(edge_predicate.is_empty());
                assert_eq!(collection, "docs");
                assert_eq!(src, "a");
                assert_eq!(dst, "b");
                assert_eq!(max_depth, 5);
                assert_eq!(edge_labels, vec!["l".to_string()]);
            }
            other => panic!("expected GraphPath, got {other:?}"),
        }
    }

    fn labels_of(sql: &str) -> Vec<String> {
        match parsed(sql) {
            NodedbStatement::Graph(
                GraphStmt::GraphTraverse { edge_labels, .. }
                | GraphStmt::GraphNeighbors { edge_labels, .. }
                | GraphStmt::GraphPath { edge_labels, .. },
            ) => edge_labels,
            other => panic!("expected a graph walk statement, got {other:?}"),
        }
    }

    fn ab() -> Vec<String> {
        vec!["a".to_string(), "b".to_string()]
    }

    #[test]
    fn label_list_parses_on_every_walk_statement() {
        assert_eq!(
            labels_of("GRAPH TRAVERSE IN 'c' FROM 'x' DEPTH 2 LABEL 'a', 'b' DIRECTION out"),
            ab()
        );
        assert_eq!(
            labels_of("GRAPH NEIGHBORS IN 'c' OF 'x' LABEL 'a', 'b' DIRECTION both"),
            ab()
        );
        assert_eq!(
            labels_of("GRAPH PATH IN 'c' FROM 'x' TO 'y' MAX_DEPTH 3 LABEL 'a', 'b'"),
            ab()
        );
    }

    #[test]
    fn parenthesised_label_list_parses() {
        assert_eq!(
            labels_of("GRAPH TRAVERSE IN 'c' FROM 'x' LABEL ('a','b')"),
            ab()
        );
        assert_eq!(
            labels_of("GRAPH NEIGHBORS IN 'c' OF 'x' LABEL ('a', 'b')"),
            ab()
        );
        assert_eq!(
            labels_of("GRAPH PATH IN 'c' FROM 'x' TO 'y' LABEL ('a','b')"),
            ab()
        );
    }

    #[test]
    fn omitted_label_clause_is_an_empty_set() {
        assert!(labels_of("GRAPH TRAVERSE IN 'c' FROM 'x' DEPTH 2").is_empty());
        assert!(labels_of("GRAPH NEIGHBORS IN 'c' OF 'x'").is_empty());
        assert!(labels_of("GRAPH PATH IN 'c' FROM 'x' TO 'y'").is_empty());
    }

    #[test]
    fn label_clause_without_quoted_labels_is_refused() {
        for sql in [
            "GRAPH TRAVERSE IN 'c' FROM 'x' LABEL DIRECTION out",
            "GRAPH TRAVERSE IN 'c' FROM 'x' LABEL knows",
            "GRAPH NEIGHBORS IN 'c' OF 'x' LABEL",
            "GRAPH PATH IN 'c' FROM 'x' TO 'y' LABEL knows",
        ] {
            let error = try_parse(sql)
                .expect("graph DSL")
                .expect_err("a LABEL clause with no quoted label must be refused");
            assert!(
                error.to_string().contains("LABEL"),
                "the error must name the LABEL clause: {error}"
            );
        }
    }

    #[test]
    fn quoted_keyword_label_does_not_shadow_the_keyword() {
        let stmt = parsed("GRAPH TRAVERSE IN 'c' FROM 'x' LABEL 'DIRECTION', 'b' DIRECTION in");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphTraverse {
                edge_labels,
                direction,
                ..
            }) => {
                assert_eq!(edge_labels, vec!["DIRECTION".to_string(), "b".to_string()]);
                assert_eq!(direction, GraphDirection::In);
            }
            other => panic!("expected GraphTraverse, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_labels_list() {
        let stmt = parsed("GRAPH LABEL 'alice' AS 'Person', 'User'");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphSetLabels {
                node_id,
                labels,
                remove,
            }) => {
                assert_eq!(node_id, "alice");
                assert_eq!(labels, vec!["Person".to_string(), "User".to_string()]);
                assert!(!remove);
            }
            other => panic!("expected GraphSetLabels, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_algo_pagerank() {
        let stmt = parsed("GRAPH ALGO PAGERANK ON users ITERATIONS 5 DAMPING 0.85");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphAlgo {
                algorithm,
                collection,
                damping,
                max_iterations,
                ..
            }) => {
                assert_eq!(algorithm, "PAGERANK");
                assert_eq!(collection, "users");
                assert_eq!(damping, Some(0.85));
                assert_eq!(max_iterations, Some(5));
            }
            other => panic!("expected GraphAlgo, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_algo_personalization() {
        let stmt = parsed(
            r#"GRAPH ALGO PAGERANK ON 'users' DAMPING 0.9 PERSONALIZATION {"alice": 1.0, "bob": 0.5}"#,
        );
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphAlgo {
                algorithm,
                collection,
                damping,
                personalization,
                ..
            }) => {
                assert_eq!(algorithm, "PAGERANK");
                assert_eq!(collection, "users");
                assert_eq!(damping, Some(0.9));
                let raw = personalization.expect("personalization object present");
                assert!(raw.contains("alice"));
                assert!(raw.contains("bob"));
                // Round-trips as a JSON node→weight map.
                let map: std::collections::HashMap<String, f64> = sonic_rs::from_str(&raw).unwrap();
                assert_eq!(map.get("alice"), Some(&1.0));
                assert_eq!(map.get("bob"), Some(&0.5));
            }
            other => panic!("expected GraphAlgo, got {other:?}"),
        }
    }

    #[test]
    fn parse_match_query_captures_raw() {
        let stmt = parsed("MATCH (x)-[:l]->(y) RETURN x, y");
        match stmt {
            NodedbStatement::Graph(GraphStmt::MatchQuery { body }) => {
                assert!(body.starts_with("MATCH"));
            }
            other => panic!("expected MatchQuery, got {other:?}"),
        }
    }

    #[test]
    fn non_graph_returns_none() {
        assert!(try_parse("SELECT * FROM users").is_none());
        assert!(try_parse("CREATE COLLECTION users").is_none());
    }

    // ── GraphRagFusion parser tests ──────────────────────────────────────

    #[test]
    fn parse_rag_fusion_full_syntax() {
        let stmt = parsed(
            "GRAPH RAG FUSION ON entities \
             QUERY ARRAY[0.1, 0.2, 0.3] \
             VECTOR_TOP_K 50 \
             EXPANSION_DEPTH 2 \
             EDGE_LABEL 'related_to' \
             FINAL_TOP_K 10 \
             RRF_K (60.0, 35.0)",
        );
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { collection, params }) => {
                assert_eq!(collection, "entities");
                let v = params.query_vector.expect("QUERY ARRAY parsed");
                assert_eq!(v.len(), 3);
                assert!((v[0] - 0.1f32).abs() < 1e-5);
                assert_eq!(params.vector_top_k, Some(50));
                assert_eq!(params.expansion_depth, Some(2));
                assert_eq!(params.edge_label.as_deref(), Some("related_to"));
                assert_eq!(params.final_top_k, Some(10));
                let (k1, k2) = params.rrf_k.unwrap();
                assert!((k1 - 60.0).abs() < 1e-10);
                assert!((k2 - 35.0).abs() < 1e-10);
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_minimal_defaults_to_none() {
        let stmt = parsed("GRAPH RAG FUSION ON mycol QUERY ARRAY[1.0, 0.0]");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { collection, params }) => {
                assert_eq!(collection, "mycol");
                assert!(params.query_vector.is_some());
                assert_eq!(params.vector_top_k, None);
                assert_eq!(params.expansion_depth, None);
                assert_eq!(params.edge_label, None);
                assert_eq!(params.final_top_k, None);
                assert_eq!(params.rrf_k, None);
                assert_eq!(params.vector_field, None);
                assert_eq!(params.direction, None);
                assert_eq!(params.max_visited, None);
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_direction_and_max_visited() {
        let stmt =
            parsed("GRAPH RAG FUSION ON col QUERY ARRAY[0.5] DIRECTION both MAX_VISITED 500");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { params, .. }) => {
                assert_eq!(params.direction, Some(GraphDirection::Both));
                assert_eq!(params.max_visited, Some(500));
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_vector_field_is_captured() {
        let stmt = parsed("GRAPH RAG FUSION ON col QUERY ARRAY[0.5] VECTOR_FIELD 'embedding'");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { params, .. }) => {
                assert_eq!(params.vector_field.as_deref(), Some("embedding"));
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_rrf_k_both_values_captured() {
        let stmt = parsed("GRAPH RAG FUSION ON col QUERY ARRAY[0.5] RRF_K (1.0, 99.5)");
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { params, .. }) => {
                let (k1, k2) = params.rrf_k.expect("RRF_K must be parsed");
                assert!((k1 - 1.0).abs() < 1e-10, "vector_k must be 1.0, got {k1}");
                assert!((k2 - 99.5).abs() < 1e-10, "graph_k must be 99.5, got {k2}");
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_missing_collection_names_the_clause() {
        let error = try_parse("GRAPH RAG FUSION QUERY ARRAY[0.1] VECTOR_TOP_K 5")
            .expect("input is graph DSL")
            .expect_err("missing ON <collection> must not produce a statement");
        assert!(
            error.to_string().contains("ON <collection>"),
            "the error must name the missing clause: {error}"
        );
    }

    /// The two states `try_parse` must never confuse.
    #[test]
    fn malformed_graph_is_distinguishable_from_non_graph() {
        assert!(
            try_parse("SELECT * FROM users").is_none(),
            "non-graph input must stay None so other families get a turn"
        );
        assert!(
            matches!(try_parse("GRAPH TRAVERSE FROM 'b'"), Some(Err(_))),
            "malformed graph input must report its own error"
        );
    }

    /// An unrecognised direction is refused; an omitted one still defaults.
    #[test]
    fn direction_vocabulary_is_closed() {
        for word in ["IN", "OUT", "BOTH", "both"] {
            let sql = format!("GRAPH TRAVERSE IN 'c' FROM 'a' DIRECTION {word}");
            assert!(
                try_parse(&sql).expect("graph DSL").is_ok(),
                "'{word}' is documented vocabulary and must parse"
            );
        }
        for word in ["INBOUND", "BANANA"] {
            let sql = format!("GRAPH TRAVERSE IN 'c' FROM 'a' DIRECTION {word}");
            let error = try_parse(&sql)
                .expect("graph DSL")
                .expect_err("'{word}' is outside the vocabulary");
            assert!(
                error.to_string().contains(word),
                "the error must name the offending value: {error}"
            );
        }
        assert!(
            try_parse("GRAPH TRAVERSE IN 'c' FROM 'a'")
                .expect("graph DSL")
                .is_ok(),
            "an omitted DIRECTION must keep defaulting"
        );
    }
}
