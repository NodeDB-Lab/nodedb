// SPDX-License-Identifier: Apache-2.0

//! One parser per graph DSL statement.
//!
//! Every parser returns `Result`, never `Option`. The dispatcher has already
//! established that the input is a graph statement by the time it calls in
//! here, so "this clause is missing" must not be reported the same way as
//! "this was never a graph statement" — the second sends the input to the
//! general SQL parser, which can only say `GRAPH` is not SQL.

use super::{
    super::statement::{GraphStmt, NodedbStatement},
    fusion_params::{FusionParams, RAG_FUSION_KEYWORDS},
    helpers::{
        direction_after, extract_properties, find_keyword, missing_clause, quoted_after,
        quoted_list_after, usize_after, usize_after_checked, word_after,
    },
    tokenizer::Tok,
};
use crate::ddl_ast::graph_types::GraphEdgeTuple;
use crate::error::SqlError;

pub(super) fn parse_insert_edge(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH INSERT EDGE";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = quoted_after(toks, "FROM").ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = quoted_after(toks, "TO").ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let label = quoted_after(toks, "TYPE").ok_or_else(|| missing_clause(STMT, "TYPE <label>"))?;
    let properties = extract_properties(toks);
    Ok(NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
        collection,
        src,
        dst,
        label,
        properties,
    }))
}

/// Maximum number of edges one batch statement may carry. Keeps one
/// statement's worth of work bounded for the write-admission and lock paths.
pub const MAX_EDGES_PER_BATCH: usize = 1000;

pub(super) fn parse_insert_edges(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    let (collection, edges) = parse_edge_batch(toks, "GRAPH INSERT EDGES")?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphInsertEdges {
        collection,
        edges,
    }))
}

pub(super) fn parse_delete_edges(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    let (collection, edges) = parse_edge_batch(toks, "GRAPH DELETE EDGES")?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphDeleteEdges {
        collection,
        edges,
    }))
}

/// Shared body of the batch parsers.
///
/// Grammar: `IN '<collection>' VALUES ('<src>','<dst>','<label>')[, (...)]*`.
/// The tokenizer drops commas and parentheses, so the tuple structure is
/// recovered by consuming string tokens in threes; a count that is not a
/// multiple of three is malformed. Per-edge `PROPERTIES` stays on the
/// single-edge form until the physical `BatchEdge` can carry one.
fn parse_edge_batch(
    toks: &[Tok<'_>],
    stmt: &str,
) -> Result<(String, Vec<GraphEdgeTuple>), SqlError> {
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(stmt, "IN <collection>"))?;
    let Some(values_pos) = find_keyword(toks, "VALUES") else {
        return Err(missing_clause(stmt, "VALUES ('<src>','<dst>','<label>')"));
    };
    let mut fields: Vec<String> = Vec::new();
    for t in &toks[values_pos + 1..] {
        match t {
            Tok::Quoted(s) => fields.push(s.clone().into_owned()),
            Tok::Word(w) => {
                if w.eq_ignore_ascii_case("PROPERTIES") {
                    return Err(SqlError::Parse {
                        detail: format!(
                            "{stmt}: per-edge PROPERTIES is not supported in the batch form"
                        ),
                    });
                }
                fields.push((*w).to_string());
            }
            Tok::Object(_) => {
                return Err(SqlError::Parse {
                    detail: format!("{stmt}: object literals are not supported in the batch form"),
                });
            }
        }
    }
    if fields.is_empty() || !fields.len().is_multiple_of(3) {
        return Err(SqlError::Parse {
            detail: format!("{stmt}: VALUES takes (src, dst, label) triples"),
        });
    }
    let count = fields.len() / 3;
    if count > MAX_EDGES_PER_BATCH {
        return Err(SqlError::Parse {
            detail: format!(
                "{stmt}: at most {MAX_EDGES_PER_BATCH} edges per statement, got {count}"
            ),
        });
    }
    let edges = fields
        .chunks(3)
        .map(|c| GraphEdgeTuple {
            src: c[0].clone(),
            dst: c[1].clone(),
            label: c[2].clone(),
        })
        .collect();
    Ok((collection, edges))
}

pub(super) fn parse_delete_edge(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH DELETE EDGE";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = quoted_after(toks, "FROM").ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = quoted_after(toks, "TO").ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let label = quoted_after(toks, "TYPE").ok_or_else(|| missing_clause(STMT, "TYPE <label>"))?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
        collection,
        src,
        dst,
        label,
    }))
}

pub(super) fn parse_set_labels(
    toks: &[Tok<'_>],
    remove: bool,
) -> Result<NodedbStatement, SqlError> {
    let keyword = if remove { "UNLABEL" } else { "LABEL" };
    let node_id = quoted_after(toks, keyword)
        .ok_or_else(|| missing_clause(&format!("GRAPH {keyword}"), "<node>"))?;
    let labels = quoted_list_after(toks, "AS");
    Ok(NodedbStatement::Graph(GraphStmt::GraphSetLabels {
        node_id,
        labels,
        remove,
    }))
}

pub(super) fn parse_traverse(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH TRAVERSE";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let start = quoted_after(toks, "FROM").ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let depth = usize_after_checked(toks, "DEPTH")?.unwrap_or(2);
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphTraverse {
        collection,
        start,
        depth,
        edge_label,
        direction,
    }))
}

pub(super) fn parse_neighbors(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH NEIGHBORS";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let node = quoted_after(toks, "OF").ok_or_else(|| missing_clause(STMT, "OF <node>"))?;
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphNeighbors {
        collection,
        node,
        edge_label,
        direction,
    }))
}

pub(super) fn parse_path(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH PATH";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = quoted_after(toks, "FROM").ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = quoted_after(toks, "TO").ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let max_depth = usize_after_checked(toks, "MAX_DEPTH")?.unwrap_or(10);
    let edge_label = quoted_after(toks, "LABEL");
    Ok(NodedbStatement::Graph(GraphStmt::GraphPath {
        collection,
        src,
        dst,
        max_depth,
        edge_label,
    }))
}

pub(super) fn parse_algo(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH ALGO";
    let algorithm = super::helpers::find_keyword(toks, "ALGO")
        .and_then(|i| match toks.get(i + 1)? {
            Tok::Word(w) => Some(w.to_ascii_uppercase()),
            _ => None,
        })
        .ok_or_else(|| missing_clause(STMT, "ALGO <algorithm>"))?;

    // Accept either a bare word (`ON users`) or a quoted literal (`ON 'users'`)
    // so clients can escape collection names safely.
    let collection_raw =
        quoted_after(toks, "ON").ok_or_else(|| missing_clause(STMT, "ON <collection>"))?;

    // Reject the `ON (subquery)` form: the tokenizer strips `(` and `)`, so
    // `ON (SELECT …)` becomes `[ON, SELECT, …]` and `quoted_after("ON")`
    // returns `"SELECT"`, which would be stored as the collection name and
    // then ignored — producing tenant-wide results.
    const SUBQUERY_KEYWORDS: &[&str] = &["SELECT", "WITH", "VALUES", "TABLE"];
    if SUBQUERY_KEYWORDS
        .iter()
        .any(|kw| collection_raw.eq_ignore_ascii_case(kw))
    {
        return Err(SqlError::Parse {
            detail: format!("{STMT} ON does not accept a subquery"),
        });
    }
    let collection = collection_raw.to_lowercase();

    Ok(NodedbStatement::Graph(GraphStmt::GraphAlgo {
        algorithm,
        collection,
        edge_label: quoted_after(toks, "EDGE_LABEL"),
        damping: super::helpers::float_after(toks, "DAMPING"),
        tolerance: super::helpers::float_after(toks, "TOLERANCE"),
        resolution: super::helpers::float_after(toks, "RESOLUTION"),
        max_iterations: usize_after(toks, "ITERATIONS"),
        sample_size: usize_after(toks, "SAMPLE"),
        source_node: quoted_after(toks, "FROM").or_else(|| quoted_after(toks, "SOURCE")),
        direction: word_after(toks, "DIRECTION"),
        mode: word_after(toks, "MODE"),
        personalization: super::helpers::object_after(toks, "PERSONALIZATION"),
    }))
}

/// Parse `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`.
///
/// All fusion parameters are delegated to [`FusionParams::extract`] so
/// every fusion SQL surface shares one typed, quote-aware extractor.
pub(super) fn parse_rag_fusion(toks: &[Tok<'_>], sql: &str) -> Result<NodedbStatement, SqlError> {
    let collection = word_after(toks, "ON")
        .or_else(|| quoted_after(toks, "ON"))
        .ok_or_else(|| missing_clause("GRAPH RAG FUSION", "ON <collection>"))?;
    let params = FusionParams::extract(toks, sql, &RAG_FUSION_KEYWORDS);
    Ok(NodedbStatement::Graph(GraphStmt::GraphRagFusion {
        collection,
        params,
    }))
}
