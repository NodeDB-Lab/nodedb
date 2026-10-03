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
        direction_after, extract_properties, label_set_after, missing_clause, quoted_after,
        quoted_list_after, usize_after, usize_after_checked, word_after,
    },
    tokenizer::Tok,
};
use crate::error::SqlError;
use nodedb_types::filter::MetadataFilter;

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

pub(super) fn parse_traverse(
    toks: &[Tok<'_>],
    edge_predicate: Vec<MetadataFilter>,
) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH TRAVERSE";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let start = quoted_after(toks, "FROM").ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let depth = usize_after_checked(toks, "DEPTH")?.unwrap_or(2);
    let edge_labels = label_set_after(toks, STMT)?;
    let direction = direction_after(toks)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphTraverse {
        collection,
        start,
        depth,
        edge_labels,
        direction,
        edge_predicate,
    }))
}

pub(super) fn parse_neighbors(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH NEIGHBORS";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let node = quoted_after(toks, "OF").ok_or_else(|| missing_clause(STMT, "OF <node>"))?;
    let edge_labels = label_set_after(toks, STMT)?;
    let direction = direction_after(toks)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphNeighbors {
        collection,
        node,
        edge_labels,
        direction,
    }))
}

pub(super) fn parse_path(
    toks: &[Tok<'_>],
    edge_predicate: Vec<MetadataFilter>,
) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH PATH";
    let collection =
        quoted_after(toks, "IN").ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = quoted_after(toks, "FROM").ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = quoted_after(toks, "TO").ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let max_depth = usize_after_checked(toks, "MAX_DEPTH")?.unwrap_or(10);
    let edge_labels = label_set_after(toks, STMT)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphPath {
        collection,
        src,
        dst,
        max_depth,
        edge_labels,
        edge_predicate,
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
