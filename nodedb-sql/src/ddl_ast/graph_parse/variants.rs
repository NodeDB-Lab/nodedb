// SPDX-License-Identifier: Apache-2.0

//! One parser per graph DSL statement.
//!
//! Every parser reads its clauses left to right through a [`Cursor`] and
//! returns `Result`, never `Option`. The dispatcher has already established
//! that the input is a graph statement, so a missing clause, a misspelled
//! clause keyword, or a stray token is a parse error that names the fault.

use super::{
    super::graph_types::{GraphDirection, GraphProperties},
    super::statement::{GraphStmt, NodedbStatement},
    cursor::{Cursor, missing_clause, set_once},
    fusion_params::{FusionParams, RAG_FUSION_KEYWORDS},
    tokenizer::Tok,
};
use crate::ddl_ast::graph_types::GraphEdgeTuple;
use crate::error::SqlError;
use nodedb_types::filter::MetadataFilter;

/// Parse `GRAPH INSERT EDGE IN <c> FROM <a> TO <b> TYPE <l> [PROPERTIES ...]`.
pub(super) fn parse_insert_edge(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH INSERT EDGE";
    let mut c = Cursor::new(toks, STMT, 3);
    let (mut collection, mut src, mut dst, mut label, mut properties) =
        (None, None, None, None, None);
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", STMT)?,
            "FROM" => set_once(&mut src, c.value("FROM")?, "FROM", STMT)?,
            "TO" => set_once(&mut dst, c.value("TO")?, "TO", STMT)?,
            "TYPE" => set_once(&mut label, c.value("TYPE")?, "TYPE", STMT)?,
            "PROPERTIES" => set_once(&mut properties, c.properties()?, "PROPERTIES", STMT)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
        collection: collection.ok_or_else(|| missing_clause(STMT, "IN <collection>"))?,
        src: src.ok_or_else(|| missing_clause(STMT, "FROM <node>"))?,
        dst: dst.ok_or_else(|| missing_clause(STMT, "TO <node>"))?,
        label: label.ok_or_else(|| missing_clause(STMT, "TYPE <label>"))?,
        properties: properties.unwrap_or(GraphProperties::None),
    }))
}

/// Maximum number of edges one batch statement may carry. Keeps one
/// statement's worth of work bounded for the write-admission and lock paths.
///
/// Sourced from the protocol bound so the client cannot hold a different
/// number: both sides read the one constant.
pub const MAX_EDGES_PER_BATCH: usize = nodedb_types::protocol::MAX_EDGES_PER_BATCH_STATEMENT;

/// Maximum SQL text one batch statement may carry.
///
/// The edge count alone does not bound the work: a triple's fields are
/// arbitrary quoted strings, so 1000 triples of long node ids run to many
/// megabytes. Whichever limit the statement exceeds first is the one reported.
pub const MAX_BATCH_TEXT_BYTES: usize = nodedb_types::protocol::MAX_BATCH_TEXT_BYTES;

/// The command words of a batch statement: `GRAPH INSERT EDGES`, `GRAPH DELETE EDGES`.
const BATCH_COMMAND_WORDS: usize = 3;

pub(super) fn parse_insert_edges(toks: &[Tok<'_>], sql: &str) -> Result<NodedbStatement, SqlError> {
    let (collection, edges) = parse_edge_batch(toks, sql, "GRAPH INSERT EDGES")?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphInsertEdges {
        collection,
        edges,
    }))
}

pub(super) fn parse_delete_edges(toks: &[Tok<'_>], sql: &str) -> Result<NodedbStatement, SqlError> {
    let (collection, edges) = parse_edge_batch(toks, sql, "GRAPH DELETE EDGES")?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphDeleteEdges {
        collection,
        edges,
    }))
}

/// Shared body of the batch parsers.
///
/// Grammar: `IN '<collection>' VALUES ('<src>','<dst>','<label>')[, (...)]*`.
/// The tokenizer drops commas and parentheses, so the tuple structure is
/// recovered by consuming value tokens in threes, and the statement text is
/// checked for the parenthesized, comma-separated shape. A count that is not a
/// multiple of three is malformed. Per-edge `PROPERTIES` stays on the
/// single-edge form until the physical `BatchEdge` can carry one.
///
/// `sql` is the statement's own text. Its length bounds the statement before
/// the field scan, and the shape check reads it because the tokenizer discards
/// the separators.
fn parse_edge_batch(
    toks: &[Tok<'_>],
    sql: &str,
    stmt: &'static str,
) -> Result<(String, Vec<GraphEdgeTuple>), SqlError> {
    let sql_len = sql.len();
    // Bounded first: the field scan below clones every value, and a statement
    // over the cap must not pay for that copy before it is refused.
    if sql_len > MAX_BATCH_TEXT_BYTES {
        return Err(SqlError::Parse {
            detail: format!(
                "{stmt}: at most {MAX_BATCH_TEXT_BYTES} bytes of SQL text per statement, got \
                 {sql_len}"
            ),
        });
    }
    let mut c = Cursor::new(toks, stmt, BATCH_COMMAND_WORDS);
    if !c.eat_keyword("IN") {
        return Err(missing_clause(stmt, "IN <collection>"));
    }
    let collection = c.value("IN")?;
    if !c.eat_keyword("VALUES") {
        return Err(missing_clause(stmt, "VALUES ('<src>','<dst>','<label>')"));
    }
    let mut fields: Vec<String> = Vec::new();
    for t in c.rest() {
        match t {
            Tok::Comma => {}
            Tok::Quoted(s) | Tok::Ident(s) => fields.push(s.clone().into_owned()),
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
            Tok::Object(_) | Tok::List(_) => {
                return Err(SqlError::Parse {
                    detail: format!(
                        "{stmt}: object and list literals are not supported in the batch form"
                    ),
                });
            }
        }
    }
    if fields.is_empty()
        || !fields.len().is_multiple_of(3)
        || !super::batch_shape::values_tail_is_triples(sql)
    {
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

/// Parse `GRAPH DELETE EDGE IN <c> FROM <a> TO <b> TYPE <l>`.
pub(super) fn parse_delete_edge(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH DELETE EDGE";
    let mut c = Cursor::new(toks, STMT, 3);
    let (mut collection, mut src, mut dst, mut label) = (None, None, None, None);
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", STMT)?,
            "FROM" => set_once(&mut src, c.value("FROM")?, "FROM", STMT)?,
            "TO" => set_once(&mut dst, c.value("TO")?, "TO", STMT)?,
            "TYPE" => set_once(&mut label, c.value("TYPE")?, "TYPE", STMT)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
        collection: collection.ok_or_else(|| missing_clause(STMT, "IN <collection>"))?,
        src: src.ok_or_else(|| missing_clause(STMT, "FROM <node>"))?,
        dst: dst.ok_or_else(|| missing_clause(STMT, "TO <node>"))?,
        label: label.ok_or_else(|| missing_clause(STMT, "TYPE <label>"))?,
    }))
}

/// Parse `GRAPH LABEL <node> [AS 'a', 'b']` or `GRAPH UNLABEL ...`.
pub(super) fn parse_set_labels(
    toks: &[Tok<'_>],
    remove: bool,
) -> Result<NodedbStatement, SqlError> {
    let statement = if remove {
        "GRAPH UNLABEL"
    } else {
        "GRAPH LABEL"
    };
    let mut c = Cursor::new(toks, statement, 2);
    let Ok(node_id) = c.value("<node>") else {
        return Err(missing_clause(statement, "<node>"));
    };
    let mut labels = None;
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "AS" => set_once(&mut labels, c.quoted_list("AS")?, "AS", statement)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphSetLabels {
        node_id,
        labels: labels.unwrap_or_default(),
        remove,
    }))
}

/// Parse `GRAPH TRAVERSE IN <c> FROM <node> [DEPTH n] [LABEL ...] [DIRECTION d]`.
pub(super) fn parse_traverse(
    toks: &[Tok<'_>],
    edge_predicate: Vec<MetadataFilter>,
) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH TRAVERSE";
    let mut c = Cursor::new(toks, STMT, 2);
    let (mut collection, mut start, mut depth, mut labels, mut direction) =
        (None, None, None, None, None);
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", STMT)?,
            "FROM" => set_once(&mut start, c.value("FROM")?, "FROM", STMT)?,
            "DEPTH" => set_once(&mut depth, c.usize("DEPTH")?, "DEPTH", STMT)?,
            "LABEL" => set_once(&mut labels, c.quoted_list("LABEL")?, "LABEL", STMT)?,
            "DIRECTION" => set_once(&mut direction, c.direction()?, "DIRECTION", STMT)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphTraverse {
        collection: collection.ok_or_else(|| missing_clause(STMT, "IN <collection>"))?,
        start: start.ok_or_else(|| missing_clause(STMT, "FROM <node>"))?,
        depth: depth.unwrap_or(2),
        edge_labels: labels.unwrap_or_default(),
        direction: direction.unwrap_or(GraphDirection::Out),
        edge_predicate,
    }))
}

/// Parse `GRAPH NEIGHBORS IN <c> OF <node> [LABEL ...] [DIRECTION d]`.
pub(super) fn parse_neighbors(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH NEIGHBORS";
    let mut c = Cursor::new(toks, STMT, 2);
    let (mut collection, mut node, mut labels, mut direction) = (None, None, None, None);
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", STMT)?,
            "OF" => set_once(&mut node, c.value("OF")?, "OF", STMT)?,
            "LABEL" => set_once(&mut labels, c.quoted_list("LABEL")?, "LABEL", STMT)?,
            "DIRECTION" => set_once(&mut direction, c.direction()?, "DIRECTION", STMT)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphNeighbors {
        collection: collection.ok_or_else(|| missing_clause(STMT, "IN <collection>"))?,
        node: node.ok_or_else(|| missing_clause(STMT, "OF <node>"))?,
        edge_labels: labels.unwrap_or_default(),
        direction: direction.unwrap_or(GraphDirection::Out),
    }))
}

/// Parse `GRAPH PATH IN <c> FROM <a> TO <b> [MAX_DEPTH n] [LABEL ...]`.
pub(super) fn parse_path(
    toks: &[Tok<'_>],
    edge_predicate: Vec<MetadataFilter>,
) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH PATH";
    let mut c = Cursor::new(toks, STMT, 2);
    let (mut collection, mut src, mut dst, mut max_depth, mut labels) =
        (None, None, None, None, None);
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", STMT)?,
            "FROM" => set_once(&mut src, c.value("FROM")?, "FROM", STMT)?,
            "TO" => set_once(&mut dst, c.value("TO")?, "TO", STMT)?,
            "MAX_DEPTH" => set_once(&mut max_depth, c.usize("MAX_DEPTH")?, "MAX_DEPTH", STMT)?,
            "LABEL" => set_once(&mut labels, c.quoted_list("LABEL")?, "LABEL", STMT)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphPath {
        collection: collection.ok_or_else(|| missing_clause(STMT, "IN <collection>"))?,
        src: src.ok_or_else(|| missing_clause(STMT, "FROM <node>"))?,
        dst: dst.ok_or_else(|| missing_clause(STMT, "TO <node>"))?,
        max_depth: max_depth.unwrap_or(10),
        edge_labels: labels.unwrap_or_default(),
        edge_predicate,
    }))
}

/// The optional clauses of `GRAPH ALGO`.
#[derive(Default)]
struct AlgoClauses {
    collection: Option<String>,
    edge_label: Option<String>,
    damping: Option<f64>,
    tolerance: Option<f64>,
    resolution: Option<f64>,
    max_iterations: Option<usize>,
    sample_size: Option<usize>,
    source_node: Option<String>,
    direction: Option<String>,
    mode: Option<String>,
    personalization: Option<String>,
}

/// Parse `GRAPH ALGO <algorithm> ON <collection> [options...]`.
pub(super) fn parse_algo(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH ALGO";
    let mut c = Cursor::new(toks, STMT, 2);
    let Some(algorithm) = c.next_clause()? else {
        return Err(missing_clause(STMT, "ALGO <algorithm>"));
    };
    let mut a = AlgoClauses::default();
    while let Some(clause) = c.next_clause()? {
        let k = clause.as_str();
        match k {
            "ON" => {
                let collection = c.value(k)?;
                refuse_subquery(&collection)?;
                set_once(&mut a.collection, collection.to_lowercase(), k, STMT)?;
            }
            "EDGE_LABEL" => set_once(&mut a.edge_label, c.value(k)?, k, STMT)?,
            "DAMPING" => set_once(&mut a.damping, c.f64(k)?, k, STMT)?,
            "TOLERANCE" => set_once(&mut a.tolerance, c.f64(k)?, k, STMT)?,
            "RESOLUTION" => set_once(&mut a.resolution, c.f64(k)?, k, STMT)?,
            "ITERATIONS" => set_once(&mut a.max_iterations, c.usize(k)?, k, STMT)?,
            "SAMPLE" => set_once(&mut a.sample_size, c.usize(k)?, k, STMT)?,
            "FROM" | "SOURCE" => set_once(&mut a.source_node, c.value(k)?, k, STMT)?,
            "DIRECTION" => set_once(&mut a.direction, c.word(k)?.to_string(), k, STMT)?,
            "MODE" => set_once(&mut a.mode, c.word(k)?.to_string(), k, STMT)?,
            "PERSONALIZATION" => set_once(&mut a.personalization, c.object(k)?, k, STMT)?,
            _ => return Err(c.unknown_clause()),
        }
    }
    Ok(NodedbStatement::Graph(GraphStmt::GraphAlgo {
        algorithm,
        collection: a
            .collection
            .ok_or_else(|| missing_clause(STMT, "ON <collection>"))?,
        edge_label: a.edge_label,
        damping: a.damping,
        tolerance: a.tolerance,
        resolution: a.resolution,
        max_iterations: a.max_iterations,
        sample_size: a.sample_size,
        source_node: a.source_node,
        direction: a.direction,
        mode: a.mode,
        personalization: a.personalization,
    }))
}

/// Refuse the `ON (subquery)` form. The tokenizer drops `(` and `)`, so
/// `ON (SELECT …)` reads `SELECT` as the collection name. Stored as such, the
/// name would be ignored and the algorithm would run tenant-wide.
fn refuse_subquery(collection: &str) -> Result<(), SqlError> {
    const SUBQUERY_KEYWORDS: &[&str] = &["SELECT", "WITH", "VALUES", "TABLE"];
    if SUBQUERY_KEYWORDS
        .iter()
        .any(|keyword| collection.eq_ignore_ascii_case(keyword))
    {
        return Err(SqlError::Parse {
            detail: "GRAPH ALGO ON does not accept a subquery".to_owned(),
        });
    }
    Ok(())
}

/// Parse `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`.
///
/// The fusion parameters go through [`FusionParams::parse`], which every
/// fusion SQL surface shares.
pub(super) fn parse_rag_fusion(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH RAG FUSION";
    let mut c = Cursor::new(toks, STMT, 3);
    if !c.eat_keyword("ON") {
        return Err(missing_clause(STMT, "ON <collection>"));
    }
    let collection = c.value("ON")?;
    let params = FusionParams::parse(&mut c, &RAG_FUSION_KEYWORDS)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphRagFusion {
        collection,
        params,
    }))
}
