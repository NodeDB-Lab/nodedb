// SPDX-License-Identifier: Apache-2.0

//! One parser per graph DSL statement.
//!
//! Every parser returns `Result`, never `Option`. The dispatcher has already
//! established that the input is a graph statement by the time it calls in
//! here, so "this clause is missing" must not be reported the same way as
//! "this was never a graph statement" — the second sends the input to the
//! general SQL parser, which can only say `GRAPH` is not SQL.
//!
//! Each parser reads its clauses through [`Cursor`], which claims the tokens
//! it consumes; the dispatcher refuses any token left unclaimed. A mistyped
//! keyword is then a parse error naming the token, not a silently defaulted
//! clause.

use super::{
    super::statement::{GraphStmt, NodedbStatement},
    cursor::Cursor,
    fusion_params::{FusionParams, RAG_FUSION_KEYWORDS},
    helpers::missing_clause,
};
use crate::error::SqlError;

pub(super) fn parse_insert_edge(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH INSERT EDGE";
    let collection = cursor
        .quoted_after("IN")
        .ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = cursor
        .quoted_after("FROM")
        .ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = cursor
        .quoted_after("TO")
        .ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let label = cursor
        .quoted_after("TYPE")
        .ok_or_else(|| missing_clause(STMT, "TYPE <label>"))?;
    let properties = cursor.extract_properties();
    Ok(NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
        collection,
        src,
        dst,
        label,
        properties,
    }))
}

pub(super) fn parse_delete_edge(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH DELETE EDGE";
    let collection = cursor
        .quoted_after("IN")
        .ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = cursor
        .quoted_after("FROM")
        .ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = cursor
        .quoted_after("TO")
        .ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let label = cursor
        .quoted_after("TYPE")
        .ok_or_else(|| missing_clause(STMT, "TYPE <label>"))?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
        collection,
        src,
        dst,
        label,
    }))
}

pub(super) fn parse_set_labels(
    cursor: &mut Cursor<'_>,
    remove: bool,
) -> Result<NodedbStatement, SqlError> {
    let keyword = if remove { "UNLABEL" } else { "LABEL" };
    let node_id = cursor
        .quoted_after(keyword)
        .ok_or_else(|| missing_clause(&format!("GRAPH {keyword}"), "<node>"))?;
    let labels = cursor.quoted_list_after("AS");
    Ok(NodedbStatement::Graph(GraphStmt::GraphSetLabels {
        node_id,
        labels,
        remove,
    }))
}

pub(super) fn parse_traverse(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH TRAVERSE";
    let collection = cursor
        .quoted_after("IN")
        .ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let start = cursor
        .quoted_after("FROM")
        .ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let depth = cursor.usize_after_checked("DEPTH")?.unwrap_or(2);
    let edge_label = cursor.quoted_after("LABEL");
    let direction = cursor.direction_after("DIRECTION")?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphTraverse {
        collection,
        start,
        depth,
        edge_label,
        direction,
    }))
}

pub(super) fn parse_neighbors(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH NEIGHBORS";
    let collection = cursor
        .quoted_after("IN")
        .ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let node = cursor
        .quoted_after("OF")
        .ok_or_else(|| missing_clause(STMT, "OF <node>"))?;
    let edge_label = cursor.quoted_after("LABEL");
    let direction = cursor.direction_after("DIRECTION")?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphNeighbors {
        collection,
        node,
        edge_label,
        direction,
    }))
}

pub(super) fn parse_path(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH PATH";
    let collection = cursor
        .quoted_after("IN")
        .ok_or_else(|| missing_clause(STMT, "IN <collection>"))?;
    let src = cursor
        .quoted_after("FROM")
        .ok_or_else(|| missing_clause(STMT, "FROM <node>"))?;
    let dst = cursor
        .quoted_after("TO")
        .ok_or_else(|| missing_clause(STMT, "TO <node>"))?;
    let max_depth = cursor.usize_after_checked("MAX_DEPTH")?.unwrap_or(10);
    let edge_label = cursor.quoted_after("LABEL");
    Ok(NodedbStatement::Graph(GraphStmt::GraphPath {
        collection,
        src,
        dst,
        max_depth,
        edge_label,
    }))
}

pub(super) fn parse_algo(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    const STMT: &str = "GRAPH ALGO";
    let algorithm = cursor
        .word_after("ALGO")
        .map(|word| word.to_ascii_uppercase())
        .ok_or_else(|| missing_clause(STMT, "ALGO <algorithm>"))?;

    // Accept either a bare word (`ON users`) or a quoted literal (`ON 'users'`)
    // so clients can escape collection names safely.
    let collection_raw = cursor
        .quoted_after("ON")
        .ok_or_else(|| missing_clause(STMT, "ON <collection>"))?;

    // Reject the `ON (subquery)` form: the tokenizer strips `(` and `)`, so
    // `ON (SELECT …)` becomes `[ON, SELECT, …]` and the reader returns
    // `"SELECT"`, which would be stored as the collection name and then
    // ignored — producing tenant-wide results.
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
        edge_label: cursor.quoted_after("EDGE_LABEL"),
        damping: cursor.float_after("DAMPING"),
        tolerance: cursor.float_after("TOLERANCE"),
        resolution: cursor.float_after("RESOLUTION"),
        max_iterations: cursor.usize_after("ITERATIONS"),
        sample_size: cursor.usize_after("SAMPLE"),
        source_node: cursor
            .quoted_after("FROM")
            .or_else(|| cursor.quoted_after("SOURCE")),
        direction: cursor.word_after("DIRECTION"),
        mode: cursor.word_after("MODE"),
        personalization: cursor.object_after("PERSONALIZATION"),
    }))
}

/// Parse `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`.
///
/// All fusion parameters are delegated to [`FusionParams::extract`] so every
/// fusion SQL surface shares one typed, quote-aware extractor. The extractor
/// reads its options through the cursor, so every keyword and value is claimed
/// here and a token no clause owns is left for the dispatcher's `finish` to
/// refuse by name.
pub(super) fn parse_rag_fusion(cursor: &mut Cursor<'_>) -> Result<NodedbStatement, SqlError> {
    let collection = cursor
        .word_after("ON")
        .or_else(|| cursor.quoted_after("ON"))
        .ok_or_else(|| missing_clause("GRAPH RAG FUSION", "ON <collection>"))?;
    let params = FusionParams::extract(cursor, &RAG_FUSION_KEYWORDS)?;
    Ok(NodedbStatement::Graph(GraphStmt::GraphRagFusion {
        collection,
        params,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::graph_parse::cursor::Cursor;
    use crate::ddl_ast::graph_parse::tokenizer;

    fn parse(sql: &str, prefix_len: usize, statement: &str) -> Result<NodedbStatement, SqlError> {
        let mut cursor = Cursor::new(tokenizer::tokenize(sql), prefix_len);
        let parsed = match statement {
            "GRAPH TRAVERSE" => parse_traverse(&mut cursor),
            "GRAPH PATH" => parse_path(&mut cursor),
            other => panic!("unhandled test statement {other}"),
        };
        parsed.and_then(|stmt| cursor.finish(statement).map(|()| stmt))
    }

    #[test]
    fn a_mistyped_keyword_fails_the_statement() {
        let err = parse("GRAPH TRAVERSE FROM 1 DEPTS 3 IN g", 2, "GRAPH TRAVERSE")
            .expect_err("DEPTS is not a clause");
        assert!(err.to_string().contains("DEPTS"), "{err}");
    }

    #[test]
    fn a_valid_statement_still_parses() {
        let stmt = parse(
            "GRAPH TRAVERSE FROM 1 DEPTH 3 IN g DIRECTION both",
            2,
            "GRAPH TRAVERSE",
        )
        .expect("every token belongs to a clause");
        let NodedbStatement::Graph(GraphStmt::GraphTraverse { depth, .. }) = stmt else {
            panic!("expected a traverse statement");
        };
        assert_eq!(depth, 3);
    }

    #[test]
    fn a_stray_literal_fails_the_statement() {
        // The statement needs its required IN clause: without it the parser
        // correctly refuses for the missing clause before it ever reaches the
        // stray token. With IN present, the unclaimed trailing literal is what
        // the cursor's finish step must name.
        let err = parse("GRAPH PATH IN g FROM 'a' TO 'b' 'stray'", 2, "GRAPH PATH")
            .expect_err("the stray literal belongs to no clause");
        assert!(err.to_string().contains("stray"), "{err}");
        assert!(err.to_string().contains("unexpected token"), "{err}");
    }
}
