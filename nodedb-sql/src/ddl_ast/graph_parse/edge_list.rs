// SPDX-License-Identifier: Apache-2.0

//! The edge-list statements: `GRAPH INSERT EDGE` and `GRAPH DELETE EDGE`.
//!
//! Both take one or more edges after `IN <collection>`. An edge is a run of
//! clauses. A clause the current edge already has starts the next edge, so
//! `FROM 'a' TO 'b' TYPE 'l' FROM 'c' TO 'd' TYPE 'l'` names two edges. An
//! edge missing a required clause refuses the whole statement.

use super::{
    super::graph_types::{GraphEdgeKey, GraphEdgeSpec, GraphProperties},
    super::statement::{GraphStmt, NodedbStatement},
    cursor::{Cursor, missing_clause, set_once},
    tokenizer::Tok,
};
use crate::error::SqlError;

const INSERT: &str = "GRAPH INSERT EDGE";
const DELETE: &str = "GRAPH DELETE EDGE";

/// Parse `GRAPH INSERT EDGE IN <c> FROM <a> TO <b> TYPE <l> [PROPERTIES ...]
/// [VALID_TIME <from> [UNTIL <until>]]`, followed by any number of further
/// edges.
pub(super) fn parse_insert_edge(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    let mut c = Cursor::new(toks, INSERT, 3);
    let mut collection = None;
    let mut edges = Vec::new();
    let mut draft = EdgeDraft::default();
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", INSERT)?,
            "FROM" | "TO" | "TYPE" | "PROPERTIES" | "VALID_TIME" | "UNTIL" => {
                if draft.has(&clause) {
                    edges.push(std::mem::take(&mut draft).finish()?);
                }
                draft.read(&clause, &mut c)?;
            }
            _ => return Err(c.unknown_clause()),
        }
    }
    edges.push(draft.finish()?);
    Ok(NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
        collection: collection.ok_or_else(|| missing_clause(INSERT, "IN <collection>"))?,
        edges,
    }))
}

/// Parse `GRAPH DELETE EDGE IN <c> FROM <a> TO <b> TYPE <l>`, followed by any
/// number of further edges.
pub(super) fn parse_delete_edge(toks: &[Tok<'_>]) -> Result<NodedbStatement, SqlError> {
    let mut c = Cursor::new(toks, DELETE, 3);
    let mut collection = None;
    let mut edges = Vec::new();
    let mut draft = KeyDraft::default();
    while let Some(clause) = c.next_clause()? {
        match clause.as_str() {
            "IN" => set_once(&mut collection, c.value("IN")?, "IN", DELETE)?,
            "FROM" | "TO" | "TYPE" => {
                if draft.has(&clause) {
                    edges.push(std::mem::take(&mut draft).finish()?);
                }
                draft.read(&clause, &mut c)?;
            }
            _ => return Err(c.unknown_clause()),
        }
    }
    edges.push(draft.finish()?);
    Ok(NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
        collection: collection.ok_or_else(|| missing_clause(DELETE, "IN <collection>"))?,
        edges,
    }))
}

/// The clauses of one `GRAPH INSERT EDGE` edge, read so far.
#[derive(Default)]
struct EdgeDraft {
    src: Option<String>,
    dst: Option<String>,
    label: Option<String>,
    properties: Option<GraphProperties>,
    valid_from_ms: Option<i64>,
    valid_until_ms: Option<i64>,
}

impl EdgeDraft {
    /// Whether the edge already has the clause `keyword`.
    fn has(&self, keyword: &str) -> bool {
        match keyword {
            "FROM" => self.src.is_some(),
            "TO" => self.dst.is_some(),
            "TYPE" => self.label.is_some(),
            "PROPERTIES" => self.properties.is_some(),
            "VALID_TIME" => self.valid_from_ms.is_some(),
            "UNTIL" => self.valid_until_ms.is_some(),
            _ => false,
        }
    }

    /// Read the value of the clause `keyword` off `c`.
    fn read(&mut self, keyword: &str, c: &mut Cursor<'_, '_>) -> Result<(), SqlError> {
        match keyword {
            "FROM" => self.src = Some(c.value("FROM")?),
            "TO" => self.dst = Some(c.value("TO")?),
            "TYPE" => self.label = Some(c.value("TYPE")?),
            "PROPERTIES" => self.properties = Some(c.properties()?),
            "VALID_TIME" => self.valid_from_ms = Some(c.timestamp("VALID_TIME")?),
            "UNTIL" => self.valid_until_ms = Some(c.timestamp("UNTIL")?),
            _ => return Err(c.unknown_clause()),
        }
        Ok(())
    }

    fn finish(self) -> Result<GraphEdgeSpec, SqlError> {
        check_valid_interval(self.valid_from_ms, self.valid_until_ms)?;
        Ok(GraphEdgeSpec {
            src: self
                .src
                .ok_or_else(|| missing_clause(INSERT, "FROM <node>"))?,
            dst: self
                .dst
                .ok_or_else(|| missing_clause(INSERT, "TO <node>"))?,
            label: self
                .label
                .ok_or_else(|| missing_clause(INSERT, "TYPE <label>"))?,
            properties: self.properties.unwrap_or(GraphProperties::None),
            valid_from_ms: self.valid_from_ms,
            valid_until_ms: self.valid_until_ms,
        })
    }
}

/// The clauses of one `GRAPH DELETE EDGE` edge, read so far.
#[derive(Default)]
struct KeyDraft {
    src: Option<String>,
    dst: Option<String>,
    label: Option<String>,
}

impl KeyDraft {
    /// Whether the edge already has the clause `keyword`.
    fn has(&self, keyword: &str) -> bool {
        match keyword {
            "FROM" => self.src.is_some(),
            "TO" => self.dst.is_some(),
            "TYPE" => self.label.is_some(),
            _ => false,
        }
    }

    /// Read the value of the clause `keyword` off `c`.
    fn read(&mut self, keyword: &str, c: &mut Cursor<'_, '_>) -> Result<(), SqlError> {
        match keyword {
            "FROM" => self.src = Some(c.value("FROM")?),
            "TO" => self.dst = Some(c.value("TO")?),
            "TYPE" => self.label = Some(c.value("TYPE")?),
            _ => return Err(c.unknown_clause()),
        }
        Ok(())
    }

    fn finish(self) -> Result<GraphEdgeKey, SqlError> {
        Ok(GraphEdgeKey {
            src: self
                .src
                .ok_or_else(|| missing_clause(DELETE, "FROM <node>"))?,
            dst: self
                .dst
                .ok_or_else(|| missing_clause(DELETE, "TO <node>"))?,
            label: self
                .label
                .ok_or_else(|| missing_clause(DELETE, "TYPE <label>"))?,
        })
    }
}

/// `UNTIL` needs `VALID_TIME`, and the interval `[from, until)` must not be
/// empty.
fn check_valid_interval(from: Option<i64>, until: Option<i64>) -> Result<(), SqlError> {
    match (from, until) {
        (None, Some(_)) => Err(SqlError::Parse {
            detail: format!("{INSERT}: UNTIL needs VALID_TIME <from>"),
        }),
        (Some(from), Some(until)) if until <= from => Err(SqlError::Parse {
            detail: format!(
                "{INSERT}: VALID_TIME {from} UNTIL {until} is empty; UNTIL must be after \
                 VALID_TIME"
            ),
        }),
        _ => Ok(()),
    }
}
