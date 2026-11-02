// SPDX-License-Identifier: Apache-2.0

//! Graph DSL types used in `NodedbStatement` graph variants.

/// Traversal direction for graph DSL variants. Mirrors the engine's
/// own `Direction` enum so `nodedb-sql` has no dependency cycle with
/// `nodedb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    In,
    Out,
    Both,
}

/// The `PROPERTIES` clause of `GRAPH INSERT EDGE`. Captured in its
/// source form so the pgwire handler — which already depends on a
/// JSON serializer (sonic_rs) — can do the conversion to storage
/// bytes without dragging JSON deps into `nodedb-sql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphProperties {
    None,
    /// Raw `{ ... }` object-literal span, including the outer braces
    /// and brace-balanced inner content. Parsed by
    /// `crate::parser::object_literal` at the handler boundary.
    Object(String),
    /// Content of `'...'` (outer quotes stripped, `''` un-escaped);
    /// expected to already be a JSON document.
    Quoted(String),
}

/// One edge of a `GRAPH INSERT EDGE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdgeSpec {
    pub src: String,
    pub dst: String,
    pub label: String,
    pub properties: GraphProperties,
    /// `VALID_TIME <from>`: start of the edge's valid time, ms since
    /// epoch. `None` takes the write's wall time.
    pub valid_from_ms: Option<i64>,
    /// `UNTIL <until>`: exclusive end of the edge's valid time. `None`
    /// leaves it open.
    pub valid_until_ms: Option<i64>,
}
