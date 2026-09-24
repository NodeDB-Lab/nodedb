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

/// One `(src, dst, label)` triple inside a batched edge statement
/// (`GRAPH INSERT EDGES` / `GRAPH DELETE EDGES`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdgeTuple {
    pub src: String,
    pub dst: String,
    pub label: String,
}
