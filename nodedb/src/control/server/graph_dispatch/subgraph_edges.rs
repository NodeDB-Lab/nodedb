// SPDX-License-Identifier: BUSL-1.1

//! Physical edge recording for subgraph traversal.
//!
//! A `NeighborsMulti` row is `(frontier node, label, neighbour)` in either
//! direction, so a row carries no orientation of its own. The walk expands
//! each direction in its own pass and records each row in the orientation
//! of that pass.

use std::collections::{HashMap, HashSet};

use nodedb_types::Value;

use super::neighbor_rows::NeighborRow;
use crate::engine::graph::edge_store::Direction;

/// The direction of one expansion pass.
#[derive(Clone, Copy)]
pub(super) enum EdgeOrientation {
    Out,
    In,
}

impl EdgeOrientation {
    /// The passes one hop in `direction` runs. `Both` expands outgoing, then
    /// incoming, edges over the same frontier.
    pub(super) fn passes(direction: Direction) -> &'static [EdgeOrientation] {
        match direction {
            Direction::Out => &[EdgeOrientation::Out],
            Direction::In => &[EdgeOrientation::In],
            Direction::Both => &[EdgeOrientation::Out, EdgeOrientation::In],
        }
    }

    pub(super) fn direction(self) -> Direction {
        match self {
            Self::Out => Direction::Out,
            Self::In => Direction::In,
        }
    }
}

/// One physical edge a walk crossed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalkEdge {
    pub src: String,
    pub label: String,
    pub dst: String,
    /// The edge's property object, when the walk returns properties.
    pub properties: Option<HashMap<String, Value>>,
}

/// The edges a walk crossed, each physical edge once, in first-crossed
/// order.
#[derive(Default)]
pub(super) struct PhysicalEdges {
    edges: Vec<WalkEdge>,
    seen: HashSet<(String, String, String)>,
}

impl PhysicalEdges {
    /// Record the rows of one pass. An incoming row is flipped to its
    /// physical `(neighbour, label, frontier node)` orientation.
    pub(super) fn record(&mut self, rows: Vec<NeighborRow>, orientation: EdgeOrientation) {
        for row in rows {
            let NeighborRow {
                src: frontier_node,
                label,
                node: neighbor,
                properties,
            } = row;
            let (src, dst) = match orientation {
                EdgeOrientation::Out => (frontier_node, neighbor),
                EdgeOrientation::In => (neighbor, frontier_node),
            };
            if self.seen.insert((src.clone(), label.clone(), dst.clone())) {
                self.edges.push(WalkEdge {
                    src,
                    label,
                    dst,
                    properties,
                });
            }
        }
    }

    pub(super) fn into_vec(self) -> Vec<WalkEdge> {
        self.edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, label: &str, neighbor: &str) -> NeighborRow {
        NeighborRow {
            src: source.into(),
            label: label.into(),
            node: neighbor.into(),
            properties: None,
        }
    }

    fn edge(src: &str, label: &str, dst: &str) -> WalkEdge {
        WalkEdge {
            src: src.into(),
            label: label.into(),
            dst: dst.into(),
            properties: None,
        }
    }

    #[test]
    fn incoming_rows_keep_physical_orientation() {
        let mut edges = PhysicalEdges::default();
        edges.record(vec![row("b", "LINK", "a")], EdgeOrientation::In);
        assert_eq!(edges.into_vec(), vec![edge("a", "LINK", "b")]);
    }

    #[test]
    fn reciprocal_edges_stay_distinct_and_self_loops_appear_once() {
        let mut edges = PhysicalEdges::default();
        edges.record(
            vec![row("a", "LINK", "b"), row("a", "SELF", "a")],
            EdgeOrientation::Out,
        );
        // The same physical edges, seen from their other endpoint.
        edges.record(
            vec![row("b", "LINK", "a"), row("a", "SELF", "a")],
            EdgeOrientation::In,
        );
        // The reverse edge `b -> a`, seen from `b`.
        edges.record(vec![row("b", "LINK", "a")], EdgeOrientation::Out);
        assert_eq!(
            edges.into_vec(),
            vec![
                edge("a", "LINK", "b"),
                edge("a", "SELF", "a"),
                edge("b", "LINK", "a"),
            ]
        );
    }

    #[test]
    fn a_recorded_edge_keeps_its_properties() {
        let mut edges = PhysicalEdges::default();
        let mut with_properties = row("b", "LINK", "a");
        with_properties.properties = Some(HashMap::from([("w".to_string(), Value::Integer(3))]));
        edges.record(vec![with_properties], EdgeOrientation::In);
        let recorded = edges.into_vec();
        assert_eq!(recorded[0].src, "a");
        assert_eq!(
            recorded[0]
                .properties
                .as_ref()
                .and_then(|p| p.get("w").cloned()),
            Some(Value::Integer(3))
        );
    }

    #[test]
    fn both_runs_an_outgoing_then_an_incoming_pass() {
        let passes: Vec<Direction> = EdgeOrientation::passes(Direction::Both)
            .iter()
            .map(|pass| pass.direction())
            .collect();
        assert_eq!(passes, vec![Direction::Out, Direction::In]);
    }
}
