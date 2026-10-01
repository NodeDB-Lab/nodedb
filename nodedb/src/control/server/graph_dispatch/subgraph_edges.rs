// SPDX-License-Identifier: BUSL-1.1

//! Physical edge recording for subgraph traversal.
//!
//! A `NeighborsMulti` row is `(frontier node, label, neighbour)` in either
//! direction, so a row carries no orientation of its own. The walk expands
//! each direction in its own pass and records each row in the orientation
//! of that pass.

use std::collections::HashSet;

use super::hop::NeighborTriple;
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

/// The `(src, label, dst)` edges a walk crossed, each physical edge once,
/// in first-crossed order.
#[derive(Default)]
pub(super) struct PhysicalEdges {
    edges: Vec<NeighborTriple>,
    seen: HashSet<NeighborTriple>,
}

impl PhysicalEdges {
    /// Record the rows of one pass. An incoming row is flipped to its
    /// physical `(neighbour, label, frontier node)` orientation.
    pub(super) fn record(&mut self, rows: Vec<NeighborTriple>, orientation: EdgeOrientation) {
        for (frontier_node, label, neighbor) in rows {
            let edge = match orientation {
                EdgeOrientation::Out => (frontier_node, label, neighbor),
                EdgeOrientation::In => (neighbor, label, frontier_node),
            };
            if self.seen.insert(edge.clone()) {
                self.edges.push(edge);
            }
        }
    }

    pub(super) fn into_vec(self) -> Vec<NeighborTriple> {
        self.edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, label: &str, neighbor: &str) -> NeighborTriple {
        (source.into(), label.into(), neighbor.into())
    }

    #[test]
    fn incoming_rows_keep_physical_orientation() {
        let mut edges = PhysicalEdges::default();
        edges.record(vec![row("b", "LINK", "a")], EdgeOrientation::In);
        assert_eq!(edges.into_vec(), vec![row("a", "LINK", "b")]);
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
                row("a", "LINK", "b"),
                row("a", "SELF", "a"),
                row("b", "LINK", "a"),
            ]
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
