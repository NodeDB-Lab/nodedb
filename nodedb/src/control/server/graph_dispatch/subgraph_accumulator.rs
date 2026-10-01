// SPDX-License-Identifier: Apache-2.0

//! Physical edge recording and bounded node admission for subgraph traversal.

use std::collections::HashSet;
use std::num::NonZeroU32;

use super::hop::NeighborTriple;
use crate::engine::graph::edge_store::Direction;

#[derive(Clone, Copy)]
pub(super) enum EdgeOrientation {
    Out,
    In,
}

impl EdgeOrientation {
    pub(super) fn direction(self) -> Direction {
        match self {
            Self::Out => Direction::Out,
            Self::In => Direction::In,
        }
    }
}

pub(super) struct RowAllowance {
    remaining: u32,
}

impl RowAllowance {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            remaining: limit.min(u32::MAX as usize) as u32,
        }
    }

    pub(super) fn dispatch_limit(&self) -> Option<NonZeroU32> {
        NonZeroU32::new(self.remaining)
    }

    pub(super) fn consume(&mut self, rows: usize) {
        self.remaining = self
            .remaining
            .saturating_sub(rows.min(u32::MAX as usize) as u32);
    }
}

pub(super) struct SubgraphAccumulator {
    pub(super) nodes: Vec<(String, u8)>,
    pub(super) edges: Vec<NeighborTriple>,
    seen_nodes: HashSet<String>,
    seen_edges: HashSet<NeighborTriple>,
    max_visited: usize,
}

impl SubgraphAccumulator {
    pub(super) fn new(start: String, max_visited: usize) -> Self {
        let mut state = Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            seen_nodes: HashSet::new(),
            seen_edges: HashSet::new(),
            max_visited,
        };
        if max_visited > 0 {
            state.seen_nodes.insert(start.clone());
            state.nodes.push((start, 0));
        }
        state
    }

    pub(super) fn remaining_nodes(&self) -> usize {
        self.max_visited.saturating_sub(self.nodes.len())
    }

    pub(super) fn record(
        &mut self,
        rows: Vec<NeighborTriple>,
        direction: EdgeOrientation,
        depth: u8,
        next_frontier: &mut Vec<String>,
    ) {
        for (source, label, neighbor) in rows {
            if !self.seen_nodes.contains(&source) {
                continue;
            }
            // NeighborsMulti's third field is the discovered node in either direction.
            if !self.seen_nodes.contains(&neighbor) {
                if self.remaining_nodes() == 0 {
                    continue;
                }
                self.seen_nodes.insert(neighbor.clone());
                self.nodes.push((neighbor.clone(), depth));
                next_frontier.push(neighbor.clone());
            }
            let edge = match direction {
                EdgeOrientation::Out => (source, label, neighbor),
                EdgeOrientation::In => (neighbor, label, source),
            };
            if self.seen_edges.insert(edge.clone()) {
                self.edges.push(edge);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, label: &str, neighbor: &str) -> NeighborTriple {
        (source.into(), label.into(), neighbor.into())
    }

    #[test]
    fn incoming_rows_discover_neighbor_and_preserve_physical_orientation() {
        let mut state = SubgraphAccumulator::new("b".into(), 10);
        let mut next = Vec::new();
        state.record(
            vec![row("b", "LINK", "a")],
            EdgeOrientation::In,
            1,
            &mut next,
        );
        assert_eq!(next, vec!["a"]);
        assert_eq!(state.nodes, vec![("b".into(), 0), ("a".into(), 1)]);
        assert_eq!(state.edges, vec![row("a", "LINK", "b")]);
    }

    #[test]
    fn reciprocal_edges_remain_distinct_and_self_loops_appear_once() {
        let mut state = SubgraphAccumulator::new("a".into(), 10);
        let mut next = Vec::new();
        state.record(
            vec![row("a", "LINK", "b"), row("a", "SELF", "a")],
            EdgeOrientation::Out,
            1,
            &mut next,
        );
        state.record(
            vec![row("a", "LINK", "b"), row("a", "SELF", "a")],
            EdgeOrientation::In,
            1,
            &mut next,
        );
        state.record(
            vec![row("b", "LINK", "a")],
            EdgeOrientation::In,
            2,
            &mut next,
        );
        assert_eq!(
            state.edges,
            vec![
                row("a", "LINK", "b"),
                row("a", "SELF", "a"),
                row("b", "LINK", "a")
            ]
        );
        assert_eq!(state.nodes, vec![("a".into(), 0), ("b".into(), 1)]);
        assert_eq!(next, vec!["b"]);
    }

    #[test]
    fn node_admission_excludes_dangling_edges_across_batches() {
        let mut state = SubgraphAccumulator::new("a".into(), 2);
        let mut next = Vec::new();
        state.record(
            vec![row("a", "LINK", "b")],
            EdgeOrientation::Out,
            1,
            &mut next,
        );
        state.record(
            vec![row("a", "LINK", "c"), row("unknown", "LINK", "a")],
            EdgeOrientation::In,
            1,
            &mut next,
        );
        assert_eq!(state.nodes.len(), 2);
        assert_eq!(state.remaining_nodes(), 0);
        assert_eq!(state.edges, vec![row("a", "LINK", "b")]);
        let empty = SubgraphAccumulator::new("a".into(), 0);
        assert!(empty.nodes.is_empty());
    }

    #[test]
    fn outgoing_rows_exhaust_shared_allowance_before_incoming_dispatch() {
        let mut allowance = RowAllowance::new(2);
        assert_eq!(allowance.dispatch_limit().map(NonZeroU32::get), Some(2));
        // Duplicate rows and already visited destinations consume raw-row allowance.
        let rows = [row("a", "SELF", "a"), row("a", "SELF", "a")];
        allowance.consume(rows.len());
        assert!(allowance.dispatch_limit().is_none());
        assert!(RowAllowance::new(0).dispatch_limit().is_none());
    }
}
