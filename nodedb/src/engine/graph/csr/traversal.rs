//! Graph traversal algorithms on the CSR index.
//!
//! BFS, bidirectional shortest path, and subgraph materialization.
//! All algorithms respect a max-visited cap to prevent supernode fan-out
//! explosion from consuming unbounded memory.

use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};

use crate::engine::graph::edge_store::Direction;

use super::index::CsrIndex;

/// Default cap on visited nodes during BFS traversals.
/// Prevents supernode fan-out explosion from consuming unbounded memory.
const DEFAULT_MAX_VISITED: usize = 100_000;

impl CsrIndex {
    /// BFS traversal. Returns all reachable node IDs within max_depth hops.
    pub fn traverse_bfs(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
    ) -> Vec<String> {
        let label_id = label_filter.and_then(|l| self.label_to_id.get(l).copied());
        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node) {
                if visited.insert(id) {
                    queue.push_back((id, 0));
                }
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= DEFAULT_MAX_VISITED {
                continue;
            }

            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.iter_out_edges(node_id) {
                    if label_id.is_none_or(|f| f == lid)
                        && visited.len() < DEFAULT_MAX_VISITED
                        && visited.insert(dst)
                    {
                        queue.push_back((dst, depth + 1));
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.iter_in_edges(node_id) {
                    if label_id.is_none_or(|f| f == lid)
                        && visited.len() < DEFAULT_MAX_VISITED
                        && visited.insert(src)
                    {
                        queue.push_back((src, depth + 1));
                    }
                }
            }
        }

        visited
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Shortest path via bidirectional BFS.
    pub fn shortest_path(
        &self,
        src: &str,
        dst: &str,
        label_filter: Option<&str>,
        max_depth: usize,
    ) -> Option<Vec<String>> {
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;
        if src_id == dst_id {
            return Some(vec![src.to_string()]);
        }

        let label_id = label_filter.and_then(|l| self.label_to_id.get(l).copied());
        let mut fwd_parent: HashMap<u32, u32> = HashMap::new();
        let mut bwd_parent: HashMap<u32, u32> = HashMap::new();
        fwd_parent.insert(src_id, src_id);
        bwd_parent.insert(dst_id, dst_id);

        let mut fwd_frontier: Vec<u32> = vec![src_id];
        let mut bwd_frontier: Vec<u32> = vec![dst_id];

        for _depth in 0..max_depth {
            if fwd_parent.len() + bwd_parent.len() >= DEFAULT_MAX_VISITED {
                break;
            }

            let mut next_fwd = Vec::new();
            for &node in &fwd_frontier {
                for (lid, neighbor) in self.iter_out_edges(node) {
                    if label_id.is_none_or(|f| f == lid) {
                        if let Entry::Vacant(e) = fwd_parent.entry(neighbor) {
                            e.insert(node);
                            next_fwd.push(neighbor);
                        }
                        if bwd_parent.contains_key(&neighbor) {
                            return Some(self.reconstruct_path(neighbor, &fwd_parent, &bwd_parent));
                        }
                    }
                }
            }
            fwd_frontier = next_fwd;

            let mut next_bwd = Vec::new();
            for &node in &bwd_frontier {
                for (lid, neighbor) in self.iter_in_edges(node) {
                    if label_id.is_none_or(|f| f == lid) {
                        if let Entry::Vacant(e) = bwd_parent.entry(neighbor) {
                            e.insert(node);
                            next_bwd.push(neighbor);
                        }
                        if fwd_parent.contains_key(&neighbor) {
                            return Some(self.reconstruct_path(neighbor, &fwd_parent, &bwd_parent));
                        }
                    }
                }
            }
            bwd_frontier = next_bwd;

            if fwd_frontier.is_empty() && bwd_frontier.is_empty() {
                break;
            }
        }
        None
    }

    fn reconstruct_path(
        &self,
        meeting: u32,
        fwd_parent: &HashMap<u32, u32>,
        bwd_parent: &HashMap<u32, u32>,
    ) -> Vec<String> {
        let mut fwd_path = Vec::new();
        let mut current = meeting;
        loop {
            fwd_path.push(current);
            let parent = fwd_parent[&current];
            if parent == current {
                break;
            }
            current = parent;
        }
        fwd_path.reverse();

        current = bwd_parent[&meeting];
        if current != meeting {
            loop {
                fwd_path.push(current);
                let parent = bwd_parent[&current];
                if parent == current {
                    break;
                }
                current = parent;
            }
        }

        fwd_path
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Materialize a subgraph as edge tuples within max_depth.
    pub fn subgraph(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        max_depth: usize,
    ) -> Vec<(String, String, String)> {
        let label_id = label_filter.and_then(|l| self.label_to_id.get(l).copied());
        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();
        let mut edges = Vec::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node) {
                if visited.insert(id) {
                    queue.push_back((id, 0));
                }
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= DEFAULT_MAX_VISITED {
                continue;
            }
            for (lid, dst) in self.iter_out_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    edges.push((
                        self.id_to_node[node_id as usize].clone(),
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                    if visited.len() < DEFAULT_MAX_VISITED && visited.insert(dst) {
                        queue.push_back((dst, depth + 1));
                    }
                }
            }
        }

        edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.add_edge("c", "KNOWS", "d");
        csr.add_edge("a", "WORKS", "e");
        csr
    }

    #[test]
    fn bfs_traversal() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(&["a"], Some("KNOWS"), Direction::Out, 2);
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_all_labels() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(&["a"], None, Direction::Out, 1);
        result.sort();
        assert_eq!(result, vec!["a", "b", "e"]);
    }

    #[test]
    fn bfs_cycle() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");
        let mut result = csr.traverse_bfs(&["a"], None, Direction::Out, 10);
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_direct() {
        let csr = make_csr();
        let path = csr.shortest_path("a", "c", Some("KNOWS"), 5).unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_same_node() {
        let csr = make_csr();
        let path = csr.shortest_path("a", "a", None, 5).unwrap();
        assert_eq!(path, vec!["a"]);
    }

    #[test]
    fn shortest_path_unreachable() {
        let csr = make_csr();
        let path = csr.shortest_path("d", "a", Some("KNOWS"), 5);
        assert!(path.is_none());
    }

    #[test]
    fn shortest_path_depth_limit() {
        let csr = make_csr();
        let path = csr.shortest_path("a", "d", Some("KNOWS"), 1);
        assert!(path.is_none());
    }

    #[test]
    fn subgraph_materialization() {
        let csr = make_csr();
        let edges = csr.subgraph(&["a"], None, 2);
        assert_eq!(edges.len(), 3);
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "b".into())));
        assert!(edges.contains(&("a".into(), "WORKS".into(), "e".into())));
        assert!(edges.contains(&("b".into(), "KNOWS".into(), "c".into())));
    }
}
