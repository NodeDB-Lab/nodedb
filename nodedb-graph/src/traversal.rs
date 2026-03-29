//! Graph traversal algorithms on the CSR index.
//!
//! BFS, bidirectional shortest path, and subgraph materialization.
//! All algorithms respect a max-visited cap to prevent supernode fan-out
//! explosion from consuming unbounded memory.
//!
//! Access tracking and prefetch hints are integrated: each traversal records
//! node access for hot/cold partition decisions, and prefetches frontier
//! neighbors for cache efficiency.

use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};

pub use nodedb_types::config::tuning::DEFAULT_MAX_VISITED;

use crate::csr::{CsrIndex, Direction};

impl CsrIndex {
    /// BFS traversal. Returns all reachable node IDs within max_depth hops.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn traverse_bfs(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<String> {
        let label_id = label_filter.and_then(|l| self.label_id(l));
        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();

        for &node in start_nodes {
            if let Some(id) = self.node_id(node)
                && visited.insert(id)
            {
                queue.push_back((id, 0));
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }

            // Track access for hot/cold partition decisions.
            self.record_access(node_id);

            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.iter_out_edges(node_id) {
                    if label_id.is_none_or(|f| f == lid)
                        && visited.len() < max_visited
                        && visited.insert(dst)
                    {
                        self.prefetch_node(dst);
                        queue.push_back((dst, depth + 1));
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.iter_in_edges(node_id) {
                    if label_id.is_none_or(|f| f == lid)
                        && visited.len() < max_visited
                        && visited.insert(src)
                    {
                        self.prefetch_node(src);
                        queue.push_back((src, depth + 1));
                    }
                }
            }
        }

        visited
            .into_iter()
            .map(|id| self.node_name(id).to_string())
            .collect()
    }

    /// BFS traversal returning nodes with depth information.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn traverse_bfs_with_depth(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, u8)> {
        let filters: Vec<&str> = label_filter.into_iter().collect();
        self.traverse_bfs_with_depth_multi(start_nodes, &filters, direction, max_depth, max_visited)
    }

    /// BFS traversal with multi-label filter. Empty labels = all edges.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn traverse_bfs_with_depth_multi(
        &self,
        start_nodes: &[&str],
        label_filters: &[&str],
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, u8)> {
        let label_ids: Vec<u16> = label_filters
            .iter()
            .filter_map(|l| self.label_id(l))
            .collect();
        let match_label = |lid: u16| label_ids.is_empty() || label_ids.contains(&lid);
        let mut visited: HashMap<u32, u8> = HashMap::new();
        let mut queue: VecDeque<(u32, u8)> = VecDeque::new();

        for &node in start_nodes {
            if let Some(id) = self.node_id(node) {
                visited.insert(id, 0);
                queue.push_back((id, 0));
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth as usize >= max_depth || visited.len() >= max_visited {
                continue;
            }

            let next_depth = depth + 1;

            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.iter_out_edges(node_id) {
                    if match_label(lid)
                        && visited.len() < max_visited
                        && !visited.contains_key(&dst)
                    {
                        visited.insert(dst, next_depth);
                        queue.push_back((dst, next_depth));
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.iter_in_edges(node_id) {
                    if match_label(lid)
                        && visited.len() < max_visited
                        && !visited.contains_key(&src)
                    {
                        visited.insert(src, next_depth);
                        queue.push_back((src, next_depth));
                    }
                }
            }
        }

        visited
            .into_iter()
            .map(|(id, depth)| (self.node_name(id).to_string(), depth))
            .collect()
    }

    /// Shortest path via bidirectional BFS.
    ///
    /// `max_visited` caps the combined forward+backward visited set to prevent
    /// supernode fan-out explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn shortest_path(
        &self,
        src: &str,
        dst: &str,
        label_filter: Option<&str>,
        max_depth: usize,
        max_visited: usize,
    ) -> Option<Vec<String>> {
        let src_id = self.node_id(src)?;
        let dst_id = self.node_id(dst)?;
        if src_id == dst_id {
            return Some(vec![src.to_string()]);
        }

        let label_id = label_filter.and_then(|l| self.label_id(l));
        let mut fwd_parent: HashMap<u32, u32> = HashMap::new();
        let mut bwd_parent: HashMap<u32, u32> = HashMap::new();
        fwd_parent.insert(src_id, src_id);
        bwd_parent.insert(dst_id, dst_id);

        let mut fwd_frontier: Vec<u32> = vec![src_id];
        let mut bwd_frontier: Vec<u32> = vec![dst_id];

        for _depth in 0..max_depth {
            if fwd_parent.len() + bwd_parent.len() >= max_visited {
                break;
            }

            let mut next_fwd = Vec::new();
            for &node in &fwd_frontier {
                self.record_access(node);
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
                self.record_access(node);
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
            .map(|id| self.node_name(id).to_string())
            .collect()
    }

    /// Materialize a subgraph as edge tuples within max_depth.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn subgraph(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, String, String)> {
        let label_id = label_filter.and_then(|l| self.label_id(l));
        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();
        let mut edges = Vec::new();

        for &node in start_nodes {
            if let Some(id) = self.node_id(node)
                && visited.insert(id)
            {
                queue.push_back((id, 0));
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }
            self.record_access(node_id);
            for (lid, dst) in self.iter_out_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    edges.push((
                        self.node_name(node_id).to_string(),
                        self.label_name(lid).to_string(),
                        self.node_name(dst).to_string(),
                    ));
                    if visited.len() < max_visited && visited.insert(dst) {
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
        let mut result = csr.traverse_bfs(
            &["a"],
            Some("KNOWS"),
            Direction::Out,
            2,
            DEFAULT_MAX_VISITED,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_all_labels() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(&["a"], None, Direction::Out, 1, DEFAULT_MAX_VISITED);
        result.sort();
        assert_eq!(result, vec!["a", "b", "e"]);
    }

    #[test]
    fn bfs_cycle() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");
        let mut result = csr.traverse_bfs(&["a"], None, Direction::Out, 10, DEFAULT_MAX_VISITED);
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_with_depth() {
        let csr = make_csr();
        let result = csr.traverse_bfs_with_depth(
            &["a"],
            Some("KNOWS"),
            Direction::Out,
            3,
            DEFAULT_MAX_VISITED,
        );
        let map: HashMap<String, u8> = result.into_iter().collect();
        assert_eq!(map["a"], 0);
        assert_eq!(map["b"], 1);
        assert_eq!(map["c"], 2);
        assert_eq!(map["d"], 3);
    }

    #[test]
    fn shortest_path_direct() {
        let csr = make_csr();
        let path = csr
            .shortest_path("a", "c", Some("KNOWS"), 5, DEFAULT_MAX_VISITED)
            .unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_same_node() {
        let csr = make_csr();
        let path = csr
            .shortest_path("a", "a", None, 5, DEFAULT_MAX_VISITED)
            .unwrap();
        assert_eq!(path, vec!["a"]);
    }

    #[test]
    fn shortest_path_unreachable() {
        let csr = make_csr();
        let path = csr.shortest_path("d", "a", Some("KNOWS"), 5, DEFAULT_MAX_VISITED);
        assert!(path.is_none());
    }

    #[test]
    fn shortest_path_depth_limit() {
        let csr = make_csr();
        let path = csr.shortest_path("a", "d", Some("KNOWS"), 1, DEFAULT_MAX_VISITED);
        assert!(path.is_none());
    }

    #[test]
    fn subgraph_materialization() {
        let csr = make_csr();
        let edges = csr.subgraph(&["a"], None, 2, DEFAULT_MAX_VISITED);
        assert_eq!(edges.len(), 3);
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "b".into())));
        assert!(edges.contains(&("a".into(), "WORKS".into(), "e".into())));
        assert!(edges.contains(&("b".into(), "KNOWS".into(), "c".into())));
    }

    #[test]
    fn large_graph_bfs() {
        let mut csr = CsrIndex::new();
        for i in 0..999 {
            csr.add_edge(&format!("n{i}"), "NEXT", &format!("n{}", i + 1));
        }
        csr.compact();

        let result = csr.traverse_bfs(
            &["n0"],
            Some("NEXT"),
            Direction::Out,
            100,
            DEFAULT_MAX_VISITED,
        );
        assert_eq!(result.len(), 101);

        let path = csr
            .shortest_path("n0", "n50", Some("NEXT"), 100, DEFAULT_MAX_VISITED)
            .unwrap();
        assert_eq!(path.len(), 51);
    }
}
