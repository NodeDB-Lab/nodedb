//! Dense integer CSR adjacency index with interned node IDs and labels.
//!
//! Core data structure, interning, mutation (add/remove), neighbor queries,
//! and compaction. Traversal algorithms live in `traversal.rs`.

use std::collections::{HashMap, hash_map::Entry};

use crate::engine::graph::edge_store::{Direction, EdgeStore};

use super::weights::extract_weight_from_properties;

/// Dense integer CSR adjacency index with interned node IDs and labels.
///
/// Memory layout at scale (1B edges):
/// - Old: `Vec<Vec<(String, u32)>>` ≈ 60 GB (heap String per edge)
/// - New: contiguous `Vec<u32>` offsets + `Vec<u32>` targets + `Vec<u16>` labels ≈ 10 GB
///
/// Writes accumulate in a mutable buffer (`buffer_out`/`buffer_in`).
/// Reads check both the dense CSR arrays and the mutable buffer.
/// `compact()` merges the buffer into the dense arrays (double-buffered swap).
///
/// ## Edge Weights
///
/// Optional `f64` weight per edge stored in parallel arrays. `None` when the
/// graph is entirely unweighted (zero memory overhead). Populated from the
/// `"weight"` edge property at insertion time. Unweighted edges default to 1.0.
///
/// Layout: `out_targets[i]`, `out_labels[i]`, `out_weights.as_ref()[i]` are
/// parallel — SIMD-friendly for bulk scans (PageRank iterates all edges per
/// superstep; Dijkstra reads weights during traversal).
pub struct CsrIndex {
    // ── Node interning ──
    pub(super) node_to_id: HashMap<String, u32>,
    pub(super) id_to_node: Vec<String>,

    // ── Label interning ──
    pub(super) label_to_id: HashMap<String, u16>,
    pub(super) id_to_label: Vec<String>,

    // ── Dense CSR (read-only between compactions) ──
    /// `out_offsets[i]..out_offsets[i+1]` = range in `out_targets`/`out_labels`.
    /// Length: `num_nodes + 1`.
    pub(super) out_offsets: Vec<u32>,
    pub(super) out_targets: Vec<u32>,
    pub(super) out_labels: Vec<u16>,
    /// Parallel edge weight array. `None` if graph has no weighted edges.
    pub(super) out_weights: Option<Vec<f64>>,

    pub(super) in_offsets: Vec<u32>,
    pub(super) in_targets: Vec<u32>,
    pub(super) in_labels: Vec<u16>,
    /// Parallel inbound edge weight array. `None` if graph has no weighted edges.
    pub(super) in_weights: Option<Vec<f64>>,

    // ── Mutable write buffer ──
    /// Per-node outbound buffer: `buffer_out[node_id]` = `[(label_id, dst_id)]`.
    pub(super) buffer_out: Vec<Vec<(u16, u32)>>,
    pub(super) buffer_in: Vec<Vec<(u16, u32)>>,
    /// Per-node outbound weight buffer (parallel to `buffer_out`).
    /// Only populated when `has_weights` is true.
    pub(super) buffer_out_weights: Vec<Vec<f64>>,
    /// Per-node inbound weight buffer (parallel to `buffer_in`).
    pub(super) buffer_in_weights: Vec<Vec<f64>>,

    /// Edges deleted since last compaction: `(src, label, dst)`.
    pub(super) deleted_edges: std::collections::HashSet<(u32, u16, u32)>,

    /// Whether any edge has a non-default weight. When false, weight arrays
    /// are `None` and weight buffers are empty — zero overhead for unweighted graphs.
    pub(super) has_weights: bool,

    // ── Hot/cold access tracking ──
    /// Per-node access counter: incremented on each neighbor/BFS/path query.
    /// Uses `Cell<u32>` so access can be tracked through `&self` references
    /// (traversal methods are `&self` for shared read access).
    pub(super) access_counts: Vec<std::cell::Cell<u32>>,
    /// Total queries served since last access counter reset.
    pub(super) query_epoch: u64,
}

impl Default for CsrIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl CsrIndex {
    pub fn new() -> Self {
        Self {
            node_to_id: HashMap::new(),
            id_to_node: Vec::new(),
            label_to_id: HashMap::new(),
            id_to_label: Vec::new(),
            out_offsets: vec![0],
            out_targets: Vec::new(),
            out_labels: Vec::new(),
            out_weights: None,
            in_offsets: vec![0],
            in_targets: Vec::new(),
            in_labels: Vec::new(),
            in_weights: None,
            buffer_out: Vec::new(),
            buffer_in: Vec::new(),
            buffer_out_weights: Vec::new(),
            buffer_in_weights: Vec::new(),
            deleted_edges: std::collections::HashSet::new(),
            has_weights: false,
            access_counts: Vec::new(),
            query_epoch: 0,
        }
    }

    /// Rebuild the entire CSR index from an EdgeStore.
    ///
    /// Extracts the `"weight"` property from edge properties (if present)
    /// and populates the parallel weight arrays. Edges without a weight
    /// property default to 1.0.
    pub fn rebuild_from(store: &EdgeStore) -> crate::Result<Self> {
        let mut csr = Self::new();
        let all_edges = store.scan_all_edges()?;
        for edge in &all_edges {
            csr.ensure_node(&edge.src_id);
            csr.ensure_node(&edge.dst_id);
        }
        for edge in &all_edges {
            let weight = extract_weight_from_properties(&edge.properties);
            if weight != 1.0 {
                csr.add_edge_weighted(&edge.src_id, &edge.label, &edge.dst_id, weight);
            } else {
                csr.add_edge(&edge.src_id, &edge.label, &edge.dst_id);
            }
        }
        csr.compact();
        Ok(csr)
    }

    /// Get or create a dense ID for a node.
    pub(super) fn ensure_node(&mut self, node: &str) -> u32 {
        match self.node_to_id.entry(node.to_string()) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let id = self.id_to_node.len() as u32;
                e.insert(id);
                self.id_to_node.push(node.to_string());
                // Extend dense offsets (new node has 0 edges in dense part).
                self.out_offsets
                    .push(*self.out_offsets.last().unwrap_or(&0));
                self.in_offsets.push(*self.in_offsets.last().unwrap_or(&0));
                // Extend buffer and access tracking.
                self.buffer_out.push(Vec::new());
                self.buffer_in.push(Vec::new());
                self.buffer_out_weights.push(Vec::new());
                self.buffer_in_weights.push(Vec::new());
                self.access_counts.push(std::cell::Cell::new(0));
                id
            }
        }
    }

    /// Get or create a dense ID for a label.
    fn ensure_label(&mut self, label: &str) -> u16 {
        match self.label_to_id.entry(label.to_string()) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let id = self.id_to_label.len() as u16;
                e.insert(id);
                self.id_to_label.push(label.to_string());
                id
            }
        }
    }

    /// Incrementally add an unweighted edge (goes into mutable buffer).
    /// Uses weight 1.0 if the graph already has weighted edges.
    pub fn add_edge(&mut self, src: &str, label: &str, dst: &str) {
        self.add_edge_internal(src, label, dst, 1.0, false);
    }

    /// Incrementally add a weighted edge (goes into mutable buffer).
    ///
    /// If this is the first weighted edge (weight != 1.0), initializes
    /// the weight tracking infrastructure (backfills existing buffer
    /// entries with 1.0).
    pub fn add_edge_weighted(&mut self, src: &str, label: &str, dst: &str, weight: f64) {
        self.add_edge_internal(src, label, dst, weight, weight != 1.0);
    }

    fn add_edge_internal(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        weight: f64,
        force_weights: bool,
    ) {
        let src_id = self.ensure_node(src);
        let dst_id = self.ensure_node(dst);
        let label_id = self.ensure_label(label);

        // Check for duplicates in buffer.
        let out = &self.buffer_out[src_id as usize];
        if out.iter().any(|&(l, d)| l == label_id && d == dst_id) {
            return;
        }
        // Check for duplicates in dense CSR.
        if self.dense_has_edge(src_id, label_id, dst_id, true) {
            return;
        }

        // Initialize weight tracking on first non-default weight.
        if force_weights && !self.has_weights {
            self.enable_weights();
        }

        self.buffer_out[src_id as usize].push((label_id, dst_id));
        self.buffer_in[dst_id as usize].push((label_id, src_id));

        if self.has_weights {
            self.buffer_out_weights[src_id as usize].push(weight);
            self.buffer_in_weights[dst_id as usize].push(weight);
        }

        // If it was previously deleted, un-delete.
        self.deleted_edges.remove(&(src_id, label_id, dst_id));
    }

    /// Incrementally remove an edge.
    pub fn remove_edge(&mut self, src: &str, label: &str, dst: &str) {
        let (Some(&src_id), Some(&dst_id)) = (self.node_to_id.get(src), self.node_to_id.get(dst))
        else {
            return;
        };
        let Some(&label_id) = self.label_to_id.get(label) else {
            return;
        };

        // Remove from buffer if present (keep weight buffers in sync).
        let out_buf = &self.buffer_out[src_id as usize];
        if let Some(pos) = out_buf
            .iter()
            .position(|&(l, d)| l == label_id && d == dst_id)
        {
            self.buffer_out[src_id as usize].swap_remove(pos);
            if self.has_weights {
                self.buffer_out_weights[src_id as usize].swap_remove(pos);
            }
        }
        let in_buf = &self.buffer_in[dst_id as usize];
        if let Some(pos) = in_buf
            .iter()
            .position(|&(l, s)| l == label_id && s == src_id)
        {
            self.buffer_in[dst_id as usize].swap_remove(pos);
            if self.has_weights {
                self.buffer_in_weights[dst_id as usize].swap_remove(pos);
            }
        }

        // Mark as deleted in dense CSR.
        if self.dense_has_edge(src_id, label_id, dst_id, true) {
            self.deleted_edges.insert((src_id, label_id, dst_id));
        }
    }

    /// Remove ALL edges touching a node.
    pub fn remove_node_edges(&mut self, node: &str) -> usize {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return 0;
        };
        let mut removed = 0;

        // Collect outgoing edges then remove reverse references.
        let out_edges: Vec<(u16, u32)> = self.iter_out_edges(node_id).collect();
        for (label_id, dst_id) in &out_edges {
            let in_buf = &self.buffer_in[*dst_id as usize];
            if let Some(pos) = in_buf
                .iter()
                .position(|&(l, s)| l == *label_id && s == node_id)
            {
                self.buffer_in[*dst_id as usize].swap_remove(pos);
                if self.has_weights {
                    self.buffer_in_weights[*dst_id as usize].swap_remove(pos);
                }
            }
            self.deleted_edges.insert((node_id, *label_id, *dst_id));
            removed += 1;
        }
        self.buffer_out[node_id as usize].clear();
        if self.has_weights {
            self.buffer_out_weights[node_id as usize].clear();
        }

        // Collect incoming edges then remove reverse references.
        let in_edges: Vec<(u16, u32)> = self.iter_in_edges(node_id).collect();
        for (label_id, src_id) in &in_edges {
            let out_buf = &self.buffer_out[*src_id as usize];
            if let Some(pos) = out_buf
                .iter()
                .position(|&(l, d)| l == *label_id && d == node_id)
            {
                self.buffer_out[*src_id as usize].swap_remove(pos);
                if self.has_weights {
                    self.buffer_out_weights[*src_id as usize].swap_remove(pos);
                }
            }
            self.deleted_edges.insert((*src_id, *label_id, node_id));
            removed += 1;
        }
        self.buffer_in[node_id as usize].clear();
        if self.has_weights {
            self.buffer_in_weights[node_id as usize].clear();
        }

        removed
    }

    /// Get immediate neighbors.
    pub fn neighbors(
        &self,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };
        self.record_access(node_id);
        let label_id = label_filter.and_then(|l| self.label_to_id.get(l).copied());

        let mut result = Vec::new();

        if matches!(direction, Direction::Out | Direction::Both) {
            for (lid, dst) in self.iter_out_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (lid, src) in self.iter_in_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[src as usize].clone(),
                    ));
                }
            }
        }

        result
    }

    pub fn node_count(&self) -> usize {
        self.id_to_node.len()
    }

    pub fn contains_node(&self, node: &str) -> bool {
        self.node_to_id.contains_key(node)
    }

    /// Get the string node ID for a dense node index.
    pub fn node_name(&self, dense_id: u32) -> &str {
        &self.id_to_node[dense_id as usize]
    }

    /// Look up the dense node ID for a string node ID.
    pub fn node_id(&self, name: &str) -> Option<u32> {
        self.node_to_id.get(name).copied()
    }

    /// Add a node without any edges (used for isolated/dangling nodes).
    /// Returns the dense node ID. Idempotent — returns existing ID if present.
    pub fn add_node(&mut self, name: &str) -> u32 {
        self.ensure_node(name)
    }

    // ── Internal helpers ──

    /// Build contiguous offset/target/label arrays from per-node edge lists.
    pub(super) fn build_dense(edges: &[Vec<(u16, u32)>]) -> (Vec<u32>, Vec<u32>, Vec<u16>) {
        let n = edges.len();
        let total: usize = edges.iter().map(|e| e.len()).sum();
        let mut offsets = Vec::with_capacity(n + 1);
        let mut targets = Vec::with_capacity(total);
        let mut labels = Vec::with_capacity(total);

        let mut offset = 0u32;
        for node_edges in edges {
            offsets.push(offset);
            for &(lid, target) in node_edges {
                targets.push(target);
                labels.push(lid);
            }
            offset += node_edges.len() as u32;
        }
        offsets.push(offset);

        (offsets, targets, labels)
    }

    /// Check if a specific edge exists in the dense CSR.
    fn dense_has_edge(&self, src: u32, label: u16, dst: u32, check_out: bool) -> bool {
        if check_out {
            for (lid, target) in self.dense_out_edges(src) {
                if lid == label && target == dst {
                    return true;
                }
            }
        }
        false
    }

    /// Iterate dense outbound edges for a node.
    pub(super) fn dense_out_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        if idx + 1 >= self.out_offsets.len() {
            return Vec::new().into_iter();
        }
        let start = self.out_offsets[idx] as usize;
        let end = self.out_offsets[idx + 1] as usize;
        (start..end)
            .map(move |i| (self.out_labels[i], self.out_targets[i]))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Iterate dense inbound edges for a node.
    pub(super) fn dense_in_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        if idx + 1 >= self.in_offsets.len() {
            return Vec::new().into_iter();
        }
        let start = self.in_offsets[idx] as usize;
        let end = self.in_offsets[idx + 1] as usize;
        (start..end)
            .map(move |i| (self.in_labels[i], self.in_targets[i]))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Iterate all outbound edges for a node (dense + buffer, minus deleted).
    pub fn iter_out_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        let dense = self
            .dense_out_edges(node)
            .filter(move |&(lid, dst)| !self.deleted_edges.contains(&(node, lid, dst)));
        let buffer = if idx < self.buffer_out.len() {
            self.buffer_out[idx].to_vec()
        } else {
            Vec::new()
        };
        dense.chain(buffer)
    }

    /// Out-degree of a node (including buffer, excluding deleted).
    pub fn out_degree(&self, node_id: u32) -> usize {
        self.iter_out_edges(node_id).count()
    }

    /// In-degree of a node.
    pub fn in_degree(&self, node_id: u32) -> usize {
        self.iter_in_edges(node_id).count()
    }

    /// Total edge count (dense + buffer - deleted). O(V).
    pub fn edge_count(&self) -> usize {
        let n = self.id_to_node.len();
        (0..n).map(|i| self.out_degree(i as u32)).sum()
    }

    /// Iterate all inbound edges for a node (dense + buffer, minus deleted).
    pub fn iter_in_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        let dense = self
            .dense_in_edges(node)
            .filter(move |&(lid, src)| !self.deleted_edges.contains(&(src, lid, node)));
        let buffer = if idx < self.buffer_in.len() {
            self.buffer_in[idx].to_vec()
        } else {
            Vec::new()
        };
        dense.chain(buffer)
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
    fn neighbors_out() {
        let csr = make_csr();
        let n = csr.neighbors("a", None, Direction::Out);
        assert_eq!(n.len(), 2);
        let dsts: Vec<&str> = n.iter().map(|(_, d)| d.as_str()).collect();
        assert!(dsts.contains(&"b"));
        assert!(dsts.contains(&"e"));
    }

    #[test]
    fn neighbors_filtered() {
        let csr = make_csr();
        let n = csr.neighbors("a", Some("KNOWS"), Direction::Out);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "b");
    }

    #[test]
    fn neighbors_in() {
        let csr = make_csr();
        let n = csr.neighbors("b", None, Direction::In);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "a");
    }

    #[test]
    fn incremental_remove() {
        let mut csr = make_csr();
        assert_eq!(csr.neighbors("a", Some("KNOWS"), Direction::Out).len(), 1);
        csr.remove_edge("a", "KNOWS", "b");
        assert_eq!(csr.neighbors("a", Some("KNOWS"), Direction::Out).len(), 0);
    }

    #[test]
    fn duplicate_add_is_idempotent() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "b");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    }

    #[test]
    fn rebuild_from_edge_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        store.put_edge("x", "REL", "y", b"").unwrap();
        store.put_edge("y", "REL", "z", b"").unwrap();

        let csr = CsrIndex::rebuild_from(&store).unwrap();
        assert_eq!(csr.node_count(), 3);
        let mut result = csr.traverse_bfs(&["x"], Some("REL"), Direction::Out, 3);
        result.sort();
        assert_eq!(result, vec!["x", "y", "z"]);
    }

    #[test]
    fn compact_merges_buffer_into_dense() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);

        csr.compact();
        // After compaction, buffer is empty, edges are in dense arrays.
        assert!(csr.buffer_out.iter().all(|b| b.is_empty()));
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
        assert_eq!(csr.neighbors("b", None, Direction::Out).len(), 1);
    }

    #[test]
    fn compact_handles_deletes() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.compact();

        csr.remove_edge("a", "L", "b");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);

        csr.compact();
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
        assert_eq!(csr.neighbors("a", None, Direction::Out)[0].1, "c");
    }

    #[test]
    fn label_interning_reduces_memory() {
        let mut csr = CsrIndex::new();
        // Same label used many times — interned to a single u16.
        for i in 0..100 {
            csr.add_edge(&format!("n{i}"), "FOLLOWS", &format!("n{}", i + 1));
        }
        // Only 1 unique label should be interned.
        assert_eq!(csr.id_to_label.len(), 1);
        assert_eq!(csr.id_to_label[0], "FOLLOWS");
    }

    // ── Weighted edge tests ──

    #[test]
    fn unweighted_graph_has_no_weight_arrays() {
        let csr = make_csr();
        assert!(!csr.has_weights());
        assert!(csr.out_weights.is_none());
        assert!(csr.in_weights.is_none());
    }

    #[test]
    fn weighted_edge_basic() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "ROAD", "b", 5.0);
        csr.add_edge_weighted("b", "ROAD", "c", 3.0);
        csr.add_edge("c", "ROAD", "d"); // unweighted → 1.0

        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "ROAD", "b"), Some(5.0));
        assert_eq!(csr.edge_weight("b", "ROAD", "c"), Some(3.0));
        assert_eq!(csr.edge_weight("c", "ROAD", "d"), Some(1.0));
        assert_eq!(csr.edge_weight("a", "ROAD", "c"), None); // nonexistent
    }

    #[test]
    fn weighted_edges_survive_compaction() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5);
        csr.add_edge_weighted("b", "R", "c", 7.0);
        csr.add_edge("c", "R", "d");

        csr.compact();

        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "R", "b"), Some(2.5));
        assert_eq!(csr.edge_weight("b", "R", "c"), Some(7.0));
        assert_eq!(csr.edge_weight("c", "R", "d"), Some(1.0));
    }

    #[test]
    fn weighted_edge_remove_keeps_weights_consistent() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.0);
        csr.add_edge_weighted("a", "R", "c", 3.0);
        csr.add_edge_weighted("a", "R", "d", 4.0);

        csr.remove_edge("a", "R", "c");

        // b and d should still have correct weights.
        assert_eq!(csr.edge_weight("a", "R", "b"), Some(2.0));
        assert_eq!(csr.edge_weight("a", "R", "c"), None);
        assert_eq!(csr.edge_weight("a", "R", "d"), Some(4.0));
    }

    #[test]
    fn iter_out_edges_weighted_returns_weights() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5);
        csr.add_edge_weighted("a", "R", "c", 7.0);
        csr.compact();

        let edges: Vec<(u16, u32, f64)> = csr.iter_out_edges_weighted(0).collect();
        assert_eq!(edges.len(), 2);

        let weights: Vec<f64> = edges.iter().map(|e| e.2).collect();
        assert!(weights.contains(&2.5));
        assert!(weights.contains(&7.0));
    }

    #[test]
    fn mixed_weighted_unweighted_backfill() {
        let mut csr = CsrIndex::new();
        // Add unweighted edges first.
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        assert!(!csr.has_weights());

        // Adding a weighted edge should backfill existing with 1.0.
        csr.add_edge_weighted("c", "L", "d", 5.0);
        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "L", "b"), Some(1.0));
        assert_eq!(csr.edge_weight("c", "L", "d"), Some(5.0));
    }

    #[test]
    fn out_degree_and_in_degree() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.add_edge("d", "L", "b");

        let a_id = *csr.node_to_id.get("a").unwrap();
        let b_id = *csr.node_to_id.get("b").unwrap();

        assert_eq!(csr.out_degree(a_id), 2);
        assert_eq!(csr.in_degree(b_id), 2);
    }

    #[test]
    fn edge_count_total() {
        let csr = make_csr();
        assert_eq!(csr.edge_count(), 4);
    }

    #[test]
    fn extract_weight_from_msgpack() {
        // Build a msgpack map with "weight": 0.75
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::F64(0.75),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();

        assert_eq!(extract_weight_from_properties(&buf), 0.75);
    }

    #[test]
    fn extract_weight_from_empty_properties() {
        assert_eq!(extract_weight_from_properties(b""), 1.0);
    }

    #[test]
    fn extract_weight_integer() {
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::Integer(rmpv::Integer::from(42)),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();

        assert_eq!(extract_weight_from_properties(&buf), 42.0);
    }

    #[test]
    fn extract_weight_missing_key() {
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("color".into()),
            rmpv::Value::String("red".into()),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();

        assert_eq!(extract_weight_from_properties(&buf), 1.0);
    }

    #[test]
    fn rebuild_from_weighted_edges() {
        const TEST_WEIGHT: f64 = 0.75;

        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();

        // Edge with weight property.
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::F64(TEST_WEIGHT),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();
        store.put_edge("x", "R", "y", &buf).unwrap();

        // Edge without weight.
        store.put_edge("y", "R", "z", b"").unwrap();

        let csr = CsrIndex::rebuild_from(&store).unwrap();
        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("x", "R", "y"), Some(TEST_WEIGHT));
        assert_eq!(csr.edge_weight("y", "R", "z"), Some(1.0));
    }
}
