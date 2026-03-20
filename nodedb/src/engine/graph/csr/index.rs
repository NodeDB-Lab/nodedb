//! Dense integer CSR adjacency index with interned node IDs and labels.
//!
//! Core data structure, interning, mutation (add/remove), neighbor queries,
//! and compaction. Traversal algorithms live in `traversal.rs`.

use std::collections::{HashMap, hash_map::Entry};

use crate::engine::graph::edge_store::{Direction, EdgeStore};

/// Dense integer CSR adjacency index with interned node IDs and labels.
///
/// Memory layout at scale (1B edges):
/// - Old: `Vec<Vec<(String, u32)>>` ≈ 60 GB (heap String per edge)
/// - New: contiguous `Vec<u32>` offsets + `Vec<u32>` targets + `Vec<u16>` labels ≈ 10 GB
///
/// Writes accumulate in a mutable buffer (`buffer_out`/`buffer_in`).
/// Reads check both the dense CSR arrays and the mutable buffer.
/// `compact()` merges the buffer into the dense arrays (double-buffered swap).
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
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u16>,

    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u16>,

    // ── Mutable write buffer ──
    /// Per-node outbound buffer: `buffer_out[node_id]` = `[(label_id, dst_id)]`.
    pub(super) buffer_out: Vec<Vec<(u16, u32)>>,
    pub(super) buffer_in: Vec<Vec<(u16, u32)>>,

    /// Edges deleted since last compaction: `(src, label, dst)`.
    pub(super) deleted_edges: std::collections::HashSet<(u32, u16, u32)>,
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
            in_offsets: vec![0],
            in_targets: Vec::new(),
            in_labels: Vec::new(),
            buffer_out: Vec::new(),
            buffer_in: Vec::new(),
            deleted_edges: std::collections::HashSet::new(),
        }
    }

    /// Rebuild the entire CSR index from an EdgeStore.
    pub fn rebuild_from(store: &EdgeStore) -> crate::Result<Self> {
        let mut csr = Self::new();
        let all_edges = store.scan_all_edges()?;
        for edge in &all_edges {
            csr.ensure_node(&edge.src_id);
            csr.ensure_node(&edge.dst_id);
        }
        for edge in &all_edges {
            csr.add_edge(&edge.src_id, &edge.label, &edge.dst_id);
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
                // Extend buffer.
                self.buffer_out.push(Vec::new());
                self.buffer_in.push(Vec::new());
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

    /// Incrementally add an edge (goes into mutable buffer).
    pub fn add_edge(&mut self, src: &str, label: &str, dst: &str) {
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

        self.buffer_out[src_id as usize].push((label_id, dst_id));
        self.buffer_in[dst_id as usize].push((label_id, src_id));
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

        // Remove from buffer if present.
        self.buffer_out[src_id as usize].retain(|&(l, d)| !(l == label_id && d == dst_id));
        self.buffer_in[dst_id as usize].retain(|&(l, s)| !(l == label_id && s == src_id));

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
            self.buffer_in[*dst_id as usize].retain(|&(l, s)| !(l == *label_id && s == node_id));
            self.deleted_edges.insert((node_id, *label_id, *dst_id));
            removed += 1;
        }
        self.buffer_out[node_id as usize].clear();

        // Collect incoming edges then remove reverse references.
        let in_edges: Vec<(u16, u32)> = self.iter_in_edges(node_id).collect();
        for (label_id, src_id) in &in_edges {
            self.buffer_out[*src_id as usize].retain(|&(l, d)| !(l == *label_id && d == node_id));
            self.deleted_edges.insert((*src_id, *label_id, node_id));
            removed += 1;
        }
        self.buffer_in[node_id as usize].clear();

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

    /// Merge the mutable buffer into the dense CSR arrays.
    ///
    /// Called during idle periods. Rebuilds the contiguous offset/target/label
    /// arrays from scratch (buffer + surviving dense edges). The old arrays
    /// are dropped, freeing memory. O(E) where E = total edges.
    pub fn compact(&mut self) {
        let n = self.id_to_node.len();
        let mut new_out_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];
        let mut new_in_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];

        // Collect surviving dense edges.
        for node in 0..n {
            let node_id = node as u32;
            for (lid, dst) in self.dense_out_edges(node_id) {
                if !self.deleted_edges.contains(&(node_id, lid, dst)) {
                    new_out_edges[node].push((lid, dst));
                }
            }
            for (lid, src) in self.dense_in_edges(node_id) {
                if !self.deleted_edges.contains(&(src, lid, node_id)) {
                    new_in_edges[node].push((lid, src));
                }
            }
        }

        // Merge buffer edges.
        for node in 0..n {
            for &(lid, dst) in &self.buffer_out[node] {
                if !new_out_edges[node]
                    .iter()
                    .any(|&(l, d)| l == lid && d == dst)
                {
                    new_out_edges[node].push((lid, dst));
                }
            }
            for &(lid, src) in &self.buffer_in[node] {
                if !new_in_edges[node]
                    .iter()
                    .any(|&(l, s)| l == lid && s == src)
                {
                    new_in_edges[node].push((lid, src));
                }
            }
        }

        // Build new dense arrays.
        let (out_offsets, out_targets, out_labels) = Self::build_dense(&new_out_edges);
        let (in_offsets, in_targets, in_labels) = Self::build_dense(&new_in_edges);

        self.out_offsets = out_offsets;
        self.out_targets = out_targets;
        self.out_labels = out_labels;
        self.in_offsets = in_offsets;
        self.in_targets = in_targets;
        self.in_labels = in_labels;

        // Clear buffer and deleted set.
        for buf in &mut self.buffer_out {
            buf.clear();
        }
        for buf in &mut self.buffer_in {
            buf.clear();
        }
        self.deleted_edges.clear();
    }

    // ── Internal helpers ──

    /// Build contiguous offset/target/label arrays from per-node edge lists.
    fn build_dense(edges: &[Vec<(u16, u32)>]) -> (Vec<u32>, Vec<u32>, Vec<u16>) {
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
    pub(super) fn iter_out_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
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

    /// Iterate all inbound edges for a node (dense + buffer, minus deleted).
    pub(super) fn iter_in_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
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
}
