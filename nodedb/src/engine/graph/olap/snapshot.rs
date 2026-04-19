//! Graph analytical snapshot — frozen read-optimized CSR copy.
//!
//! Creates an immutable snapshot of the CSR index for long-running analytics.
//! Algorithms run on the snapshot while OLTP mutations continue on the live CSR.
//!
//! The snapshot is created by first compacting the live CSR (merging buffer
//! into dense arrays), then cloning only the dense arrays and interning tables.
//! Mutable buffers, deleted edge sets, and access counters are NOT cloned —
//! the snapshot is read-only.
//!
//! Snapshot cost: O(V + E) for the clone. For a 633K vertex / 34M edge graph,
//! this is ~270 MB (offsets + targets + labels + weights). With CoW-capable
//! allocators (jemalloc huge pages), the actual memory cost is the dirty-page
//! delta during OLTP mutations on the live CSR.

use std::collections::HashMap;

use crate::engine::graph::csr::CsrIndex;
use nodedb_graph::LocalNodeId;

/// Immutable graph snapshot for analytical workloads.
///
/// Contains only the dense CSR arrays and interning tables. No mutable
/// buffers, no deleted edge set, no access counters. Safe to share
/// across concurrent algorithm executions via `Arc<CsrSnapshot>`.
pub struct CsrSnapshot {
    // ── Node interning ──
    node_to_id: HashMap<String, u32>,
    id_to_node: Vec<String>,

    // ── Label interning ──
    label_to_id: HashMap<String, u32>,
    id_to_label: Vec<String>,

    // ── Dense CSR arrays (immutable) ──
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u32>,
    out_weights: Option<Vec<f64>>,

    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u32>,
    in_weights: Option<Vec<f64>>,

    has_weights: bool,
    /// Partition tag inherited from the source `CsrIndex` — used to construct
    /// and validate `LocalNodeId` values on the tagged API.
    partition_tag: u32,
}

impl CsrSnapshot {
    /// Create a snapshot from a live CSR index.
    ///
    /// **Important**: This compacts the live CSR first to merge all buffer
    /// edges into the dense arrays. The snapshot only contains dense data.
    pub fn from_csr(csr: &mut CsrIndex) -> Self {
        csr.compact();
        Self::snapshot_dense(csr)
    }

    /// Create a read-only snapshot without compacting (snapshot of current dense
    /// arrays only — buffer edges are NOT included). Cheaper but potentially stale.
    pub fn from_csr_no_compact(csr: &CsrIndex) -> Self {
        Self::snapshot_dense(csr)
    }

    /// Snapshot the dense CSR arrays (shared by both constructors).
    fn snapshot_dense(csr: &CsrIndex) -> Self {
        Self {
            node_to_id: csr.node_to_id_map().clone(),
            id_to_node: csr.id_to_node_list().to_vec(),
            label_to_id: csr.label_to_id_map().clone(),
            id_to_label: csr.id_to_label_list().to_vec(),
            out_offsets: csr.out_offsets_slice().to_vec(),
            out_targets: csr.out_targets_slice().to_vec(),
            out_labels: csr.out_labels_slice().to_vec(),
            out_weights: csr.out_weights_slice().map(|w| w.to_vec()),
            in_offsets: csr.in_offsets_slice().to_vec(),
            in_targets: csr.in_targets_slice().to_vec(),
            in_labels: csr.in_labels_slice().to_vec(),
            in_weights: csr.in_weights_slice().map(|w| w.to_vec()),
            has_weights: csr.has_weights(),
            partition_tag: csr.partition_tag(),
        }
    }

    // ── Read-only accessors ──

    pub fn node_count(&self) -> usize {
        self.id_to_node.len()
    }

    pub fn edge_count(&self) -> usize {
        self.out_targets.len()
    }

    pub fn has_weights(&self) -> bool {
        self.has_weights
    }

    pub fn label_name(&self, label_id: u32) -> &str {
        &self.id_to_label[label_id as usize]
    }

    pub fn label_id(&self, name: &str) -> Option<u32> {
        self.label_to_id.get(name).copied()
    }

    /// Partition tag inherited from the source `CsrIndex`.
    pub fn partition_tag(&self) -> u32 {
        self.partition_tag
    }

    /// Wrap a raw dense node index into a tagged `LocalNodeId` for this snapshot.
    pub fn local(&self, raw: u32) -> LocalNodeId {
        LocalNodeId::new(raw, self.partition_tag)
    }

    // ── Tagged API (takes / returns LocalNodeId) ──

    pub fn node_name(&self, id: LocalNodeId) -> &str {
        &self.id_to_node[id.raw(self.partition_tag) as usize]
    }

    pub fn node_id(&self, name: &str) -> Option<LocalNodeId> {
        self.node_to_id
            .get(name)
            .copied()
            .map(|raw| LocalNodeId::new(raw, self.partition_tag))
    }

    /// Out-degree of a node in the snapshot.
    pub fn out_degree(&self, node: LocalNodeId) -> usize {
        self.out_degree_raw(node.raw(self.partition_tag))
    }

    /// In-degree of a node in the snapshot.
    pub fn in_degree(&self, node: LocalNodeId) -> usize {
        self.in_degree_raw(node.raw(self.partition_tag))
    }

    /// Iterate outbound edges for a node: `(label_id, dst_id)`.
    pub fn iter_out_edges(
        &self,
        node: LocalNodeId,
    ) -> impl Iterator<Item = (u32, LocalNodeId)> + '_ {
        let tag = self.partition_tag;
        self.iter_out_edges_raw(node.raw(tag))
            .map(move |(lid, dst)| (lid, LocalNodeId::new(dst, tag)))
    }

    /// Iterate inbound edges for a node: `(label_id, src_id)`.
    pub fn iter_in_edges(
        &self,
        node: LocalNodeId,
    ) -> impl Iterator<Item = (u32, LocalNodeId)> + '_ {
        let tag = self.partition_tag;
        self.iter_in_edges_raw(node.raw(tag))
            .map(move |(lid, src)| (lid, LocalNodeId::new(src, tag)))
    }

    /// Iterate outbound edges with weights: `(label_id, dst_id, weight)`.
    pub fn iter_out_edges_weighted(
        &self,
        node: LocalNodeId,
    ) -> impl Iterator<Item = (u32, LocalNodeId, f64)> + '_ {
        let tag = self.partition_tag;
        self.iter_out_edges_weighted_raw(node.raw(tag))
            .map(move |(lid, dst, w)| (lid, LocalNodeId::new(dst, tag), w))
    }

    // ── Raw u32 API (in-partition algorithm use — no tag validation) ──

    pub fn node_name_raw(&self, dense_id: u32) -> &str {
        &self.id_to_node[dense_id as usize]
    }

    pub fn node_id_raw(&self, name: &str) -> Option<u32> {
        self.node_to_id.get(name).copied()
    }

    /// Out-degree of a node (raw dense index).
    pub fn out_degree_raw(&self, node: u32) -> usize {
        let idx = node as usize;
        if idx + 1 >= self.out_offsets.len() {
            return 0;
        }
        (self.out_offsets[idx + 1] - self.out_offsets[idx]) as usize
    }

    /// In-degree of a node (raw dense index).
    pub fn in_degree_raw(&self, node: u32) -> usize {
        let idx = node as usize;
        if idx + 1 >= self.in_offsets.len() {
            return 0;
        }
        (self.in_offsets[idx + 1] - self.in_offsets[idx]) as usize
    }

    /// Iterate outbound edges for a node (raw dense index): `(label_id, dst_id)`.
    pub fn iter_out_edges_raw(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let idx = node as usize;
        let (start, end) = if idx + 1 < self.out_offsets.len() {
            (
                self.out_offsets[idx] as usize,
                self.out_offsets[idx + 1] as usize,
            )
        } else {
            (0, 0)
        };
        (start..end).map(move |i| (self.out_labels[i], self.out_targets[i]))
    }

    /// Iterate inbound edges for a node (raw dense index): `(label_id, src_id)`.
    pub fn iter_in_edges_raw(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let idx = node as usize;
        let (start, end) = if idx + 1 < self.in_offsets.len() {
            (
                self.in_offsets[idx] as usize,
                self.in_offsets[idx + 1] as usize,
            )
        } else {
            (0, 0)
        };
        (start..end).map(move |i| (self.in_labels[i], self.in_targets[i]))
    }

    /// Iterate outbound edges with weights (raw dense index): `(label_id, dst_id, weight)`.
    pub fn iter_out_edges_weighted_raw(
        &self,
        node: u32,
    ) -> impl Iterator<Item = (u32, u32, f64)> + '_ {
        let idx = node as usize;
        let (start, end) = if idx + 1 < self.out_offsets.len() {
            (
                self.out_offsets[idx] as usize,
                self.out_offsets[idx + 1] as usize,
            )
        } else {
            (0, 0)
        };
        (start..end).map(move |i| {
            let w = self
                .out_weights
                .as_ref()
                .and_then(|ws| ws.get(i).copied())
                .unwrap_or(1.0);
            (self.out_labels[i], self.out_targets[i], w)
        })
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        let offsets = (self.out_offsets.len() + self.in_offsets.len()) * 4;
        let targets = (self.out_targets.len() + self.in_targets.len()) * 4;
        let labels = (self.out_labels.len() + self.in_labels.len()) * 2;
        let weights = self.out_weights.as_ref().map_or(0, |w| w.len() * 8)
            + self.in_weights.as_ref().map_or(0, |w| w.len() * 8);
        let interning = self.id_to_node.iter().map(|s| s.len() + 24).sum::<usize>()
            + self.id_to_label.iter().map(|s| s.len() + 24).sum::<usize>();
        offsets + targets + labels + weights + interning
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "c").unwrap();
        csr.add_edge("a", "LIKES", "c").unwrap();
        csr
    }

    #[test]
    fn snapshot_captures_all_edges() {
        let mut csr = make_csr();
        let snap = CsrSnapshot::from_csr(&mut csr);

        assert_eq!(snap.node_count(), 3);
        assert_eq!(snap.edge_count(), 3);
    }

    #[test]
    fn snapshot_immutable_while_csr_mutates() {
        let mut csr = make_csr();
        let snap = CsrSnapshot::from_csr(&mut csr);

        // Mutate live CSR after snapshot.
        csr.add_edge("c", "KNOWS", "d").unwrap();

        // Snapshot still has original 3 edges.
        assert_eq!(snap.edge_count(), 3);
        assert_eq!(snap.node_count(), 3);
    }

    #[test]
    fn snapshot_node_lookup() {
        let mut csr = make_csr();
        let snap = CsrSnapshot::from_csr(&mut csr);

        assert_eq!(snap.node_id_raw("a"), Some(0));
        assert_eq!(snap.node_name_raw(0), "a");
        assert_eq!(snap.node_id_raw("nonexistent"), None);
    }

    #[test]
    fn snapshot_edge_iteration() {
        let mut csr = make_csr();
        let snap = CsrSnapshot::from_csr(&mut csr);

        let a_id = snap.node_id_raw("a").unwrap();
        let out_edges: Vec<(u32, u32)> = snap.iter_out_edges_raw(a_id).collect();
        assert_eq!(out_edges.len(), 2); // KNOWS->b, LIKES->c
    }

    #[test]
    fn snapshot_degree() {
        let mut csr = make_csr();
        let snap = CsrSnapshot::from_csr(&mut csr);

        let a_id = snap.node_id_raw("a").unwrap();
        assert_eq!(snap.out_degree_raw(a_id), 2);
        assert_eq!(snap.in_degree_raw(a_id), 0);

        let b_id = snap.node_id_raw("b").unwrap();
        assert_eq!(snap.out_degree_raw(b_id), 1);
        assert_eq!(snap.in_degree_raw(b_id), 1);
    }

    #[test]
    fn snapshot_weighted() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5).unwrap();
        csr.add_edge_weighted("b", "R", "c", 7.0).unwrap();
        let snap = CsrSnapshot::from_csr(&mut csr);

        assert!(snap.has_weights());
        let edges: Vec<(u32, u32, f64)> = snap.iter_out_edges_weighted_raw(0).collect();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].2, 2.5);
    }

    #[test]
    fn snapshot_memory_estimate() {
        let mut csr = make_csr();
        let snap = CsrSnapshot::from_csr(&mut csr);
        assert!(snap.estimated_memory_bytes() > 0);
    }

    #[test]
    fn no_compact_snapshot() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        // Don't compact — buffer edges only.
        let snap = CsrSnapshot::from_csr_no_compact(&csr);
        // No-compact snapshot captures only dense arrays (empty after no compact).
        assert_eq!(snap.edge_count(), 0);
    }
}
