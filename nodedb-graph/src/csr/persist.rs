//! CSR checkpoint serialization/deserialization.
//!
//! Serializes the full CSR state (including weights) to MessagePack for
//! persistence. Used by both Origin (via redb storage) and Lite (via
//! embedded checkpoint).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::index::CsrIndex;

#[derive(Serialize, Deserialize)]
struct CsrSnapshot {
    nodes: Vec<String>,
    labels: Vec<String>,
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u16>,
    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u16>,
    buffer_out: Vec<Vec<(u16, u32)>>,
    buffer_in: Vec<Vec<(u16, u32)>>,
    deleted: Vec<(u32, u16, u32)>,
    // Weight fields — absent in old checkpoints (serde default).
    #[serde(default)]
    has_weights: bool,
    #[serde(default)]
    out_weights: Option<Vec<f64>>,
    #[serde(default)]
    in_weights: Option<Vec<f64>>,
    #[serde(default)]
    buffer_out_weights: Vec<Vec<f64>>,
    #[serde(default)]
    buffer_in_weights: Vec<Vec<f64>>,
}

impl CsrIndex {
    /// Serialize the index to MessagePack bytes for storage.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = CsrSnapshot {
            nodes: self.id_to_node.clone(),
            labels: self.id_to_label.clone(),
            out_offsets: self.out_offsets.clone(),
            out_targets: self.out_targets.clone(),
            out_labels: self.out_labels.clone(),
            in_offsets: self.in_offsets.clone(),
            in_targets: self.in_targets.clone(),
            in_labels: self.in_labels.clone(),
            buffer_out: self.buffer_out.clone(),
            buffer_in: self.buffer_in.clone(),
            deleted: self.deleted_edges.iter().copied().collect(),
            has_weights: self.has_weights,
            out_weights: self.out_weights.clone(),
            in_weights: self.in_weights.clone(),
            buffer_out_weights: self.buffer_out_weights.clone(),
            buffer_in_weights: self.buffer_in_weights.clone(),
        };
        match rmp_serde::to_vec_named(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(error = %e, "CSR checkpoint serialization failed");
                Vec::new()
            }
        }
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Backwards-compatible: old checkpoints without weight fields are
    /// deserialized with `has_weights = false` and empty weight arrays.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snap: CsrSnapshot = rmp_serde::from_slice(bytes).ok()?;

        let node_to_id: HashMap<String, u32> = snap
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        let label_to_id: HashMap<String, u16> = snap
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), i as u16))
            .collect();

        let node_count = snap.nodes.len();
        let access_counts = (0..node_count).map(|_| std::cell::Cell::new(0)).collect();

        // Ensure weight buffers have the right length even if absent in old checkpoints.
        let buffer_out_weights = if snap.buffer_out_weights.len() == node_count {
            snap.buffer_out_weights
        } else {
            vec![Vec::new(); node_count]
        };
        let buffer_in_weights = if snap.buffer_in_weights.len() == node_count {
            snap.buffer_in_weights
        } else {
            vec![Vec::new(); node_count]
        };

        Some(Self {
            node_to_id,
            id_to_node: snap.nodes,
            label_to_id,
            id_to_label: snap.labels,
            out_offsets: snap.out_offsets,
            out_targets: snap.out_targets,
            out_labels: snap.out_labels,
            out_weights: snap.out_weights,
            in_offsets: snap.in_offsets,
            in_targets: snap.in_targets,
            in_labels: snap.in_labels,
            in_weights: snap.in_weights,
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            buffer_out_weights,
            buffer_in_weights,
            deleted_edges: snap.deleted.into_iter().collect(),
            has_weights: snap.has_weights,
            access_counts,
            query_epoch: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr::index::Direction;

    #[test]
    fn checkpoint_roundtrip_unweighted() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.compact();

        let bytes = csr.checkpoint_to_bytes();
        let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
        assert_eq!(restored.node_count(), 3);
        assert_eq!(restored.edge_count(), 2);
        assert!(!restored.has_weights());

        let n = restored.neighbors("a", Some("KNOWS"), Direction::Out);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "b");
    }

    #[test]
    fn checkpoint_roundtrip_weighted() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5);
        csr.add_edge_weighted("b", "R", "c", 7.0);
        csr.add_edge("c", "R", "d");
        csr.compact();

        let bytes = csr.checkpoint_to_bytes();
        let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
        assert!(restored.has_weights());
        assert_eq!(restored.edge_weight("a", "R", "b"), Some(2.5));
        assert_eq!(restored.edge_weight("b", "R", "c"), Some(7.0));
        assert_eq!(restored.edge_weight("c", "R", "d"), Some(1.0));
    }

    #[test]
    fn checkpoint_roundtrip_with_buffer() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        // Don't compact — edges in buffer.
        let bytes = csr.checkpoint_to_bytes();
        let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
        assert_eq!(restored.edge_count(), 1);
    }
}
