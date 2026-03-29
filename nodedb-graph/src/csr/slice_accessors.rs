//! Slice accessors for the CSR index.
//!
//! Provides read-only access to the underlying arrays for OLAP snapshot
//! cloning and other consumers that need direct array access.

use std::collections::HashMap;

use super::index::CsrIndex;

impl CsrIndex {
    /// Node-to-ID mapping (for snapshot cloning).
    pub fn node_to_id_map(&self) -> &HashMap<String, u32> {
        &self.node_to_id
    }

    /// ID-to-node list (for snapshot cloning).
    pub fn id_to_node_list(&self) -> &[String] {
        &self.id_to_node
    }

    /// Label-to-ID mapping (for snapshot cloning).
    pub fn label_to_id_map(&self) -> &HashMap<String, u16> {
        &self.label_to_id
    }

    /// ID-to-label list (for snapshot cloning).
    pub fn id_to_label_list(&self) -> &[String] {
        &self.id_to_label
    }

    /// Outbound offset array slice.
    pub fn out_offsets_slice(&self) -> &[u32] {
        &self.out_offsets
    }

    /// Outbound target array slice.
    pub fn out_targets_slice(&self) -> &[u32] {
        &self.out_targets
    }

    /// Outbound label array slice.
    pub fn out_labels_slice(&self) -> &[u16] {
        &self.out_labels
    }

    /// Outbound weight array slice (None if unweighted).
    pub fn out_weights_slice(&self) -> Option<&[f64]> {
        self.out_weights.as_deref()
    }

    /// Inbound offset array slice.
    pub fn in_offsets_slice(&self) -> &[u32] {
        &self.in_offsets
    }

    /// Inbound target array slice.
    pub fn in_targets_slice(&self) -> &[u32] {
        &self.in_targets
    }

    /// Inbound label array slice.
    pub fn in_labels_slice(&self) -> &[u16] {
        &self.in_labels
    }

    /// Inbound weight array slice (None if unweighted).
    pub fn in_weights_slice(&self) -> Option<&[f64]> {
        self.in_weights.as_deref()
    }
}
