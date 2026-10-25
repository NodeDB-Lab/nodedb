// SPDX-License-Identifier: BUSL-1.1

//! Capture a tenant's node labels for a snapshot: the CSR's current label
//! sets and every label version the edge store holds.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, NodeLabelVersionEntry, NodeLabelsEntry, TenantId};

impl CoreLoop {
    /// The current label set of every labeled node of `(database_id, tid)`,
    /// sorted by node, and every label version of the pair.
    pub(super) fn capture_node_labels(
        &self,
        database_id: u64,
        tid: TenantId,
    ) -> crate::Result<(Vec<NodeLabelsEntry>, Vec<NodeLabelVersionEntry>)> {
        let t = tid.as_u64();
        let mut current: Vec<NodeLabelsEntry> = self
            .csr_partition(database_id, t)
            .map(|partition| {
                partition
                    .labeled_nodes()
                    .into_iter()
                    .map(|(node, labels)| {
                        let mut labels: Vec<String> =
                            labels.into_iter().map(str::to_string).collect();
                        labels.sort();
                        (database_id, t, node.to_string(), labels)
                    })
                    .collect()
            })
            .unwrap_or_default();
        current.sort();
        let versions = self
            .edge_store
            .scan_node_label_versions(DatabaseId::new(database_id), tid)?
            .into_iter()
            .map(|(node, ordinal, labels)| (database_id, t, node, ordinal, labels))
            .collect();
        Ok((current, versions))
    }
}
