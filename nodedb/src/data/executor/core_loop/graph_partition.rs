//! Per-tenant CSR-partition and deleted-node-tracker helpers on
//! `CoreLoop`. Extracted from `mod.rs` so the main file stays under the
//! 500-LOC ceiling while leaving the two-field group cohesive here —
//! both concerns address the same `(TenantId, node_id)` key space and
//! always move together when graph write paths evolve.

use nodedb_graph::CsrIndex;

use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Shared-access view of a tenant's CSR partition.
    ///
    /// Returns `None` if the tenant has no graph state on this core —
    /// read paths treat that as "empty" rather than an error.
    #[inline]
    pub(in crate::data::executor) fn csr_partition(&self, tid: u32) -> Option<&CsrIndex> {
        self.csr.partition(TenantId::new(tid))
    }

    /// Mutable view of a tenant's CSR partition, creating an empty one
    /// on first use. Canonical write-path entry point — resolves the
    /// tenant once, then all subsequent operations address unprefixed
    /// node names inside that partition.
    #[inline]
    pub(in crate::data::executor) fn csr_partition_mut(&mut self, tid: u32) -> &mut CsrIndex {
        self.csr.get_or_create(TenantId::new(tid))
    }

    /// Mark `node_id` as deleted within the caller's tenant. Used by
    /// PointDelete cascade so subsequent `EdgePut` to the same node is
    /// rejected as dangling.
    #[inline]
    pub(in crate::data::executor) fn mark_node_deleted(&mut self, tid: u32, node_id: &str) {
        self.deleted_nodes
            .entry(TenantId::new(tid))
            .or_default()
            .insert(node_id.to_string());
    }

    /// Test whether `node_id` has been marked deleted within the
    /// caller's tenant.
    #[inline]
    pub(in crate::data::executor) fn is_node_deleted(&self, tid: u32, node_id: &str) -> bool {
        self.deleted_nodes
            .get(&TenantId::new(tid))
            .is_some_and(|s| s.contains(node_id))
    }
}
