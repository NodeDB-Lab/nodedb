// SPDX-License-Identifier: BUSL-1.1

//! Purge one tenant's graph data on one data group's vShards.
//!
//! An edge lives on both endpoint vShards, and a node's labels on one. The
//! group whose log carries the purge removes every edge with an endpoint home
//! among its vShards, and every label its vShards home. Other tenants' graph
//! data on the same vShards stays.

use std::collections::HashSet;
use std::sync::Arc;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantDataSnapshot, TenantId};

impl CoreLoop {
    /// Remove `tenant_id`'s graph data on `group` from this core. Returns the
    /// edge versions removed.
    pub(in crate::data::executor) fn purge_tenant_homed_graph(
        &mut self,
        database_id: DatabaseId,
        tenant_id: u64,
        group: &HashSet<u32>,
    ) -> crate::Result<usize> {
        let tid = TenantId::new(tenant_id);
        let db = database_id.as_u64();
        self.edge_store
            .purge_tenant_homed_node_labels(db, tid, group)?;
        let removed = self.edge_store.purge_tenant_homed(db, tid, group)?;
        self.historical_csr.invalidate_tenant(database_id, tid);
        self.deleted_nodes.remove(&(database_id, tid));

        if removed == 0 {
            self.drop_tenant_homed_labels(db, tenant_id, group);
            return Ok(0);
        }
        // The CSR indexes edges with no vShard split. A rebuild from the store
        // drops the removed edges, and the labels that stay go back on it.
        let kept: Vec<_> = self
            .held_node_labels(None)
            .into_iter()
            .filter(|(d, t, node, _)| {
                !(*d == db && *t == tenant_id && Self::labels_home_in(node, group))
            })
            .collect();
        self.csr = crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(
            &self.edge_store,
            Arc::clone(&self.governor),
        )?;
        self.install_snapshot_node_labels(&kept, true, None, &TenantDataSnapshot::default())?;
        Ok(removed)
    }
}
