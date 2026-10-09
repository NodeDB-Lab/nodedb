// SPDX-License-Identifier: BUSL-1.1

//! Durable host state of the metadata group: descriptor leases, the cluster
//! version, and the owner of the DDL preparation lease.
//!
//! Each apply writes its `SystemCatalog` row before it returns, and boot
//! seeds the in-memory state from the rows. No state here depends on
//! replaying the metadata log.

use nodedb_cluster::DescriptorLease;

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// `DescriptorLeaseGrant`: the cache holds the lease; persist it.
    pub(super) fn apply_lease_grant(&self, lease: &DescriptorLease) -> Result<(), crate::Error> {
        self.credentials.catalog().put_descriptor_lease(lease)
    }

    /// `ClusterVersionBump`: the cache holds the new version; persist it.
    pub(super) fn apply_cluster_version(&self, to: u16) -> Result<(), crate::Error> {
        self.credentials.catalog().put_cluster_version(to)
    }

    /// `DdlPrepareAcquire`: `token`, proposed by `node_id`, takes the
    /// preparation lease when it is free. A re-delivered acquire of the
    /// current owner keeps its first apply time.
    pub(super) fn apply_ddl_prepare_acquire(
        &self,
        token: u64,
        node_id: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        let mut owner = shared
            .metadata_ddl
            .owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if owner.is_none() {
            self.credentials
                .catalog()
                .put_ddl_owner(Some((token, node_id)))?;
            *owner = Some(crate::control::metadata_proposer::DdlPrepareOwner {
                token,
                node_id,
                acquired_at: std::time::Instant::now(),
            });
        }
        Ok(())
    }

    /// `DdlPrepareRelease`: `token` gives the preparation lease up, if it
    /// holds it.
    pub(super) fn apply_ddl_prepare_release(&self, token: u64) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        let mut owner = shared
            .metadata_ddl
            .owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if owner.is_some_and(|current| current.token == token) {
            self.credentials.catalog().put_ddl_owner(None)?;
            *owner = None;
        }
        Ok(())
    }
}
