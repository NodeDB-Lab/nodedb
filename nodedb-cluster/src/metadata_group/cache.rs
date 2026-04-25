//! Per-node in-memory view of the replicated metadata state.
//!
//! The cache tracks everything `nodedb-cluster` natively understands:
//! the applied raft index, the HLC watermark, topology / routing
//! change history, descriptor leases, and cluster version. It
//! **does not** maintain per-DDL-object descriptor state — that
//! lives on the host side via
//! `nodedb::control::catalog_entry::CatalogEntry::apply_to` writing
//! into `SystemCatalog` redb. The `CatalogDdl { payload }` variant
//! is opaque here: the cache tracks its applied index and forwards
//! the payload to the host's `MetadataCommitApplier`.

use std::collections::HashMap;

use nodedb_types::Hlc;
use tracing::{debug, warn};

use crate::metadata_group::descriptors::{DescriptorId, DescriptorLease};
use crate::metadata_group::entry::{MetadataEntry, RoutingChange, TopologyChange};

/// In-memory view of the committed metadata state.
#[derive(Debug, Default)]
pub struct MetadataCache {
    pub applied_index: u64,
    pub last_applied_hlc: Hlc,

    /// `(descriptor_id, node_id) -> lease`.
    pub leases: HashMap<(DescriptorId, u64), DescriptorLease>,

    /// Topology mutations applied so far.
    pub topology_log: Vec<TopologyChange>,
    pub routing_log: Vec<RoutingChange>,

    pub cluster_version: u16,

    /// Monotonically-increasing count of committed `CatalogDdl`
    /// entries. Exposed for tests and metrics — planners read
    /// catalog state through the host-side `SystemCatalog`, not
    /// this counter.
    pub catalog_entries_applied: u64,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed entry. Idempotent by `applied_index`:
    /// entries at or below the current watermark are ignored.
    pub fn apply(&mut self, index: u64, entry: &MetadataEntry) {
        if index != 0 && index <= self.applied_index {
            debug!(
                index,
                watermark = self.applied_index,
                "metadata cache: skipping already-applied entry"
            );
            return;
        }
        self.applied_index = index;

        match entry {
            MetadataEntry::CatalogDdl { payload: _ }
            | MetadataEntry::CatalogDdlAudited { payload: _, .. } => {
                // Opaque to the cluster crate. The host-side applier
                // decodes the payload and writes through to
                // `SystemCatalog`. We just count it — both DDL
                // shapes contribute to `catalog_entries_applied`.
                self.catalog_entries_applied += 1;
            }
            MetadataEntry::TopologyChange(change) => self.topology_log.push(change.clone()),
            MetadataEntry::RoutingChange(change) => self.routing_log.push(change.clone()),

            MetadataEntry::ClusterVersionBump { from, to } => {
                if *from != self.cluster_version && self.cluster_version != 0 {
                    warn!(
                        expected = self.cluster_version,
                        got = *from,
                        "cluster version bump mismatch"
                    );
                }
                self.cluster_version = *to;
            }

            MetadataEntry::DescriptorLeaseGrant(lease) => {
                if lease.expires_at > self.last_applied_hlc {
                    self.last_applied_hlc = lease.expires_at;
                }
                self.leases
                    .insert((lease.descriptor_id.clone(), lease.node_id), lease.clone());
            }
            MetadataEntry::DescriptorLeaseRelease {
                node_id,
                descriptor_ids,
            } => {
                for id in descriptor_ids {
                    self.leases.remove(&(id.clone(), *node_id));
                }
            }
            // Drain state is host-side (lives in
            // `nodedb::control::lease::DescriptorDrainTracker`);
            // the cluster-side cache only tracks lease state
            // directly. These no-op arms keep the exhaustive
            // match coverage so adding new variants is a
            // compile-time error here too.
            MetadataEntry::DescriptorDrainStart { expires_at, .. } => {
                if *expires_at > self.last_applied_hlc {
                    self.last_applied_hlc = *expires_at;
                }
            }
            MetadataEntry::DescriptorDrainEnd { .. } => {}
            MetadataEntry::CaTrustChange { .. } => {
                // CA trust mutations are host-side only: the production
                // applier in the nodedb crate writes/deletes
                // `tls/ca.d/<fp>.crt` and rebuilds the rustls config.
                // Cluster cache has nothing to track.
            }
            MetadataEntry::SurrogateAlloc { .. } => {
                // Surrogate HWM advance is host-side only: the production
                // applier calls `SurrogateRegistry::restore_hwm`. The
                // cluster cache has no surrogate state to track.
            }
            MetadataEntry::Batch { entries } => {
                for sub in entries {
                    self.apply(index, sub);
                }
            }
        }
    }
}
