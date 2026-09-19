// SPDX-License-Identifier: BUSL-1.1

//! Periodic lease GC on the metadata-group leader.
//!
//! Mirrors `placement_reconcile`: leader-gated, throttled by tick count in
//! `tick::core::do_tick`. Sweeps `MetadataCache.leases` and proposes
//! `DescriptorLeaseRelease` for every holder no longer in the cluster
//! topology. This is the safety net behind the Leave apply hook.

use std::collections::HashMap;
use tracing::{debug, warn};

use crate::forward::PlanExecutor;
use crate::metadata_group::cache::MetadataCache;
use crate::metadata_group::descriptors::DescriptorId;
use crate::topology::ClusterTopology;

use super::loop_core::{CommitApplier, RaftLoop};

/// Pure collection: `(node_id, descriptor_ids)` for every lease holder that is
/// gone — not in `topology`, or marked gone by the SWIM-fed
/// [`LeaseHolderLiveness`](crate::lease_liveness::LeaseHolderLiveness) set.
/// Sorted by `node_id` for deterministic proposal order. Extracted so the
/// sweep's decision logic is unit-testable without a full `RaftLoop`.
pub(super) fn collect_non_member_lease_releases(
    topology: &ClusterTopology,
    cache: &MetadataCache,
    liveness: Option<&crate::lease_liveness::LeaseHolderLiveness>,
) -> Vec<(u64, Vec<DescriptorId>)> {
    let mut by_holder: HashMap<u64, Vec<DescriptorId>> = HashMap::new();
    for (id, holder) in cache.leases.keys() {
        let gone = !topology.contains(*holder) || liveness.is_some_and(|l| l.is_gone(*holder));
        if gone {
            by_holder.entry(*holder).or_default().push(id.clone());
        }
    }
    let mut out: Vec<(u64, Vec<DescriptorId>)> = by_holder.into_iter().collect();
    out.sort_by_key(|(node_id, _)| *node_id);
    out
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// On the metadata-group leader, propose `DescriptorLeaseRelease` for
    /// every lease whose holder is no longer in `ClusterTopology`.
    pub(super) fn gc_stale_node_leases(&self) {
        let Some(cache) = &self.metadata_cache else {
            return; // not wired (some tests) — nothing to sweep
        };
        let to_release: Vec<(u64, Vec<DescriptorId>)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if !mr.group_role_is_leader(crate::metadata_group::METADATA_GROUP_ID) {
                return;
            }
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            let cache = cache.read().unwrap_or_else(|p| p.into_inner());
            collect_non_member_lease_releases(&topo, &cache, self.lease_liveness.as_deref())
        };

        for (node_id, descriptor_ids) in to_release {
            let entry = crate::metadata_group::entry::MetadataEntry::DescriptorLeaseRelease {
                node_id,
                descriptor_ids,
            };
            let bytes = match crate::metadata_group::codec::encode_entry(&entry) {
                Ok(b) => b,
                Err(e) => {
                    warn!(node_id, error = %e, "lease GC: encode DescriptorLeaseRelease failed");
                    continue;
                }
            };
            match self.propose_to_metadata_group(bytes) {
                Ok(idx) => debug!(
                    node_id,
                    log_index = idx,
                    "lease GC: released leases of non-member node"
                ),
                Err(e) => {
                    warn!(node_id, error = %e, "lease GC: proposal failed; will be retried on next sweep")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata_group::cache::MetadataCache;
    use crate::metadata_group::descriptors::{DescriptorId, DescriptorKind};
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};

    use super::collect_non_member_lease_releases;

    fn topo_with(ids: &[u64]) -> ClusterTopology {
        let mut t = ClusterTopology::new();
        for (i, id) in ids.iter().enumerate() {
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(NodeInfo::new(*id, addr, NodeState::Active));
        }
        t
    }

    fn lease(
        id: &DescriptorId,
        holder: u64,
    ) -> crate::metadata_group::descriptors::DescriptorLease {
        crate::metadata_group::descriptors::DescriptorLease {
            descriptor_id: id.clone(),
            version: 1,
            node_id: holder,
            expires_at: nodedb_types::Hlc::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64
                    + 60_000_000_000,
                0,
            ),
        }
    }

    #[test]
    fn gc_stale_node_leases_proposes_for_non_members_only() {
        let topo = topo_with(&[1]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        let metrics = DescriptorId::new(0, 1, DescriptorKind::Collection, "metrics".to_string());

        // Member holder 1: must NOT be collected.
        cache.leases.insert((orders.clone(), 1), lease(&orders, 1));
        // Non-member holder 2: must be collected.
        cache.leases.insert((orders.clone(), 2), lease(&orders, 2));
        // Non-member holder 3 with two descriptors.
        cache.leases.insert((orders.clone(), 3), lease(&orders, 3));
        cache
            .leases
            .insert((metrics.clone(), 3), lease(&metrics, 3));

        let collected = collect_non_member_lease_releases(&topo, &cache, None);
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, 2);
        assert_eq!(collected[0].1, vec![orders.clone()]);
        assert_eq!(collected[1].0, 3);
        assert_eq!(collected[1].1.len(), 2);
        assert!(collected[1].1.contains(&orders));
        assert!(collected[1].1.contains(&metrics));
    }

    #[test]
    fn gc_is_noop_when_all_holders_are_members() {
        let topo = topo_with(&[1, 2, 3]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        for holder in [1, 2, 3] {
            cache
                .leases
                .insert((orders.clone(), holder), lease(&orders, holder));
        }

        assert!(collect_non_member_lease_releases(&topo, &cache, None).is_empty());
    }

    #[test]
    fn gc_collects_empty_cache() {
        let topo = topo_with(&[1]);
        let cache = MetadataCache::new();
        assert!(collect_non_member_lease_releases(&topo, &cache, None).is_empty());
    }

    /// A holder SWIM marked Dead is collectible while it is still a topology
    /// member; a refuted (Alive) verdict clears the mark again.
    #[test]
    fn gc_collects_a_dead_member_from_liveness() {
        use crate::lease_liveness::LeaseHolderLiveness;

        let topo = topo_with(&[1, 2]);
        let mut cache = MetadataCache::new();
        let orders = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        cache.leases.insert((orders.clone(), 1), lease(&orders, 1));
        cache.leases.insert((orders.clone(), 2), lease(&orders, 2));

        let liveness = LeaseHolderLiveness::new();
        assert!(collect_non_member_lease_releases(&topo, &cache, Some(&liveness)).is_empty());

        liveness.mark_gone(2);
        let collected = collect_non_member_lease_releases(&topo, &cache, Some(&liveness));
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, 2);
        assert_eq!(collected[0].1, vec![orders]);

        liveness.revive(2);
        assert!(collect_non_member_lease_releases(&topo, &cache, Some(&liveness)).is_empty());
    }
}
