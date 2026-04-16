//! [`MetadataApplier`] trait: the contract raft_loop uses to dispatch
//! committed entries on the metadata group (group 0).

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tracing::warn;

use crate::metadata_group::cache::MetadataCache;
use crate::metadata_group::codec::decode_entry;
use crate::metadata_group::entry::{MetadataEntry, RoutingChange, TopologyChange};
use crate::routing::RoutingTable;
use crate::topology::{ClusterTopology, NodeInfo, NodeState};

/// Applies committed metadata entries to local state.
///
/// Implemented in the `nodedb-cluster` crate as [`CacheApplier`]
/// (tracks cluster-owned state only: topology/routing/leases/
/// version + a CatalogDdl counter) and wrapped by the production
/// applier in the `nodedb` crate to additionally decode the
/// `CatalogDdl` payload as a `CatalogEntry` and write through to
/// `SystemCatalog`.
pub trait MetadataApplier: Send + Sync + 'static {
    /// Apply a batch of committed raft entries. Entries with empty
    /// `data` (raft no-ops) are skipped. Returns the highest log
    /// index applied.
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64;
}

/// Default applier that writes committed entries to an in-memory
/// [`MetadataCache`]. The cache is shared with the rest of the
/// process via `Arc<RwLock<_>>`.
#[derive(Clone)]
pub struct CacheApplier {
    cache: Arc<RwLock<MetadataCache>>,
    /// Optional live topology handle. When set, committed
    /// `TopologyChange` entries mutate this handle in place so the
    /// rest of the process sees the new state immediately — decommission
    /// state transitions, joiner promotion, and `Leave` removal all
    /// flow through here.
    live_topology: Option<Arc<RwLock<ClusterTopology>>>,
    /// Optional live routing table handle. When set, committed
    /// `RoutingChange` entries (leadership transfer, member removal,
    /// vshard reassignment) mutate this handle in place.
    live_routing: Option<Arc<RwLock<RoutingTable>>>,
}

impl CacheApplier {
    pub fn new(cache: Arc<RwLock<MetadataCache>>) -> Self {
        Self {
            cache,
            live_topology: None,
            live_routing: None,
        }
    }

    /// Extend this applier with live topology/routing handles. When
    /// set, committed `TopologyChange` and `RoutingChange` entries
    /// mutate the handles in place in addition to the in-memory
    /// history log kept in `MetadataCache`. Backward-compatible:
    /// existing callers that don't attach handles see no behaviour
    /// change.
    pub fn with_live_state(
        mut self,
        topology: Arc<RwLock<ClusterTopology>>,
        routing: Arc<RwLock<RoutingTable>>,
    ) -> Self {
        self.live_topology = Some(topology);
        self.live_routing = Some(routing);
        self
    }

    pub fn cache(&self) -> Arc<RwLock<MetadataCache>> {
        self.cache.clone()
    }

    /// Mutate the live topology handle (if attached) in response to
    /// a committed `TopologyChange`. Silent no-op when no handle is
    /// set — backward-compatible with older test wiring.
    fn apply_topology_change(&self, change: &TopologyChange) {
        let Some(live) = &self.live_topology else {
            return;
        };
        let mut topo = live.write().unwrap_or_else(|p| p.into_inner());
        match change {
            TopologyChange::Join { node_id, addr } => {
                if topo.contains(*node_id) {
                    return;
                }
                let parsed: SocketAddr = addr.parse().unwrap_or_else(|_| {
                    warn!(node_id, addr, "join: invalid address, using placeholder");
                    SocketAddr::from(([0, 0, 0, 0], 0))
                });
                topo.join_as_learner(NodeInfo::new(*node_id, parsed, NodeState::Joining));
            }
            TopologyChange::PromoteToVoter { node_id } => {
                topo.promote_to_voter(*node_id);
            }
            TopologyChange::StartDecommission { node_id } => {
                topo.set_state(*node_id, NodeState::Draining);
            }
            TopologyChange::FinishDecommission { node_id } => {
                topo.set_state(*node_id, NodeState::Decommissioned);
            }
            TopologyChange::Leave { node_id } => {
                topo.remove_node(*node_id);
            }
        }
    }

    /// Mutate the live routing handle (if attached) in response to
    /// a committed `RoutingChange`.
    fn apply_routing_change(&self, change: &RoutingChange) {
        let Some(live) = &self.live_routing else {
            return;
        };
        let mut rt = live.write().unwrap_or_else(|p| p.into_inner());
        match change {
            RoutingChange::ReassignVShard {
                vshard_id,
                new_group_id,
                new_leaseholder_node_id,
            } => {
                rt.reassign_vshard(*vshard_id, *new_group_id);
                rt.set_leader(*new_group_id, *new_leaseholder_node_id);
            }
            RoutingChange::LeadershipTransfer {
                group_id,
                new_leader_node_id,
            } => {
                rt.set_leader(*group_id, *new_leader_node_id);
            }
            RoutingChange::RemoveMember { group_id, node_id } => {
                rt.remove_group_member(*group_id, *node_id);
            }
        }
    }
}

impl MetadataApplier for CacheApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let mut last = 0u64;
        let mut guard = self
            .cache
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        for (index, data) in entries {
            last = *index;
            if data.is_empty() {
                continue;
            }
            match decode_entry(data) {
                Ok(entry) => {
                    guard.apply(*index, &entry);
                    // Cascade to live state (if attached).
                    match &entry {
                        MetadataEntry::TopologyChange(change) => self.apply_topology_change(change),
                        MetadataEntry::RoutingChange(change) => self.apply_routing_change(change),
                        _ => {}
                    }
                }
                Err(e) => warn!(index = *index, error = %e, "metadata decode failed"),
            }
        }
        last
    }
}

/// No-op applier used by tests and subsystems that don't care about the
/// metadata stream. Still drains entries and returns the correct last
/// index so raft can advance its applied watermark.
pub struct NoopMetadataApplier;

impl MetadataApplier for NoopMetadataApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        entries.last().map(|(idx, _)| *idx).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::codec::encode_entry;
    use crate::metadata_group::entry::{MetadataEntry, TopologyChange};

    #[test]
    fn cache_applier_counts_catalog_ddl() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let ddl = encode_entry(&MetadataEntry::CatalogDdl {
            payload: vec![1, 2, 3],
        })
        .unwrap();
        let topo = encode_entry(&MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 7,
            addr: "10.0.0.7:9000".into(),
        }))
        .unwrap();

        let last = applier.apply(&[(1, ddl), (2, topo)]);
        assert_eq!(last, 2);

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 2);
        assert_eq!(guard.catalog_entries_applied, 1);
        assert_eq!(guard.topology_log.len(), 1);
    }

    #[test]
    fn cache_applier_idempotent() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let bytes = encode_entry(&MetadataEntry::CatalogDdl {
            payload: vec![9, 9],
        })
        .unwrap();
        applier.apply(&[(5, bytes.clone())]);
        applier.apply(&[(3, bytes)]); // Earlier index — ignored.

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 5);
        assert_eq!(guard.catalog_entries_applied, 1);
    }

    #[test]
    fn cache_applier_mutates_live_topology_on_start_decommission() {
        use crate::topology::{ClusterTopology, NodeInfo, NodeState};
        use std::net::SocketAddr;

        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let mut t = ClusterTopology::new();
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        t.add_node(NodeInfo::new(7, addr, NodeState::Active));
        let topology = Arc::new(RwLock::new(t));
        let routing = Arc::new(RwLock::new(crate::routing::RoutingTable::uniform(
            1,
            &[7],
            1,
        )));
        let applier =
            CacheApplier::new(cache.clone()).with_live_state(topology.clone(), routing.clone());

        let bytes = encode_entry(&MetadataEntry::TopologyChange(
            TopologyChange::StartDecommission { node_id: 7 },
        ))
        .unwrap();
        applier.apply(&[(1, bytes)]);

        let topo = topology.read().unwrap();
        assert_eq!(topo.get_node(7).unwrap().state, NodeState::Draining);
    }

    #[test]
    fn cache_applier_mutates_live_routing_on_remove_member() {
        use crate::metadata_group::entry::RoutingChange;

        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let topology = Arc::new(RwLock::new(crate::topology::ClusterTopology::new()));
        let routing = Arc::new(RwLock::new(crate::routing::RoutingTable::uniform(
            1,
            &[1, 2, 3],
            3,
        )));
        let applier =
            CacheApplier::new(cache.clone()).with_live_state(topology.clone(), routing.clone());

        let bytes = encode_entry(&MetadataEntry::RoutingChange(RoutingChange::RemoveMember {
            group_id: 0,
            node_id: 2,
        }))
        .unwrap();
        applier.apply(&[(1, bytes)]);

        let rt = routing.read().unwrap();
        assert!(!rt.group_info(0).unwrap().members.contains(&2));
    }

    #[test]
    fn cache_applier_without_live_state_stays_log_only() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());
        let bytes = encode_entry(&MetadataEntry::TopologyChange(
            TopologyChange::StartDecommission { node_id: 5 },
        ))
        .unwrap();
        // Must not panic and must still advance the applied index.
        let last = applier.apply(&[(1, bytes)]);
        assert_eq!(last, 1);
    }

    #[test]
    fn noop_applier_advances_watermark() {
        let noop = NoopMetadataApplier;
        assert_eq!(noop.apply(&[(7, b"x".to_vec()), (9, b"y".to_vec())]), 9);
        assert_eq!(noop.apply(&[]), 0);
    }
}
