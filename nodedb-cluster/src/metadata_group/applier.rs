// SPDX-License-Identifier: BUSL-1.1

//! [`MetadataApplier`] trait: the contract raft_loop uses to dispatch
//! committed entries on the metadata group (group 0).

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tracing::{error, warn};
use uuid;

use crate::auth::raft_backed_store::apply_token_transition_to_mirror;
use crate::auth::token_state::SharedTokenStateMirror;
use crate::error::ClusterError;
use crate::metadata_group::cache::{
    MetadataCache, apply_migration_abort, apply_migration_checkpoint,
};
use crate::metadata_group::codec::decode_entry;
use crate::metadata_group::entry::{MetadataEntry, RoutingChange, TopologyChange};
use crate::metadata_group::migration_state::SharedMigrationStateTable;
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
///
/// The apply is awaited: an entry whose effects await other work completes
/// them before the raft loop advances past it.
#[async_trait::async_trait]
pub trait MetadataApplier: Send + Sync + 'static {
    /// Apply a batch of committed raft entries, each decoded once by the
    /// caller. Entries with an empty payload (raft no-ops and conf changes)
    /// apply nothing. Returns the highest log index applied.
    async fn apply_decoded(&self, entries: &[CommittedMetadata<'_>]) -> u64;

    /// Decode each encoded entry once, then apply the batch through
    /// [`apply_decoded`](Self::apply_decoded). For callers that hold the
    /// encoded bytes.
    async fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let committed: Vec<CommittedMetadata<'_>> = entries
            .iter()
            .map(|(index, data)| CommittedMetadata::decode(*index, data))
            .collect();
        self.apply_decoded(&committed).await
    }

    /// Whether every effect of an entry `apply` reports as applied is durable
    /// when `apply` returns. When true, the raft loop saves the returned index
    /// as the group's applied floor, and a restart resumes delivery above it.
    /// When false, a restart replays the whole retained log.
    fn durable_effects(&self) -> bool {
        false
    }
}

/// One committed metadata entry: its raw payload and its payload decoded
/// once.
#[derive(Debug)]
pub struct CommittedMetadata<'a> {
    pub index: u64,
    /// The entry's payload. Empty for a raft no-op or a conf change.
    pub data: &'a [u8],
    pub payload: MetadataPayload,
}

/// The decoded payload of a [`CommittedMetadata`].
#[derive(Debug)]
pub enum MetadataPayload {
    /// A raft no-op or a conf change. It applies nothing.
    Empty,
    Decoded(MetadataEntry),
    /// The payload does not decode. Every replica fails the same way.
    Undecodable(ClusterError),
}

impl<'a> CommittedMetadata<'a> {
    /// Decode `data`, the payload committed at `index`.
    pub fn decode(index: u64, data: &'a [u8]) -> Self {
        let payload = if data.is_empty() {
            MetadataPayload::Empty
        } else {
            match decode_entry(data) {
                Ok(entry) => MetadataPayload::Decoded(entry),
                Err(e) => MetadataPayload::Undecodable(e),
            }
        };
        Self {
            index,
            data,
            payload,
        }
    }

    /// An entry that applies nothing, such as a conf change.
    pub fn empty(index: u64) -> Self {
        Self {
            index,
            data: &[],
            payload: MetadataPayload::Empty,
        }
    }

    /// The decoded entry, if the payload decoded.
    pub fn entry(&self) -> Option<&MetadataEntry> {
        match &self.payload {
            MetadataPayload::Decoded(entry) => Some(entry),
            MetadataPayload::Empty | MetadataPayload::Undecodable(_) => None,
        }
    }
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
    /// Optional migration state table handle. When set, committed
    /// `MigrationCheckpoint` and `MigrationAbort` entries mutate the
    /// table in place. Missing handle is NOT an error — tests and
    /// subsystems that don't manage migrations omit it.
    migration_state: Option<SharedMigrationStateTable>,
    /// Optional token state mirror. When set, committed
    /// `JoinTokenTransition` entries mutate the mirror so that
    /// `RaftBackedTokenStore` reads see the post-apply state immediately
    /// after `propose_and_wait` returns. Missing handle is NOT an error
    /// — tests and subsystems that don't manage join tokens omit it.
    token_state: Option<SharedTokenStateMirror>,
}

impl CacheApplier {
    pub fn new(cache: Arc<RwLock<MetadataCache>>) -> Self {
        Self {
            cache,
            live_topology: None,
            live_routing: None,
            migration_state: None,
            token_state: None,
        }
    }

    /// Extend this applier with live topology/routing handles. When
    /// set, committed `TopologyChange` and `RoutingChange` entries
    /// mutate the handles in place in addition to the in-memory
    /// history log kept in `MetadataCache`. Optional: a caller that
    /// attaches no handles sees no behaviour change.
    pub fn with_live_state(
        mut self,
        topology: Arc<RwLock<ClusterTopology>>,
        routing: Arc<RwLock<RoutingTable>>,
    ) -> Self {
        self.live_topology = Some(topology);
        self.live_routing = Some(routing);
        self
    }

    /// Attach a migration state table so that committed
    /// `MigrationCheckpoint` and `MigrationAbort` entries are
    /// durably persisted. Optional: a caller that manages no migrations
    /// omits this.
    pub fn with_migration_state(mut self, migration_state: SharedMigrationStateTable) -> Self {
        self.migration_state = Some(migration_state);
        self
    }

    /// Attach a token state mirror so that committed
    /// `JoinTokenTransition` entries are reflected into the shared
    /// mirror immediately after apply. The same `Arc` must be passed to
    /// `RaftBackedTokenStore::new` so both sides share the same table.
    /// Optional: a caller that uses no join tokens omits this.
    pub fn with_token_state(mut self, token_state: SharedTokenStateMirror) -> Self {
        self.token_state = Some(token_state);
        self
    }

    pub fn cache(&self) -> Arc<RwLock<MetadataCache>> {
        self.cache.clone()
    }

    /// Mutate the live topology handle (if attached) in response to
    /// a committed `TopologyChange`. Optional; no-op when not configured.
    fn apply_topology_change(&self, change: &TopologyChange) {
        let Some(live) = &self.live_topology else {
            return;
        };
        let mut topo = live.write().unwrap_or_else(|p| p.into_inner());
        match change {
            TopologyChange::Join {
                node_id,
                addr,
                swim_addr,
            } => {
                let swim: Option<SocketAddr> = swim_addr.as_deref().and_then(|raw| {
                    raw.parse()
                        .inspect_err(|_| warn!(node_id, raw, "join: invalid SWIM address, dropped"))
                        .ok()
                });
                if let Some(existing) = topo.get_node(*node_id) {
                    // A known node re-advertising a new SWIM address updates
                    // its entry; nothing else about it changes here.
                    if swim.is_some() && existing.swim_socket_addr() != swim {
                        let updated = existing.clone().with_swim_addr(swim);
                        topo.add_node(updated);
                    }
                    return;
                }
                // Propose refuses an invalid address. One that still
                // commits never enters topology with a placeholder.
                let parsed: SocketAddr = match addr.parse() {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        error!(node_id, addr, error = %e, "join: invalid address, join skipped");
                        return;
                    }
                };
                topo.join_as_learner(
                    NodeInfo::new(*node_id, parsed, NodeState::Joining).with_swim_addr(swim),
                );
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

    /// Cascade live-state mutations for a committed entry. Handles
    /// `Batch` by recursing into each sub-entry.
    fn cascade_live_state(&self, index: u64, entry: &MetadataEntry) {
        match entry {
            // The applied epoch advances in the raft loop, on every node,
            // regardless of which applier the host installed.
            MetadataEntry::ClusterEpochBump { .. } => {}
            MetadataEntry::TopologyChange(change) => self.apply_topology_change(change),
            MetadataEntry::RoutingChange(change) => self.apply_routing_change(index, change),
            MetadataEntry::Batch { entries } => {
                for sub in entries {
                    self.cascade_live_state(index, sub);
                }
            }
            MetadataEntry::MigrationCheckpoint {
                migration_id,
                phase,
                attempt,
                payload,
                crc32c,
                ts_ms,
            } => {
                if let Some(table) = &self.migration_state {
                    let parsed_id = migration_id
                        .parse::<uuid::Uuid>()
                        .unwrap_or_else(|_| uuid::Uuid::nil());
                    if let Err(e) = apply_migration_checkpoint(
                        table,
                        parsed_id,
                        *phase,
                        *attempt,
                        payload.clone(),
                        *crc32c,
                        *ts_ms,
                    ) {
                        // CRC32C mismatch is fatal — corruption must not be silenced.
                        error!(
                            migration_id = %migration_id,
                            error = %e,
                            "FATAL: migration checkpoint CRC32C mismatch — possible corruption"
                        );
                        panic!("migration checkpoint CRC32C mismatch: {e}");
                    }
                }
            }
            MetadataEntry::MigrationAbort {
                migration_id,
                reason,
                compensations,
            } => {
                if let Some(table) = &self.migration_state {
                    let parsed_id = migration_id
                        .parse::<uuid::Uuid>()
                        .unwrap_or_else(|_| uuid::Uuid::nil());
                    if let Err(e) = apply_migration_abort(
                        table,
                        self.live_routing.as_ref(),
                        parsed_id,
                        reason,
                        compensations,
                    ) {
                        error!(
                            migration_id = %migration_id,
                            error = %e,
                            "FATAL: migration abort compensation failed"
                        );
                        panic!("migration abort compensation failed: {e}");
                    }
                }
            }
            MetadataEntry::JoinTokenTransition {
                token_hash,
                transition,
                ts_ms,
            } => {
                if let Some(mirror) = &self.token_state {
                    apply_token_transition_to_mirror(mirror, *token_hash, transition, *ts_ms);
                }
            }
            _ => {}
        }
    }

    /// Mutate the live routing handle (if attached) in response to
    /// a committed `RoutingChange` at metadata log `index`.
    fn apply_routing_change(&self, index: u64, change: &RoutingChange) {
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
                rt.reassign_vshard(*vshard_id, *new_group_id, index);
                // The entry names a planned leaseholder with no term. It
                // fills only a hint that holds no term.
                rt.set_leader(*new_group_id, *new_leaseholder_node_id);
            }
            RoutingChange::LeadershipTransfer {
                group_id,
                new_leader_node_id,
            } => {
                // The entry names the transfer target before its election,
                // so it carries no term. The election's leader reaches the
                // hint with its term from Raft or a redirect.
                rt.set_leader(*group_id, *new_leader_node_id);
            }
            RoutingChange::RemoveMember { group_id, node_id } => {
                rt.remove_group_member(*group_id, *node_id);
            }
            RoutingChange::SetPlacement {
                group_id,
                placement,
            } => {
                rt.set_placement(*group_id, placement.clone());
            }
        }
    }
}

#[async_trait::async_trait]
impl MetadataApplier for CacheApplier {
    async fn apply_decoded(&self, entries: &[CommittedMetadata<'_>]) -> u64 {
        let mut last = 0u64;
        let mut guard = self
            .cache
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        for committed in entries {
            let index = committed.index;
            last = index;
            match &committed.payload {
                MetadataPayload::Empty => {}
                MetadataPayload::Decoded(entry) => {
                    guard.apply(index, entry);
                    self.cascade_live_state(index, entry);
                }
                MetadataPayload::Undecodable(e) => {
                    warn!(index, error = %e, "metadata decode failed")
                }
            }
        }
        last
    }
}

/// No-op applier used by tests and subsystems that don't care about the
/// metadata stream. Still drains entries and returns the correct last
/// index so raft can advance its applied watermark.
pub struct NoopMetadataApplier;

#[async_trait::async_trait]
impl MetadataApplier for NoopMetadataApplier {
    async fn apply_decoded(&self, entries: &[CommittedMetadata<'_>]) -> u64 {
        entries.last().map_or(0, |e| e.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::codec::encode_entry;
    use crate::metadata_group::entry::{MetadataEntry, TopologyChange};

    #[tokio::test]
    async fn cache_applier_counts_catalog_ddl() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let ddl = encode_entry(&MetadataEntry::CatalogDdl {
            payload: vec![1, 2, 3],
        })
        .unwrap();
        let topo = encode_entry(&MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 7,
            addr: "10.0.0.7:9000".into(),
            swim_addr: None,
        }))
        .unwrap();

        let last = applier.apply(&[(1, ddl), (2, topo)]).await;
        assert_eq!(last, 2);

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 2);
        assert_eq!(guard.catalog_entries_applied, 1);
        assert_eq!(guard.topology_log.len(), 1);
    }

    #[tokio::test]
    async fn cache_applier_idempotent() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let bytes = encode_entry(&MetadataEntry::CatalogDdl {
            payload: vec![9, 9],
        })
        .unwrap();
        applier.apply(&[(5, bytes.clone())]).await;
        applier.apply(&[(3, bytes)]).await; // Earlier index — ignored.

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 5);
        assert_eq!(guard.catalog_entries_applied, 1);
    }

    #[tokio::test]
    async fn cache_applier_mutates_live_topology_on_start_decommission() {
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
        applier.apply(&[(1, bytes)]).await;

        let topo = topology.read().unwrap();
        assert_eq!(topo.get_node(7).unwrap().state, NodeState::Draining);
    }

    /// A committed `Join` puts the joiner's SWIM address into the live
    /// topology, and a later `Join` with a new address updates it.
    #[tokio::test]
    async fn cache_applier_applies_join_swim_address() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let topology = Arc::new(RwLock::new(crate::topology::ClusterTopology::new()));
        let routing = Arc::new(RwLock::new(crate::routing::RoutingTable::uniform(
            1,
            &[1],
            1,
        )));
        let applier =
            CacheApplier::new(cache.clone()).with_live_state(topology.clone(), routing.clone());
        let join = |swim: &str| {
            encode_entry(&MetadataEntry::TopologyChange(TopologyChange::Join {
                node_id: 9,
                addr: "10.0.0.9:9400".into(),
                swim_addr: Some(swim.into()),
            }))
            .unwrap()
        };

        applier.apply(&[(1, join("10.0.0.9:9401"))]).await;
        assert_eq!(
            topology
                .read()
                .unwrap()
                .get_node(9)
                .unwrap()
                .swim_socket_addr(),
            "10.0.0.9:9401".parse().ok()
        );

        applier.apply(&[(2, join("10.0.0.9:9501"))]).await;
        assert_eq!(
            topology
                .read()
                .unwrap()
                .get_node(9)
                .unwrap()
                .swim_socket_addr(),
            "10.0.0.9:9501".parse().ok()
        );
    }

    /// A committed `Join` with an invalid address is skipped. The node
    /// never enters topology with a placeholder address.
    #[tokio::test]
    async fn cache_applier_skips_join_with_invalid_address() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let topology = Arc::new(RwLock::new(crate::topology::ClusterTopology::new()));
        let routing = Arc::new(RwLock::new(crate::routing::RoutingTable::uniform(
            1,
            &[1],
            1,
        )));
        let applier =
            CacheApplier::new(cache.clone()).with_live_state(topology.clone(), routing.clone());
        let bytes = encode_entry(&MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 9,
            addr: "not-an-address".into(),
            swim_addr: None,
        }))
        .unwrap();

        assert_eq!(applier.apply(&[(1, bytes)]).await, 1);
        assert!(!topology.read().unwrap().contains(9));
    }

    #[tokio::test]
    async fn cache_applier_mutates_live_routing_on_remove_member() {
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
        applier.apply(&[(1, bytes)]).await;

        let rt = routing.read().unwrap();
        assert!(!rt.group_info(0).unwrap().members.contains(&2));
    }

    #[tokio::test]
    async fn cache_applier_mutates_live_routing_on_set_placement() {
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

        let bytes = encode_entry(&MetadataEntry::RoutingChange(RoutingChange::SetPlacement {
            group_id: 1,
            placement: vec![1, 2],
        }))
        .unwrap();
        applier.apply(&[(1, bytes)]).await;

        let rt = routing.read().unwrap();
        assert_eq!(
            rt.group_info(1).unwrap().placement,
            Some(vec![1, 2]),
            "placement should be set on the live routing table"
        );
    }

    #[tokio::test]
    async fn cache_applier_without_live_state_stays_log_only() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());
        let bytes = encode_entry(&MetadataEntry::TopologyChange(
            TopologyChange::StartDecommission { node_id: 5 },
        ))
        .unwrap();
        // Must not panic and must still advance the applied index.
        let last = applier.apply(&[(1, bytes)]).await;
        assert_eq!(last, 1);
    }

    #[tokio::test]
    async fn noop_applier_advances_watermark() {
        let noop = NoopMetadataApplier;
        assert_eq!(
            noop.apply(&[(7, b"x".to_vec()), (9, b"y".to_vec())]).await,
            9
        );
        assert_eq!(noop.apply(&[]).await, 0);
    }
}
