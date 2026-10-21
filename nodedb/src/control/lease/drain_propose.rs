// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease drain proposer flow.
//!
//! 1. Propose `DescriptorDrainStart`. Every node's applier installs it into
//!    `shared.lease_drain`, so `force_refresh_lease` then rejects new acquires
//!    at the drained version.
//! 2. Count `metadata_cache.leases` entries and local holds on the same
//!    descriptor at `version <= up_to_version`, again at each hold or lease
//!    change. Return once none remain.
//! 3. On deadline, propose `DescriptorDrainEnd` so the cluster can progress,
//!    then return the timeout error.
//!
//! The happy path emits no `DescriptorDrainEnd`: the following `Put*` carries
//! the new version and the applier's post-apply hook ends the
//! [`DrainOwner::Ddl`] drain on every node. That saves a raft round-trip per
//! DDL. Every drain has an owner, and each end removes only its own owner's
//! drain, so a DDL never ends a `MOVE TENANT` or materializer drain.

use std::time::{Duration, Instant};

use nodedb_cluster::{DescriptorId, DrainOwner, MetadataEntry, encode_entry};
use nodedb_types::Hlc;

use crate::control::state::SharedState;
use crate::error::Error;

/// The longest the drain wait sleeps between counts with no hold or lease
/// change. Expiry and holder death end a lease with no event to wake it.
pub(super) const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Grace added to the lease duration for the `expires_at` stamped on a drain
/// entry. `is_draining` never reads it, so it affects observability only.
const DRAIN_TTL_GRACE: Duration = Duration::from_secs(30);

/// The `DescriptorDrainStart` entry of a drain of `id` under `owner`.
pub(super) fn drain_start_entry(
    shared: &SharedState,
    id: &DescriptorId,
    owner: &DrainOwner,
    up_to_version: u64,
    max_wait: Duration,
) -> MetadataEntry {
    let now_hlc = shared.hlc_clock.now();
    let ttl_ns: u64 = (max_wait + DRAIN_TTL_GRACE)
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    MetadataEntry::DescriptorDrainStart {
        descriptor_id: id.clone(),
        up_to_version,
        expires_at: Hlc::new(now_hlc.wall_ns.saturating_add(ttl_ns), 0),
        proposer_node_id: shared.node_id,
        owner: owner.clone(),
    }
}

/// One poll of a drain wait: `true` once no matching lease remains, an error
/// once `deadline` passed with some still held, `false` otherwise.
pub(super) fn drained_or_timed_out(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
    deadline: Instant,
) -> Result<bool, Error> {
    let remaining = count_matching_leases(shared, id, up_to_version, own_holds);
    if remaining == 0 {
        return Ok(true);
    }
    if Instant::now() >= deadline {
        return Err(Error::Config {
            detail: format!(
                "descriptor lease drain timed out after {max_wait:?} \
                 waiting for {id:?} up to version {up_to_version} \
                 (still held: {remaining})"
            ),
        });
    }
    Ok(false)
}

/// Count leases and admission reservations on `id` at `version <=
/// up_to_version`. `0` means the drain has cleared; a nonzero value is
/// diagnostic only, so it saturates rather than overflowing.
///
/// Three filters drop leases whose holder can no longer use them. A crashed
/// node never releases its leases, so without them every DDL on those
/// descriptors waits out the full lease duration.
///
/// - Non-member holders are dropped. Missing topology treats every holder as a
///   member, so the filter only drops holds it is certain about.
/// - Expired leases are dropped. A live holder never has one: the renewal loop
///   re-acquires before expiry.
/// - A SWIM-Dead holder's leases are dropped only on the metadata leader, once
///   [`nodedb_cluster::DEAD_HOLDER_LEASE_GRACE`] has passed since it went Dead
///   and the leader has seen no Raft response from it for
///   [`nodedb_cluster::DEAD_HOLDER_RAFT_SILENCE`]. By then the holder has
///   self-fenced, or it still hears the leader and applies the release. Any
///   other drainer waits for the leader's lease GC to release them.
///
/// `expires_at.wall_ns` is stamped on the holder's own wall clock. A lease
/// held by another node therefore stays live until
/// [`nodedb_types::MAX_CLOCK_SKEW_NS`] past it. This node's own leases get no
/// margin. A live hold on THIS node is also counted through `lease_refcount`,
/// which expiry never touches.
///
/// Expiry compares against wall time, not [`HlcClock::peek`]: `peek` stays
/// frozen on a quiet cluster, which finds every lease unexpired and
/// reinstates the wedge — and an idle cluster is exactly when a crashed node's
/// leases are the only ones left.
///
/// `own_holds` excludes that many local refcount units — the requester's own —
/// from both the refcount safety net and this node's replicated cache entry,
/// but only once no other local holder remains.
fn count_matching_leases(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    own_holds: u32,
) -> usize {
    let instant = Instant::now();
    let now = nodedb_cluster::LeaseNow {
        wall_ns: nodedb_types::wall_now_ns(),
        instant,
        // Reading the leader term takes the raft coordinator lock, so skip
        // it unless some holder can qualify for early release.
        metadata_leader_term: if shared
            .lease_runtime
            .holder_liveness
            .any_dead_grace_elapsed(instant)
        {
            metadata_leader_term(shared)
        } else {
            None
        },
    };
    let other_local_holds = shared
        .lease_refcount
        .current_at_or_below(id, up_to_version)
        .saturating_sub(own_holds);
    // Only the requester's own hold is left locally: its replicated cache
    // entry on this node is the very lease it is about to supersede, not a
    // conflicting holder, so it must not block the requester's own drain.
    let self_only = own_holds > 0 && other_local_holds == 0;
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let metadata_holds = cache
        .leases
        .iter()
        .filter(|((lid, holder), l)| {
            lid == id
                && l.version <= up_to_version
                && shared.lease_runtime.holder_liveness.lease_is_live(
                    *holder,
                    shared.node_id,
                    l.expires_at.wall_ns,
                    &now,
                )
                && lease_holder_is_member(shared, *holder)
                && !(self_only && *holder == shared.node_id)
        })
        .count();
    drop(cache);

    if other_local_holds == 0 {
        metadata_holds
    } else {
        metadata_holds.saturating_add(1)
    }
}

/// This node's metadata-group term while it leads the group, else `None`.
fn metadata_leader_term(shared: &SharedState) -> Option<u64> {
    shared
        .lease_runtime
        .metadata_leader_term
        .get()
        .and_then(|term| term())
}

/// Whether `node_id` is a current cluster member. Missing topology treats
/// every holder as a member.
fn lease_holder_is_member(shared: &SharedState, node_id: u64) -> bool {
    match &shared.cluster_topology {
        Some(topo) => topo
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(node_id),
        None => true,
    }
}

/// Encode a drain variant for the metadata group.
pub(super) fn encode_drain(entry: &MetadataEntry, operation: &str) -> Result<Vec<u8>, Error> {
    encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("descriptor drain {operation} encode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;
    use nodedb_cluster::DescriptorKind;

    #[tokio::test]
    async fn in_flight_admission_reservation_blocks_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        state.lease_refcount.increment(&descriptor, 1);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 1);
        state.lease_refcount.decrement(&descriptor, 1);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
    }

    #[tokio::test]
    async fn newer_admission_reservation_does_not_block_older_drain() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        state.lease_refcount.increment(&descriptor, 2);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
        state.lease_refcount.decrement(&descriptor, 2);
    }

    /// Build a topology containing exactly `ids` as active members.
    fn topo_with(ids: &[u64]) -> nodedb_cluster::ClusterTopology {
        let mut t = nodedb_cluster::ClusterTopology::new();
        for (i, id) in ids.iter().enumerate() {
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(nodedb_cluster::NodeInfo::new(
                *id,
                addr,
                nodedb_cluster::NodeState::Active,
            ));
        }
        t
    }

    /// Insert a lease directly into the metadata cache (as if committed via a
    /// `DescriptorLeaseGrant` entry).
    fn insert_lease(
        state: &SharedState,
        id: &DescriptorId,
        holder: u64,
        version: u64,
        expires_at: nodedb_types::Hlc,
    ) {
        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .insert(
                (id.clone(), holder),
                nodedb_cluster::DescriptorLease {
                    descriptor_id: id.clone(),
                    version,
                    node_id: holder,
                    expires_at,
                },
            );
    }

    /// A minute into the future in REAL wall time — the frame the grant path
    /// stamps in. Deriving it from `hlc_clock.peek()` puts fixture and
    /// code under test in one frozen frame, and the assertion proves
    /// nothing.
    fn unexpired() -> nodedb_types::Hlc {
        nodedb_types::Hlc::new(
            nodedb_types::wall_now_ns().saturating_add(60_000_000_000),
            0,
        )
    }

    /// A lease expiry a minute in the past, in REAL wall time.
    fn expired() -> nodedb_types::Hlc {
        nodedb_types::Hlc::new(
            nodedb_types::wall_now_ns().saturating_sub(60_000_000_000),
            0,
        )
    }

    #[tokio::test]
    async fn non_member_lease_does_not_block_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Holder 99 is not in the topology (crashed node): its lease must not
        // block the drain count.
        insert_lease(&state, &descriptor, 99, 1, unexpired());
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
    }

    #[tokio::test]
    async fn expired_lease_does_not_block_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Holder 1 is a member but its lease is already past expiry.
        insert_lease(&state, &descriptor, 1, 1, expired());
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);
    }

    /// An expired lease must stop blocking the drain even when this node's HLC
    /// has not advanced. `peek` never advances on its own, so on a quiet node
    /// it sits at `Hlc::ZERO` — and a quiet cluster is exactly when a crashed
    /// node's leases are the only ones left.
    #[tokio::test]
    async fn expired_lease_stops_blocking_even_with_an_unadvanced_hlc() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let fixture = Arc::get_mut(&mut state).expect("single owner in test");
        fixture.cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        // The WAL open stamps its empty-log time anchor from the node HLC,
        // which advances it to the open's wall time. A fresh clock stands for
        // an HLC that never advanced.
        fixture.hlc_clock = Arc::new(nodedb_types::HlcClock::new());
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Untouched HLC: `peek()` is at zero while wall time is decades ahead.
        assert_eq!(
            state.hlc_clock.peek().wall_ns,
            0,
            "this test is only meaningful while the HLC has not advanced"
        );

        insert_lease(&state, &descriptor, 1, 1, expired());
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            0,
            "an expired lease must not block the drain, however stale the HLC is"
        );
    }

    /// The other direction: an HLC dragged past wall time must not make a live
    /// lease look expired. Dropping a live hold lets the DDL proceed under a
    /// holder still using the descriptor.
    #[tokio::test]
    async fn a_live_lease_still_blocks_when_the_hlc_runs_ahead_of_wall_time() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // An HLC an hour ahead of real time, folded into the local clock.
        let skewed = nodedb_types::Hlc::new(
            nodedb_types::wall_now_ns().saturating_add(3_600_000_000_000),
            0,
        );
        state.hlc_clock.update(skewed);
        assert!(state.hlc_clock.peek().wall_ns > nodedb_types::wall_now_ns());

        insert_lease(&state, &descriptor, 1, 1, unexpired());
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            1,
            "a lease that is live in wall time must keep blocking the drain"
        );
    }

    #[tokio::test]
    async fn member_unexpired_lease_still_blocks_drain_count() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // The membership and expiry filters must never mask a real hold.
        insert_lease(&state, &descriptor, 1, 1, unexpired());
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 1);
    }

    /// A transaction altering a descriptor it still holds a statement-time
    /// lease on must not wait on its own hold. Both the refcount and this
    /// node's replicated cache entry are excluded once `own_holds` covers
    /// everything left locally.
    #[tokio::test]
    async fn own_holds_excludes_the_requesters_own_sole_local_hold() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // The requester's own statement-time hold: refcount and cache entry.
        state.lease_refcount.increment(&descriptor, 1);
        insert_lease(&state, &descriptor, state.node_id, 1, unexpired());

        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            2,
            "without exclusion the requester's own hold blocks its own drain"
        );
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 1),
            0,
            "own_holds must exclude the requester's own sole local hold"
        );
    }

    /// A different session's hold on the same node still blocks after the
    /// requester's own contribution is excluded.
    #[tokio::test]
    async fn own_holds_does_not_mask_a_different_local_holder() {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());

        // Two local holds: the requester's own (excluded) and a different
        // session's on the same node (must still block).
        state.lease_refcount.increment(&descriptor, 1);
        state.lease_refcount.increment(&descriptor, 1);
        insert_lease(&state, &descriptor, state.node_id, 1, unexpired());

        assert_ne!(
            count_matching_leases(&state, &descriptor, 1, 1),
            0,
            "a different session's hold on the same descriptor must still block the drain"
        );
    }

    /// State whose topology holds this node and one remote holder.
    fn state_with_remote_member() -> (Arc<SharedState>, u64, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create drain count test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain-count.wal"))
                .expect("open drain count test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct drain count state");
        let remote = state.node_id + 1;
        let local = state.node_id;
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[
            local, remote,
        ]))));
        (state, remote, directory)
    }

    fn orders() -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string())
    }

    /// An instant `ago` in the past on the monotonic clock.
    fn instant_ago(ago: Duration) -> Instant {
        Instant::now()
            .checked_sub(ago)
            .expect("monotonic clock far enough from its origin")
    }

    /// A remote holder stamps `expires_at` on its own clock. A drainer whose
    /// clock runs ahead must not treat that lease as expired inside the skew
    /// margin. This node's own lease gets no margin.
    #[tokio::test]
    async fn remote_lease_inside_the_skew_margin_blocks_drain() {
        let (state, remote, _directory) = state_with_remote_member();
        let descriptor = orders();
        let just_expired =
            nodedb_types::Hlc::new(nodedb_types::wall_now_ns().saturating_sub(1_000_000_000), 0);

        insert_lease(&state, &descriptor, remote, 1, just_expired);
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            1,
            "a remote lease one second past expiry is inside the skew margin"
        );

        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .clear();
        insert_lease(&state, &descriptor, state.node_id, 1, just_expired);
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            0,
            "this node's own expired lease gets no skew margin"
        );
    }

    const LEADER_TERM: u64 = 7;

    /// Make `state` report itself as metadata-group leader in [`LEADER_TERM`].
    fn make_metadata_leader(state: &SharedState) {
        let term: Arc<dyn Fn() -> Option<u64> + Send + Sync> = Arc::new(|| Some(LEADER_TERM));
        if state.lease_runtime.metadata_leader_term.set(term).is_err() {
            panic!("metadata leader term already set in a fresh test state");
        }
    }

    /// Leader samples of `holder`'s Raft responses: `first` acks long enough
    /// ago to cover the silence window, then `latest` acks now.
    fn sample_raft_acks(state: &SharedState, holder: u64, first: u64, latest: u64) {
        let sample = |acks| nodedb_cluster::multi_raft::PeerAckSample {
            term: LEADER_TERM,
            acks: vec![(holder, acks)],
        };
        let window_ago =
            instant_ago(nodedb_cluster::DEAD_HOLDER_RAFT_SILENCE + Duration::from_secs(1));
        state
            .lease_runtime
            .holder_liveness
            .observe_raft_contact(&sample(first), window_ago);
        state
            .lease_runtime
            .holder_liveness
            .observe_raft_contact(&sample(latest), Instant::now());
    }

    fn dead_past_grace(state: &SharedState, holder: u64) {
        state.lease_runtime.holder_liveness.record_dead_at(
            holder,
            instant_ago(nodedb_cluster::DEAD_HOLDER_LEASE_GRACE + Duration::from_secs(1)),
        );
    }

    /// A SWIM-Dead, Raft-silent holder stays in topology. On the metadata
    /// leader its unexpired lease blocks the drain until the dead grace
    /// passes, then stops blocking.
    #[tokio::test]
    async fn dead_holder_lease_is_released_only_after_the_clamp() {
        let (state, remote, _directory) = state_with_remote_member();
        make_metadata_leader(&state);
        sample_raft_acks(&state, remote, 3, 3);
        let descriptor = orders();
        insert_lease(&state, &descriptor, remote, 1, unexpired());

        state
            .lease_runtime
            .holder_liveness
            .record_dead_at(remote, Instant::now());
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            1,
            "a holder that just went Dead may still be serving until it self-fences"
        );

        dead_past_grace(&state, remote);
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            0,
            "past the dead grace the holder has fenced, so its lease must not block"
        );
    }

    /// SWIM says Dead, but the leader heard the holder over Raft recently.
    /// The holder has not fenced, so its lease keeps blocking.
    #[tokio::test]
    async fn dead_by_swim_but_recently_acked_by_raft_keeps_the_lease() {
        let (state, remote, _directory) = state_with_remote_member();
        make_metadata_leader(&state);
        sample_raft_acks(&state, remote, 3, 4);
        let descriptor = orders();
        insert_lease(&state, &descriptor, remote, 1, unexpired());
        dead_past_grace(&state, remote);

        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            1,
            "a holder that still answers Raft must keep its lease"
        );
    }

    /// A drainer that does not lead the metadata group keeps a Dead holder's
    /// lease live. Only the leader's lease GC releases it.
    #[tokio::test]
    async fn non_leader_drainer_keeps_a_dead_holders_lease() {
        let (state, remote, _directory) = state_with_remote_member();
        sample_raft_acks(&state, remote, 3, 3);
        let descriptor = orders();
        insert_lease(&state, &descriptor, remote, 1, unexpired());
        dead_past_grace(&state, remote);

        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 1);
    }

    /// SWIM refuting a Dead verdict with Alive clears the clamp: the holder's
    /// lease blocks the drain again until its own expiry.
    #[tokio::test]
    async fn alive_refutation_clears_the_clamp() {
        use nodedb_cluster::{MemberState, MembershipSubscriber};

        let (state, remote, _directory) = state_with_remote_member();
        make_metadata_leader(&state);
        sample_raft_acks(&state, remote, 3, 3);
        let descriptor = orders();
        insert_lease(&state, &descriptor, remote, 1, unexpired());
        dead_past_grace(&state, remote);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 0);

        let swim_id =
            nodedb_types::NodeId::try_new(remote.to_string()).expect("numeric SWIM node id");
        state.lease_runtime.holder_liveness.on_state_change(
            &swim_id,
            Some(MemberState::Dead),
            MemberState::Alive,
        );
        assert_eq!(
            count_matching_leases(&state, &descriptor, 1, 0),
            1,
            "an Alive refutation must restore the lease as a blocking hold"
        );
    }
}
