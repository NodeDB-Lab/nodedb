// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease drain proposer flow.
//!
//! 1. Propose `DescriptorDrainStart`. Every node's applier installs it into
//!    `shared.lease_drain`, so `force_refresh_lease` then rejects new acquires
//!    at the drained version.
//! 2. Poll `metadata_cache.leases` for entries on the same descriptor at
//!    `version <= up_to_version`. Return once none remain.
//! 3. On deadline, propose `DescriptorDrainEnd` so the cluster can progress,
//!    then return the timeout error.
//!
//! The happy path emits no `DescriptorDrainEnd`: the following `Put*` carries
//! the new version and the applier's post-apply hook calls `install_end` on
//! every node. That saves a raft round-trip per DDL.
//!
//! The drain variants are wire-format v4, so mixed clusters gate on
//! `DESCRIPTOR_DRAIN_VERSION` and run without drain safety until every node is
//! upgraded.

use std::time::{Duration, Instant};
use tokio::runtime::RuntimeFlavor;

use nodedb_cluster::{DescriptorId, MetadataEntry, encode_entry};
use nodedb_types::Hlc;

use crate::control::rolling_upgrade::DESCRIPTOR_DRAIN_VERSION;
use crate::control::state::SharedState;
use crate::error::Error;

/// Re-poll interval for the drain wait loop.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Grace added to the lease duration for the `expires_at` stamped on a drain
/// entry. `is_draining` never reads it, so it affects observability only.
const DRAIN_TTL_GRACE: Duration = Duration::from_secs(30);

/// Drain every lease on `id` at `version <= up_to_version` for a `Put*` DDL.
///
/// Returns `Ok(())` once they have drained, or immediately when the
/// rolling-upgrade gate is closed. Errors on timeout or propose failure.
///
/// `own_holds` is how many of those refcount units the requesting transaction
/// holds itself — `0` for a caller with no lease scope of its own. A
/// transaction altering a descriptor it also holds a statement-time lease on
/// cannot wait for its own hold: it cannot release that lease until this
/// call returns.
pub fn drain_for_ddl(
    shared: &SharedState,
    id: DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    // Rolling upgrade gate: no drain in mixed-version clusters.
    {
        let vs = shared.cluster_version_view();
        if !vs.can_activate_feature(DESCRIPTOR_DRAIN_VERSION) {
            tracing::warn!(
                min_version = vs.min_version,
                required = DESCRIPTOR_DRAIN_VERSION,
                "descriptor lease drain: cluster in compat mode, skipping drain"
            );
            return Ok(());
        }
    }

    // No prior version means no lease can exist. Callers skip this case
    // already; the guard is cheap.
    if up_to_version == 0 {
        return Ok(());
    }

    let now_hlc = shared.hlc_clock.now();
    let ttl_ns: u64 = (max_wait + DRAIN_TTL_GRACE)
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    let expires_at = Hlc::new(now_hlc.wall_ns.saturating_add(ttl_ns), 0);

    propose_drain(
        shared,
        MetadataEntry::DescriptorDrainStart {
            descriptor_id: id.clone(),
            up_to_version,
            expires_at,
        },
        "drain_start",
    )?;

    match poll_leases_drained(shared, &id, up_to_version, max_wait, own_holds) {
        Ok(()) => Ok(()),
        Err(e) => {
            // `is_draining` has no expiry backstop, so this explicit propose
            // is the only thing that clears the drain after a timeout. Its own
            // errors are logged and dropped.
            if let Err(cleanup_err) = propose_drain(
                shared,
                MetadataEntry::DescriptorDrainEnd {
                    descriptor_id: id.clone(),
                },
                "drain_end",
            ) {
                tracing::warn!(
                    error = %cleanup_err,
                    "descriptor lease drain: cleanup propose failed after timeout"
                );
            }
            Err(e)
        }
    }
}

/// Wait until no lease or admission reservation remains on `id` at
/// `version <= up_to_version`, polling every [`POLL_INTERVAL`].
///
/// Sync on purpose: `metadata_proposer` beneath it is sync because pgwire DDL
/// handlers are, so an `async fn` here would ripple through every catalog-DDL
/// call site and strand the sync callers (GC sweeper, clone materializer,
/// backup restore).
///
/// Async tasks still reach it, so on a multi-thread runtime the wait goes back
/// to tokio — parking a worker for the whole drain can delay the very
/// lease-release and raft-apply work it is waiting on.
pub(crate) fn poll_leases_drained(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    // `block_in_place` panics on the current-thread runtime and has no worker
    // pool to hand the parked work to, so it is used only where it is legal.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| {
                wait_for_lease_drain(shared, id, up_to_version, max_wait, own_holds)
            })
        }
        _ => wait_for_lease_drain(shared, id, up_to_version, max_wait, own_holds),
    }
}

/// The wait loop itself, split out so the convergence condition and deadline
/// handling are identical on both paths above.
fn wait_for_lease_drain(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    let deadline = Instant::now() + max_wait;
    loop {
        let remaining = count_matching_leases(shared, id, up_to_version, own_holds);
        if remaining == 0 {
            return Ok(());
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
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Count leases and admission reservations on `id` at `version <=
/// up_to_version`. `0` means the drain has cleared; a nonzero value is
/// diagnostic only, so it saturates rather than overflowing.
///
/// Leases from non-member nodes and leases past `expires_at` are both ignored.
/// A crashed node never releases its leases (no SIGTERM path runs), so without
/// these filters every DDL on those descriptors wedges forever. Missing
/// topology treats every holder as a member — the filter only drops holds it
/// is certain about.
///
/// Dropping an expired lease is safe because a live holder never has one: the
/// renewal loop re-acquires before expiry, so an expired record means that
/// node's renewal stopped. A live hold on THIS node is counted through
/// `lease_refcount` and is unaffected by expiry.
///
/// Expiry compares against wall time, not [`HlcClock::peek`]: `peek` stays
/// frozen on a quiet cluster, which would find every lease unexpired and
/// reinstate the wedge — and an idle cluster is exactly when a crashed node's
/// leases are the only ones left. `expires_at.wall_ns` is stamped from the
/// holder's own `HlcClock::now()`, so a raw comparison would carry that node's
/// clock offset unbounded. [`lease_expired`] gives the deadline the same skew
/// allowance the HLC ingress enforces ([`nodedb_types::MAX_CLOCK_SKEW_NS`]):
/// a lease is retired only once even a clock that far ahead of ours would
/// agree it lapsed, so a fast local clock cannot hand a still-live holder's
/// descriptor to the DDL early.
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
    let now_wall_ns = super::wall_now_ns();
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
                && !lease_expired(l.expires_at.wall_ns, now_wall_ns)
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

/// Whether a lease stamped `expires_at_wall_ns` on its holder's clock is
/// certainly past expiry at this node's `now_wall_ns`.
///
/// The holder stamped its deadline from its own wall clock and its HLC is
/// never merged into ours, so the raw comparison would carry that node's clock
/// offset. Retiring a lease early is the unsafe direction: the drain would
/// declare the descriptor free while the holder still believes its lease runs,
/// which is the window the drain exists to close. The comparison therefore
/// waits out [`nodedb_types::MAX_CLOCK_SKEW_NS`] past the stamped deadline —
/// the same bound the HLC ingress refuses observations beyond — so only a
/// clock ahead by more than the cluster's tolerance retires a lease early.
///
/// A deadline so far future that adding the bound saturates is never retired:
/// the filter drops holds only where the lapse is certain, matching
/// [`lease_holder_is_member`].
fn lease_expired(expires_at_wall_ns: u64, now_wall_ns: u64) -> bool {
    match expires_at_wall_ns.checked_add(nodedb_types::MAX_CLOCK_SKEW_NS) {
        Some(deadline) => deadline <= now_wall_ns,
        None => false,
    }
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

/// Encode and propose a drain variant, blocking until the applied-index
/// watcher confirms it applied locally. Separate from `lease::propose_and_wait`
/// because drain variants are not `CatalogDdl` and encode differently.
fn propose_drain(
    shared: &SharedState,
    entry: MetadataEntry,
    operation: &'static str,
) -> Result<(), Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        // Single-node fallback: apply through the same path the applier uses,
        // so drain state is exercised without a raft loop.
        apply_drain_locally(shared, &entry);
        return Ok(());
    };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("descriptor drain {operation} encode: {e}"),
    })?;
    let log_index = handle.propose(raw)?;
    let watcher = shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
    const DRAIN_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DRAIN_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "descriptor drain {operation} did not apply within {DRAIN_PROPOSE_TIMEOUT:?} \
                 (log index {log_index}, current: {}, outcome: {outcome:?})",
                watcher.current()
            ),
        });
    }
    Ok(())
}

/// Apply a drain variant to the local tracker without raft, so `drain_for_ddl`
/// has the same semantics in every deployment mode.
fn apply_drain_locally(shared: &SharedState, entry: &MetadataEntry) {
    match entry {
        MetadataEntry::DescriptorDrainStart {
            descriptor_id,
            up_to_version,
            expires_at,
        } => {
            // Shares plan admission's gate: an admission completes before this
            // start installs, or this drain wins and admission fails closed.
            let _admission_gate = shared
                .lease_admission_gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            shared
                .lease_drain
                .install_start(descriptor_id.clone(), *up_to_version, *expires_at);
        }
        MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
            shared.lease_drain.install_end(descriptor_id);
        }
        _ => {}
    }
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
    /// stamps in. Deriving it from `hlc_clock.peek()` would put fixture and
    /// code under test in one frozen frame, and the assertion would prove
    /// nothing.
    fn unexpired() -> nodedb_types::Hlc {
        nodedb_types::Hlc::new(
            super::super::wall_now_ns().saturating_add(60_000_000_000),
            0,
        )
    }

    /// A lease expiry a minute in the past, in REAL wall time.
    fn expired() -> nodedb_types::Hlc {
        nodedb_types::Hlc::new(
            super::super::wall_now_ns().saturating_sub(60_000_000_000),
            0,
        )
    }

    #[test]
    fn lease_expired_waits_out_the_skew_bound() {
        let deadline = 1_000_000_000_000u64;
        assert!(!lease_expired(deadline, deadline));
        assert!(!lease_expired(
            deadline,
            deadline + nodedb_types::MAX_CLOCK_SKEW_NS - 1
        ));
        assert!(lease_expired(
            deadline,
            deadline + nodedb_types::MAX_CLOCK_SKEW_NS
        ));
        assert!(lease_expired(
            deadline,
            deadline + nodedb_types::MAX_CLOCK_SKEW_NS + 1
        ));
    }

    #[test]
    fn lease_expired_saturates_at_the_clock_ceiling() {
        assert!(!lease_expired(u64::MAX, u64::MAX));
        assert!(lease_expired(0, u64::MAX));
    }

    /// A deadline that just passed in our frame is still inside the skew
    /// window: the drain keeps waiting, because the holder's clock may be
    /// behind ours and its lease is still live there.
    #[tokio::test]
    async fn lease_inside_the_skew_window_still_blocks_the_drain() {
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

        let just_past = nodedb_types::Hlc::new(super::super::wall_now_ns().saturating_sub(1), 0);
        insert_lease(&state, &descriptor, 1, 1, just_past);
        assert_eq!(count_matching_leases(&state, &descriptor, 1, 0), 1);
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
        Arc::get_mut(&mut state)
            .expect("single owner in test")
            .cluster_topology = Some(Arc::new(std::sync::RwLock::new(topo_with(&[1]))));
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
            super::super::wall_now_ns().saturating_add(3_600_000_000_000),
            0,
        );
        state.hlc_clock.update(skewed);
        assert!(state.hlc_clock.peek().wall_ns > super::super::wall_now_ns());

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
}
