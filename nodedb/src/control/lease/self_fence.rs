// SPDX-License-Identifier: BUSL-1.1

//! Holder-side lease fence.
//!
//! A holder refuses its cached lease once its last metadata-leader contact is
//! older than [`LEASE_SELF_FENCE_WINDOW`]. Other nodes release a SWIM-Dead
//! holder's leases only after that window, plus the clock-skew margin, has
//! passed. So a live holder that SWIM misjudges has stopped using its lease
//! before anyone else treats the lease as gone.
//!
//! A replica behind the leader's commit index also fails the check. It can lack
//! a release or drain the leader already committed.
//!
//! The fence covers only the cached fast path. A refused holder re-acquires
//! through a fresh raft grant, which proves leader contact again.

use nodedb_cluster::{LEASE_SELF_FENCE_WINDOW, METADATA_GROUP_ID};

use crate::control::state::SharedState;

/// Whether this node must not reuse a cached descriptor lease now.
///
/// `start_raft` installs the raft read gate. Before it runs no metadata group
/// exists to drain or release this node's leases, so the node never fences.
pub(crate) fn lease_use_is_fenced(shared: &SharedState) -> bool {
    match shared.raft_read_gate.get() {
        Some(gate) => !gate.within_staleness_bound(
            METADATA_GROUP_ID,
            LEASE_SELF_FENCE_WINDOW,
            shared.applied_index_watcher(METADATA_GROUP_ID).current(),
        ),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use nodedb_cluster::{DescriptorId, DescriptorKind};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::read_index::{RaftReadGate, ReadIndexRefusal};
    use crate::control::lease::DEFAULT_LEASE_DURATION;
    use crate::wal::WalManager;

    /// Gate whose metadata-leader contact is `contact_age_ms` old.
    struct ContactAgeGate {
        contact_age_ms: AtomicU64,
    }

    impl ContactAgeGate {
        fn set_contact_age(&self, age: Duration) {
            let ms = u64::try_from(age.as_millis()).unwrap_or(u64::MAX);
            self.contact_age_ms.store(ms, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl RaftReadGate for ContactAgeGate {
        async fn confirm_leader(
            &self,
            _group_id: u64,
            _timeout: Duration,
        ) -> Result<u64, ReadIndexRefusal> {
            Err(ReadIndexRefusal::NotLeader)
        }

        fn within_staleness_bound(
            &self,
            group_id: u64,
            max_staleness: Duration,
            _applied: u64,
        ) -> bool {
            group_id == METADATA_GROUP_ID
                && Duration::from_millis(self.contact_age_ms.load(Ordering::SeqCst))
                    <= max_staleness
        }

        fn holds_leader_lease(&self, _group_id: u64) -> bool {
            false
        }

        fn leader_lease_term(&self, _group_id: u64) -> Option<u64> {
            None
        }
    }

    fn fenced_state() -> (Arc<SharedState>, Arc<ContactAgeGate>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create self-fence test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("self-fence.wal"))
                .expect("open self-fence test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct self-fence state");
        let gate = Arc::new(ContactAgeGate {
            contact_age_ms: AtomicU64::new(0),
        });
        if state
            .raft_read_gate
            .set(Arc::clone(&gate) as Arc<dyn RaftReadGate>)
            .is_err()
        {
            panic!("raft read gate already set in a fresh test state");
        }
        (state, gate, directory)
    }

    fn orders() -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string())
    }

    /// Install a granted lease on `orders` the way the metadata applier
    /// installs a committed grant. This state runs no metadata group.
    fn grant_orders(state: &SharedState) -> nodedb_cluster::DescriptorLease {
        let lease = nodedb_cluster::DescriptorLease {
            descriptor_id: orders(),
            version: 1,
            node_id: state.node_id,
            expires_at: nodedb_types::Hlc::new(
                state.hlc_clock.peek().wall_ns + DEFAULT_LEASE_DURATION.as_nanos() as u64,
                0,
            ),
        };
        state
            .metadata_cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .leases
            .insert((orders(), state.node_id), lease.clone());
        lease
    }

    #[tokio::test]
    async fn no_gate_never_fences() {
        let directory = tempfile::tempdir().expect("create self-fence test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("self-fence.wal"))
                .expect("open self-fence test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct self-fence state");
        assert!(!lease_use_is_fenced(&state));
    }

    #[tokio::test]
    async fn cached_lease_is_used_inside_the_window() {
        let (state, gate, _directory) = fenced_state();
        gate.set_contact_age(LEASE_SELF_FENCE_WINDOW - Duration::from_secs(1));

        let first = grant_orders(&state);
        assert!(state.lookup_lease_for_self(&orders()).is_some());
        let reused = state
            .acquire_descriptor_lease(orders(), 1, DEFAULT_LEASE_DURATION)
            .await
            .expect("fast-path acquire");
        assert_eq!(
            reused.expires_at, first.expires_at,
            "fast path reuses the lease"
        );
    }

    #[tokio::test]
    async fn cached_lease_is_refused_past_the_window() {
        let (state, gate, _directory) = fenced_state();
        grant_orders(&state);

        gate.set_contact_age(LEASE_SELF_FENCE_WINDOW + Duration::from_secs(1));
        assert!(lease_use_is_fenced(&state));
        assert!(
            state.lookup_lease_for_self(&orders()).is_none(),
            "a fenced holder must not report its cached lease as usable"
        );

        // A fenced holder proposes a fresh grant instead of reusing the
        // cached lease. This state runs no metadata group, so the proposal
        // refuses.
        let reacquired = state
            .acquire_descriptor_lease(orders(), 1, DEFAULT_LEASE_DURATION)
            .await;
        assert!(
            reacquired.is_err(),
            "a fenced holder must re-acquire instead of reusing the cached lease: {reacquired:?}"
        );
    }

    /// A follower applying a drain start has not advanced its applied index
    /// past the entry, so it always reads as fenced there. The fence gates
    /// reuse only: the drain start still hands the idle lease to the
    /// releaser.
    #[tokio::test]
    async fn a_fenced_holder_still_releases_its_idle_lease_on_drain_start() {
        let (state, gate, _directory) = fenced_state();
        grant_orders(&state);
        gate.set_contact_age(LEASE_SELF_FENCE_WINDOW + Duration::from_secs(1));
        assert!(lease_use_is_fenced(&state));

        crate::control::lease::release::release_idle_on_drain(&state, &orders());

        let mut rx = state
            .lease_runtime
            .releaser
            .take_receiver()
            .expect("no releaser task runs in this state");
        assert_eq!(
            rx.try_recv().ok(),
            Some(
                crate::control::lease::releaser::ReleaseRequest::UnheldDescriptors(vec![orders()])
            ),
            "the drain start must hand the idle lease to the releaser"
        );
    }
}
