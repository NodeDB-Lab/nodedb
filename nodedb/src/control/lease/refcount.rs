// SPDX-License-Identifier: BUSL-1.1

//! Per-query descriptor lease refcount + scope guard.
//!
//! Descriptor leases are acquired at plan time, or at authorization for a
//! write that runs no planner, and held through execute. Queries touching the
//! same descriptor version share one underlying raft lease: per-node
//! exact-version refcounts mean only a missing or lower-version lease pays an
//! acquire round trip.
//!
//! A lease whose refcount returns to 0 stays granted. The next statement on
//! the descriptor reuses it with no raft traffic. It ends one of three ways:
//!
//! - a `DescriptorDrainStart` applies on this node, which releases this
//!   node's unheld leases on that descriptor at once, so the drain waits only
//!   for statements still running;
//! - the last statement still running under an active drain ends, which
//!   hands the release to the background releaser;
//! - it reaches expiry: the renewal loop renews a held lease and releases an
//!   idle one.
//!
//! ## Guard semantics
//!
//! `QueryLeaseScope` is the owned collection of leases a single statement
//! holds. Drop decrements each exact-version refcount and never waits.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_cluster::DescriptorId;

use super::holders::{HolderTicket, LeaseHolders};
use crate::control::state::SharedState;
use crate::error::Error;

/// Host-side lease reference counts. One entry per descriptor id and
/// descriptor version this node currently holds; the value is the number of
/// in-flight queries or admissions holding that exact version.
#[derive(Debug, Default)]
pub struct LeaseRefCount {
    counts: Mutex<HashMap<(DescriptorId, u64), u32>>,
}

impl LeaseRefCount {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the refcount for exact `(id, version)`. Returns the new
    /// exact-version count, saturating rather than overflowing.
    pub fn increment(&self, id: &DescriptorId, version: u64) -> u32 {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map.entry((id.clone(), version)).or_insert(0);
        *entry = entry.saturating_add(1);
        *entry
    }

    /// Decrement the refcount for exact `(id, version)`. Returns the new
    /// exact-version count and removes its entry when it reaches zero.
    pub fn decrement(&self, id: &DescriptorId, version: u64) -> u32 {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let key = (id.clone(), version);
        if let Some(entry) = map.get_mut(&key) {
            *entry = entry.saturating_sub(1);
            let count = *entry;
            if count == 0 {
                map.remove(&key);
            }
            count
        } else {
            0
        }
    }

    /// Read the total refcount across every held version of `id`.
    pub fn current(&self, id: &DescriptorId) -> u32 {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|((held_id, _), _)| held_id == id)
            .fold(0_u32, |total, (_, count)| total.saturating_add(*count))
    }

    /// Read the total refcount for `id` at versions no greater than
    /// `up_to_version`.
    pub fn current_at_or_below(&self, id: &DescriptorId, up_to_version: u64) -> u32 {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|((held_id, version), _)| held_id == id && *version <= up_to_version)
            .fold(0_u32, |total, (_, count)| total.saturating_add(*count))
    }

    /// Total number of exact descriptor-version entries with a non-zero refcount.
    pub fn distinct_count(&self) -> usize {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.len()
    }
}

/// Gives back refcount units and releases a lease whose last hold ends while
/// a drain covers it.
///
/// A drain start releases this node's idle leases on its descriptor once, as
/// it installs. A lease still held then is released here, when its last hold
/// ends: otherwise it stays granted until expiry and the drain waits for it.
/// The drain installs before its start checks the refcount, and a hold
/// decrements before it checks the drain, so one of the two always releases.
#[derive(Clone)]
pub(crate) struct HoldRelease {
    refcounts: Arc<LeaseRefCount>,
    drains: Arc<super::DescriptorDrainTracker>,
    queue: super::releaser::ReleaseQueue,
}

impl HoldRelease {
    pub(crate) fn for_state(shared: &SharedState) -> Self {
        Self {
            refcounts: Arc::clone(&shared.lease_refcount),
            drains: Arc::clone(&shared.lease_drain),
            queue: shared.lease_runtime.releaser.queue(),
        }
    }

    /// Give back one unit of each hold. A descriptor left with no hold while
    /// a drain covers the version it held is handed to the background
    /// releaser.
    fn give_back(&self, holds: impl IntoIterator<Item = (DescriptorId, u64)>) {
        let mut drained_idle: Vec<DescriptorId> = Vec::new();
        let mut drained_hold_ended = false;
        for (id, version) in holds {
            self.refcounts.decrement(&id, version);
            if !self.drains.is_draining(&id, version) {
                continue;
            }
            drained_hold_ended = true;
            if self.refcounts.current(&id) == 0 && !drained_idle.contains(&id) {
                drained_idle.push(id);
            }
        }
        // A drain counts this node's holds directly, so it re-counts now.
        if drained_hold_ended {
            self.drains.wake_drain_waiters();
        }
        if !drained_idle.is_empty() {
            self.queue
                .submit(super::releaser::ReleaseRequest::UnheldDescriptors(
                    drained_idle,
                ));
        }
    }
}

/// One exact-version refcount unit a grant in flight holds. Dropping it,
/// on return or when its future is cancelled, gives the unit back.
pub(crate) struct RefcountReservation {
    release: HoldRelease,
    id: DescriptorId,
    version: u64,
}

impl RefcountReservation {
    /// Take one unit of `(id, version)`. The caller holds the admission gate.
    pub(crate) fn reserve(shared: &SharedState, id: DescriptorId, version: u64) -> Self {
        shared.lease_refcount.increment(&id, version);
        Self {
            release: HoldRelease::for_state(shared),
            id,
            version,
        }
    }
}

impl Drop for RefcountReservation {
    fn drop(&mut self) {
        self.release.give_back([(self.id.clone(), self.version)]);
    }
}

/// Owned collection of lease holds for one query.
///
/// Created by `OriginCatalog::take_lease_scope()` after
/// planning finishes; held by the pgwire handler through the
/// execute phase; released on drop.
pub struct QueryLeaseScope {
    /// Exact descriptor-version refcounts this query holds.
    descriptor_versions: Vec<(DescriptorId, u64)>,
    /// Gives the holds back on drop, independently of the process-wide state.
    release: Option<HoldRelease>,
    /// This query's entry in the node's holder table, which lets a lost
    /// lease revoke it. `None` for an empty scope.
    holder: Option<(Arc<LeaseHolders>, HolderTicket)>,
}

impl QueryLeaseScope {
    /// Create an empty scope that releases nothing on drop.
    /// Used as a default / placeholder when the caller does
    /// not need lease tracking (e.g., internal sub-planners).
    pub fn empty() -> Self {
        Self {
            descriptor_versions: Vec::new(),
            release: None,
            holder: None,
        }
    }

    /// Build a scope from exact descriptor-version holds already incremented
    /// on the node's `lease_refcount`, and register it in the node's holder
    /// table under the connection running on this task. Only cloneable
    /// capabilities are retained, so the scope neither owns nor
    /// weak-references `SharedState`.
    ///
    /// Fails with a retryable error when the holder table is full. The
    /// caller still owns the refcounts and rolls them back.
    pub fn new(
        descriptor_versions: Vec<(DescriptorId, u64)>,
        shared: &SharedState,
    ) -> Result<Self, Error> {
        let holders = Arc::clone(&shared.lease_runtime.holders);
        let ticket = holders.register(
            &descriptor_versions,
            crate::control::server::shared::session::hold_owner::current(),
        )?;
        Ok(Self {
            descriptor_versions,
            release: Some(HoldRelease::for_state(shared)),
            holder: Some((holders, ticket)),
        })
    }

    /// The retryable error if this node lost a lease the scope holds.
    pub fn check_not_revoked(&self) -> Result<(), Error> {
        match self
            .holder
            .as_ref()
            .and_then(|(_, t)| t.revocation().revoked_error())
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Run `fut` while the scope holds its leases. Ends early with
    /// [`Error::RetryableSchemaChanged`] once this node loses one of them;
    /// dropping `fut` drops its pending Data Plane requests.
    pub async fn guard<F: std::future::Future>(&self, fut: F) -> Result<F::Output, Error> {
        let Some((_, ticket)) = self.holder.as_ref() else {
            return Ok(fut.await);
        };
        let revocation = Arc::clone(ticket.revocation());
        tokio::select! {
            biased;
            error = revocation.revoked() => Err(error),
            output = fut => Ok(output),
        }
    }

    /// The exact `(descriptor, version)` holds this scope carries.
    ///
    /// A lease grant never compares the requested version against the
    /// catalog, so holding a scope proves only that this node reserved those
    /// versions — not that the catalog still agrees with them. Callers that
    /// replay a plan later re-compare these pairs against the catalog.
    pub fn descriptor_versions(&self) -> &[(DescriptorId, u64)] {
        &self.descriptor_versions
    }

    /// Number of descriptors held in this scope.
    pub fn len(&self) -> usize {
        self.descriptor_versions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.descriptor_versions.is_empty()
    }
}

impl Drop for QueryLeaseScope {
    fn drop(&mut self) {
        if let Some((holders, ticket)) = self.holder.take() {
            holders.deregister(&ticket);
        }
        if self.descriptor_versions.is_empty() {
            return;
        }
        let Some(release) = self.release.take() else {
            return;
        };
        // The lease stays granted at refcount 0, so the next statement on the
        // descriptor reuses it with no Raft round trip. A drain start releases
        // it on this node, a drain that covers it releases it when this last
        // hold ends, and the renewal loop lets it lapse at expiry.
        release.give_back(self.descriptor_versions.drain(..));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use nodedb_cluster::DescriptorKind;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::lease::DEFAULT_LEASE_DURATION;
    use crate::wal::WalManager;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[test]
    fn first_increment_returns_one() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        assert_eq!(rc.increment(&a, 1), 1);
    }

    #[test]
    fn second_increment_returns_two() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        assert_eq!(rc.increment(&a, 1), 2);
    }

    #[test]
    fn decrement_to_zero_removes_entry() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        assert_eq!(rc.decrement(&a, 1), 0);
        assert_eq!(rc.current(&a), 0);
        assert_eq!(rc.distinct_count(), 0);
    }

    #[test]
    fn decrement_preserves_shared_lease() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        rc.increment(&a, 1);
        assert_eq!(rc.decrement(&a, 1), 1);
        assert_eq!(rc.current(&a), 1);
        assert_eq!(rc.distinct_count(), 1);
    }

    #[test]
    fn distinct_descriptors_track_independently() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        let b = id("b");
        rc.increment(&a, 1);
        rc.increment(&b, 1);
        assert_eq!(rc.distinct_count(), 2);
        rc.decrement(&a, 1);
        assert_eq!(rc.distinct_count(), 1);
        assert_eq!(rc.current(&a), 0);
        assert_eq!(rc.current(&b), 1);
    }

    #[test]
    fn decrement_on_unknown_id_is_safe() {
        let rc = LeaseRefCount::new();
        assert_eq!(rc.decrement(&id("nothing"), 1), 0);
    }

    #[test]
    fn exact_version_decrement_preserves_other_version() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a, 1);
        rc.increment(&a, 2);

        assert_eq!(rc.decrement(&a, 2), 0);
        assert_eq!(rc.current(&a), 1);
        assert_eq!(rc.current_at_or_below(&a, 1), 1);
        assert_eq!(rc.current_at_or_below(&a, 2), 1);
    }

    #[test]
    fn empty_scope_drops_cleanly() {
        let scope = QueryLeaseScope::empty();
        drop(scope); // does not panic even without a runtime
    }

    #[test]
    fn dropping_the_last_holder_keeps_the_lease_granted() {
        let (state, descriptor, scope, _directory) = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build temporary runtime");
            let values = runtime.block_on(async {
                let directory = tempfile::tempdir().expect("create lease release test directory");
                let wal = Arc::new(
                    WalManager::open_for_testing(&directory.path().join("lease-release.wal"))
                        .expect("open lease release test WAL"),
                );
                let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
                let state = crate::control::state::SharedState::new(dispatcher, wal)
                    .expect("construct lease release state");
                let descriptor = id("no-runtime-drop");
                state.lease_refcount.increment(&descriptor, 1);
                // The grant as the metadata applier installs it. This test
                // covers the holder count, not the proposal.
                let expires_at = nodedb_types::Hlc::new(
                    state.hlc_clock.peek().wall_ns + DEFAULT_LEASE_DURATION.as_nanos() as u64,
                    0,
                );
                state
                    .metadata_cache
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .leases
                    .insert(
                        (descriptor.clone(), state.node_id),
                        nodedb_cluster::DescriptorLease {
                            descriptor_id: descriptor.clone(),
                            version: 1,
                            node_id: state.node_id,
                            expires_at,
                        },
                    );
                let scope = QueryLeaseScope::new(vec![(descriptor.clone(), 1)], &state)
                    .expect("register the scope's holder");

                (state, descriptor, scope, directory)
            });
            drop(runtime);
            values
        };
        assert!(tokio::runtime::Handle::try_current().is_err());

        drop(scope);

        assert_eq!(state.lease_refcount.current(&descriptor), 0);
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            state.lookup_lease_for_self(&descriptor).is_some(),
            "an idle lease stays granted until a drain or its expiry"
        );
    }
}
