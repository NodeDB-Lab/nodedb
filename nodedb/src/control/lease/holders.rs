// SPDX-License-Identifier: BUSL-1.1

//! In-flight query holds per descriptor, so a lease this node loses can end
//! the queries still running under it.
//!
//! A query registers here when its [`super::QueryLeaseScope`] is built and
//! deregisters when the scope drops. Two events revoke holds:
//!
//! - A `DescriptorLeaseRelease` for this node applies here. Other nodes then
//!   treat the lease as gone and a DDL drain can pass.
//! - This node self-fences: it has lost metadata-leader contact long enough
//!   that other nodes can release its leases.
//!
//! A revoked query ends with [`Error::RetryableSchemaChanged`], so the client
//! retries it against the current descriptor version.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_cluster::DescriptorId;
use tokio::sync::watch;

use crate::control::state::SharedState;
use crate::error::Error;

/// Cap on query holds tracked at once on one node. An untracked hold
/// cannot be revoked, so admission past the cap fails with a retryable error.
const MAX_TRACKED_LEASE_HOLDS: usize = 65_536;

/// The revocation signal of one query's lease scope.
#[derive(Debug)]
pub struct LeaseRevocation {
    /// `Some(detail)` once revoked. Set once, never cleared.
    state: watch::Sender<Option<String>>,
}

impl LeaseRevocation {
    fn new() -> Self {
        let (state, _) = watch::channel(None);
        Self { state }
    }

    /// Mark the scope revoked because this node lost its lease on `descriptor`.
    /// A second revocation keeps the first reason.
    pub fn revoke(&self, descriptor: &DescriptorId) {
        self.state.send_if_modified(|state| {
            if state.is_some() {
                return false;
            }
            *state = Some(format!(
                "{descriptor:?} (this node lost its descriptor lease while the statement ran)"
            ));
            true
        });
    }

    /// The retryable error for a revoked scope, or `None` while it is live.
    pub fn revoked_error(&self) -> Option<Error> {
        self.state
            .borrow()
            .clone()
            .map(|descriptor| Error::RetryableSchemaChanged { descriptor })
    }

    /// Resolve with the retryable error once the scope is revoked.
    pub async fn revoked(&self) -> Error {
        let mut rx = self.state.subscribe();
        loop {
            if let Some(descriptor) = rx.borrow_and_update().clone() {
                return Error::RetryableSchemaChanged { descriptor };
            }
            if rx.changed().await.is_err() {
                // The sender lives as long as `self`, so this never resolves.
                std::future::pending::<()>().await;
            }
        }
    }
}

#[derive(Debug, Default)]
struct HoldersInner {
    next_key: u64,
    total: usize,
    by_descriptor: HashMap<DescriptorId, HashMap<u64, Arc<LeaseRevocation>>>,
    /// The exact `(descriptor, version)` holds of each ticket with an owner,
    /// by owner, then by ticket key.
    by_owner: HashMap<u64, HashMap<u64, Vec<(DescriptorId, u64)>>>,
}

/// Every in-flight query hold on this node, keyed by descriptor.
#[derive(Debug, Default)]
pub struct LeaseHolders {
    inner: Mutex<HoldersInner>,
}

/// One registered query hold. Pass it back to [`LeaseHolders::deregister`].
#[derive(Debug)]
pub struct HolderTicket {
    key: u64,
    descriptors: Vec<DescriptorId>,
    owner: Option<u64>,
    revocation: Arc<LeaseRevocation>,
}

impl HolderTicket {
    pub fn revocation(&self) -> &Arc<LeaseRevocation> {
        &self.revocation
    }
}

impl LeaseHolders {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one query holding the exact `(descriptor, version)` pairs in
    /// `holds`. `owner` is the connection the query runs on, `None` outside
    /// a connection (see `session::hold_owner`). Fails with a retryable
    /// error when the node already tracks [`MAX_TRACKED_LEASE_HOLDS`] holds.
    pub fn register(
        &self,
        holds: &[(DescriptorId, u64)],
        owner: Option<u64>,
    ) -> Result<HolderTicket, Error> {
        let mut seen = std::collections::HashSet::with_capacity(holds.len());
        let descriptors: Vec<DescriptorId> = holds
            .iter()
            .filter(|(id, _)| seen.insert(id))
            .map(|(id, _)| id.clone())
            .collect();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.total.saturating_add(descriptors.len()) > MAX_TRACKED_LEASE_HOLDS {
            return Err(Error::RetryableSchemaChanged {
                descriptor: format!(
                    "{descriptors:?} (descriptor lease holder table full at \
                     {MAX_TRACKED_LEASE_HOLDS} holds; retry once statements finish)"
                ),
            });
        }
        let key = inner.next_key;
        inner.next_key = inner.next_key.wrapping_add(1);
        let revocation = Arc::new(LeaseRevocation::new());
        for descriptor in &descriptors {
            inner
                .by_descriptor
                .entry(descriptor.clone())
                .or_default()
                .insert(key, Arc::clone(&revocation));
        }
        inner.total = inner.total.saturating_add(descriptors.len());
        if let Some(owner) = owner {
            inner
                .by_owner
                .entry(owner)
                .or_default()
                .insert(key, holds.to_vec());
        }
        Ok(HolderTicket {
            key,
            descriptors,
            owner,
            revocation,
        })
    }

    /// Remove a hold at query end.
    pub fn deregister(&self, ticket: &HolderTicket) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut removed = 0usize;
        for descriptor in &ticket.descriptors {
            if let Some(holders) = inner.by_descriptor.get_mut(descriptor) {
                if holders.remove(&ticket.key).is_some() {
                    removed += 1;
                }
                if holders.is_empty() {
                    inner.by_descriptor.remove(descriptor);
                }
            }
        }
        inner.total = inner.total.saturating_sub(removed);
        if let Some(owner) = ticket.owner
            && let Some(tickets) = inner.by_owner.get_mut(&owner)
        {
            tickets.remove(&ticket.key);
            if tickets.is_empty() {
                inner.by_owner.remove(&owner);
            }
        }
    }

    /// Whether a hold that `owner` keeps is at a version an active drain in
    /// `drains` covers. Such a drain waits for that hold, so a statement on
    /// the same connection cannot wait for the drain to end.
    pub fn owner_blocks_drain(&self, owner: u64, drains: &super::DescriptorDrainTracker) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.by_owner.get(&owner).is_some_and(|tickets| {
            tickets
                .values()
                .flatten()
                .any(|(id, version)| drains.is_draining(id, *version))
        })
    }

    /// Revoke every hold on any of `descriptors`. Returns how many queries
    /// were revoked.
    pub fn revoke(&self, descriptors: &[DescriptorId]) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut revoked = 0usize;
        for descriptor in descriptors {
            if let Some(holders) = inner.by_descriptor.get(descriptor) {
                for revocation in holders.values() {
                    revocation.revoke(descriptor);
                    revoked += 1;
                }
            }
        }
        revoked
    }

    /// Revoke every hold on this node. Returns how many holds were revoked.
    pub fn revoke_all(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut revoked = 0usize;
        for (descriptor, holders) in &inner.by_descriptor {
            for revocation in holders.values() {
                revocation.revoke(descriptor);
                revoked += 1;
            }
        }
        revoked
    }

    /// Number of tracked holds.
    pub fn tracked(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).total
    }
}

/// Revoke this node's queries on `descriptor_ids` when a committed
/// `DescriptorLeaseRelease` for `released_node` applies here.
pub fn revoke_on_release(
    shared: &SharedState,
    released_node: u64,
    descriptor_ids: &[DescriptorId],
) {
    if released_node != shared.node_id {
        return;
    }
    let revoked = shared.lease_runtime.holders.revoke(descriptor_ids);
    if revoked > 0 {
        tracing::info!(
            revoked,
            descriptors = descriptor_ids.len(),
            "descriptor lease released while in use; revoked in-flight statements"
        );
    }
}

/// Revoke every in-flight query on this node once it has gone
/// [`nodedb_cluster::LEASE_SELF_FENCE_WINDOW`] without metadata-leader
/// contact. Called periodically by the lease renewal loop.
///
/// Only lost contact counts here. A replica merely behind on apply refuses
/// its cached lease for new statements, but its running ones stay valid:
/// other nodes release a lease only after Raft silence, not apply lag. Before
/// `start_raft` installs the contact check, nothing is revoked.
pub fn revoke_if_fenced(shared: &SharedState) {
    let Some(in_contact) = shared.lease_runtime.metadata_contact.get() else {
        return;
    };
    if in_contact(nodedb_cluster::LEASE_SELF_FENCE_WINDOW) {
        return;
    }
    let revoked = shared.lease_runtime.holders.revoke_all();
    if revoked > 0 {
        tracing::warn!(
            revoked,
            "metadata-leader contact lost past the lease self-fence window; \
             revoked in-flight statements"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_cluster::DescriptorKind;

    use super::*;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[test]
    fn revoke_reaches_only_holders_of_that_descriptor() {
        let holders = LeaseHolders::new();
        let orders = holders
            .register(&[(id("orders"), 1)], None)
            .expect("register");
        let users = holders
            .register(&[(id("users"), 1)], None)
            .expect("register");

        assert_eq!(holders.revoke(&[id("orders")]), 1);
        assert!(matches!(
            orders.revocation().revoked_error(),
            Some(Error::RetryableSchemaChanged { .. })
        ));
        assert!(users.revocation().revoked_error().is_none());
    }

    #[test]
    fn deregistered_holds_are_not_revoked() {
        let holders = LeaseHolders::new();
        let ticket = holders
            .register(&[(id("orders"), 1), (id("users"), 1)], None)
            .expect("register");
        assert_eq!(holders.tracked(), 2);
        holders.deregister(&ticket);
        assert_eq!(holders.tracked(), 0);
        assert_eq!(holders.revoke_all(), 0);
    }

    /// Only a hold its owner keeps at a drained version blocks that owner,
    /// and a deregistered hold blocks nothing.
    #[test]
    fn owner_blocks_drain_only_for_its_own_drained_holds() {
        use nodedb_cluster::DrainOwner;

        let holders = LeaseHolders::new();
        let drains = super::super::DescriptorDrainTracker::new();
        let mine = holders
            .register(&[(id("orders"), 1)], Some(7))
            .expect("register");
        let _other = holders
            .register(&[(id("users"), 1)], Some(8))
            .expect("register");
        assert!(!holders.owner_blocks_drain(7, &drains), "no drain yet");

        drains.install_start(id("orders"), DrainOwner::Ddl, 1, nodedb_types::Hlc::ZERO, 1);
        assert!(holders.owner_blocks_drain(7, &drains));
        assert!(
            !holders.owner_blocks_drain(8, &drains),
            "8 holds users only"
        );
        assert!(!holders.owner_blocks_drain(9, &drains), "9 holds nothing");

        holders.deregister(&mine);
        assert!(!holders.owner_blocks_drain(7, &drains));
    }

    /// A hold at a version past the drain's range does not block its owner.
    #[test]
    fn a_newer_hold_does_not_block_an_older_drain() {
        use nodedb_cluster::DrainOwner;

        let holders = LeaseHolders::new();
        let drains = super::super::DescriptorDrainTracker::new();
        let _ticket = holders
            .register(&[(id("orders"), 2)], Some(7))
            .expect("register");
        drains.install_start(id("orders"), DrainOwner::Ddl, 1, nodedb_types::Hlc::ZERO, 1);
        assert!(!holders.owner_blocks_drain(7, &drains));
    }

    #[test]
    fn registration_is_bounded() {
        let holders = LeaseHolders::new();
        let many: Vec<(DescriptorId, u64)> = (0..MAX_TRACKED_LEASE_HOLDS)
            .map(|i| (id(&format!("c{i}")), 1))
            .collect();
        let _full = holders.register(&many, None).expect("fill to the cap");
        assert!(matches!(
            holders.register(&[(id("one_more"), 1)], None),
            Err(Error::RetryableSchemaChanged { .. })
        ));
    }

    #[tokio::test]
    async fn revoked_future_resolves_on_revoke() {
        let holders = Arc::new(LeaseHolders::new());
        let ticket = holders
            .register(&[(id("orders"), 1)], None)
            .expect("register");
        let revocation = Arc::clone(ticket.revocation());
        let waiter = tokio::spawn(async move { revocation.revoked().await });
        tokio::task::yield_now().await;
        holders.revoke(&[id("orders")]);
        let error = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("revocation observed promptly")
            .expect("waiter did not panic");
        assert!(matches!(error, Error::RetryableSchemaChanged { .. }));
    }

    /// A long-running statement under a lease is cancelled with a retryable
    /// error once a release of that lease for this node applies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn long_running_query_is_cancelled_when_its_lease_is_released() {
        use crate::control::planner::descriptor_set::DescriptorVersionSet;

        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let descriptor = id("orders");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        let scope = state
            .acquire_plan_lease_scope(&versions)
            .await
            .expect("admit the statement");
        assert_eq!(state.lease_runtime.holders.tracked(), 1);

        let running = tokio::spawn(async move {
            scope
                .guard(tokio::time::sleep(Duration::from_secs(600)))
                .await
        });
        tokio::task::yield_now().await;

        revoke_on_release(&state, state.node_id + 1, std::slice::from_ref(&descriptor));
        assert!(
            !running.is_finished(),
            "another node's release must not cancel"
        );

        revoke_on_release(&state, state.node_id, std::slice::from_ref(&descriptor));
        let outcome = tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .expect("the statement ends promptly")
            .expect("the statement task did not panic");
        assert!(matches!(outcome, Err(Error::RetryableSchemaChanged { .. })));
        assert_eq!(
            state.lease_runtime.holders.tracked(),
            0,
            "the scope deregistered on drop"
        );
        drop(state);
        cluster.shutdown().await;
    }

    /// Lost metadata-leader contact revokes every running statement; contact
    /// within the window revokes none.
    #[tokio::test]
    async fn lost_leader_contact_revokes_running_statements() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let directory = tempfile::tempdir().expect("create holder test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("fence.wal"))
                .expect("open holder test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct holder test state");
        let in_contact = Arc::new(AtomicBool::new(true));
        let probe = Arc::clone(&in_contact);
        let contact: Arc<dyn Fn(Duration) -> bool + Send + Sync> =
            Arc::new(move |_window| probe.load(Ordering::SeqCst));
        if state.lease_runtime.metadata_contact.set(contact).is_err() {
            panic!("metadata contact fn already set in a fresh test state");
        }
        let ticket = state
            .lease_runtime
            .holders
            .register(&[(id("orders"), 1)], None)
            .expect("register");

        revoke_if_fenced(&state);
        assert!(ticket.revocation().revoked_error().is_none());

        in_contact.store(false, Ordering::SeqCst);
        revoke_if_fenced(&state);
        assert!(ticket.revocation().revoked_error().is_some());
        state.lease_runtime.holders.deregister(&ticket);
    }

    /// A statement boundary after revocation fails, and a guard refuses even
    /// work that is already complete: nothing runs on a revoked scope.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revoked_scope_fails_boundary_checks_and_guards() {
        use crate::control::planner::descriptor_set::DescriptorVersionSet;

        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let descriptor = id("orders");
        let mut versions = DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        let scope = state
            .acquire_plan_lease_scope(&versions)
            .await
            .expect("admit the statement");
        assert!(scope.check_not_revoked().is_ok());
        assert!(matches!(scope.guard(async { 7 }).await, Ok(7)));

        revoke_on_release(&state, state.node_id, std::slice::from_ref(&descriptor));
        assert!(matches!(
            scope.check_not_revoked(),
            Err(Error::RetryableSchemaChanged { .. })
        ));
        assert!(matches!(
            scope.guard(async { 7 }).await,
            Err(Error::RetryableSchemaChanged { .. })
        ));
        drop(scope);
        drop(state);
        cluster.shutdown().await;
    }
}
