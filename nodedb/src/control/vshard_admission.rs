// SPDX-License-Identifier: BUSL-1.1

//! Bounded, cancellation-safe serialization for Control-Plane vShard admission.
//!
//! Each vShard has one slot. A write holds it while it proposes: until the
//! group's leader holds the entry in its log. The slot records where the
//! entry landed, and the write waits for this node's apply after it lets the
//! slot go. So the writes of one vShard enter the log in admission order,
//! and several of them wait for their applies at once. A queue place, held
//! from admission through the apply, bounds the vShard's writes in flight.
//!
//! A CRDT admission holds the slot across its preview and its fenced apply.
//! Before it previews, it waits for this node's apply through the last entry
//! the slot recorded and through the group's commit index, so the preview
//! reads every earlier admitted write.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::control::state::SharedState;
use crate::control::wal_replication::{
    AsyncRaftProposer, AsyncRaftSubmit, ProposedAt, ProposedWrite,
};
use crate::types::VShardId;

/// Maximum active plus waiting admissions for one vShard.
pub const VSHARD_ADMISSION_CAPACITY: usize = 64;

struct VShardAdmissionSlot {
    active: Mutex<SlotState>,
    capacity: Arc<Semaphore>,
}

/// What a vShard's slot remembers across admissions.
#[derive(Debug, Default)]
struct SlotState {
    /// Where the last write proposed through the slot landed.
    last_proposed: Option<ProposedAt>,
}

/// Serializes admission work independently for every valid vShard.
///
/// Capacity is acquired before waiting for the fair Tokio mutex. Both the
/// owned semaphore permit and mutex guard are held only by the returned future,
/// so cancellation, error, and unwinding release them through RAII.
pub struct VShardAdmissionSequencer {
    slots: Vec<VShardAdmissionSlot>,
    capacity: usize,
}

impl VShardAdmissionSequencer {
    /// Build the production sequencer with the configured admission bound.
    pub fn new() -> Self {
        Self::with_capacity(VSHARD_ADMISSION_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        let slots = (0..VShardId::COUNT)
            .map(|_| VShardAdmissionSlot {
                active: Mutex::new(SlotState::default()),
                capacity: Arc::new(Semaphore::new(capacity)),
            })
            .collect();
        Self { slots, capacity }
    }

    fn slot(&self, vshard_id: VShardId) -> crate::Result<&VShardAdmissionSlot> {
        let index = usize::try_from(vshard_id.as_u32()).map_err(|_| crate::Error::Internal {
            detail: format!("vShard admission index does not fit usize: {vshard_id}"),
        })?;
        self.slots.get(index).ok_or_else(|| crate::Error::Internal {
            detail: format!("vShard admission index out of range: {vshard_id}"),
        })
    }

    /// Run one admission operation after bounded, per-vShard serialization.
    ///
    /// `operation` is a factory so its future is not created before the active
    /// slot has been acquired. The Tokio mutex's FIFO fairness preserves start
    /// order among admitted waiters for a single vShard.
    pub async fn run<T, F, Fut>(&self, vshard_id: VShardId, operation: F) -> crate::Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = crate::Result<T>>,
    {
        self.run_after_proposed(vshard_id, |_| operation()).await
    }

    /// Run one admission operation like [`Self::run`], handing it where the
    /// last write proposed through the slot landed. An operation that reads
    /// local state waits for this node's apply through it first.
    pub async fn run_after_proposed<T, F, Fut>(
        &self,
        vshard_id: VShardId,
        operation: F,
    ) -> crate::Result<T>
    where
        F: FnOnce(Option<ProposedAt>) -> Fut,
        Fut: Future<Output = crate::Result<T>>,
    {
        let slot = self.slot(vshard_id)?;
        let _queued = self.reserve(slot, vshard_id)?;
        let active = slot.active.lock().await;
        operation(active.last_proposed).await
    }

    /// Propose one write through the vShard's slot, waiting in the queue
    /// only until `deadline`.
    ///
    /// The slot is held across `submit` only: until the leader holds the
    /// entry. The slot records where it landed. The returned queue place
    /// stays taken until the caller drops it after the apply, so the
    /// vShard's writes in flight stay bounded.
    ///
    /// A queued write still waiting at `deadline` leaves the queue and
    /// returns [`crate::Error::DeadlineExceeded`]. Its `submit` is never
    /// called. Waiters behind it keep their order. `timeout_at` polls the
    /// lock before the timer, so a lock granted in the same poll the deadline
    /// fires wins, and a lock future dropped on timeout hands its turn to the
    /// next waiter. So each write either proposes once or leaves unproposed.
    pub async fn propose_until<F, Fut>(
        &self,
        vshard_id: VShardId,
        deadline: tokio::time::Instant,
        submit: F,
    ) -> crate::Result<(ProposedWrite, OwnedSemaphorePermit)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = crate::Result<ProposedWrite>>,
    {
        let slot = self.slot(vshard_id)?;
        let queued = self.reserve(slot, vshard_id)?;
        let mut active = tokio::time::timeout_at(deadline, slot.active.lock())
            .await
            .map_err(|_| crate::Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(0),
            })?;
        let proposed = submit().await?;
        if proposed.at.is_some() {
            active.last_proposed = proposed.at;
        }
        Ok((proposed, queued))
    }

    /// Take one of the vShard's queue places, or fail at once when all are
    /// taken.
    fn reserve(
        &self,
        slot: &VShardAdmissionSlot,
        vshard_id: VShardId,
    ) -> crate::Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&slot.capacity).try_acquire_owned().map_err(|_| {
            crate::Error::VShardAdmissionCapacityExceeded {
                vshard_id,
                capacity: self.capacity,
            }
        })
    }
}

impl Default for VShardAdmissionSequencer {
    fn default() -> Self {
        Self::new()
    }
}

/// Install raw and admission-sequenced proposal handles, both built from
/// `submit`, in one atomic set.
pub(crate) fn install_async_raft_proposer(
    shared: &SharedState,
    submit: Arc<AsyncRaftSubmit>,
) -> crate::Result<()> {
    let sequenced = wrap_async_raft_proposer(
        Arc::clone(&shared.vshard_admission_sequencer),
        Arc::clone(&submit),
    );
    let raw = raw_async_raft_proposer(submit);
    let expected_raw = Arc::clone(&raw);
    shared.install_async_raft_proposer_pair(sequenced, raw)?;
    let installed_raw = shared.raw_async_raft_proposer()?;
    if !Arc::ptr_eq(installed_raw, &expected_raw) {
        return Err(crate::Error::Internal {
            detail: "async raft raw proposer identity changed during installation".into(),
        });
    }
    Ok(())
}

/// A submit whose proposal is applied before it returns: `proposer` answers
/// only at apply, as a test double does. The whole call is the propose phase.
#[cfg(test)]
pub(crate) fn applying_submit(proposer: Arc<AsyncRaftProposer>) -> Arc<AsyncRaftSubmit> {
    Arc::new(move |vshard_id, idempotency_key, data, deadline| {
        let proposer = Arc::clone(&proposer);
        Box::pin(async move {
            let applied = proposer(vshard_id, idempotency_key, data, deadline).await;
            Ok(ProposedWrite {
                at: None,
                applied: Box::pin(async move { applied }),
            })
        })
    })
}

/// The proposer a CRDT admission uses while it holds the vShard's slot:
/// propose, then wait for the apply, with no slot of its own.
fn raw_async_raft_proposer(submit: Arc<AsyncRaftSubmit>) -> Arc<AsyncRaftProposer> {
    Arc::new(move |vshard_id, idempotency_key, data, deadline| {
        let submit = Arc::clone(&submit);
        Box::pin(async move {
            let proposed = submit(vshard_id, idempotency_key, data, deadline).await?;
            proposed.applied.await
        })
    })
}

fn wrap_async_raft_proposer(
    sequencer: Arc<VShardAdmissionSequencer>,
    submit: Arc<AsyncRaftSubmit>,
) -> Arc<AsyncRaftProposer> {
    Arc::new(move |vshard_id, idempotency_key, data, deadline| {
        let sequencer = Arc::clone(&sequencer);
        let submit = Arc::clone(&submit);
        Box::pin(async move {
            if vshard_id >= VShardId::COUNT {
                return Err(crate::Error::Internal {
                    detail: format!("async raft proposer received invalid vShard {vshard_id}"),
                });
            }
            let vshard_id = VShardId::new(vshard_id);
            // The queue wait ends at the caller's deadline. A proposal that
            // leaves the queue then never reaches `submit`.
            let (proposed, _queued) = sequencer
                .propose_until(vshard_id, deadline, move || {
                    submit(vshard_id.as_u32(), idempotency_key, data, deadline)
                })
                .await?;
            // The slot is free again: the next write of the vShard proposes
            // while this one waits for its apply.
            proposed.applied.await
        })
    })
}

/// Wait until this node applied every write admitted to `vshard_id` before
/// the caller took its slot: through `last_proposed`, and through the commit
/// index this node holds for the vShard's group. A node with no routing
/// table runs no data group, and only `last_proposed` binds it.
pub(crate) async fn await_admitted_applies(
    state: &SharedState,
    vshard_id: VShardId,
    last_proposed: Option<ProposedAt>,
    deadline: std::time::Instant,
) -> crate::Result<()> {
    let group = if state.cluster_routing.is_some() {
        Some(
            crate::control::security::auth_fence::cluster::group_of_vshard(
                state,
                vshard_id.as_u32(),
            )?,
        )
    } else {
        None
    };
    let committed = group.and_then(|group_id| {
        state.raft_status_fn.get().and_then(|status| {
            status()
                .into_iter()
                .find(|g| g.group_id == group_id)
                .map(|g| ProposedAt {
                    group_id,
                    log_index: g.commit_index,
                })
        })
    });
    // A group this node no longer hosts applies nothing here to wait for:
    // the admission's fenced apply refuses a preview that missed a write.
    let hosted = |at: &ProposedAt| {
        group.is_none()
            || crate::control::security::auth_fence::cluster::hosts_group(state, at.group_id)
    };
    for at in [last_proposed, committed]
        .into_iter()
        .flatten()
        .filter(hosted)
    {
        crate::control::cluster::linearizable_read::wait_applied_through(
            state,
            at.group_id,
            at.log_index,
            deadline,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{Barrier, Notify};

    use super::*;
    use crate::types::ReadVersions;

    /// The versions a test proposer answers proposal `key` with.
    fn applied_at(key: u64) -> ReadVersions {
        ReadVersions::single(VShardId::new(0), nodedb_types::WriteVersion::logged(0, key))
    }

    fn test_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(30)
    }

    fn shard(id: u32) -> VShardId {
        VShardId::new(id)
    }

    #[tokio::test]
    async fn same_vshard_is_serial_and_starts_in_fifo_order() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(4));
        let started = Arc::new(Mutex::new(Vec::new()));
        let release_first = Arc::new(Notify::new());
        let first_started = Arc::new(Notify::new());

        let first = {
            let sequencer = Arc::clone(&sequencer);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release_first);
            let entered = Arc::clone(&first_started);
            tokio::spawn(async move {
                sequencer
                    .run(shard(7), move || async move {
                        started.lock().await.push(1);
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                    .await
            })
        };
        first_started.notified().await;

        let second = {
            let sequencer = Arc::clone(&sequencer);
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                sequencer
                    .run(shard(7), move || async move {
                        started.lock().await.push(2);
                        Ok(())
                    })
                    .await
            })
        };
        while sequencer.slots[7].capacity.available_permits() != 2 {
            tokio::task::yield_now().await;
        }
        let third = {
            let sequencer = Arc::clone(&sequencer);
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                sequencer
                    .run(shard(7), move || async move {
                        started.lock().await.push(3);
                        Ok(())
                    })
                    .await
            })
        };
        while sequencer.slots[7].capacity.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
        release_first.notify_one();
        first
            .await
            .expect("first task joins")
            .expect("first succeeds");
        second
            .await
            .expect("second task joins")
            .expect("second succeeds");
        third
            .await
            .expect("third task joins")
            .expect("third succeeds");
        assert_eq!(*started.lock().await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn different_vshards_overlap() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(2));
        let barrier = Arc::new(Barrier::new(2));
        let first = {
            let sequencer = Arc::clone(&sequencer);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                sequencer
                    .run(shard(1), move || async move {
                        barrier.wait().await;
                        Ok(())
                    })
                    .await
            })
        };
        let second = {
            let sequencer = Arc::clone(&sequencer);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                sequencer
                    .run(shard(2), move || async move {
                        barrier.wait().await;
                        Ok(())
                    })
                    .await
            })
        };
        first.await.expect("first joins").expect("first succeeds");
        second
            .await
            .expect("second joins")
            .expect("second succeeds");
    }

    #[tokio::test]
    async fn overflow_is_typed_and_immediate() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(1));
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let held = {
            let sequencer = Arc::clone(&sequencer);
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            tokio::spawn(async move {
                sequencer
                    .run(shard(3), move || async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                    .await
            })
        };
        entered.notified().await;
        let result = sequencer.run(shard(3), || async { Ok(()) }).await;
        assert!(matches!(
            result,
            Err(crate::Error::VShardAdmissionCapacityExceeded {
                vshard_id,
                capacity: 1
            }) if vshard_id == shard(3)
        ));
        release.notify_one();
        held.await
            .expect("held task joins")
            .expect("held task succeeds");
    }

    #[tokio::test]
    async fn abort_and_error_release_the_admission_slot() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(1));
        let entered = Arc::new(Notify::new());
        let blocked = {
            let sequencer = Arc::clone(&sequencer);
            let entered = Arc::clone(&entered);
            tokio::spawn(async move {
                sequencer
                    .run(shard(4), move || async move {
                        entered.notify_one();
                        std::future::pending::<crate::Result<()>>().await
                    })
                    .await
            })
        };
        entered.notified().await;
        blocked.abort();
        let _ = blocked.await;
        sequencer
            .run(shard(4), || async { Ok(()) })
            .await
            .expect("abort releases slot");

        let error = sequencer
            .run(shard(4), || async {
                Err::<(), _>(crate::Error::Internal {
                    detail: "expected test error".into(),
                })
            })
            .await;
        assert!(matches!(error, Err(crate::Error::Internal { .. })));
        sequencer
            .run(shard(4), || async { Ok(()) })
            .await
            .expect("error releases slot");
    }

    #[tokio::test]
    async fn panic_releases_the_admission_slot() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(1));
        let panicking = {
            let sequencer = Arc::clone(&sequencer);
            tokio::spawn(async move {
                sequencer
                    .run::<(), _, _>(shard(4), || async {
                        panic!("expected admission callback panic")
                    })
                    .await
            })
        };
        assert!(panicking.await.expect_err("task must panic").is_panic());
        sequencer
            .run(shard(4), || async { Ok(()) })
            .await
            .expect("panic releases the admission slot");
    }

    #[tokio::test]
    async fn wrapped_callback_serializes_the_unchanged_raw_proposer() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(2));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let raw: Arc<AsyncRaftProposer> = {
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            Arc::new(move |_vshard, key, data, _deadline| {
                let active = Arc::clone(&active);
                let maximum = Arc::clone(&maximum);
                let release = Arc::clone(&release);
                let entered = Arc::clone(&entered);
                Box::pin(async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok((data, applied_at(key)))
                })
            })
        };
        let wrapped = wrap_async_raft_proposer(Arc::clone(&sequencer), applying_submit(raw));
        let first = {
            let wrapped = Arc::clone(&wrapped);
            tokio::spawn(async move { wrapped(5, 11, vec![1], test_deadline()).await })
        };
        entered.notified().await;
        let second = {
            let wrapped = Arc::clone(&wrapped);
            tokio::spawn(async move { wrapped(5, 12, vec![2], test_deadline()).await })
        };
        while sequencer.slots[5].capacity.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        release.notify_one();
        assert_eq!(
            first.await.expect("first joins").expect("first success"),
            (vec![1], applied_at(11))
        );
        entered.notified().await;
        release.notify_one();
        assert_eq!(
            second.await.expect("second joins").expect("second success"),
            (vec![2], applied_at(12))
        );
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    /// Records the key of every proposal that reaches the raw proposer.
    fn recording_proposer() -> (Arc<AsyncRaftProposer>, Arc<std::sync::Mutex<Vec<u64>>>) {
        let proposed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let raw: Arc<AsyncRaftProposer> = {
            let proposed = Arc::clone(&proposed);
            Arc::new(move |_vshard, key, data, _deadline| {
                let proposed = Arc::clone(&proposed);
                Box::pin(async move {
                    proposed.lock().expect("proposed log").push(key);
                    Ok((data, applied_at(key)))
                })
            })
        };
        (raw, proposed)
    }

    /// Wait until `taken` places of `vshard`'s queue are held.
    async fn until_queued(sequencer: &VShardAdmissionSequencer, vshard: usize, taken: usize) {
        while sequencer.capacity - sequencer.slots[vshard].capacity.available_permits() != taken {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_proposal_past_its_deadline_leaves_the_queue_and_never_proposes() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(8));
        let (raw, proposed) = recording_proposer();
        let wrapped = wrap_async_raft_proposer(Arc::clone(&sequencer), applying_submit(raw));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let holder = {
            let sequencer = Arc::clone(&sequencer);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                sequencer
                    .run(shard(6), move || async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                    .await
            })
        };
        entered.notified().await;

        let start = tokio::time::Instant::now();
        let expiring = tokio::spawn(wrapped(
            6,
            21,
            vec![21],
            start + std::time::Duration::from_secs(1),
        ));
        until_queued(&sequencer, 6, 2).await;
        let second = tokio::spawn(wrapped(
            6,
            22,
            vec![22],
            start + std::time::Duration::from_secs(60),
        ));
        until_queued(&sequencer, 6, 3).await;
        let third = tokio::spawn(wrapped(
            6,
            23,
            vec![23],
            start + std::time::Duration::from_secs(60),
        ));
        until_queued(&sequencer, 6, 4).await;

        let expired = expiring.await.expect("expiring joins");
        assert!(
            matches!(expired, Err(crate::Error::DeadlineExceeded { .. })),
            "a proposal still queued at its deadline fails with the deadline error, got {expired:?}"
        );
        assert!(
            proposed.lock().expect("proposed log").is_empty(),
            "a proposal that left the queue never reaches the raw proposer"
        );
        until_queued(&sequencer, 6, 3).await;

        release.notify_one();
        holder
            .await
            .expect("holder joins")
            .expect("holder succeeds");
        assert_eq!(
            second
                .await
                .expect("second joins")
                .expect("second proposes"),
            (vec![22], applied_at(22))
        );
        assert_eq!(
            third.await.expect("third joins").expect("third proposes"),
            (vec![23], applied_at(23))
        );
        assert_eq!(
            *proposed.lock().expect("proposed log"),
            vec![22, 23],
            "the waiters behind the expired proposal propose in their queue order"
        );
    }

    /// A two-phase submit: records each proposal as it lands at
    /// `(group 1, index = key)`, and applies it only once `release` grants it
    /// a permit.
    fn two_phase_submit(
        proposed: Arc<std::sync::Mutex<Vec<u64>>>,
        release: Arc<Semaphore>,
    ) -> Arc<AsyncRaftSubmit> {
        Arc::new(move |_vshard, key, data, _deadline| {
            let proposed = Arc::clone(&proposed);
            let release = Arc::clone(&release);
            Box::pin(async move {
                proposed.lock().expect("proposed log").push(key);
                Ok(ProposedWrite {
                    at: Some(ProposedAt {
                        group_id: 1,
                        log_index: key,
                    }),
                    applied: Box::pin(async move {
                        release
                            .acquire()
                            .await
                            .map_err(|e| crate::Error::Internal {
                                detail: format!("test apply gate closed: {e}"),
                            })?
                            .forget();
                        Ok((data, applied_at(key)))
                    }),
                })
            })
        })
    }

    /// Two writes of one vShard are in flight at once: the second proposes
    /// while the first waits for its apply. The slot records where the last
    /// one landed.
    #[tokio::test]
    async fn two_writes_of_one_vshard_wait_for_their_applies_at_once() {
        let sequencer = Arc::new(VShardAdmissionSequencer::with_capacity(4));
        let proposed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let release = Arc::new(Semaphore::new(0));
        let wrapped = wrap_async_raft_proposer(
            Arc::clone(&sequencer),
            two_phase_submit(Arc::clone(&proposed), Arc::clone(&release)),
        );
        let first = tokio::spawn(wrapped(5, 11, vec![1], test_deadline()));
        let second = tokio::spawn(wrapped(5, 12, vec![2], test_deadline()));
        while proposed.lock().expect("proposed log").len() != 2 {
            tokio::task::yield_now().await;
        }
        assert!(!first.is_finished() && !second.is_finished());
        assert_eq!(
            sequencer.slots[5].capacity.available_permits(),
            2,
            "each write in flight keeps its queue place until its apply"
        );
        let last = sequencer
            .run_after_proposed(shard(5), |last| async move { Ok(last) })
            .await
            .expect("the slot is free while both writes wait for their applies");
        let order = proposed.lock().expect("proposed log").clone();
        assert_eq!(
            last,
            order.last().map(|&key| ProposedAt {
                group_id: 1,
                log_index: key,
            })
        );

        release.add_permits(2);
        let mut results = vec![
            first.await.expect("first joins").expect("first applies"),
            second.await.expect("second joins").expect("second applies"),
        ];
        results.sort_by_key(|(_, versions)| versions.of(VShardId::new(0)));
        assert_eq!(
            results,
            vec![(vec![1], applied_at(11)), (vec![2], applied_at(12))]
        );
        assert_eq!(sequencer.slots[5].capacity.available_permits(), 4);
    }

    /// An admission that reads local state waits for this node's apply
    /// through the last write the slot recorded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_admission_waits_for_the_last_admitted_apply() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&directory.path().join("admission.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");

        let at = ProposedAt {
            group_id: 9,
            log_index: 3,
        };
        let waiter = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                await_admitted_applies(
                    &state,
                    shard(2),
                    Some(at),
                    std::time::Instant::now() + std::time::Duration::from_secs(30),
                )
                .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "the entry is not applied here yet");
        state.applied_index_watcher(9).bump(3);
        waiter
            .await
            .expect("waiter joins")
            .expect("the admission proceeds once the entry applied");
    }
}
