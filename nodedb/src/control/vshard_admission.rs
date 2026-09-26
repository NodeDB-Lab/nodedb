// SPDX-License-Identifier: BUSL-1.1

//! Bounded, cancellation-safe serialization for Control-Plane vShard admission.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};

use crate::control::state::SharedState;
use crate::control::wal_replication::AsyncRaftProposer;
use crate::types::VShardId;

/// Maximum active plus waiting admissions for one vShard.
pub const VSHARD_ADMISSION_CAPACITY: usize = 64;

struct VShardAdmissionSlot {
    active: Mutex<()>,
    capacity: Arc<Semaphore>,
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
                active: Mutex::new(()),
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
        let slot = self.slot(vshard_id)?;
        let _queued = Arc::clone(&slot.capacity)
            .try_acquire_owned()
            .map_err(|_| crate::Error::VShardAdmissionCapacityExceeded {
                vshard_id,
                capacity: self.capacity,
            })?;
        let _active = slot.active.lock().await;
        operation().await
    }
}

impl Default for VShardAdmissionSequencer {
    fn default() -> Self {
        Self::new()
    }
}

/// Install raw and admission-sequenced proposal handles in one atomic set.
pub(crate) fn install_async_raft_proposer(
    shared: &SharedState,
    raw: Arc<AsyncRaftProposer>,
) -> crate::Result<()> {
    let sequenced = wrap_async_raft_proposer(
        Arc::clone(&shared.vshard_admission_sequencer),
        Arc::clone(&raw),
    );
    let expected_raw = Arc::clone(&raw);
    shared.install_async_raft_proposer_pair(sequenced, raw)?;
    let installed_raw = shared
        .raw_async_raft_proposer()
        .ok_or_else(|| crate::Error::Internal {
            detail: "async raft proposer pair missing immediately after installation".into(),
        })?;
    if !Arc::ptr_eq(installed_raw, &expected_raw) {
        return Err(crate::Error::Internal {
            detail: "async raft raw proposer identity changed during installation".into(),
        });
    }
    Ok(())
}

fn wrap_async_raft_proposer(
    sequencer: Arc<VShardAdmissionSequencer>,
    raw: Arc<AsyncRaftProposer>,
) -> Arc<AsyncRaftProposer> {
    Arc::new(move |vshard_id, idempotency_key, data, deadline| {
        let sequencer = Arc::clone(&sequencer);
        let raw = Arc::clone(&raw);
        Box::pin(async move {
            if vshard_id >= VShardId::COUNT {
                return Err(crate::Error::Internal {
                    detail: format!("async raft proposer received invalid vShard {vshard_id}"),
                });
            }
            let vshard_id = VShardId::new(vshard_id);
            sequencer
                .run(vshard_id, move || async move {
                    raw(vshard_id.as_u32(), idempotency_key, data, deadline).await
                })
                .await
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{Barrier, Notify};

    use super::*;
    use crate::types::Lsn;

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
                    Ok((data, Lsn::new(key)))
                })
            })
        };
        let wrapped = wrap_async_raft_proposer(Arc::clone(&sequencer), raw);
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
            (vec![1], Lsn::new(11))
        );
        entered.notified().await;
        release.notify_one();
        assert_eq!(
            second.await.expect("second joins").expect("second success"),
            (vec![2], Lsn::new(12))
        );
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }
}
