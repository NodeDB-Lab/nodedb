// SPDX-License-Identifier: BUSL-1.1

//! A contended write no Calvin transaction can sequence: it waits for its
//! keys at the gate.
//!
//! The waiter holds no key while it waits. It probes the lock table all or
//! nothing, and a blocked probe watches the first key that blocks it. Only a
//! release of that key wakes the waiter for its next probe. It never holds
//! one key while it waits for another. A Calvin transaction therefore never
//! waits on a waiting writer, and the two cannot deadlock.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;

use crate::control::cluster::calvin::scheduler::lock_manager::{
    GateProbe, LockKey, LockManager, LockMode, TxnId,
};
use crate::types::RequestId;

use super::gate::WriteAdmissionGuard;
use super::tickets::AdmissionTicket;

/// The keys of a contended write, and the lock table it waits on.
pub struct AdmissionWait {
    lock_manager: Arc<Mutex<LockManager>>,
    txn: TxnId,
    keys: BTreeMap<LockKey, LockMode>,
    promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
    /// The waiting write's admission ticket. The guard carries it on.
    ticket: Option<AdmissionTicket>,
}

impl AdmissionWait {
    pub(super) fn new(
        lock_manager: Arc<Mutex<LockManager>>,
        txn: TxnId,
        keys: BTreeMap<LockKey, LockMode>,
        promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
    ) -> Self {
        Self {
            lock_manager,
            txn,
            keys,
            promotion_sender,
            ticket: None,
        }
    }

    /// Carry `ticket` until the wait ends, then on in its guard.
    pub(super) fn carry_ticket(&mut self, ticket: AdmissionTicket) {
        self.ticket = Some(ticket);
    }

    /// Wait until every key is free, take them all, and return the guard
    /// that holds them.
    ///
    /// Returns [`crate::Error::DeadlineExceeded`] when the keys stay held
    /// until `deadline`. The write then holds nothing.
    pub async fn acquire(
        self,
        deadline: tokio::time::Instant,
    ) -> crate::Result<WriteAdmissionGuard> {
        let mut watch = Watch {
            lock_manager: Arc::clone(&self.lock_manager),
            signal: Arc::new(Notify::new()),
            key: None,
        };
        loop {
            let probe = self
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .try_acquire_or_watch(self.txn, &self.keys, &watch.signal);
            let blocker = match probe {
                GateProbe::Acquired => {
                    drop(watch);
                    let mut guard = WriteAdmissionGuard::new(
                        self.lock_manager,
                        self.txn,
                        self.promotion_sender,
                    );
                    if let Some(ticket) = self.ticket {
                        guard.carry_ticket(ticket);
                    }
                    return Ok(guard);
                }
                GateProbe::Blocked(blocker) => blocker,
            };
            watch.key = Some(blocker);
            // The watch stores a permit, so a release between the probe and
            // this wait still wakes the waiter.
            if tokio::time::timeout_at(deadline, watch.signal.notified())
                .await
                .is_err()
            {
                drop(watch);
                let contention = self
                    .lock_manager
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .contention(self.txn, &self.keys);
                tracing::warn!(
                    ?contention,
                    "a contended write reached its deadline waiting for its keys"
                );
                return Err(crate::Error::DeadlineExceeded {
                    request_id: RequestId::new(0),
                });
            }
            // The release that woke this waiter dropped its watch.
            watch.key = None;
        }
    }
}

/// The watch an [`AdmissionWait`] files on its blocking key. Dropping it
/// unfiles the watch, so a deadline or a cancellation leaves none behind.
struct Watch {
    lock_manager: Arc<Mutex<LockManager>>,
    signal: Arc<Notify>,
    /// The key the signal is filed under, `None` when it is filed nowhere.
    key: Option<LockKey>,
}

impl Drop for Watch {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        self.lock_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .unwatch(&key, &self.signal);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn coll() -> LockKey {
        LockKey::Collection {
            collection: Arc::from("c"),
        }
    }

    fn waiter(lock_manager: &Arc<Mutex<LockManager>>) -> AdmissionWait {
        AdmissionWait::new(
            Arc::clone(lock_manager),
            TxnId::new(TxnId::AUTOCOMMIT_EPOCH, 1),
            BTreeMap::from([(coll(), LockMode::Exclusive)]),
            None,
        )
    }

    /// The waiter takes its keys once the Calvin holder releases them.
    #[tokio::test]
    async fn a_waiter_takes_its_keys_after_the_holder_releases() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holder = TxnId::new(7, 0);
        assert!(
            lock_manager
                .lock()
                .expect("lock table")
                .try_acquire(holder, BTreeMap::from([(coll(), LockMode::Intent)]))
        );
        let wait = tokio::spawn(
            waiter(&lock_manager).acquire(tokio::time::Instant::now() + Duration::from_secs(10)),
        );
        tokio::task::yield_now().await;
        assert!(
            !wait.is_finished(),
            "the waiter waits while the key is held"
        );
        lock_manager.lock().expect("lock table").release(holder);
        let guard = wait.await.expect("join").expect("the keys free up");
        drop(guard);
        assert_eq!(lock_manager.lock().expect("lock table").lock_count(), 0);
    }

    /// The waiter gives up at its deadline and holds nothing.
    #[tokio::test]
    async fn a_waiter_gives_up_at_its_deadline() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holder = TxnId::new(7, 0);
        assert!(
            lock_manager
                .lock()
                .expect("lock table")
                .try_acquire(holder, BTreeMap::from([(coll(), LockMode::Exclusive)]))
        );
        let outcome = waiter(&lock_manager)
            .acquire(tokio::time::Instant::now() + Duration::from_millis(20))
            .await;
        assert!(matches!(
            outcome,
            Err(crate::Error::DeadlineExceeded { .. })
        ));
        assert_eq!(lock_manager.lock().expect("lock table").holder_count(), 1);
        assert_eq!(
            lock_manager.lock().expect("lock table").watcher_count(),
            0,
            "a waiter past its deadline leaves no watch"
        );
    }

    fn row(surrogate: u32) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from("other"),
            surrogate,
        }
    }

    /// Releases of keys the waiter does not wait on never wake it to probe.
    #[tokio::test]
    async fn a_waiter_sleeps_through_releases_of_other_keys() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holder = TxnId::new(7, 0);
        assert!(
            lock_manager
                .lock()
                .expect("lock table")
                .try_acquire(holder, BTreeMap::from([(coll(), LockMode::Exclusive)]))
        );
        let wait = tokio::spawn(
            waiter(&lock_manager).acquire(tokio::time::Instant::now() + Duration::from_secs(10)),
        );
        tokio::task::yield_now().await;
        assert_eq!(lock_manager.lock().expect("lock table").watcher_count(), 1);

        for n in 0..100 {
            let other = TxnId::new(8, n);
            let mut table = lock_manager.lock().expect("lock table");
            assert!(table.try_acquire(other, BTreeMap::from([(row(n), LockMode::Exclusive)])));
            table.release(other);
        }
        tokio::task::yield_now().await;
        assert!(
            !wait.is_finished(),
            "the waiter waits while its key is held"
        );
        assert_eq!(
            lock_manager
                .lock()
                .expect("lock table")
                .watch_registrations(),
            1,
            "the waiter probed once"
        );

        lock_manager.lock().expect("lock table").release(holder);
        let guard = wait.await.expect("join").expect("the keys free up");
        assert_eq!(
            lock_manager
                .lock()
                .expect("lock table")
                .watch_registrations(),
            1
        );
        drop(guard);
        assert_eq!(lock_manager.lock().expect("lock table").lock_count(), 0);
    }

    /// A waiter dropped mid-wait unfiles its watch.
    #[tokio::test]
    async fn a_cancelled_waiter_drops_its_watch() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holder = TxnId::new(7, 0);
        assert!(
            lock_manager
                .lock()
                .expect("lock table")
                .try_acquire(holder, BTreeMap::from([(coll(), LockMode::Exclusive)]))
        );
        let mut acquire = Box::pin(
            waiter(&lock_manager).acquire(tokio::time::Instant::now() + Duration::from_secs(10)),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), acquire.as_mut())
                .await
                .is_err(),
            "the waiter waits while its key is held"
        );
        assert_eq!(lock_manager.lock().expect("lock table").watcher_count(), 1);
        drop(acquire);
        assert_eq!(
            lock_manager.lock().expect("lock table").watcher_count(),
            0,
            "a cancelled waiter leaves no watch"
        );
        assert_eq!(lock_manager.lock().expect("lock table").holder_count(), 1);
    }
}
