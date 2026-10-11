// SPDX-License-Identifier: BUSL-1.1

//! A contended write no Calvin transaction can sequence: it waits for its
//! keys at the gate.
//!
//! The waiter holds no key while it waits. It probes the lock table with an
//! all-or-nothing `try_acquire` after every release, so it never holds one
//! key while it waits for another. A Calvin transaction therefore never
//! waits on a waiting writer, and the two cannot deadlock.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;

use crate::control::cluster::calvin::scheduler::lock_manager::{
    LockKey, LockManager, LockMode, TxnId,
};
use crate::types::RequestId;

use super::gate::WriteAdmissionGuard;

/// The keys of a contended write, and the lock table it waits on.
pub struct AdmissionWait {
    lock_manager: Arc<Mutex<LockManager>>,
    txn: TxnId,
    keys: BTreeMap<LockKey, LockMode>,
    promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
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
        }
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
        let signal = self
            .lock_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release_signal();
        loop {
            // Enabled before the probe, so a release between the probe and
            // the wait still wakes this waiter.
            let released = signal.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            let acquired = {
                let mut lock_manager = self.lock_manager.lock().unwrap_or_else(|p| p.into_inner());
                lock_manager.try_acquire(self.txn, self.keys.clone())
            };
            if acquired {
                return Ok(WriteAdmissionGuard::new(
                    self.lock_manager,
                    self.txn,
                    self.promotion_sender,
                ));
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
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
        }
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
    }
}
