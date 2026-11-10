// SPDX-License-Identifier: BUSL-1.1

//! The local sidecar that carries a Calvin participant's applied response
//! to the coordinator's completion path on the same node.
//!
//! Every replica of a primary-write participant deposits its applied
//! response. Only the node that coordinates the transaction drains it. A
//! replica on any other node never drains its deposit, and a coordinator
//! whose statement timed out leaves its deposit too. Each deposit therefore
//! expires one TTL after it lands, so the sidecar stays bounded.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use nodedb_cluster::calvin::TxnId;

use super::calvin_apply::CalvinApplyResult;

/// How long a deposit waits for its drain when the host sets no TTL.
pub const DEFAULT_APPLY_RESULT_TTL: Duration =
    nodedb_cluster::calvin::completion_gc::DEFAULT_WAITERLESS_TTL;

/// Applied responses of completed Calvin transactions, keyed by their
/// sequencer-assigned `TxnId`, each kept until it is drained or expires.
pub struct CalvinApplySidecar {
    inner: Mutex<Inner>,
}

struct Inner {
    results: HashMap<TxnId, CalvinApplyResult>,
    /// Each deposited transaction with the instant its first deposit landed,
    /// oldest first.
    deposits: VecDeque<(Instant, TxnId)>,
    ttl: Duration,
}

impl CalvinApplySidecar {
    /// An empty sidecar whose deposits expire after `ttl`.
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                results: HashMap::new(),
                deposits: VecDeque::new(),
                ttl,
            }),
        }
    }

    /// Set how long a deposit waits for its drain.
    pub fn set_ttl(&self, ttl: Duration) {
        self.lock().ttl = ttl;
    }

    /// Deposit into `txn`'s entry through `deposit`, which reads or fills
    /// it. Expired deposits of other transactions go first.
    pub fn deposit_with<R>(
        &self,
        txn: TxnId,
        deposit: impl FnOnce(Entry<'_, TxnId, CalvinApplyResult>) -> R,
    ) -> R {
        self.deposit_at(txn, Instant::now(), deposit)
    }

    /// Remove and return `txn`'s applied response.
    pub fn take(&self, txn: &TxnId) -> Option<CalvinApplyResult> {
        self.lock().results.remove(txn)
    }

    /// The number of transactions whose responses the sidecar holds.
    pub fn len(&self) -> usize {
        self.lock().results.len()
    }

    /// Whether the sidecar holds no response.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn deposit_at<R>(
        &self,
        txn: TxnId,
        now: Instant,
        deposit: impl FnOnce(Entry<'_, TxnId, CalvinApplyResult>) -> R,
    ) -> R {
        let mut inner = self.lock();
        inner.expire(now);
        if !inner.results.contains_key(&txn) {
            inner.deposits.push_back((now, txn));
        }
        deposit(inner.results.entry(txn))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Default for CalvinApplySidecar {
    fn default() -> Self {
        Self::new(DEFAULT_APPLY_RESULT_TTL)
    }
}

impl Inner {
    /// Drop every deposit older than the TTL. A drained deposit leaves its
    /// queue entry, which then removes nothing.
    fn expire(&mut self, now: Instant) {
        while let Some(&(deposited, txn)) = self.deposits.front() {
            if now.saturating_duration_since(deposited) < self.ttl {
                break;
            }
            self.deposits.pop_front();
            self.results.remove(&txn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deposit(sidecar: &CalvinApplySidecar, txn: TxnId, now: Instant) {
        sidecar.deposit_at(txn, now, |entry| {
            entry.or_default();
        });
    }

    /// A deposit no coordinator drains expires one TTL after it landed, on
    /// the next deposit. A drained deposit is gone at once.
    #[test]
    fn undrained_deposits_expire_and_drained_ones_leave() {
        let sidecar = CalvinApplySidecar::new(Duration::from_secs(10));
        let start = Instant::now();
        let stale = TxnId::new(1, 0);
        let drained = TxnId::new(1, 1);
        deposit(&sidecar, stale, start);
        deposit(&sidecar, drained, start);
        assert!(sidecar.take(&drained).is_some());
        assert_eq!(sidecar.len(), 1);

        let fresh = TxnId::new(2, 0);
        deposit(&sidecar, fresh, start + Duration::from_secs(10));
        assert!(
            sidecar.take(&stale).is_none(),
            "the undrained deposit expired"
        );
        assert!(sidecar.take(&fresh).is_some(), "the fresh deposit stays");
        assert!(sidecar.is_empty());
    }

    /// A second participant's deposit into a held entry keeps the entry's
    /// first deposit instant.
    #[test]
    fn a_coalesced_deposit_expires_with_its_first() {
        let sidecar = CalvinApplySidecar::new(Duration::from_secs(10));
        let start = Instant::now();
        let txn = TxnId::new(1, 0);
        deposit(&sidecar, txn, start);
        deposit(&sidecar, txn, start + Duration::from_secs(5));
        deposit(&sidecar, TxnId::new(2, 0), start + Duration::from_secs(10));
        assert!(sidecar.take(&txn).is_none());
    }
}
