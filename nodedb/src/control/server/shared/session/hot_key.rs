// SPDX-License-Identifier: BUSL-1.1

//! Feeds the Calvin scheduler's [`HotKeyTable`](crate::control::cluster::calvin::scheduler::lock::HotKeyTable)
//! from every COMMIT abort path.
//!
//! A key a transaction read and then aborted on is a candidate hot key: enough
//! repeat aborts within the rolling window promote it to "hot" so a later read
//! reserves it up front instead of contending at commit time. This module only
//! ACCUMULATES the stat — it never reads [`HotKeyTable::is_hot`] and never
//! feeds any replicated decision.

use crate::control::cluster::calvin::scheduler::lock::LockKey;
use crate::control::state::SharedState;

use super::read_set::{ReadSetEntry, lock_key_of_read};

/// Record every point-key read by an aborting txn against the hot-key table so
/// repeated conflicts on the same key promote it to "hot".
pub(super) fn record_read_set_aborts(state: &SharedState, read_set: &[ReadSetEntry]) {
    let now = std::time::Instant::now();
    let mut table = state
        .calvin
        .hot_key_table
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    for entry in read_set {
        if let Some(lock_key) = lock_key_of(entry) {
            table.record_abort(&lock_key, now);
        }
    }
}

/// Map a read-set entry to the deterministic lock key it observed, when the
/// read was a single-row point read (`Surrogate` or `KvKey`). Every other
/// `ReadKey` shape (predicate / index-eq / index-range scans, and the graph
/// `Edge` identity, which has no `LockKey` counterpart carrying string node
/// ids rather than surrogates) is a coarse or non-point observation with no
/// single lock key to charge the abort against.
///
/// Delegates to [`lock_key_of_read`], the same construction the reserve-at-read
/// path uses, so the `KeyRepr` match lives in exactly one place.
fn lock_key_of(entry: &ReadSetEntry) -> Option<LockKey> {
    lock_key_of_read(&entry.key, &entry.collection)
}
