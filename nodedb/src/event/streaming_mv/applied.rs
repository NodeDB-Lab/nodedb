// SPDX-License-Identifier: BUSL-1.1

//! Which events the streaming materialized views applied.
//!
//! A view's aggregates are not idempotent: an event applied twice counts
//! twice. Every event is applied to every view under this lock, and its key
//! is recorded in the same critical section. A flush persists the views and
//! the keys under the same lock, in one redb transaction, so the persisted
//! views and keys always agree. After a restart, a replayed event whose key
//! was persisted is already in the restored views and is skipped.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::event::sink_ledger::SinkEventKey;

#[derive(Debug, Default)]
struct Applied {
    keys: HashSet<SinkEventKey>,
    /// Bumped on every applied event, so a flush with nothing new is skipped.
    generation: u64,
}

/// Applied event keys of the streaming materialized views.
#[derive(Debug, Default)]
pub struct MvAppliedKeys {
    applied: Mutex<Applied>,
}

impl MvAppliedKeys {
    fn lock(&self) -> std::sync::MutexGuard<'_, Applied> {
        self.applied.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Run `apply` unless the views already applied `key`. An event without a
    /// key reproduces no WAL record and is delivered once, so it always
    /// applies. Returns whether `apply` ran.
    pub fn apply_once(&self, key: Option<&SinkEventKey>, apply: impl FnOnce()) -> bool {
        let mut applied = self.lock();
        if let Some(key) = key {
            if applied.keys.contains(key) {
                return false;
            }
            applied.keys.insert(key.clone());
        }
        apply();
        applied.generation = applied.generation.wrapping_add(1);
        true
    }

    /// Record a change to the views made outside an event, such as a time
    /// bucket finalized, so the next flush persists it.
    pub fn touch(&self) {
        let mut applied = self.lock();
        applied.generation = applied.generation.wrapping_add(1);
    }

    /// Run `persist` with the applied keys and their generation, holding off
    /// every apply until it returns.
    pub fn with_consistent<R>(&self, persist: impl FnOnce(&HashSet<SinkEventKey>, u64) -> R) -> R {
        let applied = self.lock();
        persist(&applied.keys, applied.generation)
    }

    /// Install the keys persisted with the restored views.
    pub fn restore(&self, keys: impl IntoIterator<Item = SinkEventKey>) {
        self.lock().keys.extend(keys);
    }

    /// Drop the keys of `core` at or below `through`.
    pub fn prune(&self, core: u32, through: u64) {
        self.lock()
            .keys
            .retain(|key| key.core != core || key.lsn > through);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(lsn: u64) -> SinkEventKey {
        SinkEventKey {
            core: 0,
            lsn,
            occurrence: 0,
            delete: false,
            collection: "orders".into(),
            row_kind: 0,
            row: "o-1".into(),
        }
    }

    #[test]
    fn an_event_applies_once() {
        let applied = MvAppliedKeys::default();
        let mut runs = 0;
        assert!(applied.apply_once(Some(&key(1)), || runs += 1));
        assert!(!applied.apply_once(Some(&key(1)), || runs += 1));
        assert!(applied.apply_once(None, || runs += 1));
        assert_eq!(runs, 2);
        applied.prune(0, 1);
        assert!(applied.apply_once(Some(&key(1)), || runs += 1));
    }
}
