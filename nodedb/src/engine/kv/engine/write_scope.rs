// SPDX-License-Identifier: BUSL-1.1

//! The system-time ordinal every KV write stamps.
//!
//! An apply scope gives every write of one statement, transaction install,
//! or replayed record one ordinal, so a read at any cut sees all of its rows
//! or none. The Data Plane opens a scope around each such apply. A scope
//! opened with a fixed ordinal (a Calvin transaction's, or the one a
//! replicated entry's leader assigned) stamps it on every replica. A scope
//! opened without one draws a fresh ordinal of the shared clock at its first
//! write. A write outside every scope draws a fresh ordinal per engine call.
//! A scope opened inside an open scope joins it, except that a fixed ordinal
//! replaces a drawn one: the outermost fixed ordinal wins.

use std::sync::Arc;

use nodedb_types::OrdinalClock;

use super::KvEngine;

/// The apply scope a KV write belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KvWriteScope {
    /// No apply scope is open.
    #[default]
    Closed,
    /// An apply scope with no fixed ordinal is open. `Some` once its first
    /// write drew one.
    Drawn(Option<i64>),
    /// An apply scope with a fixed ordinal is open.
    Fixed(i64),
}

/// What [`KvEngine::close_write_scope`] restores. Returned by
/// [`KvEngine::open_write_scope`].
#[must_use = "pass the guard back to close_write_scope"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvWriteScopeGuard {
    /// The scope to restore, `None` when the open joined the enclosing scope.
    restore: Option<KvWriteScope>,
}

impl KvEngine {
    /// Share the Data Plane core's system-time clock. Every ordinal a write
    /// draws comes from it, and every restored or fixed ordinal folds into it.
    pub fn attach_clock(&mut self, clock: Arc<OrdinalClock>) {
        self.clock = clock;
    }

    /// Open an apply scope. `fixed` names the ordinal every write of the
    /// scope stamps. With `None` the scope joins an enclosing open scope, or
    /// draws one ordinal at its first write. A fixed ordinal folds into the
    /// clock, so a later local write stamps above it.
    pub fn open_write_scope(&mut self, fixed: Option<i64>) -> KvWriteScopeGuard {
        let prior = self.write_scope;
        let next = match (fixed, prior) {
            (_, KvWriteScope::Fixed(_)) | (None, KvWriteScope::Drawn(_)) => {
                return KvWriteScopeGuard { restore: None };
            }
            (Some(ordinal), KvWriteScope::Closed | KvWriteScope::Drawn(_)) => {
                self.clock.update_from_remote(ordinal);
                KvWriteScope::Fixed(ordinal)
            }
            (None, KvWriteScope::Closed) => KvWriteScope::Drawn(None),
        };
        self.write_scope = next;
        KvWriteScopeGuard {
            restore: Some(prior),
        }
    }

    /// Close the scope `guard` opened.
    pub fn close_write_scope(&mut self, guard: KvWriteScopeGuard) {
        if let Some(prior) = guard.restore {
            self.write_scope = prior;
        }
    }

    /// The ordinal the next write stamps. Inside an open scope every call
    /// answers the same ordinal.
    pub(in crate::engine::kv) fn write_ordinal(&mut self) -> i64 {
        match self.write_scope {
            KvWriteScope::Fixed(ordinal) | KvWriteScope::Drawn(Some(ordinal)) => ordinal,
            KvWriteScope::Drawn(None) => {
                let ordinal = self.clock.next_ordinal();
                self.write_scope = KvWriteScope::Drawn(Some(ordinal));
                ordinal
            }
            KvWriteScope::Closed => self.clock.next_ordinal(),
        }
    }

    /// Fold an ordinal a stored version carries into the clock, so a later
    /// write stamps above it.
    pub(in crate::engine::kv) fn observe_ordinal(&self, ordinal: i64) {
        self.clock.update_from_remote(ordinal);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> KvEngine {
        KvEngine::new(1_000, 16, 0.75, 4, 64, 1_000, 1_024)
    }

    #[test]
    fn an_open_scope_stamps_one_ordinal() {
        let mut e = engine();
        let guard = e.open_write_scope(None);
        let first = e.write_ordinal();
        assert_eq!(e.write_ordinal(), first);
        e.close_write_scope(guard);
        assert!(
            e.write_ordinal() > first,
            "a write outside the scope draws a new ordinal"
        );
    }

    #[test]
    fn a_nested_open_joins_the_enclosing_scope() {
        let mut e = engine();
        let outer = e.open_write_scope(None);
        let first = e.write_ordinal();
        let inner = e.open_write_scope(None);
        assert_eq!(e.write_ordinal(), first);
        e.close_write_scope(inner);
        assert_eq!(
            e.write_ordinal(),
            first,
            "closing the joined scope keeps the ordinal"
        );
        e.close_write_scope(outer);
    }

    #[test]
    fn the_outermost_fixed_ordinal_wins() {
        let mut e = engine();
        let outer = e.open_write_scope(Some(7));
        let inner = e.open_write_scope(Some(9));
        assert_eq!(e.write_ordinal(), 7);
        e.close_write_scope(inner);
        e.close_write_scope(outer);
    }

    #[test]
    fn a_fixed_ordinal_stamps_and_folds_into_the_clock() {
        let mut e = engine();
        let far = i64::MAX / 2;
        let guard = e.open_write_scope(Some(far));
        assert_eq!(e.write_ordinal(), far);
        e.close_write_scope(guard);
        assert!(e.write_ordinal() > far);
    }
}
