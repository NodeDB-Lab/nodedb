// SPDX-License-Identifier: Apache-2.0

//! Per-database and per-tenant budget entries for the memory governor.
//! The allocated counter's lifetime follows the scope, never the limit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A named budget with an atomic allocated counter.
///
/// Quota changes mutate `limit` in place so live tokens keep decrementing the
/// same counter.
#[derive(Debug, Clone)]
pub(crate) struct ScopedBudget {
    /// `None` means uncapped, still counted.
    pub(crate) limit: Option<usize>,
    pub(crate) allocated: Arc<AtomicUsize>,
}

/// A rejected scoped reservation. Carries the cap that denied it.
#[derive(Debug)]
pub(crate) struct BudgetDenied {
    pub(crate) limit: usize,
}

impl ScopedBudget {
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            limit,
            allocated: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Attempt a CAS-based reservation. Returns the `Arc` to the counter on
    /// success so the token can hold a reference for drop-release.
    pub(crate) fn try_reserve(&self, size: usize) -> Result<Arc<AtomicUsize>, BudgetDenied> {
        let Some(limit) = self.limit else {
            // Uncapped scopes never reject, but still track for the tokens.
            self.allocated.fetch_add(size, Ordering::AcqRel);
            return Ok(Arc::clone(&self.allocated));
        };
        loop {
            let current = self.allocated.load(Ordering::Relaxed);
            if current + size > limit {
                return Err(BudgetDenied { limit });
            }
            match self.allocated.compare_exchange_weak(
                current,
                current + size,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(Arc::clone(&self.allocated)),
                Err(_) => continue,
            }
        }
    }

    /// Credit `size` bytes unconditionally, ignoring the cap.
    ///
    /// The caller already holds this memory — a denial here would only
    /// hide it from the scope, not free it. Returns the shared `Arc` so
    /// the caller can release it on drop, same as `try_reserve`.
    pub(crate) fn credit(&self, size: usize) -> Arc<AtomicUsize> {
        self.allocated.fetch_add(size, Ordering::AcqRel);
        Arc::clone(&self.allocated)
    }

    /// Bytes left under the cap. `usize::MAX` when uncapped.
    pub(crate) fn available(&self) -> usize {
        match self.limit {
            Some(limit) => limit.saturating_sub(self.allocated.load(Ordering::Relaxed)),
            None => usize::MAX,
        }
    }
}

/// Set the cap for `key` in place, preserving any counter live tokens hold.
pub(crate) fn set_scoped_limit<K: Eq + std::hash::Hash>(
    map: &mut HashMap<K, ScopedBudget>,
    key: K,
    max_bytes: usize,
) {
    if let Some(entry) = map.get_mut(&key) {
        entry.limit = Some(max_bytes);
    } else {
        map.insert(key, ScopedBudget::new(Some(max_bytes)));
    }
}

/// Drop the cap for `key`. The entry survives while a live token holds its
/// counter; the write lock excludes readers, so `strong_count` is exact.
pub(crate) fn clear_scoped_limit<K: Eq + std::hash::Hash>(
    map: &mut HashMap<K, ScopedBudget>,
    key: &K,
) {
    let idle = map
        .get(key)
        .is_some_and(|entry| Arc::strong_count(&entry.allocated) == 1);
    if idle {
        map.remove(key);
    } else if let Some(entry) = map.get_mut(key) {
        entry.limit = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncapped_budget_tracks_without_rejecting() {
        let budget = ScopedBudget::new(None);
        budget.try_reserve(10_000).unwrap();
        assert_eq!(budget.allocated.load(Ordering::Relaxed), 10_000);
        assert_eq!(budget.available(), usize::MAX);
    }

    #[test]
    fn capped_budget_rejects_past_the_limit() {
        let budget = ScopedBudget::new(Some(100));
        budget.try_reserve(60).unwrap();
        let denied = budget.try_reserve(60).unwrap_err();
        assert_eq!(denied.limit, 100);
        assert_eq!(budget.available(), 40);
    }

    #[test]
    fn set_scoped_limit_reuses_the_live_counter() {
        let mut map: HashMap<u8, ScopedBudget> = HashMap::new();
        set_scoped_limit(&mut map, 1, 100);
        let held = map[&1].try_reserve(60).unwrap();
        set_scoped_limit(&mut map, 1, 200);
        assert!(Arc::ptr_eq(&held, &map[&1].allocated));
        assert_eq!(map[&1].allocated.load(Ordering::Relaxed), 60);
    }

    #[test]
    fn clear_scoped_limit_keeps_an_entry_with_a_live_counter() {
        let mut map: HashMap<u8, ScopedBudget> = HashMap::new();
        set_scoped_limit(&mut map, 1, 100);
        let _held = map[&1].try_reserve(60).unwrap();
        clear_scoped_limit(&mut map, &1);
        assert_eq!(map[&1].limit, None);
        assert_eq!(map[&1].allocated.load(Ordering::Relaxed), 60);
    }

    #[test]
    fn clear_scoped_limit_removes_an_idle_entry() {
        let mut map: HashMap<u8, ScopedBudget> = HashMap::new();
        set_scoped_limit(&mut map, 1, 100);
        clear_scoped_limit(&mut map, &1);
        assert!(!map.contains_key(&1));
    }
}
