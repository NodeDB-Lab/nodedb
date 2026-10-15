// SPDX-License-Identifier: BUSL-1.1

//! Per-database background-task CPU budget tracking.
//!
//! Each database is allocated a fraction of core time for background
//! maintenance tasks (compaction, index link repair, edge sweeps, etc.).
//! The fraction is `maintenance_cpu_pct / 100 * 60` CPU-seconds per minute.
//!
//! Wall-clock time is used as a proxy for CPU time on a per-task basis
//! because maintenance tasks run on a dedicated scheduler that controls
//! their interleaving with interactive work. On a single-threaded Data
//! Plane core the approximation is exact; on a multi-core executor it
//! slightly overestimates CPU seconds by scheduling jitter, which is
//! conservative (it causes earlier deferral, never later).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nodedb_types::DatabaseId;

/// Single slot in the 60-bucket sliding window.
#[derive(Clone, Default)]
struct Bucket {
    /// Wall-clock CPU-seconds consumed in this one-second slot.
    consumed_secs: f64,
}

/// Per-database sliding-window consumption tracker (60×1-second buckets).
struct DbWindow {
    buckets: [Bucket; 60],
    /// UNIX-style second index of the last bucket written (wraps at 60).
    last_bucket_secs: u64,
    /// Cumulative CPU-seconds consumed since process start.
    cumulative_cpu_secs: f64,
}

impl DbWindow {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| Bucket::default()),
            last_bucket_secs: 0,
            cumulative_cpu_secs: 0.0,
        }
    }

    /// Advance to `now_secs`, zeroing any buckets that have rolled off.
    fn advance(&mut self, now_secs: u64) {
        if now_secs <= self.last_bucket_secs {
            return;
        }
        let elapsed = (now_secs - self.last_bucket_secs).min(60);
        for i in 1..=elapsed {
            let idx = ((self.last_bucket_secs + i) % 60) as usize;
            self.buckets[idx] = Bucket::default();
        }
        self.last_bucket_secs = now_secs;
    }

    /// Total CPU-seconds consumed in the current 60-second window.
    fn window_total(&self) -> f64 {
        self.buckets.iter().map(|b| b.consumed_secs).sum()
    }

    /// Record `secs` of consumption in the current second bucket.
    fn record(&mut self, now_secs: u64, secs: f64) {
        self.advance(now_secs);
        let idx = (now_secs % 60) as usize;
        self.buckets[idx].consumed_secs += secs;
        if secs.is_finite() && secs > 0.0 {
            self.cumulative_cpu_secs += secs;
        }
    }
}

/// Shared tracker for per-database maintenance CPU budgets.
///
/// Thread-safe: all mutations hold the inner mutex briefly.
/// Designed to be held as `Arc<MaintenanceBudgetTracker>` by the
/// Data Plane `CoreLoop` and by tests.
pub struct MaintenanceBudgetTracker {
    inner: Mutex<TrackerInner>,
}

struct TrackerInner {
    /// Per-database sliding windows.
    windows: HashMap<DatabaseId, DbWindow>,
    /// Per-database CPU-seconds cap per minute.
    /// Derived from `maintenance_cpu_pct / 100.0 * 60.0`.
    caps: HashMap<DatabaseId, f64>,
}

impl TrackerInner {
    fn new() -> Self {
        Self {
            windows: HashMap::new(),
            caps: HashMap::new(),
        }
    }

    fn cap_for(&self, db: DatabaseId) -> f64 {
        self.caps.get(&db).copied().unwrap_or(f64::INFINITY)
    }

    fn window_for_mut(&mut self, db: DatabaseId) -> &mut DbWindow {
        self.windows.entry(db).or_insert_with(DbWindow::new)
    }
}

impl std::fmt::Debug for MaintenanceBudgetTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MaintenanceBudgetTracker { .. }")
    }
}

impl MaintenanceBudgetTracker {
    /// Create a new tracker with no per-database caps configured.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TrackerInner::new()),
        }
    }

    /// Cumulative maintenance CPU-seconds consumed by `db`.
    pub fn cumulative_cpu_secs(&self, db: DatabaseId) -> f64 {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner
            .windows
            .get(&db)
            .map(|w| w.cumulative_cpu_secs)
            .unwrap_or(0.0)
    }

    /// Remove a database's budget window and cap (e.g. on database drop).
    pub fn remove_database(&self, db: DatabaseId) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.caps.remove(&db);
        inner.windows.remove(&db);
    }

    /// Install or replace the maintenance CPU cap for `db`.
    ///
    /// `maintenance_cpu_pct` is the `QuotaRecord` field (0–100).
    /// 0 means "no cap" — the resulting cap is set to `f64::INFINITY`.
    pub fn set_cap(&self, db: DatabaseId, maintenance_cpu_pct: u8) {
        let cap = if maintenance_cpu_pct == 0 {
            f64::INFINITY
        } else {
            (maintenance_cpu_pct as f64 / 100.0) * 60.0
        };
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.caps.insert(db, cap);
    }

    /// Install or replace the maintenance CPU cap and register the database name for `db`.
    pub fn set_cap_named(&self, db: DatabaseId, _name: impl Into<String>, maintenance_cpu_pct: u8) {
        self.set_cap(db, maintenance_cpu_pct);
    }

    /// Attempt to acquire a maintenance lease for `db`.
    ///
    /// Returns `Some(MaintenanceLease)` when `consumed + estimated_secs ≤ cap`
    /// for the current 60-second window. Returns `None` when the database is
    /// over its budget (caller should defer the task to the next window).
    ///
    /// `estimated_secs` is the caller's estimate of how long the task will
    /// run. The lease records the actual elapsed time on drop — the estimate
    /// is used only to pre-screen against the cap.
    pub fn try_acquire(
        self: &Arc<Self>,
        db: DatabaseId,
        estimated_secs: f64,
    ) -> Option<MaintenanceLease> {
        let now_secs = current_secs();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let cap = inner.cap_for(db);
        let window = inner.window_for_mut(db);
        window.advance(now_secs);
        let consumed = window.window_total();

        if consumed + estimated_secs <= cap {
            Some(MaintenanceLease {
                tracker: Arc::clone(self),
                db,
                start: Instant::now(),
            })
        } else {
            None
        }
    }
}

impl Default for MaintenanceBudgetTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII lease returned by [`MaintenanceBudgetTracker::try_acquire`].
///
/// On drop, the actual elapsed wall-clock seconds are recorded into the
/// sliding window and cumulative total for the database.
pub struct MaintenanceLease {
    tracker: Arc<MaintenanceBudgetTracker>,
    db: DatabaseId,
    start: Instant,
}

impl MaintenanceLease {
    /// Database id for which the lease was acquired.
    pub fn db(&self) -> DatabaseId {
        self.db
    }
}

impl Drop for MaintenanceLease {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let now_secs = current_secs();
        let mut inner = self.tracker.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.window_for_mut(self.db).record(now_secs, elapsed);
    }
}

impl std::fmt::Debug for MaintenanceLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintenanceLease")
            .field("db", &self.db)
            .finish()
    }
}

/// Current wall-clock second (wraps every ~584 billion years).
fn current_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn tracker() -> Arc<MaintenanceBudgetTracker> {
        Arc::new(MaintenanceBudgetTracker::new())
    }

    #[test]
    fn over_cap_defers() {
        let t = tracker();
        let db = DatabaseId::new(1);
        // 10% of 60s = 6s cap per minute.
        t.set_cap(db, 10);

        // Consume the full cap via direct window writes.
        {
            let now = current_secs();
            let mut inner = t.inner.lock().unwrap();
            inner.window_for_mut(db).record(now, 6.0);
        }

        // Next acquire must be deferred.
        assert!(t.try_acquire(db, 0.1).is_none());
    }

    #[test]
    fn acquire_within_cap() {
        let t = tracker();
        let db = DatabaseId::new(2);
        t.set_cap(db, 50); // 30s cap
        // 5s estimated — well within 30s.
        assert!(t.try_acquire(db, 5.0).is_some());
    }

    #[test]
    fn no_cap_is_infinite() {
        let t = tracker();
        let db = DatabaseId::new(3);
        // maintenance_cpu_pct = 0 → no cap.
        t.set_cap(db, 0);
        // Should succeed regardless.
        assert!(t.try_acquire(db, 1_000_000.0).is_some());
    }

    #[test]
    fn lease_drop_records_actual() {
        let t = tracker();
        let db = DatabaseId::new(4);
        t.set_cap(db, 100); // 60s cap

        {
            let _lease = t.try_acquire(db, 5.0).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
            // Drop here records actual ~0.01s.
        }

        // Window should have ~0.01s, which is much less than 60s.
        {
            let now = current_secs();
            let mut inner = t.inner.lock().unwrap();
            inner.window_for_mut(db).advance(now);
            let total = inner.window_for_mut(db).window_total();
            assert!(total > 0.0, "lease drop should have recorded elapsed time");
            assert!(total < 1.0, "elapsed should be under 1s");
        }
        assert!(t.cumulative_cpu_secs(db) > 0.0);
    }

    #[test]
    fn window_resets_after_sixty_seconds() {
        let t = tracker();
        let db = DatabaseId::new(5);
        t.set_cap(db, 10); // 6s cap

        // Inject 6s of consumption 61 seconds in the past.
        {
            let now = current_secs();
            let past = now.saturating_sub(61);
            let mut inner = t.inner.lock().unwrap();
            inner.window_for_mut(db).record(past, 6.0);
        }

        // After 61s have elapsed, the window should have rolled off.
        // A fresh advance will clear those buckets.
        let lease = t.try_acquire(db, 5.9); // 5.9 ≤ 6.0 cap, old data gone
        assert!(
            lease.is_some(),
            "old consumption should have expired out of the window"
        );
    }

    #[test]
    fn cumulative_cpu_secs_increments() {
        let t = tracker();
        let db = DatabaseId::new(42);
        t.set_cap(db, 100);

        assert_eq!(t.cumulative_cpu_secs(db), 0.0);
        {
            let _lease = t.try_acquire(db, 1.0).expect("acquire should succeed");
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
        assert!(
            t.cumulative_cpu_secs(db) > 0.0,
            "cumulative maintenance CPU time should be > 0 after lease drop"
        );
    }
}
