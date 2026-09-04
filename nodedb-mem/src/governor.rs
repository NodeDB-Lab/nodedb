// SPDX-License-Identifier: Apache-2.0

//! Central memory governor.
//!
//! The governor owns all budget levels and enforces a four-layer hierarchy:
//! global ceiling → per-database → per-tenant → per-engine.
//! Every subsystem that wants to allocate significant memory must go through
//! the governor.
//!
//! ## Lock-poisoning policy
//!
//! The maps guarded by `RwLock` here (`database_budgets`, `tenant_budgets`)
//! contain only `Arc<Budget>` handles — never partially-mutated invariants.
//! `Budget` itself is built from atomics and is consistent at every byte
//! boundary. A panic in another thread therefore cannot leave the *contents*
//! of these maps in an inconsistent state; only the `RwLock`'s poison flag
//! is set. We deliberately recover via `unwrap_or_else(|p| p.into_inner())`
//! so a one-off panic in a quota helper does not poison the entire memory
//! subsystem and stall every future reservation. If a Budget's atomics ever
//! grow into a multi-step protocol that *can* be partially updated, this
//! policy must be revisited.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use nodedb_types::{DatabaseId, TenantId};

use crate::budget::Budget;
use crate::engine::EngineId;
use crate::error::{MemError, Result};
use crate::pressure::{PressureLevel, PressureThresholds};
use crate::reservation_token::ReservationToken;
use crate::scoped_budget::{ScopedBudget, clear_scoped_limit, set_scoped_limit};

/// Shared atomic global usage tracker.
///
/// Separate struct so that `ReservationToken` can hold a weak-free `Arc`
/// without pulling in the full governor.
pub struct GlobalCounter {
    pub(crate) allocated: AtomicUsize,
    pub(crate) ceiling: usize,
}

impl std::fmt::Debug for GlobalCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalCounter")
            .field("allocated", &self.allocated.load(Ordering::Relaxed))
            .field("ceiling", &self.ceiling)
            .finish()
    }
}

/// Configuration for the memory governor.
#[derive(Debug, Clone)]
pub struct GovernorConfig {
    /// Global memory ceiling in bytes. The sum of all engine budgets
    /// must not exceed this.
    pub global_ceiling: usize,

    /// Per-engine budget limits.
    pub engine_limits: HashMap<EngineId, usize>,
}

impl GovernorConfig {
    /// Validate that the sum of engine limits does not exceed the global ceiling.
    pub fn validate(&self) -> Result<()> {
        let total: usize = self.engine_limits.values().sum();
        if total > self.global_ceiling {
            return Err(MemError::GlobalCeilingExceeded {
                allocated: total,
                ceiling: self.global_ceiling,
                requested: 0,
            });
        }
        Ok(())
    }
}

/// The central memory governor.
///
/// Thread-safe: global, database, and tenant counters use atomics.
/// The budget map itself is behind an `RwLock`; reads (common) take a shared
/// lock, writes (rare — only when quotas change) take an exclusive lock.
#[derive(Debug)]
pub struct MemoryGovernor {
    /// Per-engine budgets (original arity-2 tracking).
    budgets: HashMap<EngineId, Budget>,

    /// Shared global counter. Held by both the governor and every live token.
    global_counter: Arc<GlobalCounter>,

    /// Global ceiling in bytes.
    global_ceiling: usize,

    /// Pressure thresholds for graduated backpressure.
    thresholds: PressureThresholds,

    /// Per-database budget map. Keyed by `DatabaseId`. Populated lazily via
    /// `set_database_budget`; databases without an entry are uncapped.
    database_budgets: RwLock<HashMap<DatabaseId, ScopedBudget>>,

    /// Per-tenant budget map. Keyed by `(DatabaseId, TenantId)`. Populated
    /// lazily via `set_tenant_budget`.
    tenant_budgets: RwLock<HashMap<(DatabaseId, TenantId), ScopedBudget>>,
}

impl MemoryGovernor {
    /// Create a new governor with the given configuration.
    pub fn new(config: GovernorConfig) -> Result<Self> {
        config.validate()?;

        let mut budgets = HashMap::new();
        for (engine, limit) in &config.engine_limits {
            budgets.insert(*engine, Budget::new(*limit));
        }

        let global_counter = Arc::new(GlobalCounter {
            allocated: AtomicUsize::new(0),
            ceiling: config.global_ceiling,
        });

        Ok(Self {
            budgets,
            global_counter,
            global_ceiling: config.global_ceiling,
            thresholds: PressureThresholds::default(),
            database_budgets: RwLock::new(HashMap::new()),
            tenant_budgets: RwLock::new(HashMap::new()),
        })
    }

    // ── Database budget setters ───────────────────────────────────────────────

    /// Install or replace the memory ceiling for a database.
    ///
    /// Called by the catalog apply path when `ALTER DATABASE … SET QUOTA` is
    /// executed. Takes effect for all subsequent `try_reserve` calls; in-flight
    /// tokens already issued are not recalled.
    pub fn set_database_budget(&self, db: DatabaseId, max_bytes: usize) {
        let mut map = self
            .database_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        set_scoped_limit(&mut map, db, max_bytes);
    }

    /// Remove the per-database budget ceiling, making that database uncapped.
    pub fn clear_database_budget(&self, db: DatabaseId) {
        let mut map = self
            .database_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        clear_scoped_limit(&mut map, &db);
    }

    // ── Tenant budget setters ─────────────────────────────────────────────────

    /// Install or replace the memory ceiling for a tenant within a database.
    pub fn set_tenant_budget(&self, db: DatabaseId, tenant: TenantId, max_bytes: usize) {
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        set_scoped_limit(&mut map, (db, tenant), max_bytes);
    }

    /// Remove the per-tenant budget ceiling.
    pub fn clear_tenant_budget(&self, db: DatabaseId, tenant: TenantId) {
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        clear_scoped_limit(&mut map, &(db, tenant));
    }

    // ── Scope teardown ────────────────────────────────────────────────────────

    /// Remove every memory budget of a dropped database, tenants included.
    /// A dropped scope is never re-capped, so live tokens keep no entry alive.
    pub fn clear_database_scope(&self, db: DatabaseId) {
        {
            let mut map = self
                .database_budgets
                .write()
                .unwrap_or_else(|p| p.into_inner());
            map.remove(&db);
        }
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|(entry_db, _), _| *entry_db != db);
    }

    /// Remove a dropped tenant's memory budgets in every database.
    /// A dropped scope is never re-capped, so live tokens keep no entry alive.
    pub fn clear_tenant_scope(&self, tenant: TenantId) {
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|(_, entry_tenant), _| *entry_tenant != tenant);
    }

    // ── 4-arity reservation ───────────────────────────────────────────────────

    /// Reserve `size` bytes for the given (database, tenant, engine) triple.
    ///
    /// Check order: **global → database → tenant → engine** (largest scope
    /// first, to fail fast and avoid partial increments at deep levels).
    ///
    /// On any failure the function rolls back any partial increments already
    /// applied at higher layers and returns an error describing the exhausted
    /// layer. On success, returns a [`ReservationToken`] whose `Drop`
    /// implementation releases all four layers.
    ///
    /// Databases or tenants without a configured budget are skipped (uncapped).
    /// Engines without a configured budget return [`MemError::UnknownEngine`].
    pub fn try_reserve(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        engine: EngineId,
        size: usize,
    ) -> Result<ReservationToken> {
        // ── Layer 0: fail fast on unknown engine (D2 / #271) ──────────────────
        // Must resolve BEFORE any increment: with a bad engine name every
        // other path rolls back, this one used to return from the lookup
        // after global/db/tenant counters were already bumped.
        let engine_budget = self
            .budgets
            .get(&engine)
            .ok_or(MemError::UnknownEngine(engine))?;

        // ── Layer 1: global ceiling ───────────────────────────────────────────
        let global_arc = Arc::clone(&self.global_counter);
        if size > 0 {
            loop {
                let current = global_arc.allocated.load(Ordering::Relaxed);
                if current + size > global_arc.ceiling {
                    return Err(MemError::GlobalCeilingExceeded {
                        allocated: current,
                        ceiling: global_arc.ceiling,
                        requested: size,
                    });
                }
                match global_arc.allocated.compare_exchange_weak(
                    current,
                    current + size,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }
        }

        // ── Layer 2: per-database budget ──────────────────────────────────────
        let db_counter = {
            let map = self
                .database_budgets
                .read()
                .unwrap_or_else(|p| p.into_inner());
            match map.get(&db) {
                None => None,
                Some(budget) => match budget.try_reserve(size) {
                    Ok(arc) => Some(arc),
                    Err(denied) => {
                        // Roll back global.
                        if size > 0 {
                            global_arc.allocated.fetch_sub(size, Ordering::Relaxed);
                        }
                        return Err(MemError::DatabaseBudgetExhausted {
                            db,
                            requested: size,
                            available: budget.available(),
                            limit: denied.limit,
                        });
                    }
                },
            }
        };

        // ── Layer 3: per-tenant budget ────────────────────────────────────────
        let tenant_counter = {
            let map = self
                .tenant_budgets
                .read()
                .unwrap_or_else(|p| p.into_inner());
            match map.get(&(db, tenant)) {
                None => None,
                Some(budget) => match budget.try_reserve(size) {
                    Ok(arc) => Some(arc),
                    Err(denied) => {
                        // Roll back database and global.
                        if let Some(ref ctr) = db_counter
                            && size > 0
                        {
                            ctr.fetch_sub(size, Ordering::Relaxed);
                        }
                        if size > 0 {
                            global_arc.allocated.fetch_sub(size, Ordering::Relaxed);
                        }
                        return Err(MemError::TenantBudgetExhausted {
                            db,
                            tenant,
                            requested: size,
                            available: budget.available(),
                            limit: denied.limit,
                        });
                    }
                },
            }
        };

        // ── Layer 4: per-engine budget (resolved above, fail-fast) ────────────

        let engine_counter = if let Some(arc) = engine_budget.try_reserve_arc(size) {
            Some(arc)
        } else {
            // Roll back tenant, database, and global.
            if let Some(ref ctr) = tenant_counter
                && size > 0
            {
                ctr.fetch_sub(size, Ordering::Relaxed);
            }
            if let Some(ref ctr) = db_counter
                && size > 0
            {
                ctr.fetch_sub(size, Ordering::Relaxed);
            }
            if size > 0 {
                global_arc.allocated.fetch_sub(size, Ordering::Relaxed);
            }
            return Err(MemError::BudgetExhausted {
                engine,
                requested: size,
                available: engine_budget.available(),
                limit: engine_budget.limit(),
            });
        };

        Ok(ReservationToken::new(
            crate::reservation_token::ReservationParams {
                global_counter: global_arc,
                database_counter: db_counter,
                tenant_counter,
                engine_counter,
                size,
                db,
                tenant,
                engine,
            },
        ))
    }

    /// Release `size` bytes back to the given engine's budget.
    ///
    /// This method only releases the engine-layer counter; it exists for
    /// legacy compatibility with code that uses [`BudgetGuard`] rather than
    /// `ReservationToken`. New code should hold a `ReservationToken` and let
    /// drop handle all four layers.
    pub fn release(&self, engine: EngineId, size: usize) {
        if let Some(budget) = self.budgets.get(&engine) {
            budget.release(size);
        }
        // Also release from the global counter for legacy callers — saturating,
        // so a release that races a still-alive `ReservationToken` (e.g. a
        // timeseries flush releasing the memtable footprint while a per-batch
        // token is in scope) cannot wrap the counter to ~usize::MAX.
        crate::budget::atomic_saturating_sub(&self.global_counter.allocated, size);
    }

    /// Get the budget for a specific engine.
    pub fn budget(&self, engine: EngineId) -> Option<&Budget> {
        self.budgets.get(&engine)
    }

    /// Get the global ceiling.
    pub fn global_ceiling(&self) -> usize {
        self.global_ceiling
    }

    /// Total memory allocated across all engines (engine-layer sum).
    pub fn total_allocated(&self) -> usize {
        self.budgets.values().map(|b| b.allocated()).sum()
    }

    /// Total number of over-release events observed across all
    /// per-engine budgets. A non-zero value signals at least one
    /// call-site is releasing more bytes than it reserved — the
    /// "memory release exceeds allocation" warning class. Per-engine
    /// `allocated()` saturates to zero on over-release, so this
    /// counter is the only post-hoc observable for the bug.
    pub fn total_over_release_count(&self) -> usize {
        self.budgets.values().map(|b| b.over_release_count()).sum()
    }

    /// Global utilization as a percentage (0-100). Computed in `u128` so a
    /// corrupted engine-layer sum clamps to 100 % instead of overflowing.
    pub fn global_utilization_percent(&self) -> u8 {
        if self.global_ceiling == 0 {
            return 100;
        }
        ((self.total_allocated() as u128 * 100) / self.global_ceiling as u128).min(100) as u8
    }

    /// Current pressure level for a specific engine.
    pub fn engine_pressure(&self, engine: EngineId) -> PressureLevel {
        self.budgets
            .get(&engine)
            .map(|b| self.thresholds.level_for(b.utilization_percent()))
            .unwrap_or(PressureLevel::Emergency)
    }

    /// Current global pressure level.
    pub fn global_pressure(&self) -> PressureLevel {
        self.thresholds.level_for(self.global_utilization_percent())
    }

    /// Worst-case (highest) pressure level across every engine that has a
    /// configured budget. Cheap: iterates the in-memory budget map and
    /// allocates nothing — meant to be called once per Data-Plane core-loop
    /// tick, unlike [`snapshot`](Self::snapshot) which materialises a `Vec`.
    /// Returns `Normal` when no engine budgets are configured.
    pub fn worst_engine_pressure(&self) -> PressureLevel {
        self.budgets
            .values()
            .map(|b| self.thresholds.level_for(b.utilization_percent()))
            .max()
            .unwrap_or(PressureLevel::Normal)
    }

    /// Set custom pressure thresholds.
    pub fn set_thresholds(&mut self, thresholds: PressureThresholds) {
        self.thresholds = thresholds;
    }

    /// Snapshot of all engine budget states (for metrics/debugging).
    pub fn snapshot(&self) -> Vec<EngineSnapshot> {
        self.budgets
            .iter()
            .map(|(engine, budget)| EngineSnapshot {
                engine: *engine,
                allocated: budget.allocated(),
                limit: budget.limit(),
                peak: budget.peak(),
                rejections: budget.rejections(),
                utilization_percent: budget.utilization_percent(),
            })
            .collect()
    }
}

/// Point-in-time snapshot of an engine's memory state.
#[derive(Debug, Clone)]
pub struct EngineSnapshot {
    pub engine: EngineId,
    pub allocated: usize,
    pub limit: usize,
    pub peak: usize,
    pub rejections: usize,
    pub utilization_percent: u8,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;

    use nodedb_types::{DatabaseId, TenantId};

    use super::*;

    fn test_config() -> GovernorConfig {
        let mut engine_limits = HashMap::new();
        engine_limits.insert(EngineId::Vector, 4096);
        engine_limits.insert(EngineId::Query, 2048);
        engine_limits.insert(EngineId::Timeseries, 1024);

        GovernorConfig {
            global_ceiling: 8192,
            engine_limits,
        }
    }

    fn db() -> DatabaseId {
        DatabaseId::DEFAULT
    }

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    // ── Basic 4-arity reservation ────────────────────────────────────────────

    #[test]
    fn try_reserve_unknown_engine_rolls_back_global() {
        // CLAIM D2: engine lookup (`ok_or(UnknownEngine)?`) happens AFTER
        // global/db/tenant increments; error path must roll back or leak.
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Array, 1000)
            .unwrap_err();
        assert!(
            matches!(err, MemError::UnknownEngine(_)),
            "Array is not in test_config engine limits"
        );
        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "D2: UnknownEngine must not leak the global counter"
        );
    }

    #[test]
    fn reserve_within_budget() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let tok = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 1000)
            .unwrap();
        assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 1000);
        assert_eq!(tok.size(), 1000);
    }

    #[test]
    fn reserve_exceeds_engine_budget() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Query, 3000)
            .unwrap_err();
        assert!(matches!(err, MemError::BudgetExhausted { .. }));
    }

    #[test]
    fn reserve_exceeds_global_ceiling() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        // Fill up global ceiling by filling all engines.
        let _t1 = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 4096)
            .unwrap();
        let _t2 = gov
            .try_reserve(db(), tenant(), EngineId::Query, 2048)
            .unwrap();
        let _t3 = gov
            .try_reserve(db(), tenant(), EngineId::Timeseries, 1024)
            .unwrap();
        // All engine budgets are also exhausted, so either error is valid.
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Timeseries, 2000)
            .unwrap_err();
        assert!(matches!(
            err,
            MemError::BudgetExhausted { .. } | MemError::GlobalCeilingExceeded { .. }
        ));
    }

    // ── RAII release ──────────────────────────────────────────────────────────

    #[test]
    fn raii_release_returns_to_baseline() {
        let gov = MemoryGovernor::new(test_config()).unwrap();

        {
            let tok = gov
                .try_reserve(db(), tenant(), EngineId::Vector, 1000)
                .unwrap();
            assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 1000);
            assert_eq!(tok.size(), 1000);
        } // token dropped here

        assert_eq!(
            gov.budget(EngineId::Vector).unwrap().allocated(),
            0,
            "engine counter must be returned on drop"
        );
    }

    // ── Database-cap hierarchical denial ─────────────────────────────────────

    #[test]
    fn database_cap_denies_even_with_tenant_headroom() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        // Database budget: 500 bytes.
        gov.set_database_budget(db(), 500);
        // Tenant budget: generous.
        gov.set_tenant_budget(db(), tenant(), 4096);

        // Reservation of 600 must fail at the database layer even though
        // both global and tenant have headroom.
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 600)
            .unwrap_err();
        assert!(
            matches!(err, MemError::DatabaseBudgetExhausted { .. }),
            "expected DatabaseBudgetExhausted, got {err:?}"
        );
    }

    #[test]
    fn global_cap_denies_even_with_database_and_tenant_headroom() {
        // Global ceiling of 200. Engine limit also 200 (passes validation since
        // sum ≤ global). DB and tenant budgets are generous. Request 300 bytes —
        // global layer fires first and denies.
        let mut engine_limits = HashMap::new();
        engine_limits.insert(EngineId::Vector, 200);
        let gov = MemoryGovernor::new(GovernorConfig {
            global_ceiling: 200,
            engine_limits,
        })
        .unwrap();
        gov.set_database_budget(db(), 1024);
        gov.set_tenant_budget(db(), tenant(), 1024);

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 300)
            .unwrap_err();
        assert!(
            matches!(err, MemError::GlobalCeilingExceeded { .. }),
            "expected GlobalCeilingExceeded, got {err:?}"
        );
    }

    #[test]
    fn tenant_cap_denies_with_db_headroom() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 4096);
        gov.set_tenant_budget(db(), tenant(), 300);

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap_err();
        assert!(
            matches!(err, MemError::TenantBudgetExhausted { .. }),
            "expected TenantBudgetExhausted, got {err:?}"
        );
    }

    // ── Rollback correctness: partial increments must be undone on failure ────

    #[test]
    fn partial_increments_rolled_back_on_db_failure() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 50);

        // Request 100 bytes → fails at DB layer. Global should stay at 0.
        let _ = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 100)
            .unwrap_err();

        // Global counter must be 0 (rolled back).
        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "global counter must be rolled back on database-layer failure"
        );
    }

    #[test]
    fn partial_increments_rolled_back_on_tenant_failure() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 4096);
        gov.set_tenant_budget(db(), tenant(), 50);

        let _ = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 100)
            .unwrap_err();

        // Both global and db counters must be 0.
        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "global counter must be rolled back on tenant-layer failure"
        );
        let db_map = gov.database_budgets.read().unwrap();
        let db_alloc = db_map[&db()].allocated.load(Ordering::Relaxed);
        assert_eq!(db_alloc, 0, "database counter must be rolled back");
    }

    // ── Concurrent reserves ───────────────────────────────────────────────────

    #[test]
    fn concurrent_reserves_never_exceed_cap() {
        let mut limits = HashMap::new();
        limits.insert(EngineId::Vector, 10_000);
        let gov = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 10_000,
                engine_limits: limits,
            })
            .unwrap(),
        );
        gov.set_database_budget(DatabaseId::DEFAULT, 10_000);

        // N threads each try to reserve S bytes.
        let n_threads = 8;
        let reserve_size = 1_000;
        let mut handles = Vec::new();

        for i in 0..n_threads {
            let gov_clone = Arc::clone(&gov);
            handles.push(thread::spawn(move || {
                gov_clone.try_reserve(
                    DatabaseId::DEFAULT,
                    TenantId::new(i as u64),
                    EngineId::Vector,
                    reserve_size,
                )
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let successful: Vec<_> = results.into_iter().filter_map(|r| r.ok()).collect();

        // At most 10 successful reservations of 1000 bytes each against a 10000 cap.
        assert!(
            successful.len() <= 10,
            "expected at most 10 successful reservations, got {}",
            successful.len()
        );

        let engine_alloc = gov.budget(EngineId::Vector).unwrap().allocated();
        assert!(
            engine_alloc <= 10_000,
            "engine total {engine_alloc} must not exceed cap 10000"
        );

        let global_alloc = gov.global_counter.allocated.load(Ordering::Relaxed);
        assert!(
            global_alloc <= 10_000,
            "global total {global_alloc} must not exceed ceiling 10000"
        );
    }

    // ── Legacy tests ─────────────────────────────────────────────────────────

    #[test]
    fn unknown_engine_rejected() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Crdt, 100)
            .unwrap_err();
        assert!(matches!(err, MemError::UnknownEngine(EngineId::Crdt)));
    }

    #[test]
    fn snapshot_reports_all_engines() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let _tok = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 2048)
            .unwrap();

        let snap = gov.snapshot();
        assert_eq!(snap.len(), 3);

        let vector_snap = snap.iter().find(|s| s.engine == EngineId::Vector).unwrap();
        assert_eq!(vector_snap.allocated, 2048);
        assert_eq!(vector_snap.limit, 4096);
        assert_eq!(vector_snap.utilization_percent, 50);
    }

    #[test]
    fn engine_pressure_levels() {
        let gov = MemoryGovernor::new(test_config()).unwrap();

        assert_eq!(gov.engine_pressure(EngineId::Vector), PressureLevel::Normal);

        let _tok1 = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 2868)
            .unwrap();
        assert_eq!(
            gov.engine_pressure(EngineId::Vector),
            PressureLevel::Warning
        );
    }

    #[test]
    fn worst_engine_pressure_picks_highest() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        assert_eq!(gov.worst_engine_pressure(), PressureLevel::Normal);

        // Push Query to Critical (2048 limit; 1800 ≈ 87%) while Vector/Timeseries
        // stay Normal — the worst-case must follow Query.
        let _tok = gov
            .try_reserve(db(), tenant(), EngineId::Query, 1800)
            .unwrap();
        assert_eq!(gov.engine_pressure(EngineId::Vector), PressureLevel::Normal);
        assert_eq!(gov.worst_engine_pressure(), PressureLevel::Critical);
    }

    // ── Quota changes must not orphan a live counter ─────────────────────────

    #[test]
    fn raising_a_live_quota_keeps_the_allocated_bytes_tracked() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);

        let _held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();
        gov.set_database_budget(db(), 1000);

        // 400 held + 700 requested exceeds the new 1000 cap.
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 700)
            .unwrap_err();
        assert!(
            matches!(err, MemError::DatabaseBudgetExhausted { .. }),
            "raising the quota must keep the held 400 bytes counted, got {err:?}"
        );
    }

    #[test]
    fn raising_a_live_tenant_quota_keeps_the_allocated_bytes_tracked() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_tenant_budget(db(), tenant(), 500);

        let _held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();
        gov.set_tenant_budget(db(), tenant(), 1000);

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 700)
            .unwrap_err();
        assert!(
            matches!(err, MemError::TenantBudgetExhausted { .. }),
            "raising the quota must keep the held 400 bytes counted, got {err:?}"
        );
    }

    #[test]
    fn clearing_a_quota_with_live_tokens_keeps_the_counter() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);

        let _held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();
        gov.clear_database_budget(db());
        gov.set_database_budget(db(), 1000);

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 700)
            .unwrap_err();
        assert!(
            matches!(err, MemError::DatabaseBudgetExhausted { .. }),
            "clear-then-set must keep the held 400 bytes counted, got {err:?}"
        );
    }

    #[test]
    fn clearing_a_quota_with_no_live_tokens_drops_the_entry() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);
        gov.set_tenant_budget(db(), tenant(), 500);

        {
            let _tok = gov
                .try_reserve(db(), tenant(), EngineId::Vector, 400)
                .unwrap();
        }
        gov.clear_database_budget(db());
        gov.clear_tenant_budget(db(), tenant());

        assert!(
            !gov.database_budgets.read().unwrap().contains_key(&db()),
            "database entry must be removed once no token holds its counter"
        );
        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), tenant())),
            "tenant entry must be removed once no token holds its counter"
        );
    }

    #[test]
    fn released_bytes_do_not_count_against_a_later_quota() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);

        {
            let _tok = gov
                .try_reserve(db(), tenant(), EngineId::Vector, 400)
                .unwrap();
        }
        gov.set_database_budget(db(), 500);

        // The released 400 bytes are gone, so the full 500 is available.
        let _tok = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 500)
            .expect("released bytes must not be double-counted");
    }

    #[test]
    fn clear_database_scope_drops_the_database_and_its_tenants() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let other = DatabaseId::new(9);
        gov.set_database_budget(db(), 500);
        gov.set_tenant_budget(db(), tenant(), 500);
        gov.set_database_budget(other, 500);
        gov.set_tenant_budget(other, tenant(), 500);
        let held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();

        gov.clear_database_scope(db());

        assert!(!gov.database_budgets.read().unwrap().contains_key(&db()));
        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), tenant())),
            "a live token must not keep a dropped scope's entry alive"
        );
        assert!(gov.database_budgets.read().unwrap().contains_key(&other));
        assert!(
            gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(other, tenant()))
        );
        drop(held);
    }

    #[test]
    fn clear_tenant_scope_drops_that_tenant_in_every_database() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let other_db = DatabaseId::new(9);
        let other_tenant = TenantId::new(2);
        gov.set_tenant_budget(db(), tenant(), 500);
        gov.set_tenant_budget(other_db, tenant(), 500);
        gov.set_tenant_budget(db(), other_tenant, 500);

        gov.clear_tenant_scope(tenant());

        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), tenant()))
        );
        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(other_db, tenant()))
        );
        assert!(
            gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), other_tenant))
        );
    }

    #[test]
    fn invalid_config_rejected() {
        let mut config = test_config();
        config.global_ceiling = 100;
        assert!(MemoryGovernor::new(config).is_err());
    }
}
