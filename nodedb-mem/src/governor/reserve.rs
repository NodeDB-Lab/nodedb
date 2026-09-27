// SPDX-License-Identifier: Apache-2.0

//! The two reservation entry points: [`MemoryGovernor::try_reserve`] and
//! [`MemoryGovernor::charge`].

use std::sync::Arc;

use nodedb_types::{DatabaseId, TenantId};

use super::core::MemoryGovernor;
use crate::engine::EngineId;
use crate::error::{MemError, Result};
use crate::over_release::ReleaseIdentity;
use crate::reservation_token::{ReservationParams, ReservationToken};
use crate::reserve_scope::{ReserveScope, ReservedLayers};

/// Build the token both entry points return. They differ in how they reach
/// a committed [`ReservedLayers`], never in what they build from one.
fn token_from_layers(
    layers: ReservedLayers,
    size: usize,
    db: DatabaseId,
    tenant: TenantId,
    engine: EngineId,
) -> ReservationToken {
    ReservationToken::new(ReservationParams {
        global_counter: layers.global,
        database_counter: layers.database,
        tenant_counter: layers.tenant,
        engine_counter: layers.engine,
        size,
        db,
        tenant,
        engine,
    })
}

impl MemoryGovernor {
    // ── 4-arity reservation ───────────────────────────────────────────────────

    /// Reserve `size` bytes for one (database, tenant, engine) triple.
    ///
    /// Checks global, database, tenant, then engine. Widest scope first
    /// fails fast, leaving the fewest layers to roll back.
    ///
    /// Databases and tenants without a budget are uncapped and skipped. A
    /// zero engine limit denies with [`MemError::BudgetExhausted`].
    ///
    /// # Errors
    ///
    /// Names the exhausted layer. Every partial credit rolls back.
    pub fn try_reserve(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        engine: EngineId,
        size: usize,
    ) -> Result<ReservationToken> {
        let identity = ReleaseIdentity { db, tenant, engine };
        let mut scope = ReserveScope::new(Arc::clone(&self.global_counter), size, identity);

        scope.try_credit_global()?;

        {
            let map = self
                .database_budgets
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(budget) = map.get(&db) {
                match budget.try_reserve(size) {
                    Ok(arc) => scope.credit_database(arc),
                    Err(denied) => {
                        return Err(MemError::DatabaseBudgetExhausted {
                            db,
                            requested: size,
                            available: budget.available(),
                            limit: denied.limit,
                        });
                    }
                }
            }
        }

        {
            let map = self
                .tenant_budgets
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(budget) = map.get(&(db, tenant)) {
                match budget.try_reserve(size) {
                    Ok(arc) => scope.credit_tenant(arc),
                    Err(denied) => {
                        return Err(MemError::TenantBudgetExhausted {
                            db,
                            tenant,
                            requested: size,
                            available: budget.available(),
                            limit: denied.limit,
                        });
                    }
                }
            }
        }

        let engine_budget = &self.budgets[engine.index()];
        let engine_counter = match engine_budget.try_reserve_arc(size) {
            Some(arc) => arc,
            None => {
                return Err(MemError::BudgetExhausted {
                    engine,
                    requested: size,
                    available: engine_budget.available(),
                    limit: engine_budget.limit(),
                });
            }
        };
        scope.credit_engine(engine_counter);

        let layers = scope.commit();
        Ok(token_from_layers(layers, size, db, tenant, engine))
    }

    /// Account `size` bytes already resident across all four layers.
    ///
    /// Denying a charge hides allocated memory, it never frees it. Ignores
    /// every limit and never fails. The token releases all four layers on
    /// drop, as [`try_reserve`](Self::try_reserve)'s does.
    pub fn charge(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        engine: EngineId,
        size: usize,
    ) -> ReservationToken {
        let identity = ReleaseIdentity { db, tenant, engine };
        let mut scope = ReserveScope::new(Arc::clone(&self.global_counter), size, identity);

        scope.credit_global_unchecked();

        {
            let map = self
                .database_budgets
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(budget) = map.get(&db) {
                scope.credit_database(budget.credit(size));
            }
        }

        {
            let map = self
                .tenant_budgets
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(budget) = map.get(&(db, tenant)) {
                scope.credit_tenant(budget.credit(size));
            }
        }

        let engine_budget = &self.budgets[engine.index()];
        scope.credit_engine(engine_budget.credit_arc(size));

        let layers = scope.commit();
        token_from_layers(layers, size, db, tenant, engine)
    }
}

#[cfg(test)]
mod tests {
    // `Arc`, `DatabaseId` and `TenantId` are used only by the thread test that
    // wasm32 cannot run, so they are gated with it to keep the wasm test build
    // warning-free.
    #[cfg(not(target_arch = "wasm32"))]
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    #[cfg(not(target_arch = "wasm32"))]
    use std::thread;

    #[cfg(not(target_arch = "wasm32"))]
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::engine_limits::EngineLimits;
    use crate::governor::config::GovernorConfig;
    use crate::governor::test_support::{db, tenant, test_config};

    // ── Basic 4-arity reservation ────────────────────────────────────────────

    #[test]
    fn reserve_within_budget() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let tok = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 1000)
            .unwrap();
        assert_eq!(gov.budget(EngineId::Vector).allocated(), 1000);
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
            assert_eq!(gov.budget(EngineId::Vector).allocated(), 1000);
            assert_eq!(tok.size(), 1000);
        } // token dropped here

        assert_eq!(
            gov.budget(EngineId::Vector).allocated(),
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
        let engine_limits = EngineLimits::zeroed().with(EngineId::Vector, 200);
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

    // wasm32 has no thread spawn: `std::thread::spawn` fails with
    // `Unsupported`, and wasm targets abort on panic, so this test cannot run
    // there. It still runs in full on native.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn concurrent_reserves_never_exceed_cap() {
        let limits = EngineLimits::zeroed().with(EngineId::Vector, 10_000);
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

        let engine_alloc = gov.budget(EngineId::Vector).allocated();
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

    // ── Denied engine budget rejection and rollback ──────────────────────────
    //
    // `EngineId::Crdt` keeps `test_config`'s default zero limit — it still
    // carries a real `Budget` entry, so a 1000-byte reservation against it
    // denies with `MemError::BudgetExhausted`, never a missing-engine error.

    #[test]
    fn denied_engine_budget_leaves_the_global_counter_untouched() {
        let gov = MemoryGovernor::new(test_config()).unwrap();

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Crdt, 1000)
            .unwrap_err();
        assert!(matches!(err, MemError::BudgetExhausted { .. }));

        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "global counter must be rolled back when the engine budget denies"
        );
    }

    #[test]
    fn denied_engine_budget_leaves_the_database_counter_untouched() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 4096);

        let _ = gov
            .try_reserve(db(), tenant(), EngineId::Crdt, 1000)
            .unwrap_err();

        let db_map = gov.database_budgets.read().unwrap();
        assert_eq!(
            db_map[&db()].allocated.load(Ordering::Relaxed),
            0,
            "database counter must be rolled back when the engine budget denies"
        );
    }

    #[test]
    fn denied_engine_budget_leaves_the_tenant_counter_untouched() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 4096);
        gov.set_tenant_budget(db(), tenant(), 4096);

        let _ = gov
            .try_reserve(db(), tenant(), EngineId::Crdt, 1000)
            .unwrap_err();

        let tenant_map = gov.tenant_budgets.read().unwrap();
        assert_eq!(
            tenant_map[&(db(), tenant())]
                .allocated
                .load(Ordering::Relaxed),
            0,
            "tenant counter must be rolled back when the engine budget denies"
        );
    }

    #[test]
    fn rejected_reservations_do_not_consume_the_global_ceiling() {
        let gov = MemoryGovernor::new(test_config()).unwrap();

        // Nine rejected 1000-byte requests against Crdt's zero-byte engine budget.
        for _ in 0..9 {
            let _ = gov
                .try_reserve(db(), tenant(), EngineId::Crdt, 1000)
                .unwrap_err();
        }

        let tok = gov.try_reserve(db(), tenant(), EngineId::Vector, 1000);
        assert!(
            tok.is_ok(),
            "rejected reservations must not exhaust the global ceiling, got {:?}",
            tok.err()
        );
    }

    #[test]
    fn rejected_reservations_do_not_consume_a_database_quota() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 2000);

        for _ in 0..2 {
            let _ = gov
                .try_reserve(db(), tenant(), EngineId::Crdt, 1000)
                .unwrap_err();
        }

        let tok = gov.try_reserve(db(), tenant(), EngineId::Vector, 1000);
        assert!(
            tok.is_ok(),
            "rejected reservations must not exhaust the database quota, got {:?}",
            tok.err()
        );
    }
}
