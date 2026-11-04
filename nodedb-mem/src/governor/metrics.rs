// SPDX-License-Identifier: Apache-2.0

//! Read-only reporting surface: utilization, pressure, over-release counts,
//! and the engine snapshot.

use std::sync::atomic::Ordering;

use nodedb_types::DatabaseId;

use super::core::MemoryGovernor;
use crate::engine::EngineId;
use crate::pressure::{PressureLevel, PressureThresholds};

impl MemoryGovernor {
    /// Total memory allocated across all engines (engine-layer sum). A
    /// separate aggregate from [`global_utilization_percent`](Self::global_utilization_percent),
    /// which reads the global counter admission enforces the ceiling against.
    pub fn total_allocated(&self) -> usize {
        self.budgets.iter().map(|b| b.allocated()).sum()
    }

    /// Total number of over-release events observed across every layer
    /// (global, database, tenant, engine). A non-zero value signals at
    /// least one call site is releasing more bytes than it reserved — the
    /// "memory release exceeds allocation" warning class. A saturating
    /// release clamps the affected counter to zero, so this is the only
    /// post-hoc observable for the bug.
    pub fn total_over_release_count(&self) -> usize {
        self.global_counter.over_release.total()
    }

    /// Over-release events recorded against the global ceiling counter.
    pub fn global_over_release_count(&self) -> usize {
        self.global_counter.over_release.global()
    }

    /// Over-release events recorded against any per-database counter.
    pub fn database_over_release_count(&self) -> usize {
        self.global_counter.over_release.database()
    }

    /// Over-release events recorded against any per-tenant counter.
    pub fn tenant_over_release_count(&self) -> usize {
        self.global_counter.over_release.tenant()
    }

    /// Over-release events recorded against any per-engine counter.
    pub fn engine_over_release_count(&self) -> usize {
        self.global_counter.over_release.engine()
    }

    /// Global utilization as a percentage (0-100). Reads the global counter
    /// directly — the same quantity admission enforces the ceiling
    /// against — computed in `u128` so a corrupted count clamps to 100 %
    /// instead of overflowing.
    pub fn global_utilization_percent(&self) -> u8 {
        let allocated = self.global_counter.allocated.load(Ordering::Relaxed);
        if self.global_ceiling == 0 {
            return if allocated == 0 { 0 } else { 100 };
        }
        ((allocated as u128 * 100) / self.global_ceiling as u128).min(100) as u8
    }

    /// Current pressure level for a specific engine.
    pub fn engine_pressure(&self, engine: EngineId) -> PressureLevel {
        self.thresholds
            .level_for(self.budgets[engine.index()].utilization_percent())
    }

    /// Current global pressure level.
    pub fn global_pressure(&self) -> PressureLevel {
        self.thresholds.level_for(self.global_utilization_percent())
    }

    /// Worst-case (highest) pressure level across every engine. Cheap:
    /// iterates the in-memory budget array and allocates nothing — meant to
    /// be called once per Data-Plane core-loop tick, unlike
    /// [`snapshot`](Self::snapshot) which materialises a `Vec`.
    pub fn worst_engine_pressure(&self) -> PressureLevel {
        self.budgets
            .iter()
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
        EngineId::ALL
            .iter()
            .zip(self.budgets.iter())
            .map(|(&engine, budget)| EngineSnapshot {
                engine,
                allocated: budget.allocated(),
                limit: budget.limit(),
                peak: budget.peak(),
                rejections: budget.rejections(),
                utilization_percent: budget.utilization_percent(),
            })
            .collect()
    }

    /// Allocated bytes and cap for every database that holds or held memory.
    ///
    /// A database appears from its first reserve until `clear_database_scope`.
    /// Order is unspecified. Takes the map read lock once.
    pub fn database_usage_snapshot(&self) -> Vec<DatabaseUsage> {
        let map = self
            .database_budgets
            .read()
            .unwrap_or_else(|p| p.into_inner());
        map.iter()
            .map(|(&database_id, budget)| DatabaseUsage {
                database_id,
                allocated_bytes: budget.allocated.load(Ordering::Relaxed),
                limit_bytes: budget.limit,
            })
            .collect()
    }
}

/// Point-in-time memory use of one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseUsage {
    pub database_id: DatabaseId,
    pub allocated_bytes: usize,
    /// `None` means the database has no cap.
    pub limit_bytes: Option<usize>,
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
    use super::*;
    use crate::governor::test_support::{db, tenant, test_config};

    #[test]
    fn snapshot_reports_all_engines() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let _tok = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 2048)
            .unwrap();

        let snap = gov.snapshot();
        assert_eq!(snap.len(), EngineId::COUNT);

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

    fn usage_of(gov: &MemoryGovernor, id: DatabaseId) -> Option<DatabaseUsage> {
        gov.database_usage_snapshot()
            .into_iter()
            .find(|u| u.database_id == id)
    }

    #[test]
    fn usage_snapshot_lists_capped_and_uncapped_databases() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let capped = DatabaseId::new(1);
        let uncapped = DatabaseId::new(2);
        gov.set_database_budget(capped, 1000);
        let _a = gov
            .try_reserve(capped, tenant(), EngineId::Vector, 300)
            .unwrap();
        let _b = gov
            .try_reserve(uncapped, tenant(), EngineId::Vector, 200)
            .unwrap();

        assert_eq!(gov.database_usage_snapshot().len(), 2);
        let c = usage_of(&gov, capped).unwrap();
        assert_eq!((c.allocated_bytes, c.limit_bytes), (300, Some(1000)));
        let u = usage_of(&gov, uncapped).unwrap();
        assert_eq!((u.allocated_bytes, u.limit_bytes), (200, None));
    }

    #[test]
    fn usage_snapshot_covers_charged_memory_and_drops_on_release() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let tok = gov.charge(db(), tenant(), EngineId::Vector, 128);
        assert_eq!(usage_of(&gov, db()).unwrap().allocated_bytes, 128);
        drop(tok);
        assert_eq!(usage_of(&gov, db()).unwrap().allocated_bytes, 0);
    }

    #[test]
    fn clear_database_scope_removes_capped_and_uncapped_usage() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let capped = DatabaseId::new(1);
        let uncapped = DatabaseId::new(2);
        gov.set_database_budget(capped, 1000);
        let _a = gov
            .try_reserve(capped, tenant(), EngineId::Vector, 10)
            .unwrap();
        let _b = gov
            .try_reserve(uncapped, tenant(), EngineId::Vector, 10)
            .unwrap();

        gov.clear_database_scope(capped);
        gov.clear_database_scope(uncapped);

        assert!(gov.database_usage_snapshot().is_empty());
    }
}
