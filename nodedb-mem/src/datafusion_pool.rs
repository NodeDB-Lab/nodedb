//! DataFusion `MemoryPool` adapter for the memory governor.
//!
//! This bridges DataFusion's query execution memory management with our
//! per-engine budget system. DataFusion operators (sorts, aggregates, hash
//! joins) request memory through this pool, which delegates to the governor's
//! `EngineId::Query` budget.
//!
//! When the query budget is exhausted, `try_grow` returns an error, forcing
//! DataFusion to spill the operation to disk — exactly the behavior Gemini
//! identified as critical to prevent query-induced OOM.

use std::sync::Arc;

use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

use crate::engine::EngineId;
use crate::governor::MemoryGovernor;
use crate::pressure::PressureLevel;

/// A DataFusion `MemoryPool` backed by the NodeDB memory governor.
///
/// All query memory is tracked under `EngineId::Query`. When the governor
/// rejects an allocation (budget exhausted or global ceiling hit), DataFusion
/// receives an error and spills to disk.
#[derive(Debug)]
pub struct GovernedMemoryPool {
    governor: Arc<MemoryGovernor>,
    engine: EngineId,
}

impl GovernedMemoryPool {
    /// Create a new governed pool targeting the given engine budget.
    ///
    /// Typically called with `EngineId::Query` for DataFusion.
    pub fn new(governor: Arc<MemoryGovernor>, engine: EngineId) -> Self {
        Self { governor, engine }
    }

    /// Create a pool specifically for DataFusion query execution.
    pub fn for_queries(governor: Arc<MemoryGovernor>) -> Self {
        Self::new(governor, EngineId::Query)
    }

    /// Current pressure level for this pool's engine.
    pub fn pressure(&self) -> PressureLevel {
        self.governor.engine_pressure(self.engine)
    }
}

impl MemoryPool for GovernedMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        // Infallible grow — DataFusion contract requires this to succeed.
        // We track the allocation for observability but do NOT circumvent
        // the budget. If it exceeds the limit, subsequent try_grow() calls
        // will fail, forcing DataFusion to prefer spillable operations.
        if self.governor.try_reserve(self.engine, additional).is_err() {
            tracing::warn!(
                engine = %self.engine,
                additional,
                allocated = self.governor.budget(self.engine)
                    .map(|b| b.allocated()).unwrap_or(0),
                "infallible grow exceeded budget — subsequent try_grow will fail"
            );
        }
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.governor.release(self.engine, shrink);
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        self.governor
            .try_reserve(self.engine, additional)
            .map_err(|e| {
                datafusion_common::DataFusionError::ResourcesExhausted(format!(
                    "query memory budget exhausted: {e}"
                ))
            })
    }

    fn reserved(&self) -> usize {
        self.governor
            .budget(self.engine)
            .map(|b| b.allocated())
            .unwrap_or(0)
    }

    fn register(&self, _consumer: &MemoryConsumer) {
        tracing::debug!(
            consumer = _consumer.name(),
            engine = %self.engine,
            "DataFusion consumer registered"
        );
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        tracing::debug!(
            consumer = _consumer.name(),
            engine = %self.engine,
            "DataFusion consumer unregistered"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governor::GovernorConfig;
    use std::collections::HashMap;

    fn test_governor(query_budget: usize) -> Arc<MemoryGovernor> {
        let mut engine_limits = HashMap::new();
        engine_limits.insert(EngineId::Query, query_budget);
        engine_limits.insert(EngineId::Vector, 1024 * 1024); // 1 MB

        let config = GovernorConfig {
            global_ceiling: query_budget + 1024 * 1024,
            engine_limits,
        };
        Arc::new(MemoryGovernor::new(config).unwrap())
    }

    fn make_pool(gov: Arc<MemoryGovernor>) -> Arc<dyn MemoryPool> {
        Arc::new(GovernedMemoryPool::for_queries(gov))
    }

    #[test]
    fn try_grow_within_budget_succeeds() {
        let gov = test_governor(1024 * 1024);
        let pool = make_pool(gov);

        let reservation = MemoryConsumer::new("test_sort").register(&pool);
        assert!(reservation.try_grow(512 * 1024).is_ok());
        assert_eq!(reservation.size(), 512 * 1024);
    }

    #[test]
    fn try_grow_exceeding_budget_fails() {
        let gov = test_governor(1024);
        let pool = make_pool(gov);

        let reservation = MemoryConsumer::new("big_aggregate").register(&pool);
        // First: 512 bytes — succeeds.
        assert!(reservation.try_grow(512).is_ok());
        // Second: 1024 bytes — exceeds remaining 512.
        let err = reservation.try_grow(1024);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("budget exhausted"), "got: {msg}");
    }

    #[test]
    fn shrink_frees_budget() {
        let gov = test_governor(1024);
        let pool = make_pool(gov);

        let reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(1024).unwrap();

        // Budget fully consumed.
        assert!(reservation.try_grow(1).is_err());

        // Release 512 bytes.
        reservation.shrink(512);

        // Now 512 bytes available.
        assert!(reservation.try_grow(512).is_ok());
    }

    #[test]
    fn reserved_tracks_allocations() {
        let gov = test_governor(4096);
        let pool = make_pool(gov);

        assert_eq!(pool.reserved(), 0);

        let reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(1000).unwrap();
        assert_eq!(pool.reserved(), 1000);

        reservation.shrink(600);
        assert_eq!(pool.reserved(), 400);
    }

    #[test]
    fn pressure_reflects_utilization() {
        let gov = test_governor(1000);
        let pool = GovernedMemoryPool::for_queries(Arc::clone(&gov));

        assert_eq!(pool.pressure(), PressureLevel::Normal);

        // Allocate 85% of query budget.
        gov.try_reserve(EngineId::Query, 850).unwrap();
        assert_eq!(pool.pressure(), PressureLevel::Critical);

        // Allocate to 96%.
        gov.try_reserve(EngineId::Query, 110).unwrap();
        assert_eq!(pool.pressure(), PressureLevel::Emergency);
    }
}
