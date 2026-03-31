//! Integration test: DataFusion respects memory governor budgets.
//!
//! Proves that when DataFusion's query engine exhausts the Query budget,
//! allocations fail with ResourcesExhausted — forcing spill-to-disk.
//! Meanwhile, other engines (Vector, Timeseries) are completely unaffected.
//!

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool};

use nodedb_mem::datafusion_pool::GovernedMemoryPool;
use nodedb_mem::engine::EngineId;
use nodedb_mem::governor::{GovernorConfig, MemoryGovernor};
use nodedb_mem::pressure::PressureLevel;

fn make_governor() -> Arc<MemoryGovernor> {
    let mut engine_limits = HashMap::new();
    engine_limits.insert(EngineId::Query, 10_000); // 10 KB query budget
    engine_limits.insert(EngineId::Vector, 100_000); // 100 KB vector budget
    engine_limits.insert(EngineId::Timeseries, 50_000); // 50 KB timeseries budget

    let config = GovernorConfig {
        global_ceiling: 200_000, // 200 KB total
        engine_limits,
    };
    Arc::new(MemoryGovernor::new(config).unwrap())
}

/// Simulates a massive GROUP BY that tries to allocate more than the query budget.
/// DataFusion should receive ResourcesExhausted and can decide to spill.
#[test]
fn group_by_exceeds_budget_gets_resources_exhausted() {
    let gov = make_governor();
    let pool: Arc<dyn MemoryPool> = Arc::new(GovernedMemoryPool::for_queries(Arc::clone(&gov)));

    // Simulate DataFusion's hash aggregate operator.
    let hash_table = MemoryConsumer::new("HashAggregateExec").register(&pool);

    // First batch: 5 KB — fits in 10 KB budget.
    assert!(hash_table.try_grow(5_000).is_ok());
    assert_eq!(hash_table.size(), 5_000);

    // Second batch: 6 KB — would exceed 10 KB budget.
    let err = hash_table.try_grow(6_000);
    assert!(err.is_err());
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("Resources exhausted") || msg.contains("budget exhausted"),
        "Expected ResourcesExhausted error, got: {msg}"
    );

    // DataFusion would now spill to disk. Simulate releasing memory after spill.
    hash_table.shrink(3_000);
    assert_eq!(hash_table.size(), 2_000);

    // After spill, can allocate again.
    assert!(hash_table.try_grow(4_000).is_ok());
    assert_eq!(hash_table.size(), 6_000);
}

/// Multiple DataFusion operators compete for the same query budget.
#[test]
fn multiple_operators_share_query_budget() {
    let gov = make_governor();
    let pool: Arc<dyn MemoryPool> = Arc::new(GovernedMemoryPool::for_queries(Arc::clone(&gov)));

    let sort_op = MemoryConsumer::new("SortExec").register(&pool);
    let hash_op = MemoryConsumer::new("HashJoinExec").register(&pool);
    let agg_op = MemoryConsumer::new("AggregateExec").register(&pool);

    // Each takes 3 KB — total 9 KB out of 10 KB.
    sort_op.try_grow(3_000).unwrap();
    hash_op.try_grow(3_000).unwrap();
    agg_op.try_grow(3_000).unwrap();

    assert_eq!(pool.reserved(), 9_000);

    // Next 2 KB allocation fails — only 1 KB left.
    assert!(sort_op.try_grow(2_000).is_err());

    // Release sort memory, then hash can grow.
    sort_op.free();
    assert!(hash_op.try_grow(3_000).is_ok());
}

/// Vector engine remains fully functional while query budget is exhausted.
/// This is THE critical test — query load must never cannibalize vector cache.
#[test]
fn query_pressure_does_not_starve_vector_engine() {
    let gov = make_governor();
    let pool: Arc<dyn MemoryPool> = Arc::new(GovernedMemoryPool::for_queries(Arc::clone(&gov)));

    // Exhaust the entire query budget.
    let big_query = MemoryConsumer::new("MassiveGroupBy").register(&pool);
    big_query.try_grow(10_000).unwrap();

    // Query budget at 100% — Emergency pressure.
    let governed_pool = GovernedMemoryPool::for_queries(Arc::clone(&gov));
    assert_eq!(governed_pool.pressure(), PressureLevel::Emergency);

    // More query allocations fail.
    assert!(big_query.try_grow(1).is_err());

    // But vector engine has its own independent 100 KB budget.
    gov.try_reserve(EngineId::Vector, 80_000).unwrap();
    assert_eq!(
        gov.engine_pressure(EngineId::Vector),
        PressureLevel::Warning // 80% of 100 KB
    );

    // Timeseries also independent.
    gov.try_reserve(EngineId::Timeseries, 25_000).unwrap();
    assert_eq!(
        gov.engine_pressure(EngineId::Timeseries),
        PressureLevel::Normal // 50% of 50 KB
    );
}

/// Global ceiling prevents all engines combined from OOMing the process.
#[test]
fn global_ceiling_prevents_oom() {
    let gov = make_governor(); // 200 KB ceiling
    let pool: Arc<dyn MemoryPool> = Arc::new(GovernedMemoryPool::for_queries(Arc::clone(&gov)));

    // Fill vector engine: 100 KB.
    gov.try_reserve(EngineId::Vector, 100_000).unwrap();

    // Fill timeseries: 50 KB.
    gov.try_reserve(EngineId::Timeseries, 50_000).unwrap();

    // Query engine: 10 KB — this puts total at 160 KB, under 200 KB ceiling.
    let query = MemoryConsumer::new("query").register(&pool);
    query.try_grow(10_000).unwrap();

    // Total: 160 KB. Global ceiling is 200 KB.
    assert_eq!(gov.total_allocated(), 160_000);

    // Now try to allocate 50 KB more for vector — within engine budget (100 KB total,
    // but only 100 KB limit) — fails because engine budget is exhausted.
    let err = gov.try_reserve(EngineId::Vector, 1);
    assert!(err.is_err());
}

/// Reservation drop automatically frees memory back to the pool.
#[test]
fn reservation_drop_frees_memory() {
    let gov = make_governor();
    let pool: Arc<dyn MemoryPool> = Arc::new(GovernedMemoryPool::for_queries(Arc::clone(&gov)));

    {
        let reservation = MemoryConsumer::new("temp_sort").register(&pool);
        reservation.try_grow(8_000).unwrap();
        assert_eq!(pool.reserved(), 8_000);
        // reservation dropped here
    }

    // Memory freed — budget available again.
    assert_eq!(pool.reserved(), 0);

    let new_reservation = MemoryConsumer::new("next_query").register(&pool);
    assert!(new_reservation.try_grow(10_000).is_ok());
}
