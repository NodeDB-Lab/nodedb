//! # nodedb-mem
//!
//! Global NUMA-aware memory governor for NodeDB.
//!
//! Prevents subsystem OOM and cache cannibalization by enforcing per-engine
//! memory budgets backed by jemalloc's introspection APIs.
//!
//! ## Problem
//!
//! If DataFusion does a massive `GROUP BY`, it allocates RAM until OOM kills
//! the process — taking Glommio threads, HNSW caches, and open io_uring
//! submissions down with it.
//!
//! If the timeseries engine flushes 5 GB of Gorilla-encoded segments, it can
//! evict the vector engine's hot HNSW routing layers from the OS page cache.
//!
//! ## Solution
//!
//! A centralized memory governor that:
//!
//! 1. Tracks allocations per engine (Vector, Sparse, CRDT, Timeseries, Query).
//! 2. Enforces hard limits — allocation requests beyond the budget are rejected
//!    with a deterministic error, forcing the caller to spill or backpressure.
//! 3. Supports dynamic rebalancing — the governor can shift budget from idle
//!    engines to active ones within the global ceiling.
//! 4. Exposes metrics for all budget states and breach events.
//!
//! ## Validation target
//!
//! Under a mixed workload (vector search + bulk timeseries ingest + SQL GROUP BY),
//! no single engine should exceed its budget, and total RSS should stay within
//! the configured global ceiling.

pub mod arena;
pub mod budget;
pub mod engine;
pub mod error;
pub mod governor;
pub mod metrics;
pub mod overflow;
pub mod pressure;
pub mod spill;

pub use budget::Budget;
pub use engine::EngineId;
pub use error::{MemError, Result};
pub use governor::MemoryGovernor;
pub use overflow::OverflowRegion;
pub use pressure::{PressureLevel, PressureThresholds};
pub use spill::{SpillAction, SpillConfig, SpillController};
