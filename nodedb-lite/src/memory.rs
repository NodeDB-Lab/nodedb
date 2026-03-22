//! Edge Memory Governor — lightweight budget management.
//!
//! No jemalloc, no NUMA, no per-core arenas. Just soft limits and
//! usage tracking per engine. When the budget is exceeded, the governor
//! signals which engines should evict data.
//!
//! Evicted data is NOT lost — it remains in SQLite and is reloaded on
//! next access. This is a clean page-out, not data loss.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Default memory budget: 100 MB.
const DEFAULT_BUDGET_BYTES: usize = 100 * 1024 * 1024;

/// Per-engine allocation percentages (of total budget).
///
/// HNSW 40% | CSR 15% | Loro 15% | Query scratch 15% | Headroom 15%
const HNSW_PERCENT: usize = 40;
const CSR_PERCENT: usize = 15;
const LORO_PERCENT: usize = 15;
const QUERY_PERCENT: usize = 15;
// Remaining 15% = headroom (implicit, not tracked).

/// Engine identifier for budget partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineId {
    Hnsw,
    Csr,
    Loro,
    Query,
}

/// Memory pressure level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel {
    /// Under budget — no action needed.
    Normal,
    /// Approaching budget (>85%) — engines should reduce cache sizes.
    Warning,
    /// Over budget (>95%) — engines must evict immediately.
    Critical,
}

/// Edge Memory Governor.
///
/// Thread-safe: all counters are atomic. Multiple engine threads can
/// report usage concurrently without locking.
pub struct MemoryGovernor {
    total_budget: usize,
    hnsw_budget: usize,
    csr_budget: usize,
    loro_budget: usize,
    query_budget: usize,
    hnsw_used: AtomicUsize,
    csr_used: AtomicUsize,
    loro_used: AtomicUsize,
    query_used: AtomicUsize,
}

impl MemoryGovernor {
    /// Create a governor with the given total budget in bytes.
    pub fn new(total_budget: usize) -> Self {
        Self {
            total_budget,
            hnsw_budget: total_budget * HNSW_PERCENT / 100,
            csr_budget: total_budget * CSR_PERCENT / 100,
            loro_budget: total_budget * LORO_PERCENT / 100,
            query_budget: total_budget * QUERY_PERCENT / 100,
            hnsw_used: AtomicUsize::new(0),
            csr_used: AtomicUsize::new(0),
            loro_used: AtomicUsize::new(0),
            query_used: AtomicUsize::new(0),
        }
    }

    /// Create with the default 100 MB budget.
    pub fn default_budget() -> Self {
        Self::new(DEFAULT_BUDGET_BYTES)
    }

    /// Report current usage for an engine (absolute, not delta).
    pub fn report_usage(&self, engine: EngineId, bytes: usize) {
        match engine {
            EngineId::Hnsw => self.hnsw_used.store(bytes, Ordering::Relaxed),
            EngineId::Csr => self.csr_used.store(bytes, Ordering::Relaxed),
            EngineId::Loro => self.loro_used.store(bytes, Ordering::Relaxed),
            EngineId::Query => self.query_used.store(bytes, Ordering::Relaxed),
        }
    }

    /// Get the budget for a specific engine.
    pub fn budget_for(&self, engine: EngineId) -> usize {
        match engine {
            EngineId::Hnsw => self.hnsw_budget,
            EngineId::Csr => self.csr_budget,
            EngineId::Loro => self.loro_budget,
            EngineId::Query => self.query_budget,
        }
    }

    /// Get current usage for a specific engine.
    pub fn usage_for(&self, engine: EngineId) -> usize {
        match engine {
            EngineId::Hnsw => self.hnsw_used.load(Ordering::Relaxed),
            EngineId::Csr => self.csr_used.load(Ordering::Relaxed),
            EngineId::Loro => self.loro_used.load(Ordering::Relaxed),
            EngineId::Query => self.query_used.load(Ordering::Relaxed),
        }
    }

    /// Total memory used across all engines.
    pub fn total_used(&self) -> usize {
        self.hnsw_used.load(Ordering::Relaxed)
            + self.csr_used.load(Ordering::Relaxed)
            + self.loro_used.load(Ordering::Relaxed)
            + self.query_used.load(Ordering::Relaxed)
    }

    /// Total budget.
    pub fn total_budget(&self) -> usize {
        self.total_budget
    }

    /// Usage ratio (0.0 to 1.0+).
    pub fn usage_ratio(&self) -> f64 {
        self.total_used() as f64 / self.total_budget as f64
    }

    /// Current pressure level.
    pub fn pressure(&self) -> PressureLevel {
        let ratio = self.usage_ratio();
        if ratio >= 0.95 {
            PressureLevel::Critical
        } else if ratio >= 0.85 {
            PressureLevel::Warning
        } else {
            PressureLevel::Normal
        }
    }

    /// Check if a specific engine is over its budget.
    pub fn engine_over_budget(&self, engine: EngineId) -> bool {
        self.usage_for(engine) > self.budget_for(engine)
    }

    /// Per-engine pressure level.
    pub fn engine_pressure(&self, engine: EngineId) -> PressureLevel {
        let used = self.usage_for(engine) as f64;
        let budget = self.budget_for(engine) as f64;
        if budget == 0.0 {
            return PressureLevel::Critical;
        }
        let ratio = used / budget;
        if ratio >= 0.95 {
            PressureLevel::Critical
        } else if ratio >= 0.85 {
            PressureLevel::Warning
        } else {
            PressureLevel::Normal
        }
    }

    /// Get engines that should evict, ordered by most over-budget first.
    pub fn engines_to_evict(&self) -> Vec<EngineId> {
        let mut candidates: Vec<(EngineId, f64)> = [
            EngineId::Hnsw,
            EngineId::Csr,
            EngineId::Loro,
            EngineId::Query,
        ]
        .into_iter()
        .filter_map(|e| {
            let budget = self.budget_for(e) as f64;
            if budget == 0.0 {
                return Some((e, f64::MAX));
            }
            let ratio = self.usage_for(e) as f64 / budget;
            if ratio > 0.85 { Some((e, ratio)) } else { None }
        })
        .collect();

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.into_iter().map(|(e, _)| e).collect()
    }
}

impl Default for MemoryGovernor {
    fn default() -> Self {
        Self::default_budget()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget() {
        let g = MemoryGovernor::default_budget();
        assert_eq!(g.total_budget(), 100 * 1024 * 1024);
        assert_eq!(g.total_used(), 0);
        assert_eq!(g.pressure(), PressureLevel::Normal);
    }

    #[test]
    fn budget_partitioning() {
        let g = MemoryGovernor::new(100_000);
        assert_eq!(g.budget_for(EngineId::Hnsw), 40_000);
        assert_eq!(g.budget_for(EngineId::Csr), 15_000);
        assert_eq!(g.budget_for(EngineId::Loro), 15_000);
        assert_eq!(g.budget_for(EngineId::Query), 15_000);
    }

    #[test]
    fn report_and_query_usage() {
        let g = MemoryGovernor::new(100_000);
        g.report_usage(EngineId::Hnsw, 30_000);
        g.report_usage(EngineId::Csr, 10_000);

        assert_eq!(g.usage_for(EngineId::Hnsw), 30_000);
        assert_eq!(g.usage_for(EngineId::Csr), 10_000);
        assert_eq!(g.total_used(), 40_000);
    }

    #[test]
    fn pressure_levels() {
        let g = MemoryGovernor::new(100);
        assert_eq!(g.pressure(), PressureLevel::Normal);

        g.report_usage(EngineId::Hnsw, 86);
        assert_eq!(g.pressure(), PressureLevel::Warning);

        g.report_usage(EngineId::Csr, 10);
        assert_eq!(g.pressure(), PressureLevel::Critical);
    }

    #[test]
    fn engine_over_budget() {
        let g = MemoryGovernor::new(100_000);
        assert!(!g.engine_over_budget(EngineId::Hnsw));

        g.report_usage(EngineId::Hnsw, 50_000); // Budget is 40_000.
        assert!(g.engine_over_budget(EngineId::Hnsw));
        assert!(!g.engine_over_budget(EngineId::Csr));
    }

    #[test]
    fn engines_to_evict_ordered() {
        let g = MemoryGovernor::new(100_000);
        g.report_usage(EngineId::Hnsw, 38_000); // 95% of 40k = warning/critical.
        g.report_usage(EngineId::Csr, 14_000); // 93% of 15k.

        let evict = g.engines_to_evict();
        assert!(evict.contains(&EngineId::Hnsw));
        assert!(evict.contains(&EngineId::Csr));
        // HNSW should come first (higher ratio).
        assert_eq!(evict[0], EngineId::Hnsw);
    }

    #[test]
    fn usage_ratio() {
        let g = MemoryGovernor::new(200);
        g.report_usage(EngineId::Hnsw, 100);
        assert!((g.usage_ratio() - 0.5).abs() < 0.01);
    }
}
