// SPDX-License-Identifier: BUSL-1.1

//! Checkpoint spread / dirty page throttling.
//!
//! Prevents checkpoint storms by tracking dirty page counts per engine and
//! flushing incrementally (configurable % per tick), rate-limited by an I/O
//! budget. This decides WHEN an engine's accumulated writes get flushed between
//! coordinated checkpoints, so a checkpoint cycle does not arrive to a fully
//! dirty engine and stall the core flushing all of it at once.
//!
//! This is scheduling pressure and NOTHING ELSE. In particular it is **not
//! durability evidence, and nothing may treat it as such**:
//!
//!   - A dirty-page count is an estimate of pending work. It is incremented by
//!     write handlers that count rows, not pages, and an engine only appears
//!     here if some handler happens to call `mark_dirty` for it. `is_clean()`
//!     therefore means "no engine reports outstanding work to schedule", never
//!     "every engine is on stable storage".
//!   - Whether an engine's state actually survives a restart is answered ONLY
//!     by `handlers/control/checkpoint_durable_lsn.rs`, whose per-engine
//!     contributors flush and then report the LSN they truly made durable.
//!     `execute_checkpoint` folds `min` over those, and that fold is what
//!     authorises `WalManager::truncate_before` to unlink segments.
//!
//! This type deliberately holds no LSN. A `checkpoint_lsn` field set from the
//! raw watermark, with nothing reading it back, would be a second, parallel
//! "the watermark is durable" claim of exactly the kind that lets WAL
//! truncation delete the only copy of memory-only engine state. A scheduler
//! that cannot name an LSN cannot be misread as authorising a deletion, so the
//! LSN half is absent rather than merely unused.

use std::time::{Duration, Instant};

use tracing::debug;

/// Every engine the coordinator schedules flushes for.
///
/// Registration is driven off this one list so the registry cannot drift from
/// the handlers that call `mark_dirty` / `record_flush`. An engine missing here
/// makes those calls silent no-ops — the counter has nowhere to land, the
/// engine never appears in a flush plan, and its writes are never scheduled.
/// An entry here with no arm in `CoreLoop::maybe_run_maintenance` is the
/// mirror-image bug: it is planned every tick and never flushed, so its dirty
/// count only grows. Add to both sides or neither.
pub const TRACKED_ENGINES: &[&str] = &["sparse", "vector", "crdt", "timeseries", "columnar"];

/// Checkpoint configuration.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Fraction of dirty pages to flush per tick (0.0-1.0).
    /// Default: 0.10 = flush 10% per tick.
    pub flush_fraction: f64,
    /// Minimum interval between checkpoint ticks.
    pub tick_interval: Duration,
    /// Maximum dirty pages before forcing a full flush.
    pub force_flush_threshold: usize,
    /// Maximum I/O bytes per tick (rate limiting).
    pub io_budget_bytes_per_tick: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            flush_fraction: 0.10,
            tick_interval: Duration::from_secs(30),
            io_budget_bytes_per_tick: 64 * 1024 * 1024, // 64 MiB
            force_flush_threshold: 100_000,
        }
    }
}

/// Per-engine dirty page tracking.
#[derive(Debug, Clone)]
pub struct EngineCheckpointState {
    pub engine_name: String,
    pub dirty_pages: usize,
    pub total_flushed: u64,
    pub last_flush: Option<Instant>,
}

impl EngineCheckpointState {
    pub fn new(engine_name: &str) -> Self {
        Self {
            engine_name: engine_name.to_string(),
            dirty_pages: 0,
            total_flushed: 0,
            last_flush: None,
        }
    }

    /// Mark pages as dirty (called on writes).
    pub fn mark_dirty(&mut self, count: usize) {
        self.dirty_pages += count;
    }

    /// Compute how many pages to flush this tick.
    pub fn pages_to_flush(&self, config: &CheckpointConfig) -> usize {
        if self.dirty_pages >= config.force_flush_threshold {
            // Over threshold: flush everything to prevent stalling.
            self.dirty_pages
        } else {
            // Normal: flush a fraction.
            let target = (self.dirty_pages as f64 * config.flush_fraction).ceil() as usize;
            target.max(1).min(self.dirty_pages)
        }
    }

    /// Record that pages were flushed.
    pub fn record_flush(&mut self, count: usize) {
        self.dirty_pages = self.dirty_pages.saturating_sub(count);
        self.total_flushed += count as u64;
        self.last_flush = Some(Instant::now());
    }
}

/// Schedules incremental flushing across engines. Holds no durability state —
/// see the module docs for why it must never be read as if it did.
pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    engines: Vec<EngineCheckpointState>,
    last_tick: Option<Instant>,
}

impl CheckpointCoordinator {
    /// Build a coordinator with every engine in `TRACKED_ENGINES` registered.
    ///
    /// Registration is not a caller's choice: a coordinator missing an engine
    /// silently drops that engine's `mark_dirty` calls, so there is no useful
    /// partially-registered state to expose.
    pub fn new(config: CheckpointConfig) -> Self {
        let mut coord = Self {
            config,
            engines: Vec::new(),
            last_tick: None,
        };
        for name in TRACKED_ENGINES {
            coord.register_engine(name);
        }
        coord
    }

    /// Register one engine for flush scheduling. Private so `TRACKED_ENGINES`
    /// stays the only source of truth for which engines are tracked.
    fn register_engine(&mut self, name: &str) {
        if !self.engines.iter().any(|e| e.engine_name == name) {
            self.engines.push(EngineCheckpointState::new(name));
        }
    }

    /// Mark dirty pages for an engine (called on writes).
    pub fn mark_dirty(&mut self, engine: &str, count: usize) {
        if let Some(state) = self.engines.iter_mut().find(|e| e.engine_name == engine) {
            state.mark_dirty(count);
        }
    }

    /// Whether the tick interval has elapsed, so [`Self::tick`] plans a
    /// flush. Reads the clock and changes nothing.
    pub(crate) fn tick_is_due(&self) -> bool {
        self.last_tick
            .is_none_or(|last| last.elapsed() >= self.config.tick_interval)
    }

    /// Execute one checkpoint tick: compute pages to flush per engine.
    ///
    /// Returns `(engine_name, pages_to_flush)` pairs. The caller is
    /// responsible for actually performing the I/O and calling
    /// `record_flush()` after completion.
    ///
    /// Returns empty vec if the tick interval hasn't elapsed or
    /// there are no dirty pages.
    pub fn tick(&mut self) -> Vec<(String, usize)> {
        let now = Instant::now();

        // Respect tick interval.
        if let Some(last) = self.last_tick
            && now.duration_since(last) < self.config.tick_interval
        {
            return Vec::new();
        }
        self.last_tick = Some(now);

        let mut flush_plan = Vec::new();
        let mut budget_remaining = self.config.io_budget_bytes_per_tick;
        // Assume 4 KiB per page for budget calculation.
        let page_size = 4096;

        for engine in &self.engines {
            if engine.dirty_pages == 0 {
                continue;
            }
            let target = engine.pages_to_flush(&self.config);
            let budget_pages = budget_remaining / page_size;
            let actual = target.min(budget_pages);
            if actual > 0 {
                flush_plan.push((engine.engine_name.clone(), actual));
                budget_remaining = budget_remaining.saturating_sub(actual * page_size);
            }
        }

        if !flush_plan.is_empty() {
            debug!(
                engines = flush_plan.len(),
                total_pages = flush_plan.iter().map(|(_, p)| p).sum::<usize>(),
                "checkpoint tick: flushing"
            );
        }

        flush_plan
    }

    /// Record completed flush for an engine.
    pub fn record_flush(&mut self, engine: &str, count: usize) {
        if let Some(state) = self.engines.iter_mut().find(|e| e.engine_name == engine) {
            state.record_flush(count);
        }
    }

    /// Whether no engine reports outstanding work to schedule.
    ///
    /// This is a scheduling question, NOT a durability one: it says every
    /// tracked engine's pending-write estimate has been worked off, not that
    /// any engine is on stable storage. Only the per-engine durable LSNs
    /// folded by `execute_checkpoint` answer that.
    pub fn is_clean(&self) -> bool {
        self.engines.iter().all(|e| e.dirty_pages == 0)
    }

    /// Total dirty pages across all engines. Backlog depth for observability.
    pub fn total_dirty_pages(&self) -> usize {
        self.engines.iter().map(|e| e.dirty_pages).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_flush() {
        let config = CheckpointConfig {
            flush_fraction: 0.10,
            tick_interval: Duration::from_millis(0), // No delay for test.
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new(config);

        coord.mark_dirty("sparse", 100);
        coord.mark_dirty("vector", 50);

        let plan = coord.tick();
        assert!(!plan.is_empty());
        // Sparse: 10% of 100 = 10 pages.
        let sparse_flush = plan.iter().find(|(e, _)| e == "sparse").unwrap().1;
        assert_eq!(sparse_flush, 10);

        // Record flush.
        coord.record_flush("sparse", sparse_flush);
        assert_eq!(
            coord
                .engines
                .iter()
                .find(|e| e.engine_name == "sparse")
                .unwrap()
                .dirty_pages,
            90
        );
    }

    #[test]
    fn force_flush_over_threshold() {
        let config = CheckpointConfig {
            force_flush_threshold: 50,
            tick_interval: Duration::from_millis(0),
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new(config);
        coord.mark_dirty("sparse", 100); // Over threshold.

        let plan = coord.tick();
        let sparse_flush = plan.iter().find(|(e, _)| e == "sparse").unwrap().1;
        assert_eq!(sparse_flush, 100); // Force full flush.
    }

    #[test]
    fn clean_after_all_flushed() {
        let config = CheckpointConfig {
            flush_fraction: 1.0, // Flush everything.
            tick_interval: Duration::from_millis(0),
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new(config);
        coord.mark_dirty("sparse", 50);

        let plan = coord.tick();
        for (engine, count) in &plan {
            coord.record_flush(engine, *count);
        }
        assert!(coord.is_clean());
    }

    /// Every engine a handler can name must be registered, or its `mark_dirty`
    /// lands nowhere and its writes are never scheduled for flushing — the
    /// silent-no-op failure `TRACKED_ENGINES` exists to prevent.
    #[test]
    fn every_tracked_engine_accepts_dirty_pages() {
        let config = CheckpointConfig {
            tick_interval: Duration::from_millis(0),
            ..Default::default()
        };
        for engine in TRACKED_ENGINES {
            let mut coord = CheckpointCoordinator::new(config.clone());
            coord.mark_dirty(engine, 100);
            assert_eq!(
                coord.total_dirty_pages(),
                100,
                "mark_dirty({engine}) was silently dropped — engine not registered"
            );
            let plan = coord.tick();
            assert!(
                plan.iter().any(|(e, _)| e == engine),
                "{engine} marked dirty but absent from the flush plan"
            );
        }
    }
}
