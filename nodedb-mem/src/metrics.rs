// SPDX-License-Identifier: Apache-2.0

//! Metrics export for the memory governor.
//!
//! Provides jemalloc introspection to report actual RSS, mapped memory,
//! and arena statistics alongside the governor's logical budget tracking.
//!
//! Without the `jemalloc` feature, or on wasm32, `SystemMemoryStats::query()`
//! always returns `None` because jemalloc introspection is unavailable.

/// System memory statistics from jemalloc.
#[derive(Debug, Clone)]
pub struct SystemMemoryStats {
    /// Resident Set Size — actual physical memory used.
    pub rss_bytes: usize,

    /// Total bytes allocated by the application.
    pub allocated_bytes: usize,

    /// Total bytes in active pages (mapped and potentially dirty).
    pub active_bytes: usize,

    /// Total bytes mapped by the allocator (may exceed active).
    pub mapped_bytes: usize,

    /// Total bytes retained in the allocator's caches.
    pub retained_bytes: usize,

    /// Fragmentation ratio: `(active - allocated) / active`.
    ///
    /// Measures how much memory the allocator has claimed from the OS
    /// (active pages) but isn't actually used by application objects.
    /// This gap comes from jemalloc internal metadata, free-list
    /// fragmentation, and thread cache overhead.
    ///
    /// Healthy: < 0.15 (15%). Warning: 0.15–0.25. Critical: > 0.25.
    /// A sustained ratio above 0.25 indicates severe fragmentation —
    /// the process is consuming significantly more RSS than its live
    /// data warrants, and OOM risk increases under memory pressure.
    pub fragmentation_ratio: f64,
}

impl SystemMemoryStats {
    /// Query jemalloc for current system memory statistics.
    ///
    /// Returns `None` if jemalloc introspection is unavailable, including
    /// without the `jemalloc` feature and on wasm32.
    pub fn query() -> Option<Self> {
        #[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
        {
            // Trigger a stats epoch refresh.
            let _ = tikv_jemalloc_ctl::epoch::advance();

            let allocated = tikv_jemalloc_ctl::stats::allocated::read().ok()?;
            let active = tikv_jemalloc_ctl::stats::active::read().ok()?;
            let mapped = tikv_jemalloc_ctl::stats::mapped::read().ok()?;
            let retained = tikv_jemalloc_ctl::stats::retained::read().ok()?;
            let resident = tikv_jemalloc_ctl::stats::resident::read().ok()?;

            // Fragmentation: how much of jemalloc's active memory is wasted.
            // active = pages the allocator has obtained from the OS.
            // allocated = bytes the application is actually using.
            // The difference is internal fragmentation + free-list overhead.
            let fragmentation_ratio = if active > 0 {
                (active.saturating_sub(allocated)) as f64 / active as f64
            } else {
                0.0
            };

            Some(Self {
                rss_bytes: resident,
                allocated_bytes: allocated,
                active_bytes: active,
                mapped_bytes: mapped,
                retained_bytes: retained,
                fragmentation_ratio,
            })
        }
        #[cfg(any(not(feature = "jemalloc"), target_arch = "wasm32"))]
        {
            None
        }
    }

    /// Returns `true` if fragmentation exceeds the warning threshold (25%).
    pub fn is_fragmentation_critical(&self) -> bool {
        self.fragmentation_ratio > 0.25
    }
}
