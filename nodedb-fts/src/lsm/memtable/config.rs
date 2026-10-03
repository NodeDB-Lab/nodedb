// SPDX-License-Identifier: Apache-2.0

//! Memtable spill thresholds.

/// Spill threshold: flush memtable when total posting entries exceed this.
pub const DEFAULT_SPILL_POSTINGS: usize = 32 * 1024 * 1024; // 32M entries

/// Spill threshold: flush when unique terms exceed this.
pub const DEFAULT_SPILL_TERMS: usize = 100_000;

/// Configuration for memtable spill thresholds. Both bound the whole
/// memtable: postings and terms are summed across every index it holds.
#[derive(Debug, Clone, Copy)]
pub struct MemtableConfig {
    pub max_postings: usize,
    pub max_terms: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_postings: DEFAULT_SPILL_POSTINGS,
            max_terms: DEFAULT_SPILL_TERMS,
        }
    }
}
