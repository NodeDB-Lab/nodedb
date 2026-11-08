// SPDX-License-Identifier: Apache-2.0

//! Isolated native indexes for statement preparation.

use std::mem::size_of;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::FtsIndex;
use crate::backend::memory::MemoryBackend;
use crate::retained_size::sum;

impl FtsIndex<MemoryBackend> {
    /// Estimate retained bytes for an isolated preparation copy without allocating.
    ///
    /// Includes live postings, ended tokens, document bindings, metadata, segments, and memtable state.
    /// Counts inline BM25 parameters, spill config, and counters in the index size.
    /// Container and allocator overhead use conservative approximations, rather than exact allocator sizes.
    /// The shared memory governor requires no new retained allocation.
    /// Arithmetic saturates at `usize::MAX`.
    pub fn estimated_clone_bytes(&self) -> usize {
        sum([
            size_of::<Self>(),
            self.backend.estimated_clone_bytes(),
            self.memtable.estimated_clone_bytes(),
        ])
    }

    /// Copy all mutable storage and history for isolated statement preparation.
    ///
    /// Reserve `estimated_clone_bytes()` before calling this method.
    /// The copy preserves scoring, analyzer metadata, version spans, and the next segment identifier.
    /// Only the memory governor remains shared. Allocation follows standard Rust allocation behavior.
    pub fn clone_for_preparation(&self) -> Self {
        Self {
            backend: self.backend.clone_for_preparation(),
            bm25_params: self.bm25_params,
            memtable: self.memtable.clone_for_preparation(),
            next_segment_id: AtomicU64::new(self.next_segment_id.load(Ordering::Relaxed)),
            governor: Arc::clone(&self.governor),
        }
    }
}

#[cfg(test)]
#[path = "preparation_tests.rs"]
mod tests;
