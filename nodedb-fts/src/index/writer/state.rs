// SPDX-License-Identifier: Apache-2.0

//! Index state, construction, and backend access.

use crate::{
    backend::FtsBackend,
    lsm::memtable::{Memtable, MemtableConfig},
    posting::Bm25Params,
};
use nodedb_mem::MemoryGovernor;
use std::sync::{Arc, atomic::AtomicU64};

/// Full-text search index generic over storage backend.
///
/// Indexing, search, and highlighting use the [`FtsBackend`] storage contract.
///
/// Writes accumulate in an in-memory `Memtable`, one state per index.
/// Threshold-triggered flushing writes each index's postings to an
/// immutable segment of that index through the backend.
/// Queries merge the active memtable with stored segments.
/// The backend determines segment durability.
///
/// [`MemoryGovernor`] enforces per-engine memory budgets on large
/// allocations (compaction, segment merge, query term collection).
pub struct FtsIndex<B: FtsBackend> {
    pub(crate) backend: B,
    pub(crate) bm25_params: Bm25Params,
    pub(crate) memtable: Memtable,
    /// Monotonic segment ID counter.
    pub(super) next_segment_id: AtomicU64,
    /// Memory governor for budget enforcement.
    pub(crate) governor: Arc<MemoryGovernor>,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Create a new FTS index with the given backend, default BM25 params, and a memory governor.
    pub fn new(backend: B, governor: Arc<MemoryGovernor>) -> Self {
        Self {
            backend,
            bm25_params: Bm25Params::default(),
            memtable: Memtable::new(MemtableConfig::default()),
            next_segment_id: AtomicU64::new(1),
            governor,
        }
    }

    /// Create a new FTS index with custom BM25 parameters and a memory governor.
    pub fn with_params(backend: B, params: Bm25Params, governor: Arc<MemoryGovernor>) -> Self {
        Self {
            bm25_params: params,
            ..Self::new(backend, governor)
        }
    }

    /// Create a new FTS index whose memtable spills at `memtable` thresholds.
    /// Unreachable thresholds keep every posting in the memtable.
    pub fn with_memtable_config(
        backend: B,
        memtable: MemtableConfig,
        governor: Arc<MemoryGovernor>,
    ) -> Self {
        Self {
            memtable: Memtable::new(memtable),
            ..Self::new(backend, governor)
        }
    }

    /// Create a new FTS index with custom BM25 parameters whose memtable
    /// spills at `memtable` thresholds.
    pub fn with_config(
        backend: B,
        params: Bm25Params,
        memtable: MemtableConfig,
        governor: Arc<MemoryGovernor>,
    ) -> Self {
        Self {
            bm25_params: params,
            memtable: Memtable::new(memtable),
            ..Self::new(backend, governor)
        }
    }

    /// Access the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Mutable access to the underlying backend.
    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Access the active memtable (for LSM query merging).
    pub fn memtable(&self) -> &Memtable {
        &self.memtable
    }
}
