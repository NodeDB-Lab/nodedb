// SPDX-License-Identifier: Apache-2.0

//! Per-collection jemalloc arena registry.
//!
//! Each vector-primary collection receives a dedicated jemalloc arena so
//! HNSW + segment allocations are isolated from document-engine workloads.
//! On targets where jemalloc arena creation fails (e.g., Miri or custom
//! allocators), `arena_index` is `None` and the global allocator is used
//! transparently — callers need not special-case this.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{MemError, Result};

/// Opaque handle to a per-collection jemalloc arena.
///
/// Dropping the handle releases the registry entry. The underlying arena
/// index persists (jemalloc arenas are permanent once created).
#[derive(Debug, Clone)]
pub struct CollectionArenaHandle {
    /// The jemalloc arena index, or `None` when arena creation failed or
    /// the target does not support per-arena stats.
    arena_index: Option<u32>,
    /// Human-readable tag for diagnostics (`t{tenant_id}/{collection}`).
    tag: String,
}

impl CollectionArenaHandle {
    /// Returns the jemalloc arena index, if one was allocated.
    pub fn arena_index(&self) -> Option<u32> {
        self.arena_index
    }

    /// Returns the diagnostic tag.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Query the resident memory (bytes) in this arena.
    ///
    /// Returns `None` when no dedicated arena exists or the query fails.
    pub fn resident_bytes(&self) -> Option<u64> {
        let idx = self.arena_index?;
        read_arena_resident(idx).ok().map(|v| v as u64)
    }
}

/// Registry of per-collection jemalloc arenas.
///
/// Wrap in `Arc` to share between the Data Plane (writes) and the Control
/// Plane (stats reads). Internal locking is handled by the registry itself.
#[derive(Debug, Default)]
pub struct CollectionArenaRegistry {
    inner: Mutex<RegistryInner>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    handles: HashMap<(u64, String), CollectionArenaHandle>,
}

impl CollectionArenaRegistry {
    /// Create a new empty registry wrapped in `Arc`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Return (or create) a dedicated arena for `(tenant_id, collection)`.
    ///
    /// Idempotent: calling twice with the same key returns the same handle.
    pub fn get_or_create(&self, tenant_id: u64, collection: &str) -> Result<CollectionArenaHandle> {
        let key = (tenant_id, collection.to_string());
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| MemError::Jemalloc("collection arena registry lock poisoned".into()))?;
        if let Some(h) = guard.handles.get(&key) {
            return Ok(h.clone());
        }
        let handle = allocate_handle(tenant_id, collection);
        guard.handles.insert(key, handle.clone());
        Ok(handle)
    }

    /// Look up an existing handle without creating one. Returns `None` when
    /// the collection has no dedicated arena yet.
    pub fn get(&self, tenant_id: u64, collection: &str) -> Option<CollectionArenaHandle> {
        let guard = self.inner.lock().ok()?;
        guard
            .handles
            .get(&(tenant_id, collection.to_string()))
            .cloned()
    }
}

/// Build a handle, creating a new jemalloc arena when possible.
///
/// Failures (e.g., Miri or custom allocator) are silently downgraded to a
/// no-dedicated-arena handle rather than propagating an error, because arena
/// isolation is an optimisation, not a correctness requirement.
fn allocate_handle(tenant_id: u64, collection: &str) -> CollectionArenaHandle {
    let tag = format!("t{tenant_id}/{collection}");
    let arena_index = create_arena()
        .map_err(|e| {
            tracing::debug!(tag, error = %e, "per-collection arena creation failed; using global allocator");
        })
        .ok();
    CollectionArenaHandle { arena_index, tag }
}

/// Create a new jemalloc arena and return its index.
///
/// Without the `jemalloc` feature, or on wasm32, returns an error so that
/// `allocate_handle` degrades to a no-dedicated-arena handle transparently.
fn create_arena() -> Result<u32> {
    #[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
    {
        // SAFETY: `arenas.create` is a standard jemalloc mallctl that creates a
        // new arena and returns its unsigned index. No pointers are involved.
        let arena_idx: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.create\0") }
            .map_err(|e| MemError::Jemalloc(format!("failed to create collection arena: {e:?}")))?;
        Ok(arena_idx)
    }
    #[cfg(any(not(feature = "jemalloc"), target_arch = "wasm32"))]
    {
        Err(MemError::Jemalloc(
            "per-collection arenas need the jemalloc feature on a non-wasm32 target".into(),
        ))
    }
}

/// Query the resident memory (bytes) of a jemalloc arena.
///
/// Without the `jemalloc` feature, or on wasm32, always returns an error so
/// that callers degrade to returning `None`.
fn read_arena_resident(arena_index: u32) -> Result<usize> {
    #[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
    {
        // Bump the epoch so stats are up-to-date.
        if let Ok(mib) = tikv_jemalloc_ctl::epoch::mib() {
            let _ = mib.advance();
        }
        // `stats.arenas.<n>.resident` requires `--enable-stats` at jemalloc
        // compile time. Return an error (callers treat it as `None`) when not
        // available.
        let key = format!("stats.arenas.{arena_index}.resident\0");
        unsafe { tikv_jemalloc_ctl::raw::read::<usize>(key.as_bytes()) }
            .map_err(|e| MemError::Jemalloc(format!("failed to read arena resident: {e:?}")))
    }
    #[cfg(any(not(feature = "jemalloc"), target_arch = "wasm32"))]
    {
        let _ = arena_index;
        Err(MemError::Jemalloc(
            "arena resident stats need the jemalloc feature on a non-wasm32 target".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_create_and_retrieve() {
        let reg = CollectionArenaRegistry::new();
        let h1 = reg.get_or_create(1, "embeddings").expect("create");
        let h2 = reg.get_or_create(1, "embeddings").expect("re-fetch");
        // Arena index must be the same handle.
        assert_eq!(h1.arena_index(), h2.arena_index(), "same key → same arena");
        assert_eq!(h1.tag(), "t1/embeddings");
    }

    #[test]
    fn different_collections_get_different_arenas() {
        let reg = CollectionArenaRegistry::new();
        let h1 = reg.get_or_create(1, "col_a").expect("create a");
        let h2 = reg.get_or_create(1, "col_b").expect("create b");
        if h1.arena_index().is_some() && h2.arena_index().is_some() {
            assert_ne!(
                h1.arena_index(),
                h2.arena_index(),
                "distinct collections must get distinct arenas"
            );
        }
    }

    #[test]
    fn different_tenants_get_different_arenas() {
        let reg = CollectionArenaRegistry::new();
        let h1 = reg.get_or_create(1, "embeddings").expect("t1");
        let h2 = reg.get_or_create(2, "embeddings").expect("t2");
        if h1.arena_index().is_some() && h2.arena_index().is_some() {
            assert_ne!(
                h1.arena_index(),
                h2.arena_index(),
                "different tenants must get distinct arenas"
            );
        }
    }

    #[test]
    fn get_returns_none_for_unknown_collection() {
        let reg = CollectionArenaRegistry::new();
        assert!(reg.get(99, "unknown").is_none());
    }

    #[test]
    fn resident_bytes_does_not_panic() {
        let reg = CollectionArenaRegistry::new();
        let h = reg.get_or_create(1, "test_resident").expect("create");
        // Must not panic; may return None when stats are unavailable.
        let _ = h.resident_bytes();
    }
}
