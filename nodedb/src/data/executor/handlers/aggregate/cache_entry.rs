// SPDX-License-Identifier: BUSL-1.1

//! Aggregate/facet result-cache entry: the cached payload plus the KV write
//! epoch it was computed against.

/// One entry in `CoreLoop::aggregate_cache`.
///
/// `kv_epoch` is `KvEngine::write_epoch(database_id, tid, collection)` at the
/// moment `payload` was computed. It is `0` for a non-KV collection — its
/// table hash never appears in the KV engine's epoch map — so the epoch
/// comparison at lookup time always matches there and explicit invalidation
/// (`invalidate_aggregate_cache_for_collection`) stays the only eviction path
/// for non-KV writes, exactly as before this entry carried an epoch.
#[derive(Debug, Clone)]
pub(in crate::data::executor) struct AggregateCacheEntry {
    pub kv_epoch: u64,
    pub payload: Vec<u8>,
}
