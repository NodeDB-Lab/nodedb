// SPDX-License-Identifier: BUSL-1.1

//! Aggregate handler: GROUP BY, HAVING, and aggregate function execution.
//!
//! The generic (non-columnar) path uses **streaming accumulators** — see
//! `accum.rs`.  Raw document bytes are never stored; only the extracted
//! scalar / approximate values needed by each aggregate function are kept.
//! Memory is O(num_groups × num_aggregates) instead of
//! O(total_matching_docs × avg_doc_size).
//!
//! Split by concern so each file stays under the project's hard size limit:
//! `exec` (dispatch + fast paths), `streaming` (the spill-backed group-by
//! accumulation, itself split into accumulate / finalize / over_docs phases),
//! `cache_key` (result-cache key derivation), `rows` (post-aggregate alias
//! renaming and ORDER BY sorting), `state_emit` (the distributed-shuffle
//! partial-state producer), and `shuffle_merge` (the partial-state consumer).

mod cache_entry;
mod cache_key;
pub(in crate::data::executor) mod exec;
mod invalidate;
mod rows;
pub(in crate::data::executor) mod shuffle_merge;
pub(in crate::data::executor) mod state_emit;
mod streaming;

pub(in crate::data::executor) use cache_entry::AggregateCacheEntry;
