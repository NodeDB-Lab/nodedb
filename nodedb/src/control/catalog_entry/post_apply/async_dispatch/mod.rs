// SPDX-License-Identifier: BUSL-1.1

//! Asynchronous post-apply side effects.
//!
//! # Per-node contract
//!
//! `run_post_apply_async_side_effects` is awaited unconditionally on
//! **every node** (leader and followers alike) by the metadata
//! commit applier — there is NO `is_leader()` gate. Any side effect
//! that must fire on every replica (WAL tombstone append, Data Plane
//! `MetaOp::UnregisterCollection` dispatch, storage-reclaim spawning,
//! registry eviction that requires an async runtime) belongs here.
//!
//! Synchronous, always-per-node teardown (in-memory map eviction,
//! owner-row removal, grant invalidation) lives in the sibling
//! [`post_apply::sync`][sync] path, which is likewise called on every
//! replica. The two paths together form the complete per-node apply
//! contract for a committed catalog mutation.
//!
//! [sync]: crate::control::catalog_entry::post_apply::sync

pub mod array;
pub mod collection;
pub mod continuous_aggregate;
mod core_fanout;
pub mod crdt_compact;
mod dispatcher;
pub mod materialized_view;
pub mod move_tenant;
pub mod spatial_ref_sys;
pub mod synonym_group;
pub mod vector;

pub use dispatcher::run_post_apply_async_side_effects;
