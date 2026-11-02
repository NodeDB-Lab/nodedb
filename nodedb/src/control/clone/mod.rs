// SPDX-License-Identifier: BUSL-1.1

//! Copy-on-write clone resolver for the Control Plane.
//!
//! Intercepts read and write plans that target cloned databases and applies
//! the CoW resolution algorithm so storage engines never see `cloned_from`.

pub mod catalog_copy;
pub mod copy;
pub mod copyup;
pub(crate) mod cow_entry;
pub(crate) mod identity;
pub mod lsn_resolve;
pub mod metadata;
pub mod resolver;
pub mod tombstone;

pub use catalog_copy::copy_database_metadata;
pub use lsn_resolve::wall_ms_to_lsn;
pub use metadata::ClonePredicatesNote;
pub use resolver::{CloneReadParams, resolve_read};
pub use tombstone::{KvTombstoneParams, perform_clone_tombstone, perform_kv_clone_tombstone};
