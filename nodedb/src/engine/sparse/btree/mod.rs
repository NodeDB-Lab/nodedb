// SPDX-License-Identifier: BUSL-1.1

//! redb-backed B-Tree storage for the sparse engine's non-versioned tables.

pub mod chain_head;
pub mod document;
pub mod engine;
pub mod keys;
pub mod rename;
pub mod tables;

pub use engine::SparseEngine;
pub(crate) use keys::coll_prefix;
pub(in crate::engine::sparse) use keys::{tenant_prefix, with_tenant_key4};
pub(crate) use tables::DOCUMENTS;
pub(in crate::engine::sparse) use tables::{INDEXES, invalid_storage_key_err, redb_err};
