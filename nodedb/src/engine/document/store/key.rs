// SPDX-License-Identifier: BUSL-1.1

//! Re-exports of a row's surrogate identity types.
//!
//! The definitions live in `nodedb_types::row_identity`: `nodedb-physical`
//! and `nodedb-query` depend on `nodedb-types` but not on `nodedb`, and both
//! need these types on their own fields. This module keeps every existing
//! `crate::engine::document::store::{StorageKey, RowIdentity, identity_of,
//! surrogate_to_doc_id, doc_id_to_surrogate}` path compiling unchanged.
pub use nodedb_types::{
    RowIdentity, StorageKey, doc_id_to_surrogate, identity_of, surrogate_to_doc_id,
};
