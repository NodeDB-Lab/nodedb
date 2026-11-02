// SPDX-License-Identifier: BUSL-1.1

//! `DocumentEngine` — MessagePack document storage with secondary indexing.
//!
//! Documents stored as versioned MessagePack blobs keyed by
//! `(collection, doc_id, sys_from)`. Declared index paths are extracted on
//! write into the versioned index keyed `(collection, path, value, doc_id,
//! sys_from)`.

pub mod batch;
pub mod delete;
pub mod get;
pub mod put;

pub use batch::DocumentEngine;
