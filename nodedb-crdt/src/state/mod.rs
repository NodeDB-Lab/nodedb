// SPDX-License-Identifier: Apache-2.0

//! CRDT state management backed by loro.
//!
//! Each `CrdtState` wraps a `LoroDoc` representing one tenant/namespace's
//! state. Collections within the doc are `LoroMap` instances keyed by row ID,
//! where each row is itself a `LoroMap` of field→value.

pub mod bitemporal_archive;
pub mod core;
pub(crate) mod document_cell;
pub mod frontier_digest;
pub mod history;
pub(crate) mod import_admission;
pub mod preview;
pub mod rekey;
pub mod remove_fields;
pub(crate) mod restore_containers;
pub mod snapshot;
pub mod write_set;

pub use core::CrdtState;
pub use import_admission::{
    CrdtImportLimits, DEFAULT_MAX_IMPORT_BYTES, DEFAULT_MAX_IMPORT_OPS, ImportAdmission,
};
pub use preview::{
    CrdtDeltaPreview, CrdtDeltaPreviewLimits, DEFAULT_MAX_DELTA_BYTES,
    DEFAULT_MAX_ENCODED_DELTA_OPS, DEFAULT_MAX_POST_IMAGE_BYTES,
};
