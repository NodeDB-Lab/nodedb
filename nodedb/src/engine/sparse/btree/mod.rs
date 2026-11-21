// SPDX-License-Identifier: BUSL-1.1

//! The sparse engine handle, its current-state document API, and its
//! non-document tables.

pub mod chain_head;
pub mod crdt_dead_letter;
pub mod document;
pub mod engine;
pub mod maintained_index;
pub mod maintained_index_record;
pub mod redo_boot_verdict;
pub mod redo_install;
pub mod storage_bytes;
pub mod storage_conversion;
pub mod tables;
pub mod write_set_capture;

pub use engine::SparseEngine;
pub use maintained_index_record::{
    EntryShape, FenceVerdict, MaintainedIndexSet, MaintainedOwner, MaintainedPath, OwnerFence,
    TOP_LEVEL_OWNER, is_top_level_path, maintained_set_key, top_level_path,
};
pub(in crate::engine::sparse) use tables::redb_err;
pub(crate) use tables::{KeyedTable, invalid_storage_key_err};
