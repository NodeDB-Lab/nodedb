// SPDX-License-Identifier: BUSL-1.1

//! Install source rows into a clone at the source versions' own stamps.

pub mod commit;
pub mod rows;

pub use commit::{CopyTarget, commit_doc_copies, commit_kv_copies};
pub use rows::{DocCopy, KvCopy};
