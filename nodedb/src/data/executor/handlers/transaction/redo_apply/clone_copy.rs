// SPDX-License-Identifier: BUSL-1.1

//! The apply-time rule of a clone copy.
//!
//! A clone copy installs a source row's version into the clone only where
//! the clone holds no version of that row, live or deleted. A write into the
//! clone therefore always wins over a copy, whatever order the two reach the
//! clone's shard in. The rule reads the state the record applies over, which
//! every replica and every restart replay holds at that log position, so
//! each reaches the same verdict.

use nodedb_types::{StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;

use super::sub_ops::RedoDocOp;

impl CoreLoop {
    /// Whether the document row `surrogate` of `collection` holds any
    /// version, live or tombstone.
    pub(in crate::data::executor) fn doc_row_holds_a_version(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) -> crate::Result<bool> {
        let doc_id = StorageKey::for_surrogate(Surrogate::new(surrogate));
        Ok(self
            .sparse
            .versioned_latest_sys_from(database_id, tid, collection, &doc_id)?
            .is_some())
    }

    /// `ops` without the clone copies the apply skips: a copy whose row
    /// already holds a version writes nothing, so no check judges it.
    pub(super) fn without_skipped_clone_copies(
        &self,
        database_id: u64,
        tid: u64,
        ops: Vec<RedoDocOp>,
    ) -> crate::Result<Vec<RedoDocOp>> {
        let mut kept = Vec::with_capacity(ops.len());
        for op in ops {
            let skipped = match &op {
                RedoDocOp::Put {
                    collection,
                    surrogate,
                    clone_copy: true,
                    ..
                } => self.doc_row_holds_a_version(database_id, tid, collection, *surrogate)?,
                RedoDocOp::Put { .. } | RedoDocOp::Delete { .. } => false,
            };
            if !skipped {
                kept.push(op);
            }
        }
        Ok(kept)
    }
}
