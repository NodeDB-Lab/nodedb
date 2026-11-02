// SPDX-License-Identifier: BUSL-1.1

//! Commit-time first-committer-wins check on a transaction's writes.
//!
//! A transaction that replaces or deletes a row commits only while that
//! row's newest committed version sits at or below its snapshot. A fresh
//! insert finds no version and passes. Every value reads stored versions
//! only, so every replica that applied the same entries reaches one
//! verdict.

use crate::data::executor::task::ExecutionTask;
use crate::types::{KeyRepr, TenantId};

use super::super::CoreLoop;
use super::reads::at_or_below;

/// One row a transaction writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) struct TxnWrite {
    /// The collection, as the transaction's plans name it.
    pub collection: String,
    pub key: KeyRepr,
}

impl CoreLoop {
    /// Whether no row of `writes` holds a committed version above
    /// `snapshot`. A row whose engine no keyed lookup reads passes: its
    /// writes carry no per-key identity. `Err` when a store does not read.
    pub(in crate::data::executor) fn txn_writes_unchanged(
        &self,
        task: &ExecutionTask,
        tid: u64,
        writes: &[TxnWrite],
        snapshot: i64,
    ) -> crate::Result<bool> {
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        for write in writes {
            let newest = self
                .row_newest(db, tenant, &write.collection, &write.key)?
                .flatten();
            if !at_or_below(newest, snapshot) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
