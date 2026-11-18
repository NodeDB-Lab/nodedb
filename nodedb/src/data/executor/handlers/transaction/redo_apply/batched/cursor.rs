// SPDX-License-Identifier: BUSL-1.1

//! The durable progress of a batched install.
//!
//! After each batch the install commits its cursor ([`InstallCursor`]) in
//! a durable sparse transaction, then persists the edge store. A durable
//! sparse commit persists every commit begun before it, the ones a root
//! journal defers included: that is how the journal's write set commit
//! persists the effects before it. So once both commits return, every
//! row the install wrote through that batch, each row's fold ledger entry
//! and fold target rows, its edges and label versions, and the cursor that
//! names the batch are durable together. Under a root journal, a crash
//! before it leaves the state the previous batch's commit made durable.
//! With none, each row commit is durable at once, and no fold ledger row
//! is written.
//!
//! The commit that ends the install removes the cursor: the final write
//! set capture under a root journal, the cursor drop without one, or the
//! rollback end. So a live install's cursor exists exactly while the
//! install made effects durable and stored no capture.
//!
//! A cursor commit that fails leaves the stores ahead of any cursor. The
//! core fail-stops and rolls nothing back: restart replay completes the
//! record forward.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::wal::RedoStreamId;
use crate::wal::redo::install_cursor::{CursorState, InstallCursor};

impl CoreLoop {
    /// Make every effect of the install at `lsn` durable through the step
    /// `state` names, with its cursor. `rewind` sets the fold ledger back to
    /// the ops the durable state folded, for a rollback that reversed folds
    /// above it.
    pub(super) fn commit_batch_durable(
        &mut self,
        lsn: u64,
        stream: RedoStreamId,
        state: CursorState,
        rewind: Option<u64>,
    ) -> crate::Result<()> {
        let cursor = InstallCursor { lsn, stream, state }.to_bytes()?;
        // The cursor goes first. The edge store holds the node-label LSN
        // mark, and a mark durable without a cursor makes boot skip the
        // versions of the batches the crash cut. A cursor durable without
        // its edges is safe: it makes boot replay the record forward and
        // version it again.
        let txn = self.sparse.begin_durable_write()?;
        if let Some(folded_ops) = rewind {
            self.sparse
                .rewind_fold_ledger_in_txn(&txn, lsn, folded_ops)?;
        }
        self.sparse.put_install_cursor_in_txn(&txn, lsn, &cursor)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit the batched install cursor at lsn {lsn}: {e}"),
        })?;
        // A crash test kills the process here: the cursor is durable, and
        // the batch's edges and label versions are not.
        #[cfg(feature = "failpoints")]
        let at = batch_point(state);
        crate::fail_point!(
            self.fail_scope,
            &format!("redo_apply::batched::between_cursor_and_edges::{at}")
        );
        self.edge_store.persist()
    }

    /// End the install at `lsn` durably: persist the edge store, then remove
    /// the install's cursor, fold ledger and fold target rows in a durable
    /// sparse commit. The edges go first: once the cursor is gone, boot no
    /// longer replays the record forward, so every edge and label version
    /// must already be durable.
    pub(super) fn end_install_durable(&mut self, lsn: u64) -> crate::Result<()> {
        self.edge_store.persist()?;
        let txn = self.sparse.begin_durable_write()?;
        self.sparse.remove_redo_install_in_txn(&txn, &[lsn])?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit the end of the batched install at lsn {lsn}: {e}"),
        })
    }

    /// Stop the core: the install at `lsn` could not store its progress.
    /// The stores hold effects past its last cursor, so no later commit of
    /// the task may persist.
    pub(super) fn fail_install_progress(&mut self, lsn: u64, error: &crate::Error) {
        self.seal_journalled();
        self.fail_stop_core(
            FailStopCause::InstallProgressUnpersisted,
            &format!(
                "the batched install of the committed redo at lsn {lsn} could not store its \
                 durable progress: {error}"
            ),
        );
    }
}

/// The `<arm>::<batch>` the fail points of the step `state` names.
#[cfg(feature = "failpoints")]
pub(super) fn batch_point(state: CursorState) -> String {
    match state {
        CursorState::Installing { arm, batch, .. } | CursorState::RollingBack { arm, batch } => {
            format!("{arm:?}::{batch}")
        }
    }
}
