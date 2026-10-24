// SPDX-License-Identifier: BUSL-1.1

//! Undo of one TRUNCATE share's edge cut.
//!
//! The install records the cut and marks the collection's summary row for a
//! scan. The undo removes what that install recorded, and puts each edge the
//! cut changed back into the CSR as it was.

use tracing::error;

use super::UndoError;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::graph::edge_store::EdgeCutInstall;
use crate::types::{DatabaseId, TenantId};

/// What one cut install changed.
pub(in crate::data::executor) struct EdgeCutUndo {
    pub database_id: u64,
    pub tid: u64,
    pub install: EdgeCutInstall,
}

impl CoreLoop {
    /// Reverse one cut install.
    pub(in crate::data::executor) fn apply_undo_edge_cut(
        &mut self,
        entry_index: usize,
        undo: EdgeCutUndo,
    ) -> Result<(), UndoError> {
        let EdgeCutUndo {
            database_id,
            tid,
            install,
        } = undo;
        let core = self.core_id;
        let fail = |action: String, cause: crate::Error| {
            let err = UndoError::failed(entry_index, action, cause);
            error!(
                core,
                entry_index,
                error = %err,
                "transaction undo: edge cut rollback failed; shard state unknown"
            );
            err
        };
        // A historical CSR at or above the cut read what the cut hid.
        self.historical_csr.invalidate_collection(
            DatabaseId::new(database_id),
            TenantId::new(tid),
            &install.collection,
        );
        self.edge_store
            .remove_edge_cut(DatabaseId::new(database_id), TenantId::new(tid), &install)
            .map_err(|e| {
                fail(
                    format!(
                        "removing the cut of '{}' at {}",
                        install.collection, install.cut
                    ),
                    e,
                )
            })?;
        for flip in &install.flips {
            self.mirror_edge_csr(
                database_id,
                tid,
                (&flip.src, &flip.label, &flip.dst),
                &install.collection,
                flip.before.as_deref(),
            )
            .map_err(|e| {
                fail(
                    format!(
                        "restoring the CSR edge {} {}-[{}]->{}",
                        install.collection, flip.src, flip.label, flip.dst
                    ),
                    e.into(),
                )
            })?;
        }
        Ok(())
    }
}
