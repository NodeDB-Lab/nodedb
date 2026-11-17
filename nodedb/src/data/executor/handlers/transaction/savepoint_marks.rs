// SPDX-License-Identifier: BUSL-1.1

//! Savepoint positions in a transaction's staging overlays on one core.
//!
//! A core holds one value/TTL, one GRAPH and one ARRAY overlay per
//! transaction. Every vShard the core hosts stages into those same overlays,
//! so a savepoint position belongs to the core, and the core records it.
//!
//! The Control Plane sends a mark through each vShard the transaction has
//! staged to. A core that hosts several of them gets the mark several times
//! and keeps the first record: no write stages between those sends. A rewind
//! reads the core's own record. A core with no record hosted no staged vShard
//! at the mark, so its overlays were empty there.

use std::collections::BTreeMap;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TxnId;

/// The undo-journal lengths of one transaction's overlays on one core at
/// one savepoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::data::executor) struct SavepointMarks {
    pub value: usize,
    pub graph: usize,
    pub array: usize,
}

/// One transaction's savepoint records on one core, by savepoint id.
pub(in crate::data::executor) type TxnSavepoints = BTreeMap<u64, SavepointMarks>;

impl CoreLoop {
    /// The current undo-journal lengths of `txn_id`'s overlays. An absent
    /// overlay has length 0.
    fn overlay_marks(&self, txn_id: TxnId) -> SavepointMarks {
        SavepointMarks {
            value: self
                .txn_overlays
                .get(&txn_id)
                .map_or(0, |overlay| overlay.journal_len()),
            graph: self
                .graph_txn_overlays
                .get(&txn_id)
                .map_or(0, |overlay| overlay.journal_len()),
            array: self
                .array_txn_overlays
                .get(&txn_id)
                .map_or(0, |overlay| overlay.journal_len()),
        }
    }

    /// Record `txn_id`'s overlay positions under `savepoint`. An existing
    /// record for `savepoint` stays.
    pub(in crate::data::executor) fn mark_savepoint(&mut self, txn_id: TxnId, savepoint: u64) {
        self.touch_overlay(txn_id);
        let marks = self.overlay_marks(txn_id);
        self.txn_savepoints
            .entry(txn_id)
            .or_default()
            .entry(savepoint)
            .or_insert(marks);
    }

    /// Rewind `txn_id`'s overlays to the positions recorded under
    /// `savepoint`, or to empty when this core holds no record. Records of
    /// later savepoints are dropped: the rewind destroys those savepoints.
    /// A spill-file error leaves the overlay unusable and is returned: the
    /// transaction can then only roll back.
    pub(in crate::data::executor) fn rollback_to_savepoint(
        &mut self,
        txn_id: TxnId,
        savepoint: u64,
    ) -> crate::Result<()> {
        self.touch_overlay(txn_id);
        let marks = match self.txn_savepoints.get_mut(&txn_id) {
            Some(records) => {
                records.retain(|id, _| *id <= savepoint);
                records.get(&savepoint).copied().unwrap_or_default()
            }
            None => SavepointMarks::default(),
        };
        if let Some(overlay) = self.txn_overlays.get_mut(&txn_id) {
            overlay.rollback_to(marks.value)?;
        }
        if let Some(overlay) = self.graph_txn_overlays.get_mut(&txn_id) {
            overlay.rollback_to(marks.graph)?;
        }
        if let Some(overlay) = self.array_txn_overlays.get_mut(&txn_id) {
            overlay.rollback_to(marks.array)?;
        }
        Ok(())
    }
}
