// SPDX-License-Identifier: BUSL-1.1

//! Undo of a sync high-water-mark advance.
//!
//! A sync-ingested write carries its producer's sequence number, and applying
//! it advances the in-memory high-water mark of that `(producer, stream)`.
//! Rolling the write back puts the mark back, so the producer's frame is
//! admitted again instead of being taken for a duplicate.

use nodedb_types::sync::wire::SyncProvenance;

use crate::data::executor::core_loop::CoreLoop;

use super::UndoEntry;

impl CoreLoop {
    /// The undo entry for the high-water-mark advance a write with
    /// `provenance` makes. `None` when the write carries no identified
    /// producer: nothing advances.
    pub(in crate::data::executor) fn capture_sync_hwm_undo(
        &self,
        provenance: Option<&SyncProvenance>,
    ) -> Option<UndoEntry> {
        let prov = provenance.filter(|prov| prov.producer_id != 0)?;
        Some(UndoEntry::SyncHwm {
            producer_id: prov.producer_id,
            stream_id: prov.stream_id,
            prior: self
                .sync_hwm
                .get(&(prov.producer_id, prov.stream_id))
                .copied(),
        })
    }

    /// Put the high-water mark of `(producer_id, stream_id)` back to `prior`.
    pub(super) fn apply_undo_sync_hwm(
        &mut self,
        producer_id: u64,
        stream_id: u64,
        prior: Option<u64>,
    ) {
        match prior {
            Some(seq) => {
                self.sync_hwm.insert((producer_id, stream_id), seq);
            }
            None => {
                self.sync_hwm.remove(&(producer_id, stream_id));
            }
        }
    }
}
