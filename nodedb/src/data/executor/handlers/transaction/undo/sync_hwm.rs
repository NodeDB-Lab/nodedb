// SPDX-License-Identifier: BUSL-1.1

//! Undo of a sync high-water-mark advance.
//!
//! A sync-ingested write carries its producer's sequence number, and applying
//! it advances the in-memory high-water mark of that `(producer, stream)`.
//! Rolling the write back puts the mark back, so the producer's frame is
//! admitted again instead of being taken for a duplicate. A mark that a later
//! frame of the same stream raised past the written value stays: lowering it
//! would admit that frame a second time.

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
        let prior = self
            .sync_hwm
            .get(&(prov.producer_id, prov.stream_id))
            .copied();
        Some(UndoEntry::SyncHwm {
            producer_id: prov.producer_id,
            stream_id: prov.stream_id,
            prior,
            // `sync_commit` leaves the larger of the mark and the frame's seq.
            written: prior.unwrap_or(0).max(prov.seq),
        })
    }

    /// Put the high-water mark of `(producer_id, stream_id)` back to `prior`
    /// while it still holds `written`. Any other value came from a write that
    /// interleaved with the rolled-back one, and stays.
    pub(super) fn apply_undo_sync_hwm(
        &mut self,
        producer_id: u64,
        stream_id: u64,
        prior: Option<u64>,
        written: u64,
    ) {
        let key = (producer_id, stream_id);
        if self.sync_hwm.get(&key).copied().unwrap_or(0) != written {
            return;
        }
        match prior {
            Some(seq) => {
                self.sync_hwm.insert(key, seq);
            }
            None => {
                self.sync_hwm.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::SyncProvenance;

    use crate::data::executor::core_loop::tests::make_core_with_dir;

    const KEY: (u64, u64) = (9, 4);

    fn prov(seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id: KEY.0,
            epoch: 1,
            stream_id: KEY.1,
            seq,
        }
    }

    #[test]
    fn a_rollback_lowers_the_mark_when_nothing_interleaved() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.sync_hwm.insert(KEY, 5);
        let undo = core.capture_sync_hwm_undo(Some(&prov(6))).expect("undo");
        core.sync_commit(&prov(6));

        core.rollback_undo_log(0, 1, vec![undo]).expect("rollback");
        assert_eq!(core.sync_hwm.get(&KEY).copied(), Some(5));
    }

    #[test]
    fn a_rollback_keeps_a_mark_an_interleaved_write_raised() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.sync_hwm.insert(KEY, 5);
        let undo = core.capture_sync_hwm_undo(Some(&prov(6))).expect("undo");
        core.sync_commit(&prov(6));
        core.sync_commit(&prov(7));

        core.rollback_undo_log(0, 1, vec![undo]).expect("rollback");
        assert_eq!(
            core.sync_hwm.get(&KEY).copied(),
            Some(7),
            "the interleaved frame is not admitted again"
        );
    }

    #[test]
    fn a_rollback_removes_a_mark_the_write_created() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let undo = core.capture_sync_hwm_undo(Some(&prov(1))).expect("undo");
        core.sync_commit(&prov(1));

        core.rollback_undo_log(0, 1, vec![undo]).expect("rollback");
        assert!(!core.sync_hwm.contains_key(&KEY));
    }
}
