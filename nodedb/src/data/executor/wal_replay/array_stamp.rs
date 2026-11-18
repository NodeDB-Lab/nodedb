// SPDX-License-Identifier: BUSL-1.1

//! The restart gates an array record passes: the stamps in its array's
//! manifest. See `array` for what they name.

use nodedb_array::types::ArrayId;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Whether restart replay skips the record at `record_lsn` for
    /// `array_id`: the stamp in the array's manifest names it. A
    /// committed-redo apply never skips (`replay_watermark_skips`).
    pub(in crate::data::executor) fn array_replay_skips(
        &self,
        array_id: &ArrayId,
        record_lsn: u64,
    ) -> bool {
        self.replay_watermark_skips(self.array_stamp_names(array_id, record_lsn))
    }

    /// Whether restart replay skips op `op` of the record at `record_lsn`
    /// for `array_id`: the manifest names that op of the record as held by
    /// a segment. A committed-redo apply never skips, and returns before the
    /// lookup. A record with no op index skips nothing here.
    pub(in crate::data::executor) fn array_partial_replay_skips(
        &self,
        array_id: &ArrayId,
        record_lsn: u64,
        op: Option<u64>,
    ) -> bool {
        let Some(op) = op else {
            return false;
        };
        if self.applying_committed_redo() {
            return false;
        }
        self.array_engine
            .store(array_id)
            .is_ok_and(|store| store.manifest().partial.skips(record_lsn, op))
    }

    /// Whether the stamp in `array_id`'s manifest names the record at
    /// `record_lsn`. An array whose store is not open names nothing, which
    /// replays the record: the safe direction.
    pub(in crate::data::executor) fn array_stamp_names(
        &self,
        array_id: &ArrayId,
        record_lsn: u64,
    ) -> bool {
        self.array_engine
            .store(array_id)
            .is_ok_and(|store| store.manifest().replay.skips(record_lsn))
    }

    /// Whether `array_id`'s manifest stamp names an LSN above `record_lsn`.
    /// A replayed record it holds was in flight when the flush that wrote the
    /// stamp ran.
    pub(super) fn array_stamp_passes(&self, array_id: &ArrayId, record_lsn: u64) -> bool {
        self.array_engine
            .store(array_id)
            .is_ok_and(|store| store.manifest().replay.highest() > record_lsn)
    }
}
