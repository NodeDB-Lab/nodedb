// SPDX-License-Identifier: BUSL-1.1

//! The newest tile-version stamp an array store holds, read from stored
//! tiles.

use super::ArrayStore;

impl ArrayStore {
    /// The newest `system_from` stamp any stored tile version under
    /// `placement` holds, across the memtable and every segment: live cells,
    /// tombstones, and erasures. `None` when no stored tile version carries
    /// `placement`.
    ///
    /// A tile version's placement is the vShard whose write stored it. So
    /// the answer covers exactly the tiles a read of that vShard reads.
    ///
    /// Unlike [`Self::newest_version_stamp`], it reads the stored tiles, so a
    /// rolled-back memtable tile leaves no trace in it. Every replica that
    /// stored the same tiles answers alike, whenever each one flushed.
    pub fn newest_stored_tile_stamp_placed(&self, placement: u32) -> Option<i64> {
        let in_segments = self
            .segments
            .values()
            .flat_map(|handle| {
                handle
                    .reader()
                    .tiles()
                    .iter()
                    .filter(|entry| entry.placement == placement)
                    .map(|entry| entry.tile_id.system_from_ms)
                    .max()
            })
            .max();
        let in_memtable = self
            .memtable
            .iter()
            .filter(|(_, buffer)| buffer.entry_count() > 0 && buffer.placement() == placement)
            .map(|(tile_id, _)| tile_id.system_from_ms)
            .max();
        in_segments.max(in_memtable)
    }
}
