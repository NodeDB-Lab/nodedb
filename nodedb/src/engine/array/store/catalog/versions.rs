// SPDX-License-Identifier: BUSL-1.1

//! Cross-source (memtable + segment) version metadata and single-cell
//! version lookup for [`ArrayStore`]'s bitemporal reads.

use std::collections::{HashMap, HashSet};

use nodedb_array::segment::{TilePayload, extract_cell_bytes};
use nodedb_array::types::TileId;
use nodedb_array::types::coord::value::CoordValue;

use super::ArrayStore;
use crate::engine::array::store::segment_handle::SegmentHandle;

/// The newest tile-version stamp a segment holds under each placement,
/// read from its footer.
pub(super) fn segment_newest_stamps(handle: &SegmentHandle) -> HashMap<u32, i64> {
    let mut newest: HashMap<u32, i64> = HashMap::new();
    for entry in handle.reader().tiles() {
        raise_newest_stamp(&mut newest, entry.placement, entry.tile_id.system_from_ms);
    }
    newest
}

/// Raise the newest stamp of `placement` in `newest` to `stamp`.
fn raise_newest_stamp(newest: &mut HashMap<u32, i64>, placement: u32, stamp: i64) {
    let held = newest.entry(placement).or_insert(stamp);
    *held = (*held).max(stamp);
}

/// Raise each placement's newest stamp in `into` to its stamp in `from`.
pub(super) fn merge_newest_stamps(into: &mut HashMap<u32, i64>, from: HashMap<u32, i64>) {
    for (placement, stamp) in from {
        raise_newest_stamp(into, placement, stamp);
    }
}

impl ArrayStore {
    /// The newest tile-version stamp the store holds under any placement,
    /// across every segment and the memtable. `None` for an empty store.
    pub fn newest_version_stamp(&self) -> Option<i64> {
        self.newest_stamps.values().copied().max()
    }

    /// The newest tile-version stamp the store holds under `placement`. A
    /// write placed there stamps above it, so it sorts after every version
    /// of its cell. One placement's writes apply in the same order on every
    /// replica, so every replica raises alike. `None` when the placement
    /// holds no version. The value never falls: a rolled-back memtable tile
    /// can leave it above the true newest, which only raises a later stamp
    /// further.
    pub fn newest_version_stamp_at(&self, placement: u32) -> Option<i64> {
        self.newest_stamps.get(&placement).copied()
    }

    /// Raise [`Self::newest_version_stamp_at`] of `placement` to `stamp`.
    pub(crate) fn note_version_stamp(&mut self, placement: u32, stamp: i64) {
        raise_newest_stamp(&mut self.newest_stamps, placement, stamp);
    }

    /// Raise the newest stamps to every placement's newest in `stamps`.
    pub(super) fn note_version_stamps(&mut self, stamps: HashMap<u32, i64>) {
        merge_newest_stamps(&mut self.newest_stamps, stamps);
    }

    /// Collect every distinct `hilbert_prefix` present in any tile version,
    /// across both the memtable and all open segments.
    pub(super) fn all_hilbert_prefixes(&self) -> HashSet<u64> {
        let mut all_prefixes: HashSet<u64> = HashSet::new();
        for h in self.segments.values() {
            for entry in h.reader().tiles() {
                all_prefixes.insert(entry.tile_id.hilbert_prefix);
            }
        }
        for (tile_id, _) in self.memtable.iter() {
            all_prefixes.insert(tile_id.hilbert_prefix);
        }
        all_prefixes
    }

    /// Whether any tile version is at or before `system_as_of`, across every
    /// prefix of the store. Read from the segment footers and the memtable's
    /// tile ids, so no tile is decoded. A stored tile version holds at least
    /// one row: flush and compaction write no empty tile.
    pub(super) fn any_version_at_or_before(&self, system_as_of: i64) -> bool {
        let in_segments = self.segments.values().any(|h| {
            h.reader()
                .tiles()
                .iter()
                .any(|entry| entry.tile_id.system_from_ms <= system_as_of)
        });
        in_segments
            || self.memtable.iter().any(|(tile_id, buf)| {
                buf.entry_count() > 0 && tile_id.system_from_ms <= system_as_of
            })
    }

    /// Build a `(TileId, raw_bytes)` list for a specific `coord` across all
    /// versions (memtable + segments), ordered newest-first by `system_from_ms`.
    pub(super) fn cell_versions_for_coord(
        &self,
        hilbert_prefix: u64,
        coord: &[CoordValue],
        system_as_of: i64,
    ) -> Result<Vec<(TileId, Vec<u8>)>, nodedb_array::ArrayError> {
        let mut versions: Vec<(TileId, Vec<u8>)> = Vec::new();

        // Memtable (most recent writes, already newest-first from iter_tile_versions).
        for (tile_id, buf) in self
            .memtable
            .iter_tile_versions(hilbert_prefix, system_as_of)
        {
            if let Some(bytes) = buf.get_cell_bytes(coord) {
                versions.push((tile_id, bytes.to_vec()));
            }
        }

        // Segment versions — gather all qualifying versions across all segments,
        // then sort newest-first so memtable + segment ordering is correct.
        let mut seg_versions: Vec<(TileId, Vec<u8>)> = Vec::new();
        for h in self.segments.values() {
            let reader = h.reader();
            for item in reader.iter_tile_versions(hilbert_prefix, system_as_of)? {
                let (tile_id, tile_payload) = item?;
                if let TilePayload::Sparse(sparse) = &tile_payload
                    && let Some(bytes) = extract_cell_bytes(sparse, coord)?
                {
                    seg_versions.push((tile_id, bytes));
                }
            }
        }
        // Sort segment versions newest-first by system_from_ms.
        seg_versions.sort_by_key(|(a, _)| std::cmp::Reverse(a.system_from_ms));
        versions.extend(seg_versions);

        Ok(versions)
    }
}
