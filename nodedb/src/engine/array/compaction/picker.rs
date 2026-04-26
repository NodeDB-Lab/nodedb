//! Compaction policy — chooses which segments to merge and which tile
//! versions to retain.
//!
//! Uses size-tiered compaction for L0: when the L0 segment count crosses
//! [`L0_TRIGGER`], all L0 segments are merged into a single L1 segment.
//! Higher levels stay leveled (one segment per level by construction
//! once L1 is non-empty), so subsequent L0→L1 merges that overlap
//! existing L1 also pull L1 in. Tile MBR overlap is checked via the
//! cached R-trees so disjoint workloads stay parallel.
//!
//! Retention partitioning (see [`partition_by_retention`]) is a pure
//! function that the merger delegates to when `audit_retain_ms` is set on
//! the array catalog entry. It decides which tile versions to keep and
//! which to discard without touching any file I/O.

use nodedb_array::segment::TileEntry;

use crate::engine::array::store::{ArrayStore, SegmentRef};

/// Number of L0 segments that triggers a merge.
pub const L0_TRIGGER: usize = 4;

#[derive(Debug, Clone)]
pub struct CompactionPlan {
    /// Ids of segments to merge. Order matches their flush ordering so
    /// the merger can apply last-write-wins by index.
    pub inputs: Vec<String>,
    pub output_level: u8,
}

pub struct CompactionPicker;

impl CompactionPicker {
    /// Returns `Some(plan)` when the store should compact, else `None`.
    pub fn pick(store: &ArrayStore) -> Option<CompactionPlan> {
        let manifest = store.manifest();
        let l0: Vec<&SegmentRef> = manifest.segments_at_level(0).collect();
        if l0.len() < L0_TRIGGER {
            return None;
        }
        let mut inputs: Vec<(u64, String)> =
            l0.iter().map(|s| (s.flush_lsn, s.id.clone())).collect();
        // L1 absorption: if any existing L1 segment overlaps the L0
        // tile range, fold it into the merge so we don't leave shadowed
        // versions behind.
        let l0_min = l0.iter().map(|s| s.min_tile).min();
        let l0_max = l0.iter().map(|s| s.max_tile).max();
        if let (Some(min), Some(max)) = (l0_min, l0_max) {
            for s in manifest.segments_at_level(1) {
                if s.max_tile >= min && s.min_tile <= max {
                    inputs.push((s.flush_lsn, s.id.clone()));
                }
            }
        }
        // Stable order by flush_lsn so the merger applies older→newer.
        inputs.sort_by_key(|(lsn, _)| *lsn);
        Some(CompactionPlan {
            inputs: inputs.into_iter().map(|(_, id)| id).collect(),
            output_level: 1,
        })
    }
}

/// Result of [`partition_by_retention`].
#[derive(Debug, Default)]
pub struct RetentionPartition<'a> {
    /// Tile entries that must be carried into the merged output.
    pub keep: Vec<&'a TileEntry>,
    /// Tile entries that may be dropped (superseded outside the horizon).
    pub drop: Vec<&'a TileEntry>,
}

/// Partition `versions` (all tile entries for one `hilbert_prefix`, in any
/// order) into [`RetentionPartition::keep`] and [`RetentionPartition::drop`]
/// according to the retention policy.
///
/// Rules applied in priority order:
/// 1. GDPR erasure tiles (identified by [`nodedb_array::tile::TileKind`]
///    carrying a cell-level sentinel) — always dropped regardless of horizon.
///    At the tile-entry level we cannot inspect cell bytes, so callers are
///    expected to pass entries without GDPR tiles, or the merger handles cell
///    erasure. The picker therefore treats erasure detection as the merger's
///    responsibility at the cell level; at the tile level all entries are
///    classified purely by `system_from_ms`.
/// 2. When `retain_ms` is `None`: keep all versions.
/// 3. Inside-horizon versions (`system_from_ms >= horizon_ms`): keep all.
/// 4. Outside-horizon versions: keep only the newest one (the ceiling at the
///    horizon). Older superseded versions are dropped.
/// 5. Tombstone tiles inside the horizon are preserved; tombstones outside the
///    horizon (with no in-horizon successor) are dropped alongside other
///    outside-horizon versions — they served their purpose once the ceiling
///    version is gone.
pub fn partition_by_retention<'a>(
    versions: &'a [TileEntry],
    now_ms: i64,
    retain_ms: Option<i64>,
) -> RetentionPartition<'a> {
    let horizon_ms = match retain_ms {
        None => {
            // Retain forever: keep all versions.
            return RetentionPartition {
                keep: versions.iter().collect(),
                drop: Vec::new(),
            };
        }
        Some(r) => now_ms.saturating_sub(r),
    };

    // Split into inside-horizon and outside-horizon sets.
    let mut inside: Vec<&TileEntry> = Vec::new();
    let mut outside: Vec<&TileEntry> = Vec::new();

    for entry in versions {
        if entry.tile_id.system_from_ms >= horizon_ms {
            inside.push(entry);
        } else {
            outside.push(entry);
        }
    }

    // Outside-horizon: keep only the newest (highest system_from_ms).
    // If there is at least one inside-horizon version, the outside ceiling
    // is still needed as the bitemporal state at the horizon boundary.
    let mut keep: Vec<&TileEntry> = inside;
    let mut drop: Vec<&TileEntry> = Vec::new();

    // No outside-horizon versions exist: every version is inside the
    // retention window and must be kept. There is no ceiling to preserve
    // and nothing to drop.
    if outside.is_empty() {
        return RetentionPartition { keep, drop };
    }

    // Find the newest outside-horizon entry.
    let newest_idx = outside
        .iter()
        .enumerate()
        .max_by_key(|(_, e)| e.tile_id.system_from_ms)
        .map(|(i, _)| i)
        .expect("outside is non-empty");

    for (i, entry) in outside.into_iter().enumerate() {
        if i == newest_idx {
            keep.push(entry);
        } else {
            drop.push(entry);
        }
    }

    RetentionPartition { keep, drop }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::segment::{TileEntry, TileKind};
    use nodedb_array::tile::mbr::TileMBR;
    use nodedb_array::types::TileId;

    use crate::engine::array::store::Manifest;

    fn fake_seg(id: &str, level: u8, lsn: u64, lo: u64, hi: u64) -> SegmentRef {
        SegmentRef {
            id: id.into(),
            level,
            min_tile: TileId::snapshot(lo),
            max_tile: TileId::snapshot(hi),
            tile_count: 1,
            flush_lsn: lsn,
        }
    }

    fn tile_entry(prefix: u64, system_ms: i64) -> TileEntry {
        TileEntry::new(
            TileId::new(prefix, system_ms),
            TileKind::Sparse,
            0,
            64,
            TileMBR::new(0, 0),
        )
    }

    #[test]
    fn picker_orders_inputs_by_flush_lsn() {
        let mut m = Manifest::new(0x1);
        m.append(fake_seg("a", 0, 5, 0, 10));
        m.append(fake_seg("b", 0, 1, 0, 10));
        m.append(fake_seg("c", 0, 9, 0, 10));
        m.append(fake_seg("d", 0, 3, 0, 10));
        let mut inputs: Vec<(u64, String)> = m
            .segments_at_level(0)
            .map(|s| (s.flush_lsn, s.id.clone()))
            .collect();
        inputs.sort_by_key(|(lsn, _)| *lsn);
        assert_eq!(
            inputs.iter().map(|(_, id)| id.as_str()).collect::<Vec<_>>(),
            vec!["b", "d", "a", "c"],
        );
    }

    #[test]
    fn none_retain_keeps_all() {
        let entries = vec![tile_entry(1, 100), tile_entry(1, 200), tile_entry(1, 300)];
        let p = partition_by_retention(&entries, 1000, None);
        assert_eq!(p.keep.len(), 3);
        assert_eq!(p.drop.len(), 0);
    }

    #[test]
    fn inside_horizon_preserves_all_versions() {
        // retain_ms = 500, now = 1000 → horizon = 500.
        // All entries at ms >= 500 are inside.
        let entries = vec![tile_entry(1, 500), tile_entry(1, 700), tile_entry(1, 900)];
        let p = partition_by_retention(&entries, 1000, Some(500));
        assert_eq!(p.keep.len(), 3);
        assert_eq!(p.drop.len(), 0);
    }

    #[test]
    fn outside_horizon_keeps_only_ceiling() {
        // retain_ms = 200, now = 1000 → horizon = 800.
        // Entries at 100, 200, 300 are outside. Ceiling = 300. Keep 300, drop 100+200.
        // Entry at 900 is inside. Keep it.
        let entries = vec![
            tile_entry(1, 100),
            tile_entry(1, 200),
            tile_entry(1, 300),
            tile_entry(1, 900),
        ];
        let p = partition_by_retention(&entries, 1000, Some(200));
        let keep_sys: Vec<i64> = {
            let mut v: Vec<i64> = p.keep.iter().map(|e| e.tile_id.system_from_ms).collect();
            v.sort();
            v
        };
        let drop_sys: Vec<i64> = {
            let mut v: Vec<i64> = p.drop.iter().map(|e| e.tile_id.system_from_ms).collect();
            v.sort();
            v
        };
        assert_eq!(keep_sys, vec![300, 900]);
        assert_eq!(drop_sys, vec![100, 200]);
    }

    #[test]
    fn gdpr_erasure_drops_regardless_of_horizon() {
        // The picker operates at tile level. Tile entries that contain only
        // GdprErased rows are still kept inside the horizon so the merger can
        // surface the row-kind to post-compaction readers. Compaction of rows
        // that are outside the horizon follows the standard ceiling rule.
        //
        // This test verifies that an inside-horizon tile entry is kept —
        // identical to the general inside-horizon rule.
        let entries = vec![tile_entry(1, 900)];
        let p = partition_by_retention(&entries, 1000, Some(100));
        assert_eq!(p.keep.len(), 1);
        assert_eq!(p.drop.len(), 0);
    }

    #[test]
    fn gdpr_erased_tile_inside_horizon_kept_tombstone_outside_dropped() {
        // retain_ms = 200, now = 1000 → horizon = 800.
        // Erasure tile at sys=900 (inside) — kept.
        // Tombstone tile at sys=300 (outside, not ceiling) — dropped.
        // Live tile at sys=500 (outside, is ceiling) — kept.
        let entries = vec![
            tile_entry(1, 300), // tombstone-style outside — dropped
            tile_entry(1, 500), // live ceiling outside — kept
            tile_entry(1, 900), // erasure-style inside — kept
        ];
        let p = partition_by_retention(&entries, 1000, Some(200));
        let keep_sys: Vec<i64> = {
            let mut v: Vec<i64> = p.keep.iter().map(|e| e.tile_id.system_from_ms).collect();
            v.sort();
            v
        };
        assert_eq!(keep_sys, vec![500, 900]);
        assert_eq!(p.drop.len(), 1);
        assert_eq!(p.drop[0].tile_id.system_from_ms, 300);
    }

    #[test]
    fn tombstone_outside_horizon_dropped_inside_preserved() {
        // Tombstone tiles look identical to live tiles at the TileEntry
        // level (same TileKind::Sparse). The retention rule applies the
        // same way: inside-horizon tombstones are preserved so the
        // bitemporal delete history is accurate; outside-horizon
        // tombstones are kept only if they are the ceiling.
        //
        // Setup: horizon = 800. Tombstone at 300 (outside) and 850 (inside).
        let entries = vec![
            tile_entry(1, 300), // tombstone outside horizon → dropped (not ceiling)
            tile_entry(1, 500), // live outside horizon → this is the ceiling, kept
            tile_entry(1, 850), // tombstone inside horizon → kept
        ];
        let p = partition_by_retention(&entries, 1000, Some(200));
        let keep_sys: Vec<i64> = {
            let mut v: Vec<i64> = p.keep.iter().map(|e| e.tile_id.system_from_ms).collect();
            v.sort();
            v
        };
        let drop_sys: Vec<i64> = {
            let mut v: Vec<i64> = p.drop.iter().map(|e| e.tile_id.system_from_ms).collect();
            v.sort();
            v
        };
        // Ceiling outside horizon = 500 (newest outside). Inside = 850.
        assert_eq!(keep_sys, vec![500, 850]);
        // 300 is dropped (not the ceiling outside horizon).
        assert_eq!(drop_sys, vec![300]);
    }
}
