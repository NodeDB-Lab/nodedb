//! Compaction policy — chooses which segments to merge.
//!
//! Uses size-tiered compaction for L0: when the L0 segment count crosses
//! [`L0_TRIGGER`], all L0 segments are merged into a single L1 segment.
//! Higher levels stay leveled (one segment per level by construction
//! once L1 is non-empty), so subsequent L0→L1 merges that overlap
//! existing L1 also pull L1 in. Tile MBR overlap is checked via the
//! cached R-trees so disjoint workloads stay parallel.
//!
//! Per-array retention is handled by [`super::merger`] after the initial
//! cell merge, operating at cell granularity to preserve the sparse tile
//! model.

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

#[cfg(test)]
mod tests {
    use crate::engine::array::store::{Manifest, SegmentRef};
    use nodedb_array::types::TileId;

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
}
