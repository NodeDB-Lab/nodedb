// SPDX-License-Identifier: Apache-2.0

//! History purge and compacted-segment install.

use crate::delete_bitmap::DeleteBitmap;
use crate::pk_index::RowLocation;
use crate::versions::SegmentVersions;

use super::engine::MutationEngine;

impl MutationEngine {
    /// Remove every version whose `sys_to` is at or below `horizon`.
    ///
    /// Such a version has a successor (or a delete) at or below the
    /// horizon, so no read at or above the horizon sees it. The newest
    /// version at or below the horizon stays. A key whose last version ended
    /// at or below the horizon loses its whole history, the bitemporal
    /// tombstone included. A purged row stays in the delete bitmap and keeps
    /// no `sys_to`: no read at any cut sees it. Chains and closed-key
    /// entries that name a purged row are dropped.
    ///
    /// Returns the number of rows purged.
    pub fn purge_versions_below(&mut self, horizon: i64) -> usize {
        let purged: usize = self
            .versions
            .values_mut()
            .map(|stamps| stamps.purge_below(horizon).len())
            .sum();
        if purged == 0 {
            return 0;
        }
        let versions = &self.versions;
        let kept = |loc: &RowLocation| {
            versions
                .get(&loc.segment_id)
                .and_then(|v| v.sys_to(loc.row_index))
                .is_some()
        };
        self.chains.retain(kept);
        self.closed_index.retain(kept);
        purged
    }

    /// Install a compaction of segment `segment_id`: `row_map` maps each
    /// old row index to its new one (`None` for a dropped row), `versions`
    /// and `ended` are the new segment's stamps and delete bitmap. The PK
    /// index, the closed-key index, and the chains move to the new
    /// positions. The caller swaps the segment bytes and its surrogate
    /// sidecar alike.
    pub fn install_compacted_segment(
        &mut self,
        segment_id: u64,
        row_map: &[Option<u32>],
        versions: SegmentVersions,
        ended: DeleteBitmap,
    ) {
        let remap = |row: u32| {
            row_map
                .get(row as usize)
                .copied()
                .flatten()
                .map(|row_index| RowLocation {
                    segment_id,
                    row_index,
                })
        };
        self.pk_index.remap_segment(segment_id, remap);
        self.closed_index.remap_segment(segment_id, remap);
        self.chains.remap_segment(segment_id, remap);
        self.versions.insert(segment_id, versions);
        if ended.is_empty() {
            self.delete_bitmaps.remove(&segment_id);
        } else {
            self.delete_bitmaps.insert(segment_id, ended);
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use crate::pk_index::encode_pk;
    use crate::versions::VersionCut;

    use super::*;

    fn engine() -> MutationEngine {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        MutationEngine::new("t".into(), schema)
    }

    fn row(id: i64, v: i64) -> Vec<Value> {
        vec![Value::Integer(id), Value::Integer(v)]
    }

    fn rows_at(e: &MutationEngine, cut: VersionCut) -> Vec<Vec<Value>> {
        e.scan_memtable_versions(cut)
            .map(|r| r.map(|(_, _, values)| values))
            .collect::<Result<_, _>>()
            .expect("read")
    }

    /// The purge keeps the version visible at the horizon and removes every
    /// version ended at or below it. A key deleted at or below the horizon
    /// loses its whole history.
    #[test]
    fn the_purge_keeps_the_version_visible_at_the_horizon() {
        let mut e = engine();
        e.insert(&row(1, 10), 10).expect("put");
        e.insert(&row(1, 20), 20).expect("overwrite");
        e.insert(&row(1, 30), 40).expect("overwrite");
        e.insert(&row(2, 5), 10).expect("put");
        e.delete(&Value::Integer(2), 15).expect("delete");

        let purged = e.purge_versions_below(25);

        assert_eq!(purged, 2, "id 1 at 10 and id 2's only version");
        assert_eq!(rows_at(&e, VersionCut::At(25)), vec![row(1, 20)]);
        assert_eq!(rows_at(&e, VersionCut::At(45)), vec![row(1, 30)]);
        assert!(
            rows_at(&e, VersionCut::At(12)).is_empty(),
            "history below the horizon is gone"
        );
        assert_eq!(rows_at(&e, VersionCut::All), vec![row(1, 20), row(1, 30)]);
        assert_eq!(rows_at(&e, VersionCut::Current), vec![row(1, 30)]);
        assert!(
            e.version_chains()
                .get(&encode_pk(&Value::Integer(2)))
                .is_empty()
        );
        assert_eq!(
            e.version_chains().get(&encode_pk(&Value::Integer(1))).len(),
            1
        );
    }

    /// A compaction drops purged rows, keeps history with its stamps, and
    /// the PK index and chains follow the new positions.
    #[test]
    fn a_compaction_install_remaps_every_index() {
        let mut e = engine();
        e.insert(&row(1, 10), 10).expect("put");
        e.insert(&row(2, 5), 10).expect("put");
        e.insert(&row(1, 20), 20).expect("overwrite");
        e.insert(&row(1, 30), 30).expect("overwrite");
        let seg = e.next_segment_id();
        e.on_memtable_flushed(seg).expect("flush");
        e.purge_versions_below(20);

        // Old rows: 0 = id1@10 (purged), 1 = id2, 2 = id1@20 (history),
        // 3 = id1@30.
        let row_map = vec![None, Some(0), Some(1), Some(2)];
        let mut versions = SegmentVersions::new();
        for (row, from) in [(1u32, 10), (2, 20), (3, 30)] {
            versions.push(from);
            if let Some(to) = e.sys_to_at(RowLocation {
                segment_id: seg,
                row_index: row,
            }) {
                versions.end(row - 1, to);
            }
        }
        let mut ended = DeleteBitmap::new();
        ended.mark_deleted(1);
        e.install_compacted_segment(seg, &row_map, versions, ended);

        let pk1 = encode_pk(&Value::Integer(1));
        let loc = |row_index| RowLocation {
            segment_id: seg,
            row_index,
        };
        assert_eq!(e.pk_index().get(&pk1).copied(), Some(loc(2)));
        assert_eq!(e.version_at(&pk1, VersionCut::At(25)), Some(loc(1)));
        assert_eq!(e.version_chains().get(&pk1), &[loc(1)]);
        assert!(e.row_visible(seg, 0, VersionCut::Current));
        assert!(!e.row_visible(seg, 1, VersionCut::Current));
    }
}
