// SPDX-License-Identifier: BUSL-1.1

//! Tile/cell scanning and bitemporal ceiling resolution for [`ArrayStore`].

use nodedb_array::query::ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
use nodedb_array::query::region::ArrayRegion;
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::ShardBucket;
use nodedb_types::array_route::shard_bucket_admits;

use super::{ArrayStore, CellVersion};

impl ArrayStore {
    /// Run the MBR predicate against every segment + the memtable.
    /// Returns decoded tile payloads in segment-then-memtable order.
    pub fn scan_tiles(
        &self,
        pred: &MbrQueryPredicate,
    ) -> Result<Vec<TilePayload>, nodedb_array::ArrayError> {
        Ok(self
            .scan_tiles_with_hilbert_prefix(pred)?
            .into_iter()
            .map(|(_hp, tile)| tile)
            .collect())
    }

    /// Like `scan_tiles` but also returns the tile's `hilbert_prefix` so
    /// callers can apply per-shard Hilbert-range filters (distributed agg).
    pub fn scan_tiles_with_hilbert_prefix(
        &self,
        pred: &MbrQueryPredicate,
    ) -> Result<Vec<(u64, TilePayload)>, nodedb_array::ArrayError> {
        let mut out = Vec::new();
        for h in self.segments.values() {
            let reader = h.reader();
            for idx in h.rtree().query(pred) {
                let hilbert_prefix = reader
                    .tiles()
                    .get(idx)
                    .map(|e| e.tile_id.hilbert_prefix)
                    .unwrap_or(0);
                out.push((hilbert_prefix, reader.read_tile(idx)?));
            }
        }
        for (tile_id, buf) in self.memtable.iter() {
            if buf.entry_count() == 0 {
                continue;
            }
            out.push((
                tile_id.hilbert_prefix,
                TilePayload::Sparse(buf.materialise(&self.schema)?),
            ));
        }
        Ok(out)
    }

    /// Bitemporal scan: resolve the ceiling for every cell coordinate at the
    /// given `system_as_of` and optional `valid_at_ms` point.
    ///
    /// Returns one `(hilbert_prefix, SparseTile)` pair per prefix that has at
    /// least one `Live` cell after ceiling resolution. Tombstoned and erased
    /// coords are omitted.
    ///
    /// Also returns `truncated_before_horizon`: `true` when the store contains
    /// at least one tile version but the `system_as_of` cutoff is below every
    /// version's `system_from_ms` (i.e., the cutoff predates all data).
    pub fn scan_tiles_at(
        &self,
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> Result<(Vec<(u64, SparseTile)>, bool), nodedb_array::ArrayError> {
        self.scan_shard_tiles_at(system_as_of, valid_at_ms, None, None)
    }

    /// [`scan_tiles_at`](Self::scan_tiles_at) over the prefixes that
    /// `shard_bucket` admits and that can hold a cell of `region`. A prefix
    /// either test rejects is never decoded. The tiles returned can still
    /// hold cells outside `region`: the caller filters cells exactly.
    /// `truncated_before_horizon` is computed over the whole store.
    pub fn scan_shard_tiles_at(
        &self,
        system_as_of: i64,
        valid_at_ms: Option<i64>,
        shard_bucket: Option<ShardBucket>,
        region: Option<&ArrayRegion>,
    ) -> Result<(Vec<(u64, SparseTile)>, bool), nodedb_array::ArrayError> {
        let params = CeilingParams {
            system_as_of,
            valid_at_ms,
        };
        let all_prefixes = self.all_hilbert_prefixes();
        let truncated_before_horizon =
            !all_prefixes.is_empty() && !self.any_version_at_or_before(system_as_of);

        let mut out: Vec<(u64, SparseTile)> = Vec::new();
        for prefix in all_prefixes {
            if !shard_bucket_admits(shard_bucket, prefix) {
                continue;
            }
            if let Some(region) = region
                && !self.prefix_may_hold(prefix, system_as_of, region)
            {
                continue;
            }
            if let Some(tile) = self.resolve_prefix_at(prefix, &params)?
                && tile.nnz() > 0
            {
                out.push((prefix, tile));
            }
        }
        Ok((out, truncated_before_horizon))
    }

    /// Audit-log scan: return every **live** cell-version across all system times.
    ///
    /// Each returned entry is `(hilbert_prefix, coord, system_from_ms, payload)`.
    /// Tombstoned and erased versions are skipped — mirrors `versioned_scan_all`
    /// in the document engine.
    ///
    /// When `valid_at_ms` is `Some(vt)`, only versions whose
    /// `valid_from_ms <= vt < valid_until_ms` are included. Only prefixes
    /// that `shard_bucket` admits are read.
    ///
    /// The caller is responsible for sorting and applying limits.
    pub fn scan_tiles_all_versions(
        &self,
        valid_at_ms: Option<i64>,
        shard_bucket: Option<ShardBucket>,
    ) -> Result<Vec<CellVersion>, nodedb_array::ArrayError> {
        let mut out: Vec<CellVersion> = Vec::new();
        for prefix in self.all_hilbert_prefixes() {
            if shard_bucket_admits(shard_bucket, prefix) {
                self.prefix_live_versions(prefix, valid_at_ms, &mut out)?;
            }
        }
        Ok(out)
    }

    /// Point lookup: whether `coord` holds a live cell in the current state.
    ///
    /// Tile-pruned: the coordinate's Hilbert prefix selects exactly the
    /// memtable tile versions and segment footer entries that can hold it
    /// (`iter_tile_versions` binary-searches the footer by prefix), and only
    /// those tiles are decoded. The newest version wins, so a coordinate
    /// whose latest version is a tombstone or erasure reports `false`.
    pub fn contains_cell(&self, coord: &[CoordValue]) -> nodedb_array::ArrayResult<bool> {
        Ok(matches!(
            self.ceiling_for_coord(coord, i64::MAX, None)?,
            CeilingResult::Live(_)
        ))
    }

    /// Resolve the ceiling for a specific cell coordinate.
    ///
    /// Returns the raw `CeilingResult` so callers can distinguish between
    /// `Live`, `Tombstoned`, `Erased`, and `NotFound` — unlike `scan_tiles_at`
    /// which collapses Tombstoned/Erased/NotFound into "no row in output tile".
    ///
    /// Useful for testing and diagnostic code that needs the exact sentinel type.
    pub fn ceiling_for_coord(
        &self,
        coord: &[CoordValue],
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> nodedb_array::ArrayResult<nodedb_array::query::ceiling::CeilingResult> {
        use nodedb_array::query::ceiling::CeilingParams;
        // Find the hilbert_prefix for this coord. A coordinate outside the
        // domain has no tile: a cell write refuses it, so no cell is there.
        let hilbert_prefix = match nodedb_array::tile::tile_id_for_cell(&self.schema, coord, 0) {
            Ok(tile) => tile.hilbert_prefix,
            Err(nodedb_array::ArrayError::CoordOutOfDomain { .. }) => {
                return Ok(CeilingResult::NotFound);
            }
            Err(e) => {
                return Err(nodedb_array::ArrayError::SegmentCorruption {
                    detail: format!("ceiling_for_coord: tile id: {e}"),
                });
            }
        };
        let versions = self.cell_versions_for_coord(hilbert_prefix, coord, system_as_of)?;
        let params = CeilingParams {
            system_as_of,
            valid_at_ms,
        };
        ceiling_resolve_cell(
            versions.iter().map(|(tid, b)| (*tid, b.as_slice())),
            coord,
            &params,
        )
    }

    /// The stamp of the newest version `coord` holds: a live cell, a
    /// tombstone or an erasure, across the memtable and every segment.
    /// `None` when the coordinate holds no version, or lies outside the
    /// domain.
    pub fn newest_cell_stamp(
        &self,
        coord: &[CoordValue],
    ) -> nodedb_array::ArrayResult<Option<i64>> {
        let hilbert_prefix = match nodedb_array::tile::tile_id_for_cell(&self.schema, coord, 0) {
            Ok(tile) => tile.hilbert_prefix,
            Err(nodedb_array::ArrayError::CoordOutOfDomain { .. }) => return Ok(None),
            Err(e) => {
                return Err(nodedb_array::ArrayError::SegmentCorruption {
                    detail: format!("newest_cell_stamp: tile id: {e}"),
                });
            }
        };
        Ok(self
            .cell_versions_for_coord(hilbert_prefix, coord, i64::MAX)?
            .iter()
            .map(|(tile_id, _)| tile_id.system_from_ms)
            .max())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_array::schema::ArraySchema;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};
    use tempfile::TempDir;

    use super::super::error::ArrayStoreError;
    use super::super::segments::parse_segment_seq;
    use super::*;

    fn schema() -> Arc<ArraySchema> {
        Arc::new(
            ArraySchemaBuilder::new("a")
                .dim(DimSpec::new(
                    "x",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
                ))
                .dim(DimSpec::new(
                    "y",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
                ))
                .attr(AttrSpec::new("v", AttrType::Int64, true))
                .tile_extents(vec![4, 4])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn open_creates_directory_and_empty_manifest() {
        let dir = TempDir::new().unwrap();
        let s = ArrayStore::open(dir.path().join("g"), schema(), 0xCAFE, None).unwrap();
        assert_eq!(s.manifest().segments.len(), 0);
        assert_eq!(s.schema_hash(), 0xCAFE);
        assert_eq!(s.allocate_segment_id_peek(), "0000000001.ndas");
    }

    /// An array whose segments were flushed under at-rest encryption must reopen
    /// when the key is supplied. Opening the manifest's segments before the key is
    /// available makes every such array permanently unopenable — the cells the
    /// segments hold are no longer in the WAL, the checkpoint that followed the
    /// flush truncated it.
    #[test]
    fn reopen_supplies_the_kek_to_encrypted_segments() {
        use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig, array_dir};
        use crate::engine::array::test_support::{aid, put_one, schema as engine_schema};
        use nodedb_wal::crypto::WalEncryptionKey;

        let dir = TempDir::new().unwrap();
        let kek = WalEncryptionKey::from_bytes(&[0x5A; 32]).unwrap();

        // Write and flush one encrypted (SEGA) segment.
        {
            let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
            cfg.flush_cell_threshold = 1;
            let mut e = ArrayEngine::new(cfg).unwrap();
            e.set_kek(kek.clone());
            e.open_array(aid(), engine_schema(), 0xBEEF).unwrap();
            put_one(&mut e, 1, 1, 7, 1);
            e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(1))
                .unwrap();
            assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 1);
        }

        // Reopening with the key must succeed and see the segment.
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.set_kek(kek);
        e.open_array(aid(), engine_schema(), 0xBEEF).unwrap();
        assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 1);

        // Reopening WITHOUT the key must still be the typed error it always was —
        // a missing key may never be treated as "open it as plaintext".
        // `ArrayStore` is not `Debug`, so match the result rather than `expect_err`.
        match ArrayStore::open(array_dir(dir.path(), &aid()), engine_schema(), 0xBEEF, None) {
            Ok(_) => panic!("encrypted segment without a KEK must not open"),
            Err(err) => assert!(
                matches!(err, ArrayStoreError::Segment(_)),
                "expected a typed segment-open error, got {err:?}"
            ),
        }
    }

    /// `contains_cell` reports the newest version only: a live put is found,
    /// a later tombstone hides it, a re-put after the tombstone restores it,
    /// and a coordinate never written stays absent. Covers the memtable and
    /// the flushed-segment source.
    #[test]
    fn contains_cell_reports_newest_version_across_memtable_and_segments() {
        use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
        use crate::engine::array::test_support::{aid, put_one, schema as engine_schema};
        use crate::engine::array::wal::ArrayDeleteCell;
        use nodedb_array::types::coord::value::CoordValue;

        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), engine_schema(), 0xC0DE).unwrap();
        let coord = |x: i64, y: i64| vec![CoordValue::Int64(x), CoordValue::Int64(y)];

        // Never written.
        assert!(
            !e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 1))
                .unwrap()
        );

        // Live in the memtable.
        put_one(&mut e, 1, 1, 7, 1);
        assert!(
            e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 1))
                .unwrap()
        );
        // A different coordinate in the same tile is still absent.
        assert!(
            !e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 2))
                .unwrap()
        );

        // Flushed to a segment: still found.
        e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(2))
            .unwrap();
        assert!(
            e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 1))
                .unwrap()
        );

        // Tombstone at a later system time hides it.
        e.delete_cells(
            &aid(),
            0,
            vec![ArrayDeleteCell {
                coord: coord(1, 1),
                system_from_ms: 10,
                erasure: false,
            }],
            3,
        )
        .unwrap();
        assert!(
            !e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 1))
                .unwrap()
        );

        // Re-put at a later system time restores it.
        e.put_cells(
            &aid(),
            0,
            vec![crate::engine::array::wal::ArrayPutCell {
                coord: coord(1, 1),
                attrs: vec![nodedb_array::types::cell_value::value::CellValue::Int64(8)],
                // The surrogate `put_one` binds to (1, 1).
                surrogate: nodedb_types::Surrogate::new(102),
                system_from_ms: 20,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }],
            4,
        )
        .unwrap();
        assert!(
            e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 1))
                .unwrap()
        );
    }

    /// A region scan skips every prefix whose footer MBRs miss the region.
    /// A prefix whose newer version tombstones a cell inside the region is
    /// still read, so the tombstone hides the older cell.
    #[test]
    fn a_region_scan_reads_only_prefixes_that_can_hold_its_cells() {
        use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
        use crate::engine::array::test_support::{aid, put_one, schema as engine_schema};
        use crate::engine::array::wal::ArrayDeleteCell;
        use crate::types::replay_stamp::ReplayStamp;
        use nodedb_array::query::slice::{DimRange, Slice};

        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), engine_schema(), 0xD1CE).unwrap();
        // One cell in tile (0, 0), one in tile (2, 2).
        put_one(&mut e, 1, 1, 7, 1);
        put_one(&mut e, 9, 9, 8, 2);
        e.flush(&aid(), ReplayStamp::through(2)).unwrap();

        let low = || Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(3)));
        let region = ArrayRegion::new(Slice::new(vec![low(), low()]), None);
        let (tiles, _) = e
            .store(&aid())
            .unwrap()
            .scan_shard_tiles_at(i64::MAX, None, None, Some(&region))
            .unwrap();
        assert_eq!(tiles.len(), 1, "the far tile's prefix is pruned");
        assert_eq!(tiles[0].1.nnz(), 1);

        e.delete_cells(
            &aid(),
            0,
            vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(1), CoordValue::Int64(1)],
                system_from_ms: 10,
                erasure: false,
            }],
            3,
        )
        .unwrap();
        e.flush(&aid(), ReplayStamp::through(3)).unwrap();
        let (tiles, _) = e
            .store(&aid())
            .unwrap()
            .scan_shard_tiles_at(i64::MAX, None, None, Some(&region))
            .unwrap();
        assert!(
            tiles.iter().all(|(_, t)| t.nnz() == 0),
            "the tombstone hides the cell: {} tiles",
            tiles.len()
        );
    }

    /// The newest stamp of a coordinate counts a tombstone, reads the
    /// memtable and the segments, and ignores every other coordinate.
    #[test]
    fn newest_cell_stamp_counts_tombstones_across_sources() {
        use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
        use crate::engine::array::test_support::{aid, put_one, schema as engine_schema};
        use crate::engine::array::wal::ArrayDeleteCell;

        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), engine_schema(), 0xC0DE).unwrap();
        let coord = |x: i64, y: i64| vec![CoordValue::Int64(x), CoordValue::Int64(y)];
        let newest = |e: &ArrayEngine, x: i64, y: i64| {
            e.store(&aid())
                .unwrap()
                .newest_cell_stamp(&coord(x, y))
                .unwrap()
        };

        assert_eq!(newest(&e, 1, 1), None);
        put_one(&mut e, 1, 1, 7, 1);
        assert_eq!(newest(&e, 1, 1), Some(0));
        e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(2))
            .unwrap();
        e.delete_cells(
            &aid(),
            0,
            vec![ArrayDeleteCell {
                coord: coord(1, 1),
                system_from_ms: 10,
                erasure: false,
            }],
            3,
        )
        .unwrap();
        assert_eq!(newest(&e, 1, 1), Some(10));
        assert_eq!(newest(&e, 1, 2), None, "another coordinate holds nothing");
    }

    #[test]
    fn parse_seq_round_trips() {
        assert_eq!(parse_segment_seq("0000000042.ndas"), Some(42));
        assert_eq!(parse_segment_seq("garbage"), None);
    }

    impl ArrayStore {
        // Test-only helper that doesn't bump the counter so we can
        // observe the next id without consuming it.
        fn allocate_segment_id_peek(&self) -> String {
            format!("{:010}.ndas", self.next_segment_seq)
        }
    }
}
