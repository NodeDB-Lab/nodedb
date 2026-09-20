// SPDX-License-Identifier: BUSL-1.1

//! Tile/cell scanning and bitemporal ceiling resolution for [`ArrayStore`].

use nodedb_array::query::ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::cell_payload::{CellPayload, is_cell_sentinel};
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use nodedb_array::types::coord::value::CoordValue;

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
        let params = CeilingParams {
            system_as_of,
            valid_at_ms,
        };

        // Collect all distinct hilbert_prefix values present in any version.
        let all_prefixes = self.all_hilbert_prefixes();

        // Did any version exist at all in the store?
        let any_versions = !all_prefixes.is_empty();

        let mut out: Vec<(u64, SparseTile)> = Vec::new();
        let mut any_qualifying = false;

        for prefix in all_prefixes {
            // Collect all distinct coords across every version for this prefix.
            let coords = self.distinct_coords_for_prefix(prefix)?;

            let mut builder = SparseTileBuilder::new(&self.schema);
            for coord in &coords {
                // Build the version iterator for this coord across all sources.
                // Memtable versions (newer) first, then segment versions (older).
                let cell_versions = self.cell_versions_for_coord(prefix, coord, i64::MAX)?;

                // Check if there are any versions at or before the cutoff.
                if cell_versions
                    .iter()
                    .any(|(tid, _)| tid.system_from_ms <= system_as_of)
                {
                    any_qualifying = true;
                }

                let iter = cell_versions
                    .iter()
                    .map(|(tid, bytes)| (*tid, bytes.as_slice()));
                match ceiling_resolve_cell(iter, coord, &params)? {
                    CeilingResult::Live(payload) => {
                        builder
                            .push_row(nodedb_array::tile::sparse_tile::SparseRow {
                                coord,
                                attrs: &payload.attrs,
                                surrogate: payload.surrogate,
                                valid_from_ms: payload.valid_from_ms,
                                valid_until_ms: payload.valid_until_ms,
                                kind: nodedb_array::tile::sparse_tile::RowKind::Live,
                            })
                            .map_err(|e| nodedb_array::ArrayError::SegmentCorruption {
                                detail: format!("scan_tiles_at builder: {e}"),
                            })?;
                    }
                    CeilingResult::Tombstoned | CeilingResult::Erased | CeilingResult::NotFound => {
                    }
                }
            }

            let tile = builder.build();
            if tile.nnz() > 0 {
                out.push((prefix, tile));
            }
        }

        let truncated_before_horizon = any_versions && !any_qualifying;
        Ok((out, truncated_before_horizon))
    }

    /// Audit-log scan: return every **live** cell-version across all system times.
    ///
    /// Each returned entry is `(hilbert_prefix, coord, system_from_ms, payload)`.
    /// Tombstoned and erased versions are skipped — mirrors `versioned_scan_all`
    /// in the document engine.
    ///
    /// When `valid_at_ms` is `Some(vt)`, only versions whose
    /// `valid_from_ms <= vt < valid_until_ms` are included.
    ///
    /// The caller is responsible for sorting and applying limits.
    pub fn scan_tiles_all_versions(
        &self,
        valid_at_ms: Option<i64>,
    ) -> Result<Vec<CellVersion>, nodedb_array::ArrayError> {
        // Collect all distinct (hilbert_prefix, coord) pairs.
        let all_prefixes = self.all_hilbert_prefixes();

        let mut out: Vec<CellVersion> = Vec::new();

        for prefix in all_prefixes {
            // Collect all distinct coords for this prefix (across all versions).
            let coords = self.distinct_coords_for_prefix(prefix)?;

            for coord in &coords {
                // All versions for this coord across memtable + segments,
                // newest-first by system_from_ms.
                let versions = self.cell_versions_for_coord(prefix, coord, i64::MAX)?;
                for (tile_id, bytes) in &versions {
                    // Skip tombstones and erasures — emit only live payloads.
                    if is_cell_sentinel(bytes) {
                        continue;
                    }
                    let payload = CellPayload::decode(bytes)?;
                    // Apply valid-time point filter if requested.
                    if let Some(vt) = valid_at_ms
                        && !(payload.valid_from_ms <= vt && vt < payload.valid_until_ms)
                    {
                        continue;
                    }
                    out.push((prefix, coord.clone(), tile_id.system_from_ms, payload));
                }
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
        // Find the hilbert_prefix for this coord.
        let hilbert_prefix = {
            use nodedb_array::tile::tile_id_for_cell;
            let tile = tile_id_for_cell(&self.schema, coord, 0).map_err(|e| {
                nodedb_array::ArrayError::SegmentCorruption {
                    detail: format!("ceiling_for_coord: tile id: {e}"),
                }
            })?;
            tile.hilbert_prefix
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
        e.flush(&aid(), 2).unwrap();
        assert!(
            e.store(&aid())
                .unwrap()
                .contains_cell(&coord(1, 1))
                .unwrap()
        );

        // Tombstone at a later system time hides it.
        e.delete_cells(
            &aid(),
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
            vec![crate::engine::array::wal::ArrayPutCell {
                coord: coord(1, 1),
                attrs: vec![nodedb_array::types::cell_value::value::CellValue::Int64(8)],
                surrogate: nodedb_types::Surrogate::ZERO,
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
