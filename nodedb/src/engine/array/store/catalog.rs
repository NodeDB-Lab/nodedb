//! Per-array LSM store — manifest, memtable, open segment handles.
//!
//! Each [`ArrayStore`] manages one array's directory. The engine in
//! `engine.rs` keeps a `HashMap<ArrayId, ArrayStore>`. Stores are
//! Data-Plane only (`!Send`-compatible — no atomics, no shared mutability).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use nodedb_array::query::ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::{MbrQueryPredicate, TilePayload, extract_cell_bytes};
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use nodedb_array::types::TileId;
use nodedb_array::types::coord::value::CoordValue;

use super::manifest::{Manifest, ManifestError, SegmentRef, segment_path};
use super::segment_handle::{SegmentHandle, SegmentHandleError};
use crate::engine::array::memtable::Memtable;

/// One open array. Owns the directory layout below `root`:
///
/// ```text
/// <root>/manifest.ndam
/// <root>/<segment-id-1>.ndas
/// <root>/<segment-id-2>.ndas
/// ...
/// ```
pub struct ArrayStore {
    root: PathBuf,
    schema: Arc<ArraySchema>,
    schema_hash: u64,
    manifest: Manifest,
    pub(crate) memtable: Memtable,
    pub(crate) segments: HashMap<String, SegmentHandle>,
    next_segment_seq: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ArrayStoreError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Segment(#[from] SegmentHandleError),
    #[error("array store io: {detail}")]
    Io { detail: String },
    #[error("schema_hash mismatch: store={store:x} new={new:x}")]
    SchemaHashMismatch { store: u64, new: u64 },
}

impl ArrayStore {
    /// Open or create the array store. Loads the manifest if present;
    /// mmap's every referenced segment and validates schema_hash.
    pub fn open(
        root: PathBuf,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
    ) -> Result<Self, ArrayStoreError> {
        std::fs::create_dir_all(&root).map_err(|e| ArrayStoreError::Io {
            detail: format!("mkdir {root:?}: {e}"),
        })?;
        let manifest = Manifest::load_or_new(&root, schema_hash)?;
        if manifest.schema_hash != schema_hash && !manifest.segments.is_empty() {
            return Err(ArrayStoreError::SchemaHashMismatch {
                store: manifest.schema_hash,
                new: schema_hash,
            });
        }
        let mut segments = HashMap::with_capacity(manifest.segments.len());
        let mut max_seq: u64 = 0;
        for seg in &manifest.segments {
            let h =
                SegmentHandle::open(&segment_path(&root, &seg.id), seg.id.clone(), schema_hash)?;
            if let Some(seq) = parse_segment_seq(&seg.id) {
                max_seq = max_seq.max(seq);
            }
            segments.insert(seg.id.clone(), h);
        }
        Ok(Self {
            root,
            schema,
            schema_hash,
            manifest,
            memtable: Memtable::new(),
            segments,
            next_segment_seq: max_seq + 1,
        })
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn schema(&self) -> &Arc<ArraySchema> {
        &self.schema
    }

    pub fn schema_hash(&self) -> u64 {
        self.schema_hash
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_mut(&mut self) -> &mut Manifest {
        &mut self.manifest
    }

    /// Allocate the next segment file name and bump the sequence.
    pub fn allocate_segment_id(&mut self) -> String {
        let seq = self.next_segment_seq;
        self.next_segment_seq += 1;
        format!("{seq:010}.ndas")
    }

    /// Register a freshly-flushed (or freshly-merged) segment. The file
    /// must already exist on disk. Updates the manifest in-memory only;
    /// callers must call [`ArrayStore::persist_manifest`] afterwards.
    pub fn install_segment(&mut self, seg: SegmentRef) -> Result<(), ArrayStoreError> {
        let h = SegmentHandle::open(
            &segment_path(&self.root, &seg.id),
            seg.id.clone(),
            self.schema_hash,
        )?;
        self.segments.insert(seg.id.clone(), h);
        self.manifest.append(seg);
        Ok(())
    }

    /// Remove segments from the manifest and drop their handles. The
    /// underlying file is deleted only after the manifest is persisted
    /// (caller's responsibility — see [`ArrayStore::unlink_segment`]).
    pub fn replace_segments(
        &mut self,
        removed: &[String],
        added: Vec<SegmentRef>,
    ) -> Result<(), ArrayStoreError> {
        let mut new_handles = Vec::with_capacity(added.len());
        for seg in &added {
            let h = SegmentHandle::open(
                &segment_path(&self.root, &seg.id),
                seg.id.clone(),
                self.schema_hash,
            )?;
            new_handles.push(h);
        }
        self.manifest.replace(removed, added);
        for id in removed {
            self.segments.remove(id);
        }
        for h in new_handles {
            self.segments.insert(h.id().to_string(), h);
        }
        Ok(())
    }

    pub fn persist_manifest(&self) -> Result<(), ArrayStoreError> {
        self.manifest.persist(&self.root)?;
        Ok(())
    }

    pub fn unlink_segment(&self, id: &str) -> Result<(), ArrayStoreError> {
        let path = segment_path(&self.root, id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ArrayStoreError::Io {
                detail: format!("unlink {path:?}: {e}"),
            }),
        }
    }

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
        let mut all_prefixes: HashSet<u64> = HashSet::new();
        for h in self.segments.values() {
            let reader = h.reader();
            for entry in reader.tiles() {
                all_prefixes.insert(entry.tile_id.hilbert_prefix);
            }
        }
        for (tile_id, _) in self.memtable.iter() {
            all_prefixes.insert(tile_id.hilbert_prefix);
        }

        // Did any version exist at all in the store?
        let any_versions = !all_prefixes.is_empty();

        let mut out: Vec<(u64, SparseTile)> = Vec::new();
        let mut any_qualifying = false;

        for prefix in all_prefixes {
            // Collect all distinct coords across every version for this prefix.
            let mut coords: Vec<Vec<CoordValue>> = Vec::new();

            // From memtable versions.
            for (_, buf) in self.memtable.iter_tile_versions(prefix, i64::MAX) {
                for coord_key in buf.all_coord_keys() {
                    let coord = Memtable::decode_coord_key(coord_key)?;
                    if !coords.contains(&coord) {
                        coords.push(coord);
                    }
                }
            }

            // From segment versions (newest-first per segment, but we only need
            // coords here so order within a segment doesn't matter).
            for h in self.segments.values() {
                let reader = h.reader();
                for item in reader.iter_tile_versions(prefix, i64::MAX)? {
                    let (_, tile_payload) = item?;
                    if let TilePayload::Sparse(sparse) = &tile_payload {
                        let n = sparse.nnz() as usize;
                        for row in 0..n {
                            let coord: Vec<CoordValue> = sparse
                                .dim_dicts
                                .iter()
                                .map(|d| d.values[d.indices[row] as usize].clone())
                                .collect();
                            if !coords.contains(&coord) {
                                coords.push(coord);
                            }
                        }
                    }
                }
            }

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

    /// Build a `(TileId, raw_bytes)` list for a specific `coord` across all
    /// versions (memtable + segments), ordered newest-first by `system_from_ms`.
    fn cell_versions_for_coord(
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

fn parse_segment_seq(id: &str) -> Option<u64> {
    id.split_once('.').and_then(|(stem, _)| stem.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};
    use tempfile::TempDir;

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
        let s = ArrayStore::open(dir.path().join("g"), schema(), 0xCAFE).unwrap();
        assert_eq!(s.manifest().segments.len(), 0);
        assert_eq!(s.schema_hash(), 0xCAFE);
        assert_eq!(s.allocate_segment_id_peek(), "0000000001.ndas");
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
