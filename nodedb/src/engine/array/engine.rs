//! `ArrayEngine` — Data-Plane handle that owns every array's LSM store.
//!
//! The engine is `!Send` (`HashMap` of stores with no sync wrappers).
//! Persistence routes through [`ArrayWalAppender`]; Origin wires the
//! real group-commit WAL writer at construction.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nodedb_array::ArrayResult;
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::MbrQueryPredicate;
use nodedb_array::segment::TilePayload;
use nodedb_array::segment::writer::SegmentWriter;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::{ArrayId, TileId};

use super::compaction::{CompactionMerger, CompactionPicker};
use super::memtable::Memtable;
use super::store::{ArrayStore, SegmentRef};
use super::wal::{
    ArrayDeletePayload, ArrayFlushPayload, ArrayPutCell, ArrayPutPayload, ArrayWalAppender,
    ArrayWalError,
};

#[derive(Debug, Clone)]
pub struct ArrayEngineConfig {
    /// Root directory containing one subdirectory per array.
    pub root: PathBuf,
    /// Auto-flush when a memtable holds at least this many cells.
    pub flush_cell_threshold: usize,
}

impl ArrayEngineConfig {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            flush_cell_threshold: 4096,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ArrayEngineError {
    #[error(transparent)]
    Array(#[from] nodedb_array::ArrayError),
    #[error(transparent)]
    Store(#[from] super::store::catalog::ArrayStoreError),
    #[error(transparent)]
    Compaction(#[from] super::compaction::merger::CompactionError),
    #[error(transparent)]
    Wal(#[from] ArrayWalError),
    #[error("array engine io: {detail}")]
    Io { detail: String },
    #[error("unknown array: {0}")]
    UnknownArray(String),
}

pub type ArrayEngineResult<T> = Result<T, ArrayEngineError>;

pub struct ArrayEngine<W: ArrayWalAppender> {
    cfg: ArrayEngineConfig,
    arrays: HashMap<ArrayId, ArrayStore>,
    wal: W,
}

impl<W: ArrayWalAppender> ArrayEngine<W> {
    pub fn new(cfg: ArrayEngineConfig, wal: W) -> ArrayEngineResult<Self> {
        std::fs::create_dir_all(&cfg.root).map_err(|e| ArrayEngineError::Io {
            detail: format!("mkdir {:?}: {e}", cfg.root),
        })?;
        Ok(Self {
            cfg,
            arrays: HashMap::new(),
            wal,
        })
    }

    pub fn config(&self) -> &ArrayEngineConfig {
        &self.cfg
    }

    /// Open or create an array. The schema is fixed for the array's
    /// lifetime; passing a schema with a different hash than the
    /// existing manifest fails fast.
    pub fn open_array(
        &mut self,
        id: ArrayId,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
    ) -> ArrayEngineResult<()> {
        let dir = array_dir(&self.cfg.root, &id);
        let store = ArrayStore::open(dir, schema, schema_hash)?;
        self.arrays.insert(id, store);
        Ok(())
    }

    pub fn array_ids(&self) -> impl Iterator<Item = &ArrayId> {
        self.arrays.keys()
    }

    pub fn store(&self, id: &ArrayId) -> ArrayEngineResult<&ArrayStore> {
        self.arrays
            .get(id)
            .ok_or_else(|| ArrayEngineError::UnknownArray(format!("{:?}", id)))
    }

    pub fn store_mut(&mut self, id: &ArrayId) -> ArrayEngineResult<&mut ArrayStore> {
        self.arrays
            .get_mut(id)
            .ok_or_else(|| ArrayEngineError::UnknownArray(format!("{:?}", id)))
    }

    /// Insert one cell. Auto-flushes when the memtable crosses the
    /// configured cell threshold.
    pub fn put_cell(
        &mut self,
        id: &ArrayId,
        coord: Vec<CoordValue>,
        attrs: Vec<CellValue>,
    ) -> ArrayEngineResult<()> {
        self.put_cells(id, vec![ArrayPutCell { coord, attrs }])
    }

    pub fn put_cells(&mut self, id: &ArrayId, cells: Vec<ArrayPutCell>) -> ArrayEngineResult<()> {
        if cells.is_empty() {
            return Ok(());
        }
        let payload = ArrayPutPayload {
            array_id: id.clone(),
            cells: cells.clone(),
        };
        let lsn = self.wal.append_put(&payload)?;
        let store = self.store_mut(id)?;
        let schema = store.schema().clone();
        for c in cells {
            store.memtable.put_cell(&schema, c.coord, c.attrs, lsn)?;
        }
        self.maybe_flush(id)?;
        Ok(())
    }

    pub fn delete_cells(
        &mut self,
        id: &ArrayId,
        coords: Vec<Vec<CoordValue>>,
    ) -> ArrayEngineResult<()> {
        if coords.is_empty() {
            return Ok(());
        }
        let payload = ArrayDeletePayload {
            array_id: id.clone(),
            coords: coords.clone(),
        };
        let lsn = self.wal.append_delete(&payload)?;
        let store = self.store_mut(id)?;
        let schema = store.schema().clone();
        for coord in coords {
            store.memtable.delete_cell(&schema, coord, lsn)?;
        }
        self.maybe_flush(id)?;
        Ok(())
    }

    fn maybe_flush(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        let stats = self.store(id)?.memtable.stats();
        if stats.cell_count >= self.cfg.flush_cell_threshold {
            self.flush(id)?;
        }
        Ok(())
    }

    /// Flush the array's memtable to a new on-disk segment. Returns the
    /// new [`SegmentRef`] (already installed in the manifest). A no-op
    /// if the memtable is empty.
    pub fn flush(&mut self, id: &ArrayId) -> ArrayEngineResult<Option<SegmentRef>> {
        let store = self.store_mut(id)?;
        if store.memtable.is_empty() {
            return Ok(None);
        }
        let schema = store.schema().clone();
        let schema_hash = store.schema_hash();
        let drained = std::mem::replace(&mut store.memtable, Memtable::new()).drain_sorted();
        let BuiltSegment {
            bytes,
            tile_ids,
            min_tile,
            max_tile,
        } = build_segment_from_memtable(&schema, schema_hash, &drained)?;
        let segment_id = store.allocate_segment_id();

        let flush_payload = ArrayFlushPayload {
            array_id: id.clone(),
            segment_id: segment_id.clone(),
            tile_ids,
        };
        let flush_lsn = self.wal.append_flush(&flush_payload)?;
        let store = self.store_mut(id)?;
        let path = store.root().join(&segment_id);
        write_atomic(&path, &bytes).map_err(|e| ArrayEngineError::Io {
            detail: format!("write segment {path:?}: {e}"),
        })?;
        let seg_ref = SegmentRef {
            id: segment_id.clone(),
            level: 0,
            min_tile: min_tile.unwrap_or_else(|| TileId::snapshot(0)),
            max_tile: max_tile.unwrap_or_else(|| TileId::snapshot(0)),
            tile_count: drained.len() as u32,
            flush_lsn,
        };
        store.install_segment(seg_ref.clone())?;
        store.persist_manifest()?;
        Ok(Some(seg_ref))
    }

    /// Run compaction on the array if the picker chooses one. Returns
    /// `true` if a merge happened.
    pub fn maybe_compact(&mut self, id: &ArrayId) -> ArrayEngineResult<bool> {
        let plan = match CompactionPicker::pick(self.store(id)?) {
            Some(p) => p,
            None => return Ok(false),
        };
        let store = self.store(id)?;
        let out = CompactionMerger::run(store, &plan.inputs, plan.output_level)?;
        let store = self.store_mut(id)?;
        let removed = out.removed.clone();
        store.replace_segments(&removed, vec![out.segment_ref])?;
        store.persist_manifest()?;
        for old in removed {
            // Best-effort unlink — the manifest is already authoritative.
            let _ = store.unlink_segment(&old);
        }
        Ok(true)
    }

    pub fn scan_tiles(
        &self,
        id: &ArrayId,
        pred: &MbrQueryPredicate,
    ) -> ArrayEngineResult<Vec<TilePayload>> {
        Ok(self.store(id)?.scan_tiles(pred)?)
    }
}

fn array_dir(root: &std::path::Path, id: &ArrayId) -> PathBuf {
    root.join(format!("t{}-{}", id.tenant_id.as_u32(), id.name))
}

fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp = path.to_path_buf();
    let ext = path
        .extension()
        .map(|e| e.to_string_lossy().into_owned())
        .unwrap_or_default();
    tmp.set_extension(format!("{ext}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent()
        && let Ok(d) = std::fs::File::open(dir)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

struct BuiltSegment {
    bytes: Vec<u8>,
    tile_ids: Vec<TileId>,
    min_tile: Option<TileId>,
    max_tile: Option<TileId>,
}

fn build_segment_from_memtable(
    schema: &ArraySchema,
    schema_hash: u64,
    drained: &[(TileId, super::memtable::TileBuffer)],
) -> ArrayResult<BuiltSegment> {
    let mut writer = SegmentWriter::new(schema_hash);
    let mut tile_ids = Vec::with_capacity(drained.len());
    let mut min_tile: Option<TileId> = None;
    let mut max_tile: Option<TileId> = None;
    for (tile_id, buf) in drained {
        let tile = buf.materialise(schema)?;
        if tile.nnz() == 0 {
            continue;
        }
        writer.append_sparse(*tile_id, &tile)?;
        tile_ids.push(*tile_id);
        min_tile = Some(min_tile.map_or(*tile_id, |m| m.min(*tile_id)));
        max_tile = Some(max_tile.map_or(*tile_id, |m| m.max(*tile_id)));
    }
    Ok(BuiltSegment {
        bytes: writer.finish()?,
        tile_ids,
        min_tile,
        max_tile,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::TenantId;
    use std::cell::RefCell;
    use tempfile::TempDir;

    #[derive(Default)]
    struct InMemWal {
        next_lsn: RefCell<u64>,
        puts: RefCell<Vec<ArrayPutPayload>>,
        deletes: RefCell<Vec<ArrayDeletePayload>>,
        flushes: RefCell<Vec<ArrayFlushPayload>>,
    }

    impl ArrayWalAppender for InMemWal {
        fn append_put(&mut self, p: &ArrayPutPayload) -> Result<u64, ArrayWalError> {
            let mut n = self.next_lsn.borrow_mut();
            *n += 1;
            self.puts.borrow_mut().push(p.clone());
            Ok(*n)
        }
        fn append_delete(&mut self, p: &ArrayDeletePayload) -> Result<u64, ArrayWalError> {
            let mut n = self.next_lsn.borrow_mut();
            *n += 1;
            self.deletes.borrow_mut().push(p.clone());
            Ok(*n)
        }
        fn append_flush(&mut self, p: &ArrayFlushPayload) -> Result<u64, ArrayWalError> {
            let mut n = self.next_lsn.borrow_mut();
            *n += 1;
            self.flushes.borrow_mut().push(p.clone());
            Ok(*n)
        }
    }

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

    fn aid() -> ArrayId {
        ArrayId::new(TenantId::new(1), "g")
    }

    #[test]
    fn put_then_flush_emits_segment() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(
            ArrayEngineConfig::new(dir.path().to_path_buf()),
            InMemWal::default(),
        )
        .unwrap();
        e.open_array(aid(), schema(), 0xCAFE).unwrap();
        e.put_cell(
            &aid(),
            vec![CoordValue::Int64(1), CoordValue::Int64(2)],
            vec![CellValue::Int64(10)],
        )
        .unwrap();
        let seg = e.flush(&aid()).unwrap().expect("non-empty flush");
        assert_eq!(seg.level, 0);
        assert_eq!(seg.tile_count, 1);
        assert!(seg.flush_lsn > 0);
        // memtable is now empty
        assert!(e.store(&aid()).unwrap().manifest().segments.len() == 1);
    }

    #[test]
    fn flush_no_op_when_memtable_empty() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(
            ArrayEngineConfig::new(dir.path().to_path_buf()),
            InMemWal::default(),
        )
        .unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        assert!(e.flush(&aid()).unwrap().is_none());
    }

    #[test]
    fn auto_flush_triggers_at_threshold() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 2;
        let mut e = ArrayEngine::new(cfg, InMemWal::default()).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        for i in 0..2 {
            e.put_cell(
                &aid(),
                vec![CoordValue::Int64(i), CoordValue::Int64(i)],
                vec![CellValue::Int64(i)],
            )
            .unwrap();
        }
        assert!(!e.store(&aid()).unwrap().manifest().segments.is_empty());
    }

    #[test]
    fn compaction_merges_l0_segments() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg, InMemWal::default()).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        // Four flushes → triggers L0→L1 merge.
        for i in 0..4 {
            e.put_cell(
                &aid(),
                vec![CoordValue::Int64(i), CoordValue::Int64(0)],
                vec![CellValue::Int64(i)],
            )
            .unwrap();
        }
        assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 4);
        let merged = e.maybe_compact(&aid()).unwrap();
        assert!(merged);
        let m = e.store(&aid()).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].level, 1);
    }

    #[test]
    fn reopen_loads_manifest_and_segments() {
        let dir = TempDir::new().unwrap();
        let aid = aid();
        {
            let mut e = ArrayEngine::new(
                ArrayEngineConfig::new(dir.path().to_path_buf()),
                InMemWal::default(),
            )
            .unwrap();
            e.open_array(aid.clone(), schema(), 0xBEEF).unwrap();
            e.put_cell(
                &aid,
                vec![CoordValue::Int64(1), CoordValue::Int64(1)],
                vec![CellValue::Int64(7)],
            )
            .unwrap();
            e.flush(&aid).unwrap();
        }
        // Re-open from the same root.
        let mut e = ArrayEngine::new(
            ArrayEngineConfig::new(dir.path().to_path_buf()),
            InMemWal::default(),
        )
        .unwrap();
        e.open_array(aid.clone(), schema(), 0xBEEF).unwrap();
        let m = e.store(&aid).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        assert!(m.durable_lsn > 0);
    }
}
