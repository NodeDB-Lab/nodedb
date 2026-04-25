//! Flush path: drain the memtable, build a sparse segment, atomically
//! install it on disk, and persist the manifest.

use nodedb_array::ArrayResult;
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::writer::SegmentWriter;
use nodedb_array::types::{ArrayId, TileId};

use super::engine::{ArrayEngine, ArrayEngineError, ArrayEngineResult};
use super::memtable::{Memtable, TileBuffer};
use super::store::SegmentRef;

impl ArrayEngine {
    /// Flush the array's memtable to a new on-disk segment using a
    /// caller-supplied LSN as the segment's flush watermark. A no-op if
    /// the memtable is empty.
    pub fn flush(&mut self, id: &ArrayId, wal_lsn: u64) -> ArrayEngineResult<Option<SegmentRef>> {
        let Some(prepared) = self.prepare_flush(id)? else {
            return Ok(None);
        };
        self.install_flushed_segment(id, prepared, wal_lsn)
            .map(Some)
    }

    fn prepare_flush(&mut self, id: &ArrayId) -> ArrayEngineResult<Option<PreparedFlush>> {
        let store = self.store_mut(id)?;
        if store.memtable.is_empty() {
            return Ok(None);
        }
        let schema = store.schema().clone();
        let schema_hash = store.schema_hash();
        let drained = std::mem::replace(&mut store.memtable, Memtable::new()).drain_sorted();
        let BuiltSegment {
            bytes,
            min_tile,
            max_tile,
        } = build_segment_from_memtable(&schema, schema_hash, &drained)?;
        let segment_id = store.allocate_segment_id();
        Ok(Some(PreparedFlush {
            segment_id,
            bytes,
            tile_count: drained.len() as u32,
            min_tile,
            max_tile,
        }))
    }

    fn install_flushed_segment(
        &mut self,
        id: &ArrayId,
        prepared: PreparedFlush,
        flush_lsn: u64,
    ) -> ArrayEngineResult<SegmentRef> {
        let store = self.store_mut(id)?;
        let path = store.root().join(&prepared.segment_id);
        write_atomic(&path, &prepared.bytes).map_err(|e| ArrayEngineError::Io {
            detail: format!("write segment {path:?}: {e}"),
        })?;
        let seg_ref = SegmentRef {
            id: prepared.segment_id,
            level: 0,
            min_tile: prepared.min_tile.unwrap_or_else(|| TileId::snapshot(0)),
            max_tile: prepared.max_tile.unwrap_or_else(|| TileId::snapshot(0)),
            tile_count: prepared.tile_count,
            flush_lsn,
        };
        store.install_segment(seg_ref.clone())?;
        store.persist_manifest()?;
        Ok(seg_ref)
    }
}

struct PreparedFlush {
    segment_id: String,
    bytes: Vec<u8>,
    tile_count: u32,
    min_tile: Option<TileId>,
    max_tile: Option<TileId>,
}

struct BuiltSegment {
    bytes: Vec<u8>,
    min_tile: Option<TileId>,
    max_tile: Option<TileId>,
}

fn build_segment_from_memtable(
    schema: &ArraySchema,
    schema_hash: u64,
    drained: &[(TileId, TileBuffer)],
) -> ArrayResult<BuiltSegment> {
    let mut writer = SegmentWriter::new(schema_hash);
    let mut min_tile: Option<TileId> = None;
    let mut max_tile: Option<TileId> = None;
    for (tile_id, buf) in drained {
        let tile = buf.materialise(schema)?;
        if tile.nnz() == 0 {
            continue;
        }
        writer.append_sparse(*tile_id, &tile)?;
        min_tile = Some(min_tile.map_or(*tile_id, |m| m.min(*tile_id)));
        max_tile = Some(max_tile.map_or(*tile_id, |m| m.max(*tile_id)));
    }
    Ok(BuiltSegment {
        bytes: writer.finish()?,
        min_tile,
        max_tile,
    })
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

#[cfg(test)]
mod tests {
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::engine::array::test_support::{aid, put_one, schema};
    use tempfile::TempDir;

    #[test]
    fn put_then_flush_emits_segment() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0xCAFE).unwrap();
        put_one(&mut e, 1, 2, 10, 1);
        let seg = e.flush(&aid(), 7).unwrap().expect("non-empty flush");
        assert_eq!(seg.level, 0);
        assert_eq!(seg.tile_count, 1);
        assert_eq!(seg.flush_lsn, 7);
        assert!(e.store(&aid()).unwrap().manifest().segments.len() == 1);
    }

    #[test]
    fn flush_no_op_when_memtable_empty() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        assert!(e.flush(&aid(), 1).unwrap().is_none());
    }
}
