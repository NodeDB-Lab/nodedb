// SPDX-License-Identifier: BUSL-1.1

//! Flush path: drain the memtable, build a sparse segment, atomically
//! install it on disk, and persist the manifest.

use nodedb_array::ArrayResult;
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::writer::SegmentWriter;
use nodedb_array::types::{ArrayId, TileId};

use super::engine::{ArrayEngine, ArrayEngineError, ArrayEngineResult};
use super::memtable::{Memtable, TileBuffer};
use super::store::SegmentRef;
use crate::types::replay_stamp::ReplayStamp;

impl ArrayEngine {
    /// Flush the array's memtable to a new on-disk segment and publish the
    /// manifest naming it with `stamp`. A no-op if the memtable is empty.
    ///
    /// `stamp` must name every record whose cells the memtable holds, and no
    /// record it does not: restart replay skips exactly the records it names.
    /// A core passes its applied-prefix stamp, taken after it noted every
    /// record it applied to this array.
    ///
    /// The memtable is cleared LAST, once the segment and the manifest naming
    /// it are both on disk. Every failure path leaves the memtable exactly as
    /// it was: reads keep returning the cells, and a failed flush costs only
    /// the retry. The caller's clamped checkpoint LSN keeps the WAL records
    /// that back the cells.
    pub fn flush(
        &mut self,
        id: &ArrayId,
        stamp: ReplayStamp,
    ) -> ArrayEngineResult<Option<SegmentRef>> {
        let Some(prepared) = self.prepare_flush(id)? else {
            return Ok(None);
        };
        let seg_ref = self.install_flushed_segment(id, prepared, stamp)?;
        self.store_mut(id)?.memtable = Memtable::new();
        Ok(Some(seg_ref))
    }

    fn prepare_flush(&mut self, id: &ArrayId) -> ArrayEngineResult<Option<PreparedFlush>> {
        let store = self.store_mut(id)?;
        if store.memtable.is_empty() {
            return Ok(None);
        }
        let schema = store.schema().clone();
        let schema_hash = store.schema_hash();
        let kek = store.kek().cloned();
        // Built from a BORROW of the memtable, never a drain: nothing may leave
        // memory before it is durable. `Memtable::iter` walks a `BTreeMap`, so
        // tiles arrive in `TileId`-ascending order exactly as before.
        let built =
            build_segment_from_memtable(&schema, schema_hash, kek.as_ref(), store.memtable.iter())?;
        let segment_id = store.allocate_segment_id();
        Ok(Some(PreparedFlush {
            segment_id,
            bytes: built.bytes,
            tile_count: built.tile_count,
            min_tile: built.min_tile,
            max_tile: built.max_tile,
        }))
    }

    fn install_flushed_segment(
        &mut self,
        id: &ArrayId,
        prepared: PreparedFlush,
        stamp: ReplayStamp,
    ) -> ArrayEngineResult<SegmentRef> {
        let store = self.store_mut(id)?;
        let root = store.root().to_path_buf();
        // Routed through the shared WAL helper because this is a
        // checkpoint-class write: the coordinated checkpoint reports these
        // segments as the array engine's durability and the WAL segments below
        // that LSN are then deleted. A correctly-named file full of zeros after
        // power loss — what a rename that reaches disk ahead of the data pages
        // produces — is not a degraded segment, it is the only copy of those
        // cells, gone.
        nodedb_wal::segment::atomic_write_fsync(&root, &prepared.segment_id, &prepared.bytes)
            .map_err(|e| ArrayEngineError::Io {
                detail: format!("write segment {:?}: {e}", root.join(&prepared.segment_id)),
            })?;
        // Any failure past this point leaves the segment file on disk but
        // unreferenced by the manifest — inert bytes, not a half-published
        // state, and the caller clamps the checkpoint LSN so the WAL records it
        // holds are kept.
        let seg_ref = SegmentRef {
            id: prepared.segment_id,
            level: 0,
            min_tile: prepared.min_tile.unwrap_or_else(|| TileId::snapshot(0)),
            max_tile: prepared.max_tile.unwrap_or_else(|| TileId::snapshot(0)),
            tile_count: prepared.tile_count,
            flush_lsn: stamp.highest(),
        };
        // The segment and the stamp naming its records publish together: the
        // manifest write below is the commit point for both.
        store.install_segment(seg_ref.clone())?;
        let previous = std::mem::replace(&mut store.manifest_mut().replay, stamp);
        if let Err(e) = store.persist_manifest() {
            // The manifest on disk names neither the segment nor the stamp,
            // and the memtable still holds the cells. Withdraw both from
            // memory too, so reads do not see the cells twice and a later
            // publish does not claim records this one never made durable.
            store.manifest_mut().replay = previous;
            store.replace_segments(std::slice::from_ref(&seg_ref.id), Vec::new())?;
            return Err(e.into());
        }
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
    tile_count: u32,
}

fn build_segment_from_memtable<'a>(
    schema: &ArraySchema,
    schema_hash: u64,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    tiles: impl Iterator<Item = (&'a TileId, &'a TileBuffer)>,
) -> ArrayResult<BuiltSegment> {
    let mut writer = SegmentWriter::new(schema_hash);
    let mut min_tile: Option<TileId> = None;
    let mut max_tile: Option<TileId> = None;
    let mut tile_count: u32 = 0;
    for (tile_id, buf) in tiles {
        if buf.entry_count() == 0 {
            continue;
        }
        let tile = buf.materialise(schema)?;
        writer.append_sparse(*tile_id, &tile)?;
        min_tile = Some(min_tile.map_or(*tile_id, |m| m.min(*tile_id)));
        max_tile = Some(max_tile.map_or(*tile_id, |m| m.max(*tile_id)));
        tile_count += 1;
    }
    Ok(BuiltSegment {
        bytes: writer.finish(kek)?,
        min_tile,
        max_tile,
        tile_count,
    })
}

#[cfg(test)]
mod tests {
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::engine::array::test_support::{aid, put_one, schema};
    use crate::types::replay_stamp::ReplayStamp;
    use tempfile::TempDir;

    #[test]
    fn put_then_flush_emits_segment() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0xCAFE).unwrap();
        put_one(&mut e, 1, 2, 10, 1);
        let seg = e
            .flush(&aid(), ReplayStamp::through(7))
            .unwrap()
            .expect("non-empty flush");
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
        assert!(e.flush(&aid(), ReplayStamp::through(1)).unwrap().is_none());
    }

    /// A flush that cannot write its segment must leave the memtable untouched.
    /// Draining first would take the cells out of memory without putting them
    /// anywhere — reads would stop returning them until a restart replayed the
    /// WAL, which the coordinated checkpoint now calls this on a timer.
    #[test]
    fn failed_flush_leaves_the_memtable_intact() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0xCAFE).unwrap();
        put_one(&mut e, 1, 2, 10, 1);

        // Take the array's directory away so the segment write has nowhere to
        // land.
        let root = e.store(&aid()).unwrap().root().to_path_buf();
        std::fs::remove_dir_all(&root).unwrap();

        assert!(
            e.flush(&aid(), ReplayStamp::through(7)).is_err(),
            "a flush that cannot write its segment must report the failure, not \
             swallow it — the caller clamps its checkpoint LSN on this Err"
        );
        assert_eq!(
            e.store(&aid()).unwrap().memtable.stats().cell_count,
            1,
            "the cell must still be live in the memtable after the failed flush"
        );

        // And the retry, once the directory is back, still publishes it.
        std::fs::create_dir_all(&root).unwrap();
        let seg = e
            .flush(&aid(), ReplayStamp::through(7))
            .unwrap()
            .expect("retry must flush");
        assert_eq!(seg.tile_count, 1);
        assert!(
            e.store(&aid()).unwrap().memtable.is_empty(),
            "a SUCCESSFUL flush must clear the memtable — the cells are durable now"
        );
    }

    /// A flush publishes the caller's stamp in the manifest, and a reopened
    /// store reads it back: it is what restart replay decides records by.
    #[test]
    fn a_flush_publishes_its_stamp_in_the_manifest() {
        let dir = TempDir::new().unwrap();
        let stamp = ReplayStamp {
            prefix: 5,
            applied_above: vec![crate::types::replay_stamp::LsnRange { start: 9, end: 9 }],
        };
        {
            let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
            e.open_array(aid(), schema(), 0xCAFE).unwrap();
            put_one(&mut e, 1, 2, 10, 9);
            let seg = e.flush(&aid(), stamp.clone()).unwrap().expect("flush");
            assert_eq!(seg.flush_lsn, 9);
            assert_eq!(e.store(&aid()).unwrap().manifest().replay, stamp);
        }
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0xCAFE).unwrap();
        assert_eq!(e.store(&aid()).unwrap().manifest().replay, stamp);
    }
}
