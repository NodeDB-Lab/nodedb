// SPDX-License-Identifier: BUSL-1.1

//! Replace part of an array's cell versions, for a Raft snapshot install.
//!
//! A snapshot install replaces the array state of its group's vShards. One
//! array store on one core holds cells of every vShard that core hosts, so
//! the install rewrites the store: every version the install does not
//! replace stays, every replaced version goes, and the snapshot's versions
//! join. The result publishes as one segment. The manifest write is the
//! commit point, and its stamp names every record this core applied, so
//! restart replay never re-applies a pre-install record over the installed
//! state. A crash before the manifest write leaves the old store whole.

use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::{ArrayId, TileId};
use tracing::warn;

use super::engine::{ArrayEngine, ArrayEngineError, ArrayEngineResult, array_dir};
use super::export::{ArrayCellVersion, export_store};
use super::flush::build_segment_from_memtable;
use super::memtable::{Memtable, PutCell};
use super::store::SegmentRef;
use super::store::catalog::ArrayStoreError;
use crate::types::replay_stamp::ReplayStamp;

impl ArrayEngine {
    /// Replace the versions of `id` whose coordinate `replaced` selects with
    /// `incoming`, and publish the store under `stamp`. An array this core
    /// holds no directory for is opened only when `incoming` has versions.
    /// A store with nothing to replace and nothing incoming is left alone.
    pub fn replace_cells(
        &mut self,
        id: &ArrayId,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
        replaced: impl Fn(&[CoordValue]) -> bool,
        incoming: Vec<ArrayCellVersion>,
        stamp: ReplayStamp,
    ) -> ArrayEngineResult<()> {
        if !self.arrays.contains_key(id) {
            if incoming.is_empty() && !array_dir(&self.cfg.root, id).exists() {
                return Ok(());
            }
            self.open_array(id.clone(), schema, schema_hash)?;
        }
        let current = export_store(self.store(id)?)?;
        let before = current.len();
        let mut versions: Vec<ArrayCellVersion> = current
            .into_iter()
            .filter(|version| !replaced(&version.coord))
            .collect();
        if versions.len() == before && incoming.is_empty() {
            return Ok(());
        }
        // A snapshot version wins over a kept one of the same coordinate and
        // system time: the memtable keeps the later write.
        versions.extend(incoming);
        self.publish_versions(id, versions, stamp)
    }

    fn publish_versions(
        &mut self,
        id: &ArrayId,
        versions: Vec<ArrayCellVersion>,
        stamp: ReplayStamp,
    ) -> ArrayEngineResult<()> {
        let store = self.store_mut(id)?;
        let schema = store.schema().clone();
        let mut memtable = Memtable::new();
        for version in versions {
            match version.payload {
                Some(payload) => {
                    memtable.put_cell(
                        &schema,
                        PutCell {
                            coord: version.coord,
                            attrs: payload.attrs,
                            surrogate: payload.surrogate,
                            system_from_ms: version.system_from_ms,
                            valid_from_ms: payload.valid_from_ms,
                            valid_until_ms: payload.valid_until_ms,
                            placement: version.placement,
                            lsn: 0,
                        },
                    )?;
                }
                None if version.erased => {
                    memtable.erase_cell(
                        &schema,
                        version.coord,
                        version.system_from_ms,
                        version.placement,
                        0,
                    )?;
                }
                None => {
                    memtable.delete_cell(
                        &schema,
                        version.coord,
                        version.system_from_ms,
                        version.placement,
                        0,
                    )?;
                }
            }
        }

        let added = if memtable.is_empty() {
            Vec::new()
        } else {
            let built = build_segment_from_memtable(
                &schema,
                store.schema_hash(),
                store.kek(),
                memtable.iter(),
            )?;
            let segment_id = store.allocate_segment_id();
            nodedb_wal::segment::atomic_write_fsync(store.root(), &segment_id, &built.bytes)
                .map_err(|e| ArrayEngineError::Io {
                    detail: format!("write segment {:?}: {e}", store.root().join(&segment_id)),
                })?;
            vec![SegmentRef {
                id: segment_id,
                level: 0,
                min_tile: built.min_tile.unwrap_or_else(|| TileId::snapshot(0)),
                max_tile: built.max_tile.unwrap_or_else(|| TileId::snapshot(0)),
                tile_count: built.tile_count,
                flush_lsn: stamp.highest(),
            }]
        };

        let removed: Vec<String> = store
            .manifest()
            .segments
            .iter()
            .map(|segment| segment.id.clone())
            .collect();
        let mut next = store.manifest().clone();
        next.replace(&removed, added.clone());
        next.replay = stamp;
        // The commit point: the manifest names the new segment and the stamp.
        next.persist(store.root()).map_err(ArrayStoreError::from)?;

        store.replace_segments(&removed, added)?;
        store.manifest_mut().replay = next.replay;
        store.memtable = Memtable::new();
        for segment in &removed {
            // An unreferenced file is inert: the manifest no longer names it.
            if let Err(e) = store.unlink_segment(segment) {
                warn!(array = %id.name, segment, error = %e, "install: old segment not unlinked");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::tile::cell_payload::CellPayload;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_types::{OPEN_UPPER, Surrogate};
    use tempfile::TempDir;

    use super::*;
    use crate::engine::array::engine::ArrayEngineConfig;
    use crate::engine::array::test_support::{aid, put_one, schema};

    fn cell(x: i64, y: i64, v: i64, system_from_ms: i64) -> ArrayCellVersion {
        ArrayCellVersion {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            system_from_ms,
            payload: Some(CellPayload {
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                attrs: vec![CellValue::Int64(v)],
                surrogate: Surrogate::new(1),
            }),
            erased: false,
            placement: 5,
        }
    }

    fn versions(engine: &ArrayEngine) -> Vec<(Vec<CoordValue>, i64)> {
        let mut out: Vec<_> = export_store(engine.store(&aid()).expect("store"))
            .expect("export")
            .into_iter()
            .map(|v| (v.coord, v.system_from_ms))
            .collect();
        out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        out
    }

    /// The replaced coordinates' versions go, the rest stay, the incoming
    /// versions join, and a reopened store reads the same state.
    #[test]
    fn replace_keeps_other_cells_and_installs_incoming() {
        let dir = TempDir::new().expect("tempdir");
        let config = ArrayEngineConfig::new(dir.path().to_path_buf());
        let mut engine = ArrayEngine::new(config.clone()).expect("engine");
        engine.open_array(aid(), schema(), 0xCAFE).expect("open");
        put_one(&mut engine, 1, 1, 10, 1);
        put_one(&mut engine, 2, 2, 20, 2);
        engine
            .flush(&aid(), ReplayStamp::through(2))
            .expect("flush");
        put_one(&mut engine, 2, 2, 21, 3);

        let replaced = |coord: &[CoordValue]| coord.first() == Some(&CoordValue::Int64(2));
        engine
            .replace_cells(
                &aid(),
                schema(),
                0xCAFE,
                replaced,
                vec![cell(2, 2, 99, 50)],
                ReplayStamp::through(9),
            )
            .expect("replace");

        let expected = |engine: &ArrayEngine| {
            let found = versions(engine);
            assert_eq!(found.len(), 2, "{found:?}");
            // `put_one` writes at system time 0.
            assert!(
                found
                    .iter()
                    .any(|(c, t)| c[0] == CoordValue::Int64(1) && *t == 0)
            );
            assert!(
                found
                    .iter()
                    .any(|(c, t)| c[0] == CoordValue::Int64(2) && *t == 50)
            );
            // Each version keeps the vShard that stored it.
            let placements: Vec<(i64, u32)> = export_store(engine.store(&aid()).expect("store"))
                .expect("export")
                .into_iter()
                .map(|v| (v.system_from_ms, v.placement))
                .collect();
            assert!(placements.contains(&(0, 0)), "{placements:?}");
            assert!(placements.contains(&(50, 5)), "{placements:?}");
        };
        expected(&engine);
        assert!(
            engine
                .store(&aid())
                .expect("store")
                .manifest()
                .replay
                .skips(9)
        );

        let mut reopened = ArrayEngine::new(config).expect("engine");
        reopened
            .open_array(aid(), schema(), 0xCAFE)
            .expect("reopen");
        expected(&reopened);
    }

    #[test]
    fn nothing_to_replace_leaves_the_store_alone() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine =
            ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).expect("engine");
        engine.open_array(aid(), schema(), 0xCAFE).expect("open");
        put_one(&mut engine, 1, 1, 10, 1);
        engine
            .replace_cells(
                &aid(),
                schema(),
                0xCAFE,
                |_| false,
                Vec::new(),
                ReplayStamp::through(5),
            )
            .expect("replace");
        assert!(
            !engine
                .store(&aid())
                .expect("store")
                .manifest()
                .replay
                .skips(5)
        );
        assert_eq!(versions(&engine).len(), 1);
    }
}
