//! Executes a [`PurgePlan`] by rewriting affected segments in place.
//!
//! For each [`SegmentPurgeAction`]:
//!
//! 1. Read the source segment, skip dropped tile-versions, copy the rest.
//! 2. Append any synthetic ceiling tiles in TileId order.
//! 3. Write the new segment to a temp file, fsync, rename (atomic).
//! 4. Swap the manifest (replace old → new segment), persist, fsync.
//! 5. Unlink the old segment file.
//!
//! If execution is interrupted between step 3 and step 4 (new file on
//! disk, manifest not yet swapped), a temp file may be left behind. On
//! restart the store re-opens from the durable manifest and ignores
//! orphan `.tmp` files — the next purge run will overwrite them.
//!
//! If a segment becomes fully empty after dropping all its tiles and
//! no ceiling tile is emitted, the segment is removed from the manifest
//! entirely rather than writing an empty file.

use std::io::Write;

use nodedb_array::ArrayError;
use nodedb_array::segment::reader::TilePayload;
use nodedb_array::segment::writer::SegmentWriter;
use nodedb_array::types::TileId;

use crate::engine::array::store::ArrayStore;
use crate::engine::array::store::manifest::SegmentRef;

use super::plan::{PurgePlan, SegmentPurgeAction};

/// Execute the purge plan against `store`.
///
/// Returns the total number of tile-version drops across all segment
/// rewrites. The caller is responsible for any tracing / audit records.
pub fn execute(store: &mut ArrayStore, plan: PurgePlan) -> Result<u64, ArrayError> {
    let mut total_dropped: u64 = 0;

    for action in plan.segment_actions {
        let dropped = rewrite_segment(store, &action)?;
        total_dropped += dropped;
    }

    Ok(total_dropped)
}

/// Intermediate output from the read phase — everything needed to decide
/// what to write and then perform the manifest swap.
enum ReadPhaseResult {
    /// Segment becomes fully empty; remove it.
    RemoveEntirely { seg_ref: SegmentRef },
    /// Segment has surviving content; write a new segment file.
    Rewrite {
        seg_ref: SegmentRef,
        new_bytes: Vec<u8>,
        new_tile_count: u32,
        new_min_tile: TileId,
        new_max_tile: TileId,
    },
    /// Nothing to do (no drops, no ceilings).
    Noop,
}

/// Rewrite one segment according to `action`. Returns the number of
/// tile-versions dropped (i.e. `action.drop_tile_ids.len()`).
fn rewrite_segment(store: &mut ArrayStore, action: &SegmentPurgeAction) -> Result<u64, ArrayError> {
    let dropped_count = action.drop_tile_ids.len() as u64;

    // ── Read phase: all borrows from `store` end before this function ──────────
    // modifies `store`. Collect owned data.
    let result = {
        let seg_ref = store
            .manifest()
            .segments
            .iter()
            .find(|s| s.id == action.segment_id)
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!("purge: segment {} not in manifest", action.segment_id),
            })?
            .clone();

        let schema_hash = store.schema_hash();

        let handle = store.segments.get(&action.segment_id).ok_or_else(|| {
            ArrayError::SegmentCorruption {
                detail: format!("purge: no open handle for segment {}", action.segment_id),
            }
        })?;

        let reader = handle.reader();
        let source_tiles: Vec<_> = reader.tiles().to_vec();

        let surviving: Vec<TileId> = source_tiles
            .iter()
            .filter(|e| !action.drop_tile_ids.contains(&e.tile_id))
            .map(|e| e.tile_id)
            .collect();

        let has_ceiling = !action.emit_ceiling_tiles.is_empty();
        let is_empty_after_rewrite = surviving.is_empty() && !has_ceiling;

        if is_empty_after_rewrite && dropped_count == 0 {
            ReadPhaseResult::Noop
        } else if is_empty_after_rewrite {
            ReadPhaseResult::RemoveEntirely { seg_ref }
        } else {
            // Build new segment bytes from surviving tiles + ceiling tiles.
            let mut all_output_ids: Vec<TileId> = surviving.clone();
            for (ceiling_id, _) in &action.emit_ceiling_tiles {
                all_output_ids.push(*ceiling_id);
            }
            all_output_ids.sort();

            let mut writer = SegmentWriter::new(schema_hash);
            let mut new_min_tile: Option<TileId> = None;
            let mut new_max_tile: Option<TileId> = None;
            let mut new_tile_count: u32 = 0;

            for output_id in &all_output_ids {
                if let Some((_cid, ceiling_tile)) = action
                    .emit_ceiling_tiles
                    .iter()
                    .find(|(cid, _)| cid == output_id)
                {
                    let has_rows = ceiling_tile.nnz() > 0 || !ceiling_tile.row_kinds.is_empty();
                    if has_rows {
                        writer
                            .append_sparse(*output_id, ceiling_tile)
                            .map_err(|e| ArrayError::SegmentCorruption {
                                detail: format!("purge: append ceiling tile: {e}"),
                            })?;
                        update_bounds(&mut new_min_tile, &mut new_max_tile, *output_id);
                        new_tile_count += 1;
                    }
                } else if surviving.contains(output_id) {
                    let tile_idx = source_tiles
                        .iter()
                        .position(|e| e.tile_id == *output_id)
                        .ok_or_else(|| ArrayError::SegmentCorruption {
                            detail: format!(
                                "purge: surviving tile {:?} not in source reader",
                                output_id
                            ),
                        })?;
                    let payload = reader.read_tile(tile_idx)?;
                    match &payload {
                        TilePayload::Sparse(tile) => {
                            let has_rows = tile.nnz() > 0 || !tile.row_kinds.is_empty();
                            if has_rows {
                                writer.append_sparse(*output_id, tile).map_err(|e| {
                                    ArrayError::SegmentCorruption {
                                        detail: format!("purge: append sparse tile: {e}"),
                                    }
                                })?;
                                update_bounds(&mut new_min_tile, &mut new_max_tile, *output_id);
                                new_tile_count += 1;
                            }
                        }
                        TilePayload::Dense(_) => {
                            return Err(ArrayError::SegmentCorruption {
                                detail: format!(
                                    "purge: unexpected dense tile {:?} in segment {}",
                                    output_id, action.segment_id
                                ),
                            });
                        }
                    }
                }
            }

            // `reader`, `handle`, and all borrows of `store` end here at
            // the close of this block. `new_bytes` etc. are owned.
            let new_bytes = writer.finish().map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("purge: segment writer finish: {e}"),
            })?;

            if new_tile_count == 0 {
                ReadPhaseResult::RemoveEntirely { seg_ref }
            } else {
                ReadPhaseResult::Rewrite {
                    seg_ref,
                    new_bytes,
                    new_tile_count,
                    new_min_tile: new_min_tile.unwrap_or(TileId::snapshot(0)),
                    new_max_tile: new_max_tile.unwrap_or(TileId::snapshot(0)),
                }
            }
        }
        // `reader`, `handle` dropped here — all shared borrows of `store` end.
    };

    // ── Write phase: `store` is now freely mutably borrowable. ────────────────
    match result {
        ReadPhaseResult::Noop => Ok(0),

        ReadPhaseResult::RemoveEntirely { seg_ref } => {
            remove_segment(store, &seg_ref)?;
            Ok(dropped_count)
        }

        ReadPhaseResult::Rewrite {
            seg_ref,
            new_bytes,
            new_tile_count,
            new_min_tile,
            new_max_tile,
        } => {
            let new_seg_id = store.allocate_segment_id();
            let new_seg_path = store.root().join(&new_seg_id);
            write_atomic(&new_seg_path, &new_bytes).map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("purge: write segment {:?}: {e}", new_seg_path),
            })?;

            let new_ref = SegmentRef {
                id: new_seg_id,
                level: seg_ref.level,
                min_tile: new_min_tile,
                max_tile: new_max_tile,
                tile_count: new_tile_count,
                flush_lsn: seg_ref.flush_lsn,
            };

            store
                .replace_segments(std::slice::from_ref(&seg_ref.id), vec![new_ref])
                .map_err(|e| ArrayError::SegmentCorruption {
                    detail: format!("purge: replace_segments: {e}"),
                })?;
            store
                .persist_manifest()
                .map_err(|e| ArrayError::SegmentCorruption {
                    detail: format!("purge: persist_manifest: {e}"),
                })?;
            let _ = store.unlink_segment(&seg_ref.id);
            Ok(dropped_count)
        }
    }
}

/// Remove a segment from the manifest and unlink its file.
fn remove_segment(store: &mut ArrayStore, seg_ref: &SegmentRef) -> Result<(), ArrayError> {
    store
        .replace_segments(std::slice::from_ref(&seg_ref.id), vec![])
        .map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("purge: remove_segments {}: {e}", seg_ref.id),
        })?;
    store
        .persist_manifest()
        .map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("purge: persist_manifest (remove): {e}"),
        })?;
    let _ = store.unlink_segment(&seg_ref.id);
    Ok(())
}

fn update_bounds(min: &mut Option<TileId>, max: &mut Option<TileId>, id: TileId) {
    *min = Some(min.map_or(id, |m| m.min(id)));
    *max = Some(max.map_or(id, |m| m.max(id)));
}

fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};
    use tempfile::TempDir;

    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::engine::array::wal::ArrayPutCell;
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{Surrogate, TenantId};

    fn test_schema() -> Arc<nodedb_array::schema::ArraySchema> {
        Arc::new(
            ArraySchemaBuilder::new("t")
                .dim(DimSpec::new(
                    "x",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(1000)),
                ))
                .attr(AttrSpec::new("v", AttrType::Int64, true))
                .tile_extents(vec![100])
                .build()
                .unwrap(),
        )
    }

    fn test_aid() -> ArrayId {
        ArrayId::new(TenantId::new(0), "t")
    }

    fn put_cell(engine: &mut ArrayEngine, x: i64, v: i64, sys_ms: i64, lsn: u64) {
        engine
            .put_cells(
                &test_aid(),
                vec![ArrayPutCell {
                    coord: vec![CoordValue::Int64(x)],
                    attrs: vec![CellValue::Int64(v)],
                    surrogate: Surrogate::ZERO,
                    system_from_ms: sys_ms,
                    valid_from_ms: 0,
                    valid_until_ms: i64::MAX,
                }],
                lsn,
            )
            .unwrap();
    }

    fn run_purge(e: &mut ArrayEngine, horizon_ms: i64) -> u64 {
        let schema = test_schema();
        let plan = {
            let store = e.store(&test_aid()).unwrap();
            super::super::plan::plan(store, horizon_ms, &schema).unwrap()
        };
        let store = e.store_mut(&test_aid()).unwrap();
        super::execute(store, plan).unwrap()
    }

    #[test]
    fn execute_rewrites_segment_dropping_tiles() {
        // Two tiles in one segment: one outside horizon, one inside.
        // After purge: 1 dropped, 1 surviving, ceiling added.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 4096;
        let mut e = ArrayEngine::new(cfg).unwrap();
        let schema = test_schema();
        e.open_array(test_aid(), schema.clone(), 0x1).unwrap();
        put_cell(&mut e, 1, 10, 100, 1);
        put_cell(&mut e, 1, 60, 600, 2);
        e.flush(&test_aid(), 3).unwrap();

        let horizon = 500;
        let dropped = run_purge(&mut e, horizon);

        assert_eq!(dropped, 1, "one tile-version must be dropped");
        let m = e.store(&test_aid()).unwrap().manifest();
        assert!(!m.segments.is_empty(), "segment must remain after purge");
    }

    #[test]
    fn execute_removes_segment_when_all_tiles_dropped() {
        // Live cell then GdprErased in same segment, both outside horizon.
        // GdprErased newest version → ceiling is empty → segment removed.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 4096;
        let mut e = ArrayEngine::new(cfg).unwrap();
        let schema = test_schema();
        e.open_array(test_aid(), schema.clone(), 0x1).unwrap();
        put_cell(&mut e, 2, 20, 100, 1);
        e.gdpr_erase_cell(&test_aid(), vec![CoordValue::Int64(2)], 200, 2)
            .unwrap();
        e.flush(&test_aid(), 3).unwrap();

        let dropped = run_purge(&mut e, 500);

        assert_eq!(dropped, 2, "both tiles dropped (live + erased)");
        let m = e.store(&test_aid()).unwrap().manifest();
        assert!(
            m.segments.is_empty(),
            "segment must be removed when fully empty after purge"
        );
    }

    #[test]
    fn execute_idempotent_when_replan_after_partial() {
        // First purge run drops the outside tile. Second run sees nothing
        // droppable. Dropped count must be 0 on second run.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 4096;
        let mut e = ArrayEngine::new(cfg).unwrap();
        let schema = test_schema();
        e.open_array(test_aid(), schema.clone(), 0x1).unwrap();
        put_cell(&mut e, 3, 30, 100, 1);
        put_cell(&mut e, 3, 35, 700, 2);
        e.flush(&test_aid(), 3).unwrap();

        let horizon = 500;
        let d1 = run_purge(&mut e, horizon);
        assert_eq!(d1, 1);

        let d2 = run_purge(&mut e, horizon);
        assert_eq!(d2, 0, "second purge run must drop nothing");
    }
}
