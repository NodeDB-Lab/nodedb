//! Computes which segments to rewrite and what to drop, for a single array
//! at a given system-time cutoff.
//!
//! Pure decision logic — performs no I/O beyond reading the manifest and
//! already-open segment handles. Per-prefix tile-version pooling happens
//! here so the plan accounts for cross-segment supersession.
//!
//! Row decoding (coord encoding, payload extraction, GDPR/tombstone handling)
//! is delegated to `nodedb_array::query::retention::decode_sparse_rows` so
//! this multi-segment planner and the in-segment compaction merger share a
//! single source of truth for retention semantics.

use std::collections::{HashMap, HashSet};

use nodedb_array::ArrayError;
use nodedb_array::query::retention::{DecodedRow, decode_sparse_rows};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::reader::TilePayload;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::TileId;
use nodedb_types::{OPEN_UPPER, Surrogate};

use crate::engine::array::store::ArrayStore;

/// Decision for one segment: which tile-versions to drop, and which ceiling
/// tiles to append in their place.
pub struct SegmentPurgeAction {
    /// Segment file identifier (as in `SegmentRef::id`).
    pub segment_id: String,
    /// TileIds within this segment that must be omitted when rewriting it.
    pub drop_tile_ids: HashSet<TileId>,
    /// Synthetic ceiling tiles to append to the rewritten segment.
    ///
    /// Each ceiling tile was computed from the out-of-horizon versions that
    /// are being dropped. A ceiling tile is placed on the earliest-flushed
    /// segment that hosts any tile for its prefix, so exactly one segment
    /// gets the ceiling for each prefix.
    pub emit_ceiling_tiles: Vec<(TileId, SparseTile)>,
}

/// Full purge plan for one array at a given horizon.
pub struct PurgePlan {
    /// One entry per segment that requires rewriting. Segments with no
    /// tiles to drop and no ceiling to emit are excluded.
    pub segment_actions: Vec<SegmentPurgeAction>,
    /// Total cells materialised into ceiling tiles (telemetry).
    pub cells_carried_forward: usize,
}

/// Compute the purge plan for an array whose store is `store` and whose
/// bitemporal horizon is `horizon_ms`.
///
/// `horizon_ms` must already be the absolute system-time cutoff (i.e.
/// `now_ms - audit_retain_ms`). This function does **not** recompute it.
pub fn plan(
    store: &ArrayStore,
    horizon_ms: i64,
    schema: &ArraySchema,
) -> Result<PurgePlan, ArrayError> {
    if store.manifest().segments.is_empty() {
        return Ok(PurgePlan {
            segment_actions: Vec::new(),
            cells_carried_forward: 0,
        });
    }

    // ── Step 1: Collect all TileEntries tagged with their owning segment id and
    //   flush_lsn (used to choose the ceiling host). ────────────────────────────
    struct TaggedEntry {
        segment_id: String,
        flush_lsn: u64,
        tile_id: TileId,
    }

    let mut all_entries: Vec<TaggedEntry> = Vec::new();
    for seg_ref in &store.manifest().segments {
        let handle = match store.segments.get(&seg_ref.id) {
            Some(h) => h,
            None => continue, // segment in manifest but not in open handles — skip
        };
        let reader = handle.reader();
        for entry in reader.tiles() {
            all_entries.push(TaggedEntry {
                segment_id: seg_ref.id.clone(),
                flush_lsn: seg_ref.flush_lsn,
                tile_id: entry.tile_id,
            });
        }
    }

    if all_entries.is_empty() {
        return Ok(PurgePlan {
            segment_actions: Vec::new(),
            cells_carried_forward: 0,
        });
    }

    // ── Step 2: Check whether any tile is out-of-horizon. If none, no-op. ──────
    let any_outside = all_entries
        .iter()
        .any(|e| e.tile_id.system_from_ms < horizon_ms);
    if !any_outside {
        return Ok(PurgePlan {
            segment_actions: Vec::new(),
            cells_carried_forward: 0,
        });
    }

    // ── Step 3: Group entries by hilbert_prefix. ─────────────────────────────
    // For each prefix, build: inside list, outside list (newest→oldest), and
    // the earliest-flushed segment id for the ceiling host.
    struct PrefixGroup {
        /// (flush_lsn, segment_id, tile_id) for in-horizon versions.
        inside: Vec<(u64, String, TileId)>,
        /// (flush_lsn, segment_id, tile_id) for out-of-horizon versions.
        outside: Vec<(u64, String, TileId)>,
    }

    let mut by_prefix: HashMap<u64, PrefixGroup> = HashMap::new();
    for e in &all_entries {
        let g = by_prefix
            .entry(e.tile_id.hilbert_prefix)
            .or_insert_with(|| PrefixGroup {
                inside: Vec::new(),
                outside: Vec::new(),
            });
        if e.tile_id.system_from_ms >= horizon_ms {
            g.inside
                .push((e.flush_lsn, e.segment_id.clone(), e.tile_id));
        } else {
            g.outside
                .push((e.flush_lsn, e.segment_id.clone(), e.tile_id));
        }
    }

    // ── Step 4: Per-prefix retention decision. ───────────────────────────────
    // Maps segment_id → (drop_tile_ids, ceiling_tiles).
    let mut seg_drops: HashMap<String, HashSet<TileId>> = HashMap::new();
    let mut seg_ceilings: HashMap<String, Vec<(TileId, SparseTile)>> = HashMap::new();
    let mut total_cells_carried: usize = 0;

    for (prefix, group) in &mut by_prefix {
        if group.outside.is_empty() {
            continue;
        }

        // Collect coord keys present in any in-horizon tile version.
        let mut inhorizon_coord_keys: HashSet<Vec<u8>> = HashSet::new();
        for (_, seg_id, tile_id) in &group.inside {
            let handle = match store.segments.get(seg_id) {
                Some(h) => h,
                None => continue,
            };
            let reader = handle.reader();
            let tile_idx = match reader.tiles().iter().position(|e| e.tile_id == *tile_id) {
                Some(i) => i,
                None => continue,
            };
            let payload = reader.read_tile(tile_idx)?;
            if let TilePayload::Sparse(tile) = &payload {
                for row in decode_sparse_rows(tile)? {
                    inhorizon_coord_keys.insert(row.coord_key);
                }
            }
        }

        // Sort outside-horizon versions newest → oldest.
        group
            .outside
            .sort_by_key(|(_, _, tid)| std::cmp::Reverse(tid.system_from_ms));

        // Accumulate ceiling: coord_key → DecodedRow.
        let mut ceiling: HashMap<Vec<u8>, DecodedRow> = HashMap::new();
        for (_, seg_id, tile_id) in &group.outside {
            let handle = match store.segments.get(seg_id) {
                Some(h) => h,
                None => continue,
            };
            let reader = handle.reader();
            let tile_idx = match reader.tiles().iter().position(|e| e.tile_id == *tile_id) {
                Some(i) => i,
                None => continue,
            };
            let payload = reader.read_tile(tile_idx)?;
            if let TilePayload::Sparse(tile) = &payload {
                for row in decode_sparse_rows(tile)? {
                    if inhorizon_coord_keys.contains(&row.coord_key) {
                        continue;
                    }
                    ceiling.entry(row.coord_key.clone()).or_insert(row);
                }
            }
        }

        // All outside tile-versions are dropped from their owning segments.
        for (_, seg_id, tile_id) in &group.outside {
            seg_drops
                .entry(seg_id.clone())
                .or_default()
                .insert(*tile_id);
        }

        // Build the synthetic ceiling SparseTile (excluding GDPR-erased).
        let mut ceiling_rows: Vec<(Vec<u8>, DecodedRow)> = ceiling.into_iter().collect();
        ceiling_rows.sort_by(|a, b| a.0.cmp(&b.0));

        let mut builder = SparseTileBuilder::new(schema);
        let mut cells_in_ceiling: usize = 0;
        for (_key, row) in ceiling_rows {
            match row.kind {
                RowKind::GdprErased => {}
                RowKind::Tombstone => {
                    builder.push_row(SparseRow {
                        coord: &row.coord,
                        attrs: &[],
                        surrogate: Surrogate::ZERO,
                        valid_from_ms: 0,
                        valid_until_ms: OPEN_UPPER,
                        kind: RowKind::Tombstone,
                    })?;
                    cells_in_ceiling += 1;
                }
                RowKind::Live => {
                    let p = row.payload.ok_or_else(|| ArrayError::SegmentCorruption {
                        detail: "plan: Live row missing payload".into(),
                    })?;
                    builder.push_row(SparseRow {
                        coord: &row.coord,
                        attrs: &p.attrs,
                        surrogate: p.surrogate,
                        valid_from_ms: p.valid_from_ms,
                        valid_until_ms: p.valid_until_ms,
                        kind: RowKind::Live,
                    })?;
                    cells_in_ceiling += 1;
                }
            }
        }

        if cells_in_ceiling > 0 {
            total_cells_carried += cells_in_ceiling;
            let ceiling_tile = builder.build();
            // Place ceiling just below the horizon so it sorts before all
            // in-horizon tiles for this prefix (mirrors compaction merger).
            let ceiling_sys_ms = horizon_ms.saturating_sub(1);
            let ceiling_tile_id = TileId::new(*prefix, ceiling_sys_ms);

            // Host: earliest-flushed segment among all outside-horizon versions
            // for this prefix (lowest flush_lsn wins; among ties, smallest id).
            let host_seg_id = group
                .outside
                .iter()
                .min_by_key(|(lsn, sid, _)| (*lsn, sid.clone()))
                .map(|(_, sid, _)| sid.clone())
                .expect("outside is non-empty");

            seg_ceilings
                .entry(host_seg_id)
                .or_default()
                .push((ceiling_tile_id, ceiling_tile));
        }
    }

    // ── Step 5: Build SegmentPurgeActions for segments with any work. ─────────
    let mut touched: HashSet<String> = HashSet::new();
    for id in seg_drops.keys() {
        touched.insert(id.clone());
    }
    for id in seg_ceilings.keys() {
        touched.insert(id.clone());
    }

    let mut segment_actions: Vec<SegmentPurgeAction> = Vec::new();
    for seg_id in touched {
        let drop_tile_ids = seg_drops.remove(&seg_id).unwrap_or_default();
        let emit_ceiling_tiles = seg_ceilings.remove(&seg_id).unwrap_or_default();
        segment_actions.push(SegmentPurgeAction {
            segment_id: seg_id,
            drop_tile_ids,
            emit_ceiling_tiles,
        });
    }

    Ok(PurgePlan {
        segment_actions,
        cells_carried_forward: total_cells_carried,
    })
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

    #[test]
    fn plan_empty_array_returns_empty_plan() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        let schema = test_schema();
        e.open_array(test_aid(), schema.clone(), 0x1).unwrap();
        let store = e.store(&test_aid()).unwrap();
        let plan = super::plan(store, 1000, &schema).unwrap();
        assert!(plan.segment_actions.is_empty());
        assert_eq!(plan.cells_carried_forward, 0);
    }

    #[test]
    fn plan_groups_versions_by_prefix_across_segments() {
        // Two cells at the same hilbert_prefix written at different system_ms →
        // both land in separate segments (flush_cell_threshold = 1). Horizon
        // set above both → both outside, plan must produce one drop action per
        // segment and one ceiling tile.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        let schema = test_schema();
        e.open_array(test_aid(), schema.clone(), 0x1).unwrap();
        put_cell(&mut e, 1, 10, 100, 1); // sys=100, outside
        put_cell(&mut e, 1, 20, 200, 2); // sys=200, outside
        assert_eq!(e.store(&test_aid()).unwrap().manifest().segments.len(), 2);

        let store = e.store(&test_aid()).unwrap();
        let plan = super::plan(store, 500, &schema).unwrap();

        // Both segments touched, total 2 drops, 1 cell carried forward.
        assert_eq!(plan.segment_actions.len(), 2);
        let total_drops: usize = plan
            .segment_actions
            .iter()
            .map(|a| a.drop_tile_ids.len())
            .sum();
        assert_eq!(total_drops, 2);
        // One ceiling tile emitted on the host segment.
        let total_ceilings: usize = plan
            .segment_actions
            .iter()
            .map(|a| a.emit_ceiling_tiles.len())
            .sum();
        assert_eq!(total_ceilings, 1);
        assert_eq!(plan.cells_carried_forward, 1);
    }

    #[test]
    fn plan_no_droppable_versions_returns_empty() {
        // All versions inside horizon — plan must be empty.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        let schema = test_schema();
        e.open_array(test_aid(), schema.clone(), 0x1).unwrap();
        put_cell(&mut e, 5, 50, 1000, 1); // sys=1000, inside horizon=500
        put_cell(&mut e, 6, 60, 1100, 2); // sys=1100, inside
        assert_eq!(e.store(&test_aid()).unwrap().manifest().segments.len(), 2);

        let store = e.store(&test_aid()).unwrap();
        let plan = super::plan(store, 500, &schema).unwrap();
        assert!(plan.segment_actions.is_empty());
        assert_eq!(plan.cells_carried_forward, 0);
    }
}
