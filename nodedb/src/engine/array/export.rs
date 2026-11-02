// SPDX-License-Identifier: BUSL-1.1

//! Export every cell version of an array for a backup.
//!
//! A version keeps its system time, and tombstones and erasures are kept, so
//! a restore that re-issues the versions in system-time order rebuilds the
//! array's history. A version held twice, in the memtable and a segment or in
//! two segments, exports once: the memtable copy wins.

use std::collections::HashSet;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::TilePayload;
use nodedb_array::tile::cell_payload::{CellPayload, is_cell_gdpr_erasure, is_cell_tombstone};
use nodedb_array::tile::sparse_tile::{RowKind, SparseTile};
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use serde::{Deserialize, Serialize};

use super::engine::{ArrayEngine, ArrayEngineError, ArrayEngineResult, array_dir};
use super::memtable::Memtable;
use super::store::ArrayStore;

/// One version of one cell.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCellVersion {
    pub coord: Vec<CoordValue>,
    /// The system time the version was written at.
    pub system_from_ms: i64,
    /// The cell's contents. `None` for a tombstone or an erasure.
    pub payload: Option<CellPayload>,
    /// A GDPR erasure. Meaningful only when `payload` is `None`.
    pub erased: bool,
    /// The vShard whose write stored the version. A snapshot install keeps
    /// it. A backup restore writes under the vShard it restores into.
    pub placement: u32,
}

impl ArrayEngine {
    /// Every cell version of `id` on this core. An array this core holds no
    /// directory for has none, and it is not opened.
    pub fn export_cell_versions(
        &mut self,
        id: &ArrayId,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
    ) -> ArrayEngineResult<Vec<ArrayCellVersion>> {
        if !self.arrays.contains_key(id) {
            if !array_dir(&self.cfg.root, id).exists() {
                return Ok(Vec::new());
            }
            self.open_array(id.clone(), schema, schema_hash)?;
        }
        let store = self.store(id)?;
        export_store(store)
    }
}

pub(super) fn export_store(store: &ArrayStore) -> ArrayEngineResult<Vec<ArrayCellVersion>> {
    let mut out = Vec::new();
    let mut seen: HashSet<(Vec<u8>, i64)> = HashSet::new();
    for (tile_id, buffer) in store.memtable.iter() {
        for (coord_key, bytes) in buffer.iter_raw() {
            if seen.insert((coord_key.to_vec(), tile_id.system_from_ms)) {
                out.push(version(
                    Memtable::decode_coord_key(coord_key)?,
                    tile_id.system_from_ms,
                    buffer.placement(),
                    bytes,
                )?);
            }
        }
    }
    for (segment_id, handle) in &store.segments {
        let reader = handle.reader();
        for (idx, entry) in reader.tiles().iter().enumerate() {
            match reader.read_tile(idx)? {
                TilePayload::Sparse(tile) => {
                    let mut rows = Vec::new();
                    sparse_versions(
                        &tile,
                        entry.tile_id.system_from_ms,
                        entry.placement,
                        &mut rows,
                    )?;
                    for row in rows {
                        let key = zerompk::to_msgpack_vec(&row.coord).map_err(|e| {
                            ArrayEngineError::Io {
                                detail: format!("array export: encode a coordinate: {e}"),
                            }
                        })?;
                        if seen.insert((key, row.system_from_ms)) {
                            out.push(row);
                        }
                    }
                }
                TilePayload::Dense(_) => {
                    return Err(ArrayEngineError::Io {
                        detail: format!(
                            "array export: unexpected dense tile {:?} in segment {segment_id}",
                            entry.tile_id
                        ),
                    });
                }
            }
        }
    }
    Ok(out)
}

fn version(
    coord: Vec<CoordValue>,
    system_from_ms: i64,
    placement: u32,
    bytes: &[u8],
) -> ArrayEngineResult<ArrayCellVersion> {
    let (payload, erased) = if is_cell_tombstone(bytes) {
        (None, false)
    } else if is_cell_gdpr_erasure(bytes) {
        (None, true)
    } else {
        (Some(CellPayload::decode(bytes)?), false)
    };
    Ok(ArrayCellVersion {
        coord,
        system_from_ms,
        payload,
        erased,
        placement,
    })
}

/// The rows of one flushed tile version. Attribute columns hold live rows
/// only, so they are indexed by the live-row index.
fn sparse_versions(
    tile: &SparseTile,
    system_from_ms: i64,
    placement: u32,
    out: &mut Vec<ArrayCellVersion>,
) -> ArrayEngineResult<()> {
    let mut live_row = 0usize;
    for row in 0..tile.row_count() {
        let coord = tile
            .dim_dicts
            .iter()
            .map(|dict| {
                dict.indices
                    .get(row)
                    .and_then(|&i| dict.values.get(i as usize))
                    .cloned()
                    .ok_or_else(|| corrupt(row))
            })
            .collect::<ArrayEngineResult<Vec<_>>>()?;
        let kind = tile.row_kind(row)?;
        let payload = match kind {
            RowKind::Live => {
                let attrs = tile
                    .attr_cols
                    .iter()
                    .map(|col| col.get(live_row).cloned().ok_or_else(|| corrupt(row)))
                    .collect::<ArrayEngineResult<Vec<_>>>()?;
                live_row += 1;
                Some(CellPayload {
                    valid_from_ms: *tile.valid_from_ms.get(row).ok_or_else(|| corrupt(row))?,
                    valid_until_ms: *tile.valid_until_ms.get(row).ok_or_else(|| corrupt(row))?,
                    attrs,
                    surrogate: tile.live_surrogate(row)?,
                })
            }
            RowKind::Tombstone | RowKind::GdprErased => None,
        };
        out.push(ArrayCellVersion {
            coord,
            system_from_ms,
            payload,
            erased: kind == RowKind::GdprErased,
            placement,
        });
    }
    Ok(())
}

fn corrupt(row: usize) -> ArrayEngineError {
    ArrayEngineError::Io {
        detail: format!("array export: sparse tile row {row} is out of range"),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::tile::sparse_tile::{SparseRow, SparseTileBuilder};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::{OPEN_UPPER, Surrogate};

    use super::*;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .expect("schema")
    }

    /// A tombstone, an erasure and a live row of one tile version export in
    /// row order, each live row with its own attributes.
    #[test]
    fn a_flushed_tile_exports_every_row_kind() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let rows = [
            (1, RowKind::Tombstone, None),
            (2, RowKind::Live, Some(20)),
            (3, RowKind::GdprErased, None),
            (4, RowKind::Live, Some(40)),
        ];
        for (x, kind, v) in rows {
            let attrs: Vec<CellValue> = v.map(CellValue::Int64).into_iter().collect();
            b.push_row(SparseRow {
                coord: &[CoordValue::Int64(x)],
                attrs: &attrs,
                // Only a live row carries an identity.
                surrogate: (kind == RowKind::Live).then_some(Surrogate::new(x as u32)),
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                kind,
            })
            .expect("push");
        }
        let mut out = Vec::new();
        sparse_versions(&b.build(), 70, 3, &mut out).expect("export");

        assert_eq!(out.len(), 4);
        assert!(out.iter().all(|v| v.system_from_ms == 70));
        assert!(out.iter().all(|v| v.placement == 3));
        assert_eq!((out[0].payload.is_none(), out[0].erased), (true, false));
        assert_eq!((out[2].payload.is_none(), out[2].erased), (true, true));
        let live = |i: usize| out[i].payload.as_ref().expect("live").attrs.clone();
        assert_eq!(live(1), vec![CellValue::Int64(20)]);
        assert_eq!(live(3), vec![CellValue::Int64(40)]);
        assert_eq!(out[3].coord, vec![CoordValue::Int64(4)]);
        let surrogate = |i: usize| out[i].payload.as_ref().expect("live").surrogate;
        assert_eq!(surrogate(1), Surrogate::new(2));
        assert_eq!(surrogate(3), Surrogate::new(4));
    }

    #[test]
    fn a_memtable_sentinel_exports_its_kind() {
        let tomb = version(vec![CoordValue::Int64(1)], 5, 0, &[0xFF]).expect("tombstone");
        assert_eq!((tomb.payload.is_none(), tomb.erased), (true, false));
        let erased = version(vec![CoordValue::Int64(1)], 5, 0, &[0xFE]).expect("erasure");
        assert_eq!((erased.payload.is_none(), erased.erased), (true, true));
    }
}
