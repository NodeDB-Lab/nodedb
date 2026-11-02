// SPDX-License-Identifier: Apache-2.0

//! Sparse tile payload — coordinate list + per-attribute columns.
//!
//! Storage layout is column-major so per-attribute codecs
//! (`nodedb-codec`) can be applied without a transpose. Dimension values are
//! dictionary-encoded inline: rare in genomic / single-cell workloads
//! that have many repeats per tile.

use nodedb_types::{OPEN_UPPER, Surrogate};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::mbr::{MbrBuilder, TileMBR};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

/// Per-row lifecycle classification for a [`SparseTile`] row.
///
/// Stored as a parallel `u8` column (`row_kinds`) so the segment can
/// carry tombstone and GDPR-erasure markers that survive flush and are
/// visible to post-flush reads. Compaction's retention policy is the
/// only path allowed to drop these rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Live = 0,
    Tombstone = 1,
    GdprErased = 2,
}

impl RowKind {
    /// Convert a raw byte to a `RowKind`. Fails on unknown values so
    /// corrupt data surfaces as a typed error rather than silent `Live`.
    pub fn from_u8(b: u8) -> ArrayResult<Self> {
        match b {
            0 => Ok(Self::Live),
            1 => Ok(Self::Tombstone),
            2 => Ok(Self::GdprErased),
            other => Err(ArrayError::SegmentCorruption {
                detail: format!("unknown RowKind byte: {other:#04x}"),
            }),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl From<RowKind> for u8 {
    fn from(k: RowKind) -> u8 {
        k.as_u8()
    }
}

/// Per-dim dictionary: distinct values seen, and one index per cell
/// pointing into the dictionary. Index width is selected by callers
/// when picking the codec.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DimDict {
    pub values: Vec<CoordValue>,
    pub indices: Vec<u32>,
}

impl DimDict {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            indices: Vec::new(),
        }
    }

    pub fn cardinality(&self) -> usize {
        self.values.len()
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// Sparse tile payload.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SparseTile {
    /// One dictionary per schema dim, parallel to `schema.dims`.
    pub dim_dicts: Vec<DimDict>,
    /// One column per schema attr, parallel to `schema.attrs`.
    pub attr_cols: Vec<Vec<CellValue>>,
    /// Per-row global surrogate, parallel to the dim-dict index streams.
    /// Cross-engine bitmap joins read this column directly without
    /// translating coords back to user-visible primary keys.
    ///
    /// A stored live cell holds `Some` bound surrogate. A tombstone or
    /// erasure row holds `None`: it names a coordinate, not a row. A
    /// derived query result row, such as an elementwise output, holds
    /// `None` and is never stored. `Surrogate::ZERO` never appears.
    pub surrogates: Vec<Option<Surrogate>>,
    /// Per-cell valid-time lower bound in milliseconds (inclusive).
    /// Parallel to `surrogates`.
    pub valid_from_ms: Vec<i64>,
    /// Per-cell valid-time upper bound in milliseconds (exclusive).
    /// [`nodedb_types::OPEN_UPPER`] (`i64::MAX`) means open-ended.
    /// Parallel to `surrogates`.
    pub valid_until_ms: Vec<i64>,
    /// Per-row [`RowKind`] encoded as `u8`. Parallel to `surrogates`.
    /// `0` = Live, `1` = Tombstone, `2` = GdprErased.
    pub row_kinds: Vec<u8>,
    pub mbr: TileMBR,
}

impl SparseTile {
    /// Empty tile sized for the given schema.
    pub fn empty(schema: &ArraySchema) -> Self {
        Self {
            dim_dicts: (0..schema.arity()).map(|_| DimDict::new()).collect(),
            attr_cols: (0..schema.attrs.len()).map(|_| Vec::new()).collect(),
            surrogates: Vec::new(),
            valid_from_ms: Vec::new(),
            valid_until_ms: Vec::new(),
            row_kinds: Vec::new(),
            mbr: TileMBR::new(schema.arity(), schema.attrs.len()),
        }
    }

    /// Count of live (non-sentinel) rows. Used for MBR statistics and
    /// predicate-pushdown decisions. Sentinel rows are excluded.
    ///
    /// **Do not use this value as a row-iteration bound.** Iteration must
    /// cover sentinel rows so callers can observe Tombstone / GdprErased
    /// markers when applying bitemporal visibility rules — use
    /// [`row_count`](Self::row_count) and filter with
    /// [`row_kind`](Self::row_kind) instead.
    pub fn nnz(&self) -> u32 {
        self.mbr.nnz
    }

    /// Total physical row count including sentinel (Tombstone / GdprErased)
    /// rows. Use this as the upper bound when iterating over rows; check
    /// [`row_kind`](Self::row_kind) inside the loop and skip non-Live rows
    /// when reading attribute columns. The attribute column vectors are
    /// indexed by the **live-row index** (incremented only for Live rows),
    /// not by the iteration index — confusing the two yields incorrect or
    /// out-of-bounds reads.
    pub fn row_count(&self) -> usize {
        self.surrogates.len()
    }

    /// Decode the `RowKind` for the given row index.
    ///
    /// `row_kinds` is parallel to `surrogates`. A row index past it is a
    /// malformed tile.
    pub fn row_kind(&self, row: usize) -> ArrayResult<RowKind> {
        let kind =
            self.row_kinds
                .get(row)
                .copied()
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!(
                        "tile row {row} is past the row-kind column of {} rows",
                        self.row_kinds.len()
                    ),
                })?;
        RowKind::from_u8(kind)
    }

    /// The surrogate `row` holds: `Some` for a stored live cell, `None` for a
    /// tombstone, an erasure, or a derived result row. A row index past the
    /// surrogate column is a malformed tile.
    pub fn row_surrogate(&self, row: usize) -> ArrayResult<Option<Surrogate>> {
        self.surrogates
            .get(row)
            .copied()
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!(
                    "tile row {row} is past the surrogate column ({} rows)",
                    self.surrogates.len()
                ),
            })
    }

    /// The surrogate of live `row`. A live row read from storage always holds
    /// one, so a row without it is a malformed tile.
    pub fn live_surrogate(&self, row: usize) -> ArrayResult<Surrogate> {
        self.row_surrogate(row)?
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!("live tile row {row} carries no surrogate"),
            })
    }

    /// Check the identity invariant of a stored tile: every live row holds a
    /// bound surrogate and every tombstone or erasure row holds none. The
    /// segment codec runs it on every tile it encodes or decodes.
    pub fn check_stored_identities(&self) -> ArrayResult<()> {
        if self.row_kinds.len() != self.surrogates.len() {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "stored tile carries {} row kinds but {} surrogates",
                    self.row_kinds.len(),
                    self.surrogates.len()
                ),
            });
        }
        for (row, surrogate) in self.surrogates.iter().enumerate() {
            let kind = self.row_kind(row)?;
            match (kind, surrogate) {
                (RowKind::Live, Some(s)) if *s != Surrogate::ZERO => {}
                (RowKind::Tombstone | RowKind::GdprErased, None) => {}
                (kind, surrogate) => {
                    return Err(ArrayError::SegmentCorruption {
                        detail: format!(
                            "stored tile row {row} of kind {kind:?} holds surrogate \
                             {surrogate:?}; a live row holds a bound surrogate and a \
                             tombstone or erasure row holds none"
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

/// All row-level data passed to [`SparseTileBuilder::push_row`].
pub struct SparseRow<'a> {
    pub coord: &'a [CoordValue],
    pub attrs: &'a [CellValue],
    /// `Some` bound surrogate for a stored live cell. `None` for a tombstone,
    /// an erasure, or a derived result row that is never stored.
    pub surrogate: Option<Surrogate>,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub kind: RowKind,
}

impl<'a> SparseRow<'a> {
    /// Construct a live row that holds its bound surrogate.
    pub fn live(
        coord: &'a [CoordValue],
        attrs: &'a [CellValue],
        surrogate: Surrogate,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Self {
        Self {
            coord,
            attrs,
            surrogate: Some(surrogate),
            valid_from_ms,
            valid_until_ms,
            kind: RowKind::Live,
        }
    }

    /// Construct a tombstone or erasure row. It carries no attributes and no
    /// identity.
    pub fn sentinel(coord: &'a [CoordValue], kind: RowKind) -> Self {
        Self {
            coord,
            attrs: &[],
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind,
        }
    }
}

/// Streaming builder. Folds `(coord, attrs)` pairs into the tile body
/// and the MBR in one pass.
pub struct SparseTileBuilder<'a> {
    schema: &'a ArraySchema,
    dim_dicts: Vec<DimDict>,
    attr_cols: Vec<Vec<CellValue>>,
    surrogates: Vec<Option<Surrogate>>,
    valid_from_ms: Vec<i64>,
    valid_until_ms: Vec<i64>,
    row_kinds: Vec<u8>,
    mbr: MbrBuilder,
    /// Per dim: dictionary value → its index in `dim_dicts`, so a row's
    /// dictionary lookup costs one hash, not a scan of the dictionary.
    dim_index: Vec<FxHashMap<CoordValue, u32>>,
}

impl<'a> SparseTileBuilder<'a> {
    pub fn new(schema: &'a ArraySchema) -> Self {
        Self {
            schema,
            dim_index: (0..schema.arity()).map(|_| FxHashMap::default()).collect(),
            dim_dicts: (0..schema.arity()).map(|_| DimDict::new()).collect(),
            attr_cols: (0..schema.attrs.len()).map(|_| Vec::new()).collect(),
            surrogates: Vec::new(),
            valid_from_ms: Vec::new(),
            valid_until_ms: Vec::new(),
            row_kinds: Vec::new(),
            mbr: MbrBuilder::new(schema.arity(), schema.attrs.len()),
        }
    }

    /// Push a row with full bitemporal metadata.
    ///
    /// Sentinel rows (`Tombstone`, `GdprErased`) may have an empty `attrs`
    /// slice — the attribute columns receive no entry for these rows, which
    /// is correct because sentinel rows carry no payload.
    pub fn push_row(&mut self, row: SparseRow<'_>) -> ArrayResult<()> {
        let SparseRow {
            coord,
            attrs,
            surrogate,
            valid_from_ms,
            valid_until_ms,
            kind,
        } = row;
        match (kind, surrogate) {
            (_, Some(s)) if s == Surrogate::ZERO => {
                return Err(ArrayError::InvalidOp {
                    detail: format!(
                        "row pushed into array '{}' carries Surrogate::ZERO, which names no row",
                        self.schema.name
                    ),
                });
            }
            (RowKind::Tombstone | RowKind::GdprErased, Some(s)) => {
                return Err(ArrayError::InvalidOp {
                    detail: format!(
                        "{kind:?} row pushed into array '{}' carries surrogate {s}; a \
                         tombstone or erasure row carries no identity",
                        self.schema.name
                    ),
                });
            }
            _ => {}
        }
        if coord.len() != self.schema.arity() {
            return Err(ArrayError::CoordArityMismatch {
                array: self.schema.name.clone(),
                expected: self.schema.arity(),
                got: coord.len(),
            });
        }
        // Sentinel rows carry no payload — allow empty attrs. Live rows must
        // match the schema attr count exactly.
        if kind == RowKind::Live && attrs.len() != self.schema.attrs.len() {
            return Err(ArrayError::CellTypeMismatch {
                array: self.schema.name.clone(),
                attr: "<all>".to_string(),
                detail: format!(
                    "expected {} attr columns, got {}",
                    self.schema.attrs.len(),
                    attrs.len()
                ),
            });
        }
        for (i, c) in coord.iter().enumerate() {
            self.push_dim(i, c);
        }
        // For sentinel rows `attrs` is empty — no entries added to attr_cols.
        // For live rows, every attr col gets one entry.
        for (i, a) in attrs.iter().enumerate() {
            self.attr_cols[i].push(a.clone());
        }
        self.surrogates.push(surrogate);
        self.valid_from_ms.push(valid_from_ms);
        self.valid_until_ms.push(valid_until_ms);
        self.row_kinds.push(kind.as_u8());
        // Only fold MBR for live rows; sentinel rows have no meaningful attrs
        // and may have coords outside the declared domain.
        if kind == RowKind::Live {
            self.mbr.fold(coord, attrs);
        }
        Ok(())
    }

    fn push_dim(&mut self, dim: usize, value: &CoordValue) {
        let dict = &mut self.dim_dicts[dim];
        let index = *self.dim_index[dim].entry(value.clone()).or_insert_with(|| {
            dict.values.push(value.clone());
            (dict.values.len() - 1) as u32
        });
        dict.indices.push(index);
    }

    /// Push live row `row` of the stored tile `tile`, whose coordinate is
    /// `coord`, reading its attrs, surrogate and valid time from the tile's
    /// own columns. `live_row` is the row's index in the attr columns.
    pub fn push_tile_row(
        &mut self,
        tile: &SparseTile,
        row: usize,
        live_row: usize,
        coord: &[CoordValue],
    ) -> ArrayResult<()> {
        let corrupt = |detail: String| ArrayError::SegmentCorruption { detail };
        if coord.len() != self.schema.arity() {
            return Err(ArrayError::CoordArityMismatch {
                array: self.schema.name.clone(),
                expected: self.schema.arity(),
                got: coord.len(),
            });
        }
        if tile.attr_cols.len() != self.schema.attrs.len()
            || tile.attr_cols.iter().any(|col| col.len() <= live_row)
        {
            return Err(corrupt(format!(
                "tile live row {live_row} is missing from an attr column of array '{}'",
                self.schema.name
            )));
        }
        let surrogate = tile.live_surrogate(row)?;
        let valid_from_ms = *tile
            .valid_from_ms
            .get(row)
            .ok_or_else(|| corrupt(format!("tile row {row} has no valid_from_ms")))?;
        let valid_until_ms = *tile
            .valid_until_ms
            .get(row)
            .ok_or_else(|| corrupt(format!("tile row {row} has no valid_until_ms")))?;
        for (i, c) in coord.iter().enumerate() {
            self.push_dim(i, c);
        }
        for (out, col) in self.attr_cols.iter_mut().zip(&tile.attr_cols) {
            out.push(col[live_row].clone());
        }
        self.surrogates.push(Some(surrogate));
        self.valid_from_ms.push(valid_from_ms);
        self.valid_until_ms.push(valid_until_ms);
        self.row_kinds.push(RowKind::Live.as_u8());
        self.mbr.fold_with(coord, |i| &tile.attr_cols[i][live_row]);
        Ok(())
    }

    /// Push a derived live row, such as an elementwise result. It holds no
    /// surrogate, so the tile it lands in is a query result and never stored.
    /// A stored cell goes through [`Self::push_row`] with its bound surrogate.
    pub fn push(&mut self, coord: &[CoordValue], attrs: &[CellValue]) -> ArrayResult<()> {
        self.push_row(SparseRow {
            coord,
            attrs,
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
    }

    pub fn build(self) -> SparseTile {
        SparseTile {
            dim_dicts: self.dim_dicts,
            attr_cols: self.attr_cols,
            surrogates: self.surrogates,
            valid_from_ms: self.valid_from_ms,
            valid_until_ms: self.valid_until_ms,
            row_kinds: self.row_kinds,
            mbr: self.mbr.build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "chrom",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(24)),
            ))
            .dim(DimSpec::new(
                "pos",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .attr(AttrSpec::new("variant", AttrType::String, false))
            .attr(AttrSpec::new("qual", AttrType::Float64, true))
            .tile_extents(vec![1, 1_000_000])
            .build()
            .unwrap()
    }

    #[test]
    fn sparse_tile_collects_columns_in_order() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(100)],
            &[CellValue::String("ALT".into()), CellValue::Float64(40.0)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(200)],
            &[CellValue::String("REF".into()), CellValue::Float64(50.0)],
        )
        .unwrap();
        let t = b.build();
        assert_eq!(t.nnz(), 2);
        assert_eq!(t.attr_cols.len(), 2);
        assert_eq!(t.attr_cols[0].len(), 2);
        assert_eq!(t.attr_cols[1].len(), 2);
    }

    #[test]
    fn dim_dict_dedups_repeated_values() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(100)],
            &[CellValue::String("ALT".into()), CellValue::Float64(40.0)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(200)],
            &[CellValue::String("REF".into()), CellValue::Float64(50.0)],
        )
        .unwrap();
        let t = b.build();
        // chrom dictionary should have one entry (1 repeated).
        assert_eq!(t.dim_dicts[0].cardinality(), 1);
        assert_eq!(t.dim_dicts[0].len(), 2);
    }

    #[test]
    fn sparse_tile_rejects_bad_arity() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push(
            &[CoordValue::Int64(1)],
            &[CellValue::String("ALT".into()), CellValue::Float64(0.0)],
        );
        assert!(r.is_err());
    }

    #[test]
    fn sparse_tile_rejects_bad_attr_count() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(0)],
            &[CellValue::String("ALT".into())],
        );
        assert!(r.is_err());
    }

    #[test]
    fn empty_sparse_tile_has_zero_nnz() {
        let s = schema();
        let t = SparseTile::empty(&s);
        assert_eq!(t.nnz(), 0);
        assert_eq!(t.dim_dicts.len(), 2);
        assert_eq!(t.attr_cols.len(), 2);
    }

    #[test]
    fn sparse_tile_msgpack_roundtrip_carries_valid_time_bounds() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(1), CoordValue::Int64(100)],
            attrs: &[CellValue::String("A".into()), CellValue::Float64(1.0)],
            surrogate: Some(Surrogate::new(1)),
            valid_from_ms: 50,
            valid_until_ms: 150,
            kind: RowKind::Live,
        })
        .unwrap();
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(2), CoordValue::Int64(200)],
            attrs: &[CellValue::String("B".into()), CellValue::Float64(2.0)],
            surrogate: Some(Surrogate::new(2)),
            valid_from_ms: 300,
            valid_until_ms: 900,
            kind: RowKind::Live,
        })
        .unwrap();
        let tile = b.build();

        let bytes = zerompk::to_msgpack_vec(&tile).unwrap();
        let decoded: SparseTile = zerompk::from_msgpack(&bytes).unwrap();

        assert_eq!(decoded.valid_from_ms, vec![50, 300]);
        assert_eq!(decoded.valid_until_ms, vec![150, 900]);
        assert_eq!(decoded.nnz(), 2);
    }

    #[test]
    fn sparse_tile_msgpack_roundtrip_carries_row_kinds() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        // Live row
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(1), CoordValue::Int64(10)],
            attrs: &[CellValue::String("X".into()), CellValue::Float64(1.0)],
            surrogate: Some(Surrogate::new(1)),
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
        // Tombstone row (no attrs)
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(2), CoordValue::Int64(20)],
            attrs: &[],
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Tombstone,
        })
        .unwrap();
        // GdprErased row (no attrs)
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(3), CoordValue::Int64(30)],
            attrs: &[],
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::GdprErased,
        })
        .unwrap();
        let tile = b.build();

        let bytes = zerompk::to_msgpack_vec(&tile).unwrap();
        let decoded: SparseTile = zerompk::from_msgpack(&bytes).unwrap();

        assert_eq!(decoded.row_kinds.len(), 3);
        assert_eq!(
            RowKind::from_u8(decoded.row_kinds[0]).unwrap(),
            RowKind::Live
        );
        assert_eq!(
            RowKind::from_u8(decoded.row_kinds[1]).unwrap(),
            RowKind::Tombstone
        );
        assert_eq!(
            RowKind::from_u8(decoded.row_kinds[2]).unwrap(),
            RowKind::GdprErased
        );
        assert_eq!(
            decoded.surrogates,
            vec![Some(Surrogate::new(1)), None, None]
        );
        decoded.check_stored_identities().unwrap();
    }

    /// `Surrogate::ZERO` names no row, so the builder refuses it on any row.
    #[test]
    fn builder_refuses_a_zero_surrogate() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push_row(SparseRow::live(
            &[CoordValue::Int64(1), CoordValue::Int64(10)],
            &[CellValue::String("X".into()), CellValue::Float64(1.0)],
            Surrogate::ZERO,
            0,
            OPEN_UPPER,
        ));
        assert!(r.is_err(), "a live row under ZERO is refused");
        assert_eq!(b.build().row_count(), 0, "the refused row is not stored");
    }

    /// A tombstone or erasure row names a coordinate, never a row identity.
    #[test]
    fn builder_refuses_a_sentinel_row_with_an_identity() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let coord = [CoordValue::Int64(1), CoordValue::Int64(10)];
        let mut row = SparseRow::sentinel(&coord, RowKind::Tombstone);
        row.surrogate = Some(Surrogate::new(4));
        assert!(b.push_row(row).is_err());
    }

    /// A derived row holds no surrogate, so its tile fails the stored-tile
    /// check: it can be read but never written to a segment.
    #[test]
    fn a_derived_row_fails_the_stored_identity_check() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(10)],
            &[CellValue::String("X".into()), CellValue::Float64(1.0)],
        )
        .unwrap();
        let tile = b.build();
        assert_eq!(tile.row_surrogate(0).unwrap(), None);
        assert!(tile.live_surrogate(0).is_err());
        assert!(tile.check_stored_identities().is_err());
    }
}
