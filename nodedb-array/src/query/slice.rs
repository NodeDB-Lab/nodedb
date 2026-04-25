//! Coordinate-range slice over a sparse tile.
//!
//! A [`Slice`] carries one optional [`DimRange`] per schema dimension
//! (None = unconstrained). Bounds are typed [`DomainBound`]s so the
//! filter dispatches by dim type without re-parsing.
//!
//! Two entry points:
//! * [`tile_overlaps_slice`] — cheap MBR-only check; the query
//!   executor calls this against each segment tile entry to skip
//!   non-overlapping tiles before any payload decode.
//! * [`slice_sparse`] — per-cell filter, returns a new [`SparseTile`]
//!   containing only the surviving cells (preserving column order).

use crate::error::ArrayResult;
use crate::schema::ArraySchema;
use crate::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;
use crate::types::domain::DomainBound;

/// Inclusive-by-default closed range on one dim. `lo`/`hi` variants
/// must match the dim's type — mismatches are silently treated as
/// "does not intersect" (the cell drops out).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DimRange {
    pub lo: DomainBound,
    pub hi: DomainBound,
}

impl DimRange {
    pub fn new(lo: DomainBound, hi: DomainBound) -> Self {
        Self { lo, hi }
    }
}

/// Slice predicate. `dim_ranges.len()` must equal `schema.arity()`.
#[derive(
    Debug,
    Clone,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Slice {
    pub dim_ranges: Vec<Option<DimRange>>,
}

impl Slice {
    pub fn new(dim_ranges: Vec<Option<DimRange>>) -> Self {
        Self { dim_ranges }
    }
}

/// True if a tile's per-dim MBR overlaps every constrained dim range.
/// An empty `dim_ranges` (no constraints) trivially overlaps.
pub fn tile_overlaps_slice(
    dim_mins: &[DomainBound],
    dim_maxs: &[DomainBound],
    slice: &Slice,
) -> bool {
    for (i, range) in slice.dim_ranges.iter().enumerate() {
        let Some(r) = range else { continue };
        let Some(tile_min) = dim_mins.get(i) else {
            return false;
        };
        let Some(tile_max) = dim_maxs.get(i) else {
            return false;
        };
        // Disjoint when tile_max < range.lo OR tile_min > range.hi.
        if bound_lt(tile_max, &r.lo) || bound_lt(&r.hi, tile_min) {
            return false;
        }
    }
    true
}

/// Filter cells in `tile` to those whose coords pass every dim range.
/// Result is a freshly-built [`SparseTile`] — dictionaries shrink to
/// surviving values, MBR/attr_stats are recomputed.
pub fn slice_sparse(
    schema: &ArraySchema,
    tile: &SparseTile,
    slice: &Slice,
) -> ArrayResult<SparseTile> {
    let mut b = SparseTileBuilder::new(schema);
    let n = tile.nnz() as usize;
    for row in 0..n {
        let coord: Vec<CoordValue> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        if !cell_in_slice(&coord, slice) {
            continue;
        }
        let attrs: Vec<CellValue> = tile.attr_cols.iter().map(|col| col[row].clone()).collect();
        let surrogate = tile
            .surrogates
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::Surrogate::ZERO);
        b.push_with_surrogate(&coord, &attrs, surrogate)?;
    }
    Ok(b.build())
}

fn cell_in_slice(coord: &[CoordValue], slice: &Slice) -> bool {
    for (i, range) in slice.dim_ranges.iter().enumerate() {
        let Some(r) = range else { continue };
        let Some(c) = coord.get(i) else {
            return false;
        };
        let cb = coord_to_bound(c);
        if bound_lt(&cb, &r.lo) || bound_lt(&r.hi, &cb) {
            return false;
        }
    }
    true
}

fn bound_lt(a: &DomainBound, b: &DomainBound) -> bool {
    match (a, b) {
        (DomainBound::Int64(x), DomainBound::Int64(y)) => x < y,
        (DomainBound::Float64(x), DomainBound::Float64(y)) => x < y,
        (DomainBound::TimestampMs(x), DomainBound::TimestampMs(y)) => x < y,
        (DomainBound::String(x), DomainBound::String(y)) => x < y,
        // Type-mismatched bounds never satisfy a less-than relation.
        _ => false,
    }
}

fn coord_to_bound(c: &CoordValue) -> DomainBound {
    match c {
        CoordValue::Int64(v) => DomainBound::Int64(*v),
        CoordValue::Float64(v) => DomainBound::Float64(*v),
        CoordValue::TimestampMs(v) => DomainBound::TimestampMs(*v),
        CoordValue::String(v) => DomainBound::String(v.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::Domain;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![10, 10])
            .build()
            .unwrap()
    }

    fn tile(rows: &[(i64, i64, i64)]) -> SparseTile {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for (x, y, v) in rows {
            b.push(
                &[CoordValue::Int64(*x), CoordValue::Int64(*y)],
                &[CellValue::Int64(*v)],
            )
            .unwrap();
        }
        b.build()
    }

    #[test]
    fn slice_keeps_only_in_range_cells() {
        let s = schema();
        let t = tile(&[(0, 0, 1), (5, 5, 2), (9, 9, 3)]);
        let sl = Slice::new(vec![
            Some(DimRange::new(DomainBound::Int64(4), DomainBound::Int64(10))),
            None,
        ]);
        let out = slice_sparse(&s, &t, &sl).unwrap();
        assert_eq!(out.nnz(), 2);
    }

    #[test]
    fn empty_slice_keeps_everything() {
        let s = schema();
        let t = tile(&[(0, 0, 1), (5, 5, 2)]);
        let sl = Slice::new(vec![None, None]);
        let out = slice_sparse(&s, &t, &sl).unwrap();
        assert_eq!(out.nnz(), 2);
    }

    #[test]
    fn mbr_overlap_skip_disjoint() {
        let mins = vec![DomainBound::Int64(0), DomainBound::Int64(0)];
        let maxs = vec![DomainBound::Int64(9), DomainBound::Int64(9)];
        let sl = Slice::new(vec![
            Some(DimRange::new(
                DomainBound::Int64(20),
                DomainBound::Int64(30),
            )),
            None,
        ]);
        assert!(!tile_overlaps_slice(&mins, &maxs, &sl));
    }

    #[test]
    fn mbr_overlap_accepts_intersection() {
        let mins = vec![DomainBound::Int64(0), DomainBound::Int64(0)];
        let maxs = vec![DomainBound::Int64(9), DomainBound::Int64(9)];
        let sl = Slice::new(vec![
            Some(DimRange::new(DomainBound::Int64(5), DomainBound::Int64(15))),
            None,
        ]);
        assert!(tile_overlaps_slice(&mins, &maxs, &sl));
    }
}
