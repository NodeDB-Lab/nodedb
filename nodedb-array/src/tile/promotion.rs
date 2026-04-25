//! Sparse → dense promotion at fill ratio > [`DENSE_PROMOTION_THRESHOLD`].
//!
//! The threshold is fixed in code rather than in the schema because
//! it's a storage-level decision, not a workload one — at fill ratios
//! above 70%, the per-cell overhead of coordinate columns exceeds the
//! cost of storing nulls densely.

use super::dense_tile::{DenseTile, cells_per_tile};
use super::mbr::MbrBuilder;
use super::sparse_tile::SparseTile;
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::types::cell_value::value::CellValue;

/// Fill ratio above which a sparse tile is rewritten into a dense
/// tile. From the checklist: "auto-promotion from sparse at fill ratio
/// > 0.7".
pub const DENSE_PROMOTION_THRESHOLD: f64 = 0.7;

/// True if `nnz / cells_per_tile > threshold`.
pub fn should_promote_to_dense(tile: &SparseTile, schema: &ArraySchema) -> bool {
    let total = cells_per_tile(&schema.tile_extents);
    if total == 0 {
        return false;
    }
    (tile.nnz() as f64) / (total as f64) > DENSE_PROMOTION_THRESHOLD
}

/// Convert a sparse tile to a dense tile by materialising every cell.
///
/// Cells absent in the sparse payload become [`CellValue::Null`]. The
/// caller is responsible for the integer-only-dim precondition (dense
/// indexing relies on integer cell offsets) — this is enforced
/// because non-`Int64`/`TimestampMs` dims do not have well-defined
/// `tile_extent` semantics in dense layout.
pub fn sparse_to_dense(tile: &SparseTile, schema: &ArraySchema) -> ArrayResult<DenseTile> {
    use crate::schema::dim_spec::DimType;
    use crate::types::coord::value::CoordValue;
    for d in &schema.dims {
        if !matches!(d.dtype, DimType::Int64 | DimType::TimestampMs) {
            return Err(ArrayError::InvalidSchema {
                array: schema.name.clone(),
                detail: format!(
                    "dense promotion requires integer dims; '{}' is {:?}",
                    d.name, d.dtype
                ),
            });
        }
    }
    let mut dense = DenseTile::empty(schema);
    let mut mbr = MbrBuilder::new(schema.arity(), schema.attrs.len());
    let n_rows = tile.nnz() as usize;
    for row in 0..n_rows {
        // Reconstruct row's coord from the dim dictionaries.
        let coord: Vec<CoordValue> = schema
            .dims
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let dict = &tile.dim_dicts[i];
                let idx = dict.indices[row] as usize;
                dict.values[idx].clone()
            })
            .collect();
        let attrs: Vec<CellValue> = (0..schema.attrs.len())
            .map(|i| tile.attr_cols[i][row].clone())
            .collect();
        let flat = flat_index_for_coord(schema, &coord)?;
        for (i, a) in attrs.iter().enumerate() {
            dense.attr_cols[i][flat] = a.clone();
        }
        mbr.fold(&coord, &attrs);
    }
    dense.mbr = mbr.build();
    Ok(dense)
}

/// Row-major flat index `((c0 - lo0) * extent1 * extent2 ...) + ...`.
fn flat_index_for_coord(
    schema: &ArraySchema,
    coord: &[crate::types::coord::value::CoordValue],
) -> ArrayResult<usize> {
    use crate::schema::dim_spec::DimType;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::DomainBound;
    let mut flat: usize = 0;
    for (i, dim) in schema.dims.iter().enumerate() {
        let extent = schema.tile_extents[i] as usize;
        let lo = match (&dim.dtype, &dim.domain.lo) {
            (DimType::Int64, DomainBound::Int64(v))
            | (DimType::TimestampMs, DomainBound::TimestampMs(v)) => *v,
            _ => 0,
        };
        let off = match coord.get(i) {
            Some(CoordValue::Int64(v)) | Some(CoordValue::TimestampMs(v)) => {
                ((*v - lo) as usize) % extent
            }
            _ => {
                return Err(ArrayError::CoordOutOfDomain {
                    array: schema.name.clone(),
                    dim: dim.name.clone(),
                    detail: "non-integer coord in dense promotion".to_string(),
                });
            }
        };
        flat = flat * extent + off;
    }
    Ok(flat)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::{Domain, DomainBound};

    fn schema_2x2() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![2, 2])
            .build()
            .unwrap()
    }

    fn build_tile(s: &ArraySchema, n: usize) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        let cells = [(0i64, 0i64, 10i64), (0, 1, 20), (1, 0, 30), (1, 1, 40)];
        for (x, y, v) in cells.iter().take(n) {
            b.push(
                &[CoordValue::Int64(*x), CoordValue::Int64(*y)],
                &[CellValue::Int64(*v)],
            )
            .unwrap();
        }
        b.build()
    }

    #[test]
    fn promotes_above_threshold() {
        let s = schema_2x2();
        let t = build_tile(&s, 3); // 3/4 = 0.75 > 0.7
        assert!(should_promote_to_dense(&t, &s));
    }

    #[test]
    fn does_not_promote_at_or_below_threshold() {
        let s = schema_2x2();
        let t = build_tile(&s, 2); // 2/4 = 0.5
        assert!(!should_promote_to_dense(&t, &s));
    }

    #[test]
    fn dense_conversion_places_cells_at_flat_index() {
        let s = schema_2x2();
        let t = build_tile(&s, 4);
        let d = sparse_to_dense(&t, &s).unwrap();
        // Row-major (x, y) for 2x2 with extents [2, 2]: (0,0)=0,
        // (0,1)=1, (1,0)=2, (1,1)=3.
        assert_eq!(d.attr_cols[0][0], CellValue::Int64(10));
        assert_eq!(d.attr_cols[0][1], CellValue::Int64(20));
        assert_eq!(d.attr_cols[0][2], CellValue::Int64(30));
        assert_eq!(d.attr_cols[0][3], CellValue::Int64(40));
        assert_eq!(d.mbr.nnz, 4);
    }

    #[test]
    fn dense_conversion_leaves_absent_cells_null() {
        let s = schema_2x2();
        let t = build_tile(&s, 2);
        let d = sparse_to_dense(&t, &s).unwrap();
        // Two cells populated, two should remain Null.
        let nulls = d.attr_cols[0]
            .iter()
            .filter(|c| matches!(c, CellValue::Null))
            .count();
        assert_eq!(nulls, 2);
    }
}
