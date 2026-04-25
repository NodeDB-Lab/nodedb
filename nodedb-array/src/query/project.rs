//! Attribute projection over a sparse tile.
//!
//! Drops attribute columns the caller doesn't need. Coordinates and
//! per-dim dictionaries are preserved; only `attr_cols` and the MBR's
//! `attr_stats` are subsetted. The output schema (with the same subset
//! of attrs in the same order) must be produced by the caller; this
//! operator does not mutate schemas.

use crate::error::{ArrayError, ArrayResult};
use crate::tile::sparse_tile::SparseTile;

/// Ordered subset of attribute indices into `schema.attrs`. Duplicates
/// are allowed (lets callers fan out a column) but each index must be
/// in range.
#[derive(Debug, Clone)]
pub struct Projection {
    pub attr_indices: Vec<usize>,
}

impl Projection {
    pub fn new(attr_indices: Vec<usize>) -> Self {
        Self { attr_indices }
    }
}

/// Build a new tile carrying only the projected attribute columns.
/// `proj` is validated against `tile.attr_cols.len()` (the tile's own
/// attribute count, not a schema), so the operator is schema-free.
pub fn project_sparse(tile: &SparseTile, proj: &Projection) -> ArrayResult<SparseTile> {
    for &i in &proj.attr_indices {
        if i >= tile.attr_cols.len() {
            return Err(ArrayError::InvalidAttr {
                array: String::new(),
                attr: format!("idx {i}"),
                detail: format!("attr index out of range (have {})", tile.attr_cols.len()),
            });
        }
    }
    let attr_cols = proj
        .attr_indices
        .iter()
        .map(|&i| tile.attr_cols[i].clone())
        .collect();
    let attr_stats = proj
        .attr_indices
        .iter()
        .map(|&i| tile.mbr.attr_stats[i].clone())
        .collect();
    let mut mbr = tile.mbr.clone();
    mbr.attr_stats = attr_stats;
    Ok(SparseTile {
        dim_dicts: tile.dim_dicts.clone(),
        attr_cols,
        surrogates: tile.surrogates.clone(),
        mbr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchema;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("a", AttrType::Int64, true))
            .attr(AttrSpec::new("b", AttrType::Float64, true))
            .attr(AttrSpec::new("c", AttrType::String, true))
            .tile_extents(vec![16])
            .build()
            .unwrap()
    }

    fn tile() -> SparseTile {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(0)],
            &[
                CellValue::Int64(1),
                CellValue::Float64(1.5),
                CellValue::String("x".into()),
            ],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1)],
            &[
                CellValue::Int64(2),
                CellValue::Float64(2.5),
                CellValue::String("y".into()),
            ],
        )
        .unwrap();
        b.build()
    }

    #[test]
    fn project_keeps_subset_in_order() {
        let t = tile();
        let p = Projection::new(vec![2, 0]);
        let out = project_sparse(&t, &p).unwrap();
        assert_eq!(out.attr_cols.len(), 2);
        assert_eq!(out.attr_cols[0][0], CellValue::String("x".into()));
        assert_eq!(out.attr_cols[1][0], CellValue::Int64(1));
        assert_eq!(out.mbr.attr_stats.len(), 2);
    }

    #[test]
    fn project_rejects_out_of_range() {
        let t = tile();
        let p = Projection::new(vec![5]);
        assert!(project_sparse(&t, &p).is_err());
    }
}
