//! Re-bucket cells into a different tile-extent layout.
//!
//! The same schema may be re-tiled (different `tile_extents`) for a
//! query that needs a different access pattern — e.g. a slice along
//! one axis benefits from longer extents on that axis. Rechunk takes
//! a single source tile and emits the (possibly multiple) target
//! tiles that contain its cells.
//!
//! Both source and target schemas must share name, dim arity, attrs
//! and dim domains; only `tile_extents` may differ. The caller is
//! responsible for that constraint — this is a pure re-bucket.

use std::collections::BTreeMap;

use crate::error::ArrayResult;
use crate::schema::ArraySchema;
use crate::tile::layout::tile_id_for_cell;
use crate::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use crate::types::TileId;
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

/// Re-bucket every cell in `tile` according to `target_schema.tile_extents`.
/// Returns one entry per resulting target tile, ordered by [`TileId`].
pub fn rechunk_sparse(
    target_schema: &ArraySchema,
    tile: &SparseTile,
) -> ArrayResult<Vec<(TileId, SparseTile)>> {
    let n = tile.nnz() as usize;
    let mut buckets: BTreeMap<TileId, SparseTileBuilder<'_>> = BTreeMap::new();
    for row in 0..n {
        let coord: Vec<CoordValue> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        let attrs: Vec<CellValue> = tile.attr_cols.iter().map(|col| col[row].clone()).collect();
        let tid = tile_id_for_cell(target_schema, &coord, 0)?;
        let entry = buckets
            .entry(tid)
            .or_insert_with(|| SparseTileBuilder::new(target_schema));
        entry.push(&coord, &attrs)?;
    }
    Ok(buckets.into_iter().map(|(k, v)| (k, v.build())).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::{Domain, DomainBound};

    fn schema(extents: Vec<u64>) -> ArraySchema {
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
            .tile_extents(extents)
            .build()
            .unwrap()
    }

    #[test]
    fn rechunk_splits_into_smaller_tiles() {
        let src = schema(vec![100, 100]);
        let dst = schema(vec![10, 10]);
        let mut b = SparseTileBuilder::new(&src);
        b.push(
            &[CoordValue::Int64(5), CoordValue::Int64(5)],
            &[CellValue::Int64(1)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(50), CoordValue::Int64(50)],
            &[CellValue::Int64(2)],
        )
        .unwrap();
        let big = b.build();
        let out = rechunk_sparse(&dst, &big).unwrap();
        assert_eq!(out.len(), 2);
        for (_, t) in &out {
            assert_eq!(t.nnz(), 1);
        }
    }

    #[test]
    fn rechunk_preserves_total_cells() {
        let src = schema(vec![16, 16]);
        let dst = schema(vec![4, 4]);
        let mut b = SparseTileBuilder::new(&src);
        for i in 0..8i64 {
            b.push(
                &[CoordValue::Int64(i), CoordValue::Int64(i)],
                &[CellValue::Int64(i)],
            )
            .unwrap();
        }
        let big = b.build();
        let out = rechunk_sparse(&dst, &big).unwrap();
        let total: u32 = out.iter().map(|(_, t)| t.nnz()).sum();
        assert_eq!(total, 8);
    }
}
