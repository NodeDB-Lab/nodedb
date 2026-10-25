// SPDX-License-Identifier: Apache-2.0

//! Physical reads: every attr cell through its attr's metadata.
//!
//! A nodata cell reads as NULL. A numeric cell of an attr with a scale or
//! offset reads as `raw * scale + offset`, a `FLOAT64`. Every other cell
//! reads unchanged. See [`crate::schema::AttrMeta::physical_cell`].

use crate::schema::ArraySchema;
use crate::tile::sparse_tile::SparseTile;
use crate::types::cell_value::value::CellValue;

/// `tile` with every attr column in schema order read physically.
pub fn physical_sparse(schema: &ArraySchema, mut tile: SparseTile) -> SparseTile {
    for (col, attr) in tile.attr_cols.iter_mut().zip(&schema.attrs) {
        if attr.meta.nodata.is_none() && attr.meta.transform().is_none() {
            continue;
        }
        for cell in col.iter_mut() {
            *cell = attr
                .meta
                .physical_cell(std::mem::replace(cell, CellValue::Null));
        }
    }
    tile
}

/// One cell's attrs, in schema order, read physically.
pub fn physical_cells(schema: &ArraySchema, attrs: Vec<CellValue>) -> Vec<CellValue> {
    attrs
        .into_iter()
        .zip(&schema.attrs)
        .map(|(cell, attr)| attr.meta.physical_cell(cell))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_meta::AttrMeta;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("scene")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(
                AttrSpec::new("red", AttrType::UInt16, true).with_meta(AttrMeta {
                    nodata: Some(0.0),
                    scale: Some(0.5),
                    offset: Some(1.0),
                    units: Some("reflectance".into()),
                }),
            )
            .attr(AttrSpec::new("qa", AttrType::UInt8, true))
            .tile_extents(vec![16])
            .build()
            .unwrap()
    }

    #[test]
    fn nodata_reads_null_and_scaled_attrs_read_floats() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(0)],
            &[CellValue::UInt16(0), CellValue::UInt8(3)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1)],
            &[CellValue::UInt16(4), CellValue::UInt8(0)],
        )
        .unwrap();
        let t = physical_sparse(&s, b.build());
        assert_eq!(
            t.attr_cols[0],
            vec![CellValue::Null, CellValue::Float64(3.0)]
        );
        assert_eq!(
            t.attr_cols[1],
            vec![CellValue::UInt8(3), CellValue::UInt8(0)]
        );
        assert_eq!(
            physical_cells(&s, vec![CellValue::UInt16(2), CellValue::UInt8(9)]),
            vec![CellValue::Float64(2.0), CellValue::UInt8(9)]
        );
    }
}
