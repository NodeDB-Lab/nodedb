//! Dense tile payload — flat row-major attribute arrays.
//!
//! Used when a tile's fill ratio crosses
//! [`super::DENSE_PROMOTION_THRESHOLD`]. The dense layout drops
//! coordinate columns entirely: cell `i`'s coordinates are recovered
//! from `i` and the tile's per-dim extents.

use serde::{Deserialize, Serialize};

use super::mbr::TileMBR;
use crate::schema::ArraySchema;
use crate::types::cell_value::value::CellValue;

/// Dense tile payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DenseTile {
    /// One column per schema attr, parallel to `schema.attrs`. Each
    /// column has `cells_per_tile` entries; absent cells are
    /// [`CellValue::Null`].
    pub attr_cols: Vec<Vec<CellValue>>,
    /// `tile_extents` snapshot — needed to recover cell coordinates
    /// from a flat index without re-reading the schema.
    pub tile_extents: Vec<u64>,
    pub mbr: TileMBR,
}

impl DenseTile {
    /// Empty dense tile sized for the given schema. All cells are
    /// pre-filled with [`CellValue::Null`].
    pub fn empty(schema: &ArraySchema) -> Self {
        let cells = cells_per_tile(&schema.tile_extents);
        Self {
            attr_cols: (0..schema.attrs.len())
                .map(|_| vec![CellValue::Null; cells])
                .collect(),
            tile_extents: schema.tile_extents.clone(),
            mbr: TileMBR::new(schema.arity(), schema.attrs.len()),
        }
    }

    pub fn cell_count(&self) -> usize {
        cells_per_tile(&self.tile_extents)
    }

    /// Cell at flat index `i` for attribute column `attr_idx`.
    pub fn cell(&self, attr_idx: usize, i: usize) -> Option<&CellValue> {
        self.attr_cols.get(attr_idx).and_then(|c| c.get(i))
    }
}

/// Total cells per tile = product of extents (saturating at usize::MAX).
pub fn cells_per_tile(extents: &[u64]) -> usize {
    let mut p: u128 = 1;
    for e in extents {
        p = p.saturating_mul(*e as u128);
    }
    if p > usize::MAX as u128 {
        usize::MAX
    } else {
        p as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::{Domain, DomainBound};

    fn schema_4x4() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(3)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(3)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4, 4])
            .build()
            .unwrap()
    }

    #[test]
    fn cells_per_tile_is_product_of_extents() {
        assert_eq!(cells_per_tile(&[4, 4]), 16);
        assert_eq!(cells_per_tile(&[2, 3, 5]), 30);
        assert_eq!(cells_per_tile(&[1]), 1);
    }

    #[test]
    fn dense_tile_starts_all_null() {
        let s = schema_4x4();
        let t = DenseTile::empty(&s);
        assert_eq!(t.cell_count(), 16);
        for col in &t.attr_cols {
            assert!(col.iter().all(|c| matches!(c, CellValue::Null)));
        }
    }
}
