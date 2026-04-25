//! High-level coordinate-prefix encoding.
//!
//! Combines [`super::normalize`] with [`super::hilbert`] /
//! [`super::zorder`] and dispatches off the schema's
//! [`crate::schema::CellOrder`] for cell-prefix encoding, or
//! [`crate::schema::TileOrder`] for tile-prefix encoding (tile coords
//! are cell coords integer-divided by `tile_extents`).

use super::{hilbert, normalize, zorder};
use crate::error::ArrayResult;
use crate::schema::ArraySchema;
use crate::schema::cell_order::{CellOrder, TileOrder};
use crate::types::coord::value::CoordValue;

/// Encode a Hilbert prefix for one cell coordinate using the schema's
/// configured per-dim bit budget.
pub fn encode_hilbert_prefix(schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<u64> {
    let bits = normalize::bits_per_dim(schema.arity());
    let normalized = normalize::normalize_coord(schema, coord, bits)?;
    hilbert::encode(&normalized, bits)
}

/// Encode a Z-order prefix for one cell coordinate.
pub fn encode_zorder_prefix(schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<u64> {
    let bits = normalize::bits_per_dim(schema.arity());
    let normalized = normalize::normalize_coord(schema, coord, bits)?;
    zorder::encode(&normalized, bits)
}

/// Encode whichever space-filling curve the schema declares for cells.
pub fn encode_cell_prefix(schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<u64> {
    match schema.cell_order {
        CellOrder::Hilbert | CellOrder::RowMajor | CellOrder::ColMajor => {
            encode_hilbert_prefix(schema, coord)
        }
        CellOrder::ZOrder => encode_zorder_prefix(schema, coord),
    }
}

/// Encode whichever curve the schema declares for tile boundaries.
pub fn encode_tile_prefix_with_order(
    schema: &ArraySchema,
    tile_indices: &[u64],
    bits: u32,
) -> ArrayResult<u64> {
    match schema.tile_order {
        TileOrder::Hilbert | TileOrder::RowMajor | TileOrder::ColMajor => {
            hilbert::encode(tile_indices, bits)
        }
        TileOrder::ZOrder => zorder::encode(tile_indices, bits),
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
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(31)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(31)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![8, 8])
            .build()
            .unwrap()
    }

    #[test]
    fn hilbert_prefix_distinct_for_distinct_cells() {
        let s = schema();
        let a = encode_hilbert_prefix(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)]).unwrap();
        let b = encode_hilbert_prefix(&s, &[CoordValue::Int64(31), CoordValue::Int64(31)]).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn zorder_prefix_distinct_for_distinct_cells() {
        let s = schema();
        let a = encode_zorder_prefix(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)]).unwrap();
        let b = encode_zorder_prefix(&s, &[CoordValue::Int64(31), CoordValue::Int64(31)]).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn cell_prefix_dispatches_on_order() {
        let s = schema();
        let p = encode_cell_prefix(&s, &[CoordValue::Int64(7), CoordValue::Int64(13)]);
        assert!(p.is_ok());
    }
}
