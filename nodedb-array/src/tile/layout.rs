//! Tile layout — mapping cell coordinates to tile boundaries.
//!
//! Tile coordinates are cell coordinates integer-divided by the
//! schema's `tile_extents`. The space-filling-curve prefix is computed
//! over the *tile* coordinates (not cell coordinates), so cells in the
//! same tile share a [`TileId::hilbert_prefix`].

use crate::coord::encode::encode_tile_prefix_with_order;
use crate::coord::normalize::bits_per_dim;
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::schema::dim_spec::DimType;
use crate::types::TileId;
use crate::types::coord::value::CoordValue;
use crate::types::domain::DomainBound;

/// Compute the per-dim tile index for one cell coordinate.
pub fn tile_indices_for_cell(schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<Vec<u64>> {
    if coord.len() != schema.arity() {
        return Err(ArrayError::CoordArityMismatch {
            array: schema.name.clone(),
            expected: schema.arity(),
            got: coord.len(),
        });
    }
    let mut out = Vec::with_capacity(schema.arity());
    for (i, dim) in schema.dims.iter().enumerate() {
        let extent = schema.tile_extents[i];
        let cell = &coord[i];
        let lo = &dim.domain.lo;
        let off = match (dim.dtype, cell, lo) {
            (DimType::Int64, CoordValue::Int64(v), DomainBound::Int64(lo))
            | (DimType::TimestampMs, CoordValue::TimestampMs(v), DomainBound::TimestampMs(lo)) => {
                if v < lo {
                    return out_of_domain(schema, dim.name.as_str(), "below domain_lo");
                }
                ((v - lo) as u64) / extent
            }
            (DimType::Float64, CoordValue::Float64(v), DomainBound::Float64(lo)) => {
                if !v.is_finite() || v < lo {
                    return out_of_domain(
                        schema,
                        dim.name.as_str(),
                        "below domain_lo or non-finite",
                    );
                }
                ((v - lo) as u64) / extent
            }
            (DimType::String, CoordValue::String(s), DomainBound::String(_)) => {
                // Strings have no natural tile-extent semantics — bucket
                // by hash modulo extent for stable grouping.
                crate::coord::string_hash::hash_string_modulo(s, extent.max(1))
            }
            _ => {
                return Err(ArrayError::CoordOutOfDomain {
                    array: schema.name.clone(),
                    dim: dim.name.clone(),
                    detail: "coord variant does not match dim dtype".to_string(),
                });
            }
        };
        out.push(off);
    }
    Ok(out)
}

/// Compute the [`TileId`] for one cell coordinate at a given system
/// time. Tier 1 callers pass `system_from_ms = 0`; bitemporal Tier 9
/// supplies the leader-stamped HLC component.
pub fn tile_id_for_cell(
    schema: &ArraySchema,
    coord: &[CoordValue],
    system_from_ms: i64,
) -> ArrayResult<TileId> {
    let tile_indices = tile_indices_for_cell(schema, coord)?;
    let bits = bits_per_dim(schema.arity());
    let prefix = encode_tile_prefix_with_order(schema, &tile_indices, bits)?;
    Ok(TileId::new(prefix, system_from_ms))
}

fn out_of_domain<T>(schema: &ArraySchema, dim: &str, detail: &str) -> ArrayResult<T> {
    Err(ArrayError::CoordOutOfDomain {
        array: schema.name.clone(),
        dim: dim.to_string(),
        detail: detail.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::DimSpec;
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
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![10, 10])
            .build()
            .unwrap()
    }

    #[test]
    fn tile_indices_floor_by_extent() {
        let s = schema();
        let t = tile_indices_for_cell(&s, &[CoordValue::Int64(7), CoordValue::Int64(15)]).unwrap();
        assert_eq!(t, vec![0, 1]);
    }

    #[test]
    fn cells_in_same_tile_share_prefix() {
        let s = schema();
        let t1 = tile_id_for_cell(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)], 0).unwrap();
        let t2 = tile_id_for_cell(&s, &[CoordValue::Int64(9), CoordValue::Int64(9)], 0).unwrap();
        assert_eq!(t1.hilbert_prefix, t2.hilbert_prefix);
    }

    #[test]
    fn cells_in_different_tiles_have_different_prefixes() {
        let s = schema();
        let t1 = tile_id_for_cell(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)], 0).unwrap();
        let t2 = tile_id_for_cell(&s, &[CoordValue::Int64(50), CoordValue::Int64(50)], 0).unwrap();
        assert_ne!(t1.hilbert_prefix, t2.hilbert_prefix);
    }

    #[test]
    fn system_from_ms_propagates() {
        let s = schema();
        let t = tile_id_for_cell(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)], 1234).unwrap();
        assert_eq!(t.system_from_ms, 1234);
    }
}
