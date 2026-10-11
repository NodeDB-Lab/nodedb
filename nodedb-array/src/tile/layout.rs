// SPDX-License-Identifier: Apache-2.0

//! Tile layout — mapping cell coordinates to tile boundaries.
//!
//! Tile coordinates are cell coordinates integer-divided by the
//! schema's `tile_extents`. The space-filling-curve prefix is computed
//! over the *tile* coordinates (not cell coordinates), so cells in the
//! same tile share a [`TileId::hilbert_prefix`].

use crate::coord::encode::encode_tile_prefix_with_order;
use crate::coord::normalize::{bits_per_dim, normalize_coord};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::schema::dim_spec::{DimSpec, DimType};
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
                let delta = (*v as i128) - (*lo as i128);
                (delta as u128 / u128::from(extent)) as u64
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
/// time. Non-bitemporal callers pass `system_from_ms = 0`; bitemporal
/// writes supply the leader-stamped HLC component.
pub fn tile_id_for_cell(
    schema: &ArraySchema,
    coord: &[CoordValue],
    system_from_ms: i64,
) -> ArrayResult<TileId> {
    let tile_indices = tile_indices_for_cell(schema, coord)?;
    let prefix = tile_prefix(schema, &tile_indices)?;
    Ok(TileId::new(prefix, system_from_ms))
}

/// The space-filling-curve key of the tile at `tile_indices`.
///
/// The memtable, the segment index, the transaction overlay and retention
/// group cells by this key, and a cluster routes a tile by its top bits.
/// Every cell inside a group stays addressed by its full coordinate: a
/// lookup matches the coordinate within the group
/// (`TileBuffer::get_cell_bytes`, `extract_cell_bytes`), and a tile's MBR
/// is built from the cells it holds. Two tiles that share a key therefore
/// share one group and lose no cell.
///
/// A dimension whose tile count fits its share of the key bits scales each
/// index over that count to the full bit width. Its tiles keep distinct,
/// ordered keys that spread over the top bits. A dimension with more tiles
/// than its bits can number, such as one with the default full-range
/// domain, keeps the low bits of the index instead. Nearby tiles then keep
/// distinct keys, and tiles `2^bits` apart share one.
pub fn tile_prefix(schema: &ArraySchema, tile_indices: &[u64]) -> ArrayResult<u64> {
    if tile_indices.len() != schema.arity() {
        return Err(ArrayError::CoordArityMismatch {
            array: schema.name.clone(),
            expected: schema.arity(),
            got: tile_indices.len(),
        });
    }
    let bits = bits_per_dim(schema.arity());
    let scaled: Vec<u64> = tile_indices
        .iter()
        .zip(schema.dims.iter().zip(schema.tile_extents.iter()))
        .map(|(index, (dim, extent))| scale_tile_index(*index, tiles_per_dim(dim, *extent), bits))
        .collect();
    encode_tile_prefix_with_order(schema, &scaled, bits)
}

/// The key of the tile that holds the cell at `coord`: the key every
/// cluster path routes the cell by. Fails when `coord` lies outside the
/// schema's domain.
pub fn cell_tile_prefix(schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<u64> {
    normalize_coord(schema, coord, bits_per_dim(schema.arity()))?;
    tile_prefix(schema, &tile_indices_for_cell(schema, coord)?)
}

/// The number of tiles along `dim` for tile extent `extent`. Mirrors how
/// [`tile_indices_for_cell`] numbers them: a cell in the domain has a tile
/// index below this count.
pub fn tiles_per_dim(dim: &DimSpec, extent: u64) -> u128 {
    let extent = u128::from(extent.max(1));
    match (dim.dtype, &dim.domain.lo, &dim.domain.hi) {
        (DimType::Int64, DomainBound::Int64(lo), DomainBound::Int64(hi))
        | (DimType::TimestampMs, DomainBound::TimestampMs(lo), DomainBound::TimestampMs(hi)) => {
            let span = ((*hi as i128) - (*lo as i128)).max(0) as u128;
            span / extent + 1
        }
        (DimType::Float64, DomainBound::Float64(lo), DomainBound::Float64(hi)) => {
            if hi.is_finite() && lo.is_finite() && hi > lo {
                u128::from((hi - lo) as u64) / extent + 1
            } else {
                1
            }
        }
        (DimType::String, DomainBound::String(_), DomainBound::String(_)) => extent,
        _ => 1,
    }
}

/// Map tile index `index` of `tiles` tiles to `[0, 2^bits)`.
///
/// With `tiles <= 2^bits` the index is scaled over the tile count, so
/// distinct indices keep distinct, ordered values. An index past the last
/// tile, a cell above the domain, takes the top value. With more tiles the
/// index keeps its low `bits` bits.
fn scale_tile_index(index: u64, tiles: u128, bits: u32) -> u64 {
    let limit: u128 = 1u128 << bits.min(64);
    let max = limit - 1;
    if tiles > limit {
        return u64::try_from(u128::from(index) & max).unwrap_or(u64::MAX);
    }
    if tiles <= 1 {
        return if index == 0 { 0 } else { max as u64 };
    }
    let scaled = (u128::from(index) * max / (tiles - 1)).min(max);
    u64::try_from(scaled).unwrap_or(u64::MAX)
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
    fn tile_keys_spread_over_the_top_bits() {
        let s = schema();
        let tops: std::collections::BTreeSet<u64> = (0..10)
            .flat_map(|x| (0..10).map(move |y| (x * 10, y * 10)))
            .map(|(x, y)| {
                tile_id_for_cell(&s, &[CoordValue::Int64(x), CoordValue::Int64(y)], 0)
                    .unwrap()
                    .hilbert_prefix
                    >> 56
            })
            .collect();
        assert!(
            tops.len() > 1,
            "the tiles of one array route to more than one bucket: {tops:?}"
        );
    }

    #[test]
    fn every_tile_keeps_a_distinct_key() {
        let s = schema();
        let keys: std::collections::BTreeSet<u64> = (0..10)
            .flat_map(|x| (0..10).map(move |y| (x * 10, y * 10)))
            .map(|(x, y)| {
                tile_id_for_cell(&s, &[CoordValue::Int64(x), CoordValue::Int64(y)], 0)
                    .unwrap()
                    .hilbert_prefix
            })
            .collect();
        assert_eq!(keys.len(), 100);
    }

    #[test]
    fn a_cell_routes_by_its_tile_key() {
        let s = schema();
        let coord = [CoordValue::Int64(37), CoordValue::Int64(81)];
        assert_eq!(
            cell_tile_prefix(&s, &coord).unwrap(),
            tile_id_for_cell(&s, &coord, 0).unwrap().hilbert_prefix
        );
        assert_eq!(
            cell_tile_prefix(&s, &[CoordValue::Int64(30), CoordValue::Int64(89)]).unwrap(),
            cell_tile_prefix(&s, &coord).unwrap(),
            "cells of one tile route together"
        );
    }

    #[test]
    fn a_cell_outside_the_domain_has_no_route() {
        let s = schema();
        assert!(cell_tile_prefix(&s, &[CoordValue::Int64(100), CoordValue::Int64(0)]).is_err());
    }

    #[test]
    fn a_cell_above_the_domain_takes_the_last_key() {
        let s = schema();
        let above = tile_id_for_cell(&s, &[CoordValue::Int64(150), CoordValue::Int64(0)], 0)
            .unwrap()
            .hilbert_prefix;
        let last = tile_id_for_cell(&s, &[CoordValue::Int64(99), CoordValue::Int64(0)], 0)
            .unwrap()
            .hilbert_prefix;
        assert_eq!(above, last);
    }

    #[test]
    fn nearby_tiles_of_a_full_range_dimension_keep_distinct_keys() {
        let s = ArraySchemaBuilder::new("open")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(i64::MIN), DomainBound::Int64(i64::MAX)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(i64::MIN), DomainBound::Int64(i64::MAX)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![10, 10])
            .build()
            .unwrap();
        let keys: std::collections::BTreeSet<u64> = (0..10)
            .flat_map(|x| (0..10).map(move |y| (x * 10, y * 10)))
            .map(|(x, y)| {
                tile_id_for_cell(&s, &[CoordValue::Int64(x), CoordValue::Int64(y)], 0)
                    .unwrap()
                    .hilbert_prefix
            })
            .collect();
        assert_eq!(keys.len(), 100, "a schema with any domain builds");
    }

    #[test]
    fn system_from_ms_propagates() {
        let s = schema();
        let t = tile_id_for_cell(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)], 1234).unwrap();
        assert_eq!(t.system_from_ms, 1234);
    }
}
