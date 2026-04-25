//! Per-dimension coordinate normalization.
//!
//! Space-filling curves (Hilbert, Z-order) operate on equal-width
//! unsigned integer coordinates. This module maps each [`CoordValue`]
//! into a `u64` in `[0, 2^bits)` relative to the dimension's
//! [`Domain`]. The bit budget is shared across dims so the total prefix
//! fits in 64 bits.

use crate::error::{ArrayError, ArrayResult};
use crate::schema::dim_spec::DimType;
use crate::schema::{ArraySchema, DimSpec};
use crate::types::coord::value::CoordValue;
use crate::types::domain::DomainBound;

/// Maximum dims supported by ND Hilbert in this implementation. Mirrors
/// the checklist contract ("supports up to 16 dims").
pub const MAX_DIMS: usize = 16;

/// Total bit budget for the combined space-filling-curve prefix.
pub const PREFIX_BITS: u32 = 64;

/// Bits per dim for an `n`-dim schema, dividing [`PREFIX_BITS`] evenly
/// (truncated). Returns at least 1 bit per dim.
pub fn bits_per_dim(n_dims: usize) -> u32 {
    if n_dims == 0 {
        return 0;
    }
    let raw = PREFIX_BITS / n_dims as u32;
    raw.max(1)
}

/// Normalize one cell coordinate into per-dim integer coordinates in
/// `[0, 2^bits)`. The output `Vec` has the same arity as the schema.
pub fn normalize_coord(
    schema: &ArraySchema,
    components: &[CoordValue],
    bits: u32,
) -> ArrayResult<Vec<u64>> {
    if components.len() != schema.arity() {
        return Err(ArrayError::CoordArityMismatch {
            array: schema.name.clone(),
            expected: schema.arity(),
            got: components.len(),
        });
    }
    let mut out = Vec::with_capacity(components.len());
    for (dim, value) in schema.dims.iter().zip(components.iter()) {
        out.push(normalize_one(&schema.name, dim, value, bits)?);
    }
    Ok(out)
}

fn normalize_one(array: &str, dim: &DimSpec, value: &CoordValue, bits: u32) -> ArrayResult<u64> {
    let max = if bits >= 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    match (&dim.dtype, value, &dim.domain.lo, &dim.domain.hi) {
        (DimType::Int64, CoordValue::Int64(v), DomainBound::Int64(lo), DomainBound::Int64(hi))
        | (
            DimType::TimestampMs,
            CoordValue::TimestampMs(v),
            DomainBound::TimestampMs(lo),
            DomainBound::TimestampMs(hi),
        ) => map_int(array, dim, *v, *lo, *hi, max),
        (
            DimType::Float64,
            CoordValue::Float64(v),
            DomainBound::Float64(lo),
            DomainBound::Float64(hi),
        ) => map_float(array, dim, *v, *lo, *hi, max),
        (
            DimType::String,
            CoordValue::String(v),
            DomainBound::String(_),
            DomainBound::String(_),
        ) => Ok(map_string(v, max)),
        _ => Err(ArrayError::CoordOutOfDomain {
            array: array.to_string(),
            dim: dim.name.clone(),
            detail: "coord variant does not match dim dtype".to_string(),
        }),
    }
}

fn map_int(array: &str, dim: &DimSpec, v: i64, lo: i64, hi: i64, max: u64) -> ArrayResult<u64> {
    if v < lo || v > hi {
        return Err(ArrayError::CoordOutOfDomain {
            array: array.to_string(),
            dim: dim.name.clone(),
            detail: format!("value {v} outside [{lo}, {hi}]"),
        });
    }
    let span = (hi as i128) - (lo as i128);
    if span <= 0 {
        return Ok(0);
    }
    let off = (v as i128) - (lo as i128);
    // Linear scale `off` ∈ [0, span] → [0, max].
    let scaled = ((off as u128) * (max as u128)) / (span as u128);
    Ok(scaled as u64)
}

fn map_float(array: &str, dim: &DimSpec, v: f64, lo: f64, hi: f64, max: u64) -> ArrayResult<u64> {
    if !v.is_finite() || v < lo || v > hi {
        return Err(ArrayError::CoordOutOfDomain {
            array: array.to_string(),
            dim: dim.name.clone(),
            detail: format!("value {v} outside [{lo}, {hi}] or non-finite"),
        });
    }
    let span = hi - lo;
    if span <= 0.0 {
        return Ok(0);
    }
    let frac = ((v - lo) / span).clamp(0.0, 1.0);
    Ok((frac * (max as f64)) as u64)
}

/// String dims fall back to a deterministic hash bucket. Hilbert/Z-order
/// over strings is inherently lossy — preserves equality, not ordering.
/// Tier 1 accepts the loss; future work may swap in dictionary-based
/// sort-preserving codes.
fn map_string(v: &str, max: u64) -> u64 {
    super::string_hash::hash_string_masked(v, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::types::domain::Domain;

    fn schema_2d() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(100)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(100)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![10, 10])
            .build()
            .unwrap()
    }

    #[test]
    fn bits_per_dim_distributes_64() {
        assert_eq!(bits_per_dim(1), 64);
        assert_eq!(bits_per_dim(2), 32);
        assert_eq!(bits_per_dim(4), 16);
        assert_eq!(bits_per_dim(16), 4);
    }

    #[test]
    fn bits_per_dim_floor_one() {
        assert_eq!(bits_per_dim(100), 1);
    }

    #[test]
    fn normalize_int_lo_maps_to_zero() {
        let s = schema_2d();
        let n = normalize_coord(&s, &[CoordValue::Int64(0), CoordValue::Int64(0)], 8).unwrap();
        assert_eq!(n, vec![0, 0]);
    }

    #[test]
    fn normalize_int_hi_maps_to_max() {
        let s = schema_2d();
        let n = normalize_coord(&s, &[CoordValue::Int64(100), CoordValue::Int64(100)], 8).unwrap();
        assert_eq!(n, vec![255, 255]);
    }

    #[test]
    fn normalize_rejects_out_of_domain() {
        let s = schema_2d();
        let r = normalize_coord(&s, &[CoordValue::Int64(101), CoordValue::Int64(0)], 8);
        assert!(r.is_err());
    }

    #[test]
    fn normalize_rejects_arity_mismatch() {
        let s = schema_2d();
        let r = normalize_coord(&s, &[CoordValue::Int64(0)], 8);
        assert!(r.is_err());
    }

    #[test]
    fn normalize_rejects_type_mismatch() {
        let s = schema_2d();
        let r = normalize_coord(&s, &[CoordValue::Float64(0.0), CoordValue::Int64(0)], 8);
        assert!(r.is_err());
    }
}
