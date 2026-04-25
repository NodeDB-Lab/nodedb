//! Dimension-list validation.
//!
//! Rules:
//! - At least one dim.
//! - Dim names are unique within the schema.
//! - Each dim's domain bound variant matches its declared `DimType`.
//! - For ordered numeric dims, `lo <= hi` (lexicographic for strings).

use std::collections::HashSet;

use crate::error::{ArrayError, ArrayResult};
use crate::schema::dim_spec::{DimSpec, DimType};
use crate::types::domain::DomainBound;

pub fn check(array: &str, dims: &[DimSpec]) -> ArrayResult<()> {
    if dims.is_empty() {
        return Err(ArrayError::InvalidSchema {
            array: array.to_string(),
            detail: "at least one dimension is required".to_string(),
        });
    }
    let mut seen = HashSet::with_capacity(dims.len());
    for d in dims {
        if !seen.insert(d.name.as_str()) {
            return Err(ArrayError::InvalidDim {
                array: array.to_string(),
                dim: d.name.clone(),
                detail: "duplicate dimension name".to_string(),
            });
        }
        check_bounds_match_type(array, d)?;
        check_bound_order(array, d)?;
    }
    Ok(())
}

fn check_bounds_match_type(array: &str, d: &DimSpec) -> ArrayResult<()> {
    let ok = matches!(
        (&d.dtype, &d.domain.lo, &d.domain.hi),
        (DimType::Int64, DomainBound::Int64(_), DomainBound::Int64(_))
            | (
                DimType::Float64,
                DomainBound::Float64(_),
                DomainBound::Float64(_),
            )
            | (
                DimType::TimestampMs,
                DomainBound::TimestampMs(_),
                DomainBound::TimestampMs(_),
            )
            | (
                DimType::String,
                DomainBound::String(_),
                DomainBound::String(_),
            )
    );
    if !ok {
        return Err(ArrayError::InvalidDim {
            array: array.to_string(),
            dim: d.name.clone(),
            detail: "domain bound variant does not match declared dtype".to_string(),
        });
    }
    Ok(())
}

fn check_bound_order(array: &str, d: &DimSpec) -> ArrayResult<()> {
    let ordered = match (&d.domain.lo, &d.domain.hi) {
        (DomainBound::Int64(lo), DomainBound::Int64(hi)) => lo <= hi,
        (DomainBound::Float64(lo), DomainBound::Float64(hi)) => {
            !lo.is_nan() && !hi.is_nan() && lo <= hi
        }
        (DomainBound::TimestampMs(lo), DomainBound::TimestampMs(hi)) => lo <= hi,
        (DomainBound::String(lo), DomainBound::String(hi)) => lo <= hi,
        _ => true,
    };
    if !ordered {
        return Err(ArrayError::InvalidDim {
            array: array.to_string(),
            dim: d.name.clone(),
            detail: "domain lo > hi (or non-finite float bound)".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::domain::Domain;

    fn int_dim(name: &str, lo: i64, hi: i64) -> DimSpec {
        DimSpec::new(
            name,
            DimType::Int64,
            Domain::new(DomainBound::Int64(lo), DomainBound::Int64(hi)),
        )
    }

    #[test]
    fn rejects_empty_dim_list() {
        assert!(check("a", &[]).is_err());
    }

    #[test]
    fn rejects_duplicate_dim_names() {
        let dims = vec![int_dim("x", 0, 10), int_dim("x", 0, 10)];
        assert!(check("a", &dims).is_err());
    }

    #[test]
    fn rejects_lo_greater_than_hi() {
        let dims = vec![int_dim("x", 10, 0)];
        assert!(check("a", &dims).is_err());
    }

    #[test]
    fn rejects_bound_type_mismatch() {
        let d = DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Float64(0.0), DomainBound::Float64(10.0)),
        );
        assert!(check("a", &[d]).is_err());
    }

    #[test]
    fn rejects_nan_float_bound() {
        let d = DimSpec::new(
            "x",
            DimType::Float64,
            Domain::new(DomainBound::Float64(f64::NAN), DomainBound::Float64(1.0)),
        );
        assert!(check("a", &[d]).is_err());
    }

    #[test]
    fn accepts_well_formed_dims() {
        let dims = vec![int_dim("chrom", 0, 24), int_dim("pos", 0, 300_000_000)];
        assert!(check("a", &dims).is_ok());
    }
}
