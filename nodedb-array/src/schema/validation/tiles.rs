//! Tile-extent validation.
//!
//! Rules:
//! - `tile_extents.len() == dims.len()`.
//! - Each extent is non-zero (a zero-extent tile is degenerate).

use crate::error::{ArrayError, ArrayResult};
use crate::schema::dim_spec::DimSpec;

pub fn check(array: &str, dims: &[DimSpec], extents: &[u64]) -> ArrayResult<()> {
    if extents.len() != dims.len() {
        return Err(ArrayError::InvalidTileExtents {
            array: array.to_string(),
            detail: format!(
                "expected {} extents (one per dim), got {}",
                dims.len(),
                extents.len()
            ),
        });
    }
    for (i, e) in extents.iter().enumerate() {
        if *e == 0 {
            return Err(ArrayError::InvalidTileExtents {
                array: array.to_string(),
                detail: format!(
                    "tile extent for dim '{}' is zero",
                    dims.get(i).map(|d| d.name.as_str()).unwrap_or("?"),
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::dim_spec::DimType;
    use crate::types::domain::{Domain, DomainBound};

    fn int_dim(name: &str) -> DimSpec {
        DimSpec::new(
            name,
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(10)),
        )
    }

    #[test]
    fn rejects_arity_mismatch() {
        let dims = vec![int_dim("x"), int_dim("y")];
        assert!(check("a", &dims, &[1]).is_err());
    }

    #[test]
    fn rejects_zero_extent() {
        let dims = vec![int_dim("x")];
        assert!(check("a", &dims, &[0]).is_err());
    }

    #[test]
    fn accepts_well_formed_extents() {
        let dims = vec![int_dim("x"), int_dim("y")];
        assert!(check("a", &dims, &[1, 100]).is_ok());
    }
}
