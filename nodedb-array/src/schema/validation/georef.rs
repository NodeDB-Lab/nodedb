// SPDX-License-Identifier: Apache-2.0

//! Georef validation.
//!
//! Rules:
//! - `x_dim` and `y_dim` name existing dims and differ.
//! - World mode (no transform): each dim is `INT64` or `FLOAT64`.
//! - Pixel mode (transform): each dim is `INT64`, all six coefficients are
//!   finite, and the determinant is finite and non-zero.

use crate::error::{ArrayError, ArrayResult};
use crate::schema::dim_spec::{DimSpec, DimType};
use crate::schema::georef::ArrayGeoref;

pub fn check(array: &str, dims: &[DimSpec], georef: Option<&ArrayGeoref>) -> ArrayResult<()> {
    let Some(georef) = georef else {
        return Ok(());
    };
    let refuse = |detail: String| ArrayError::InvalidGeoref {
        array: array.to_string(),
        detail,
    };
    let dim_at = |axis: &str, index: u32| {
        usize::try_from(index)
            .ok()
            .and_then(|i| dims.get(i))
            .ok_or_else(|| {
                refuse(format!(
                    "{axis} dim index {index} is out of range for {} dims",
                    dims.len()
                ))
            })
    };
    let x = dim_at("X", georef.x_dim)?;
    let y = dim_at("Y", georef.y_dim)?;
    if georef.x_dim == georef.y_dim {
        return Err(refuse(format!("X and Y name the same dim '{}'", x.name)));
    }
    match &georef.transform {
        None => {
            for (axis, dim) in [("X", x), ("Y", y)] {
                if !matches!(dim.dtype, DimType::Int64 | DimType::Float64) {
                    return Err(refuse(format!(
                        "{axis} dim '{}' is {:?}; world coordinates need INT64 or FLOAT64",
                        dim.name, dim.dtype
                    )));
                }
            }
        }
        Some(transform) => {
            for (axis, dim) in [("X", x), ("Y", y)] {
                if dim.dtype != DimType::Int64 {
                    return Err(refuse(format!(
                        "{axis} dim '{}' is {:?}; pixel indices with TRANSFORM need INT64",
                        dim.name, dim.dtype
                    )));
                }
            }
            if let Some(i) = transform.0.iter().position(|c| !c.is_finite()) {
                return Err(refuse(format!(
                    "TRANSFORM coefficient {i} is {}; every coefficient must be finite",
                    transform.0[i]
                )));
            }
            let det = transform.determinant();
            if det == 0.0 || !det.is_finite() {
                return Err(refuse(format!(
                    "TRANSFORM determinant GT1*GT5 - GT2*GT4 is {det}; it must be finite and non-zero"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::georef::GeoTransform;
    use crate::types::domain::{Domain, DomainBound};
    use nodedb_types::geometry::Srid;

    fn int_dim(name: &str) -> DimSpec {
        DimSpec::new(
            name,
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(1000)),
        )
    }

    fn float_dim(name: &str) -> DimSpec {
        DimSpec::new(
            name,
            DimType::Float64,
            Domain::new(DomainBound::Float64(-180.0), DomainBound::Float64(180.0)),
        )
    }

    fn string_dim(name: &str) -> DimSpec {
        DimSpec::new(
            name,
            DimType::String,
            Domain::new(
                DomainBound::String(String::new()),
                DomainBound::String("z".into()),
            ),
        )
    }

    fn georef(x: u32, y: u32, transform: Option<[f64; 6]>) -> ArrayGeoref {
        ArrayGeoref {
            srid: Srid::new(32647).unwrap(),
            x_dim: x,
            y_dim: y,
            transform: transform.map(GeoTransform),
        }
    }

    const NORTH_UP: [f64; 6] = [600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0];

    #[test]
    fn none_is_accepted() {
        assert!(check("a", &[int_dim("x")], None).is_ok());
    }

    #[test]
    fn accepts_world_mode_int_and_float() {
        let dims = [float_dim("lon"), int_dim("northing")];
        assert!(check("a", &dims, Some(&georef(0, 1, None))).is_ok());
    }

    #[test]
    fn accepts_pixel_mode() {
        let dims = [int_dim("row"), int_dim("col")];
        assert!(check("a", &dims, Some(&georef(1, 0, Some(NORTH_UP)))).is_ok());
    }

    #[test]
    fn refuses_out_of_range_dim() {
        let dims = [int_dim("x"), int_dim("y")];
        assert!(check("a", &dims, Some(&georef(0, 2, None))).is_err());
        assert!(check("a", &dims, Some(&georef(5, 1, None))).is_err());
    }

    #[test]
    fn refuses_same_dim() {
        let dims = [int_dim("x"), int_dim("y")];
        assert!(check("a", &dims, Some(&georef(1, 1, None))).is_err());
    }

    #[test]
    fn refuses_world_mode_string_dim() {
        let dims = [string_dim("x"), int_dim("y")];
        assert!(check("a", &dims, Some(&georef(0, 1, None))).is_err());
    }

    #[test]
    fn refuses_pixel_mode_float_dim() {
        let dims = [float_dim("x"), int_dim("y")];
        assert!(check("a", &dims, Some(&georef(0, 1, Some(NORTH_UP)))).is_err());
    }

    #[test]
    fn refuses_non_finite_coefficient() {
        let dims = [int_dim("x"), int_dim("y")];
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut gt = NORTH_UP;
            gt[3] = bad;
            assert!(check("a", &dims, Some(&georef(0, 1, Some(gt)))).is_err());
        }
    }

    #[test]
    fn refuses_singular_transform() {
        let dims = [int_dim("x"), int_dim("y")];
        let gt = [0.0, 1.0, 2.0, 0.0, 2.0, 4.0];
        assert!(check("a", &dims, Some(&georef(0, 1, Some(gt)))).is_err());
    }

    #[test]
    fn refuses_overflowing_determinant() {
        let dims = [int_dim("x"), int_dim("y")];
        let gt = [0.0, 1e300, 0.0, 0.0, 0.0, -1e300];
        assert!(check("a", &dims, Some(&georef(0, 1, Some(gt)))).is_err());
    }
}
