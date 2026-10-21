// SPDX-License-Identifier: Apache-2.0

//! Plan-time checks for an array `GEOREF` clause.
//!
//! The same rules run again in the engine schema builder. This copy
//! reports each refusal with its own SQLSTATE class.
//!
//! - SRID no CRS defines: `22000`.
//! - X or Y names no dim: `42703`.
//! - X equals Y, wrong dim type, or bad coefficients: `22000`.

use nodedb_crs::CrsContext;

use crate::error::{Result, SqlError};
use crate::types_array::{ArrayDimAst, ArrayDimType, ArrayGeorefAst};

/// Check `georef` against the dims of array `array`. The SRID must resolve
/// in `crs`.
pub fn check_georef(
    array: &str,
    georef: &ArrayGeorefAst,
    dims: &[ArrayDimAst],
    crs: &CrsContext<'_>,
) -> Result<()> {
    let data = |detail: String| SqlError::DataException {
        detail: format!("array '{array}' GEOREF: {detail}"),
    };
    crs.resolve_u32(georef.srid)
        .map_err(|e| data(e.to_string()))?;
    let dim = |name: &str| {
        dims.iter()
            .find(|d| d.name == name)
            .ok_or_else(|| SqlError::UnknownColumn {
                table: array.to_string(),
                column: name.to_string(),
            })
    };
    let x = dim(&georef.x_dim)?;
    let y = dim(&georef.y_dim)?;
    if x.name == y.name {
        return Err(data(format!("X and Y name the same dim '{}'", x.name)));
    }
    match &georef.transform {
        None => {
            for (axis, d) in [("X", x), ("Y", y)] {
                if !matches!(d.dtype, ArrayDimType::Int64 | ArrayDimType::Float64) {
                    return Err(data(format!(
                        "{axis} dim '{}' is {}; world coordinates need INT64 or FLOAT64",
                        d.name,
                        dim_type_name(d.dtype)
                    )));
                }
            }
        }
        Some(gt) => {
            for (axis, d) in [("X", x), ("Y", y)] {
                if d.dtype != ArrayDimType::Int64 {
                    return Err(data(format!(
                        "{axis} dim '{}' is {}; pixel indices with TRANSFORM need INT64",
                        d.name,
                        dim_type_name(d.dtype)
                    )));
                }
            }
            if let Some(i) = gt.iter().position(|c| !c.is_finite()) {
                return Err(data(format!(
                    "TRANSFORM coefficient {i} is {}; every coefficient must be finite",
                    gt[i]
                )));
            }
            let det = gt[1] * gt[5] - gt[2] * gt[4];
            if det == 0.0 || !det.is_finite() {
                return Err(data(format!(
                    "TRANSFORM determinant GT1*GT5 - GT2*GT4 is {det}; it must be finite and non-zero"
                )));
            }
        }
    }
    Ok(())
}

fn dim_type_name(dtype: ArrayDimType) -> &'static str {
    match dtype {
        ArrayDimType::Int64 => "INT64",
        ArrayDimType::Float64 => "FLOAT64",
        ArrayDimType::TimestampMs => "TIMESTAMP_MS",
        ArrayDimType::String => "STRING",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crs() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }
    use crate::types_array::ArrayDomainBound;

    fn dim(name: &str, dtype: ArrayDimType) -> ArrayDimAst {
        let (lo, hi) = match dtype {
            ArrayDimType::Int64 => (ArrayDomainBound::Int64(0), ArrayDomainBound::Int64(9)),
            ArrayDimType::Float64 => (
                ArrayDomainBound::Float64(0.0),
                ArrayDomainBound::Float64(9.0),
            ),
            ArrayDimType::TimestampMs => (
                ArrayDomainBound::TimestampMs(0),
                ArrayDomainBound::TimestampMs(9),
            ),
            ArrayDimType::String => (
                ArrayDomainBound::String(String::new()),
                ArrayDomainBound::String("z".into()),
            ),
        };
        ArrayDimAst {
            name: name.into(),
            dtype,
            lo,
            hi,
        }
    }

    fn georef(transform: Option<[f64; 6]>) -> ArrayGeorefAst {
        ArrayGeorefAst {
            srid: 32647,
            x_dim: "col".into(),
            y_dim: "row".into(),
            transform,
        }
    }

    const GT: [f64; 6] = [600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0];

    fn ints() -> Vec<ArrayDimAst> {
        vec![
            dim("row", ArrayDimType::Int64),
            dim("col", ArrayDimType::Int64),
        ]
    }

    #[test]
    fn accepts_pixel_and_world() {
        assert!(check_georef("a", &georef(Some(GT)), &ints(), &crs()).is_ok());
        let world = vec![
            dim("row", ArrayDimType::Float64),
            dim("col", ArrayDimType::Int64),
        ];
        assert!(check_georef("a", &georef(None), &world, &crs()).is_ok());
    }

    #[test]
    fn undefined_srid_is_22000() {
        let mut g = georef(None);
        g.srid = 999;
        let err = check_georef("a", &g, &ints(), &crs()).unwrap_err();
        let SqlError::DataException { detail } = err else {
            panic!("expected DataException, got {err:?}");
        };
        assert!(detail.contains("SRID 999 is not defined"), "{detail}");
    }

    /// Any EPSG projected CRS is accepted, such as UTM 50N for Sabah.
    #[test]
    fn epsg_projected_srid_is_accepted() {
        let mut g = georef(Some(GT));
        g.srid = 32650;
        assert!(check_georef("a", &g, &ints(), &crs()).is_ok());
    }

    #[test]
    fn unknown_dim_is_42703() {
        let mut g = georef(None);
        g.y_dim = "lat".into();
        let err = check_georef("a", &g, &ints(), &crs()).unwrap_err();
        assert_eq!(
            err,
            SqlError::UnknownColumn {
                table: "a".into(),
                column: "lat".into()
            }
        );
    }

    #[test]
    fn same_dim_is_22000() {
        let mut g = georef(None);
        g.y_dim = "col".into();
        let err = check_georef("a", &g, &ints(), &crs()).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
    }

    #[test]
    fn wrong_dim_type_is_22000() {
        let strings = vec![
            dim("row", ArrayDimType::String),
            dim("col", ArrayDimType::Int64),
        ];
        let err = check_georef("a", &georef(None), &strings, &crs()).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        let floats = vec![
            dim("row", ArrayDimType::Float64),
            dim("col", ArrayDimType::Float64),
        ];
        let err = check_georef("a", &georef(Some(GT)), &floats, &crs()).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        let times = vec![
            dim("row", ArrayDimType::TimestampMs),
            dim("col", ArrayDimType::Int64),
        ];
        let err = check_georef("a", &georef(None), &times, &crs()).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
    }

    #[test]
    fn bad_coefficients_are_22000() {
        let mut inf = GT;
        inf[0] = f64::INFINITY;
        for gt in [
            inf,
            [0.0, 1.0, 2.0, 0.0, 2.0, 4.0],
            [0.0, 1e300, 0.0, 0.0, 0.0, 1e300],
        ] {
            let err = check_georef("a", &georef(Some(gt)), &ints(), &crs()).unwrap_err();
            assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        }
    }
}
