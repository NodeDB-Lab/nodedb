// SPDX-License-Identifier: Apache-2.0

//! GeoKey codes and units shared by the user-defined CRS decoder.

use core::fmt::Write;

use nodedb_crs::LinearUnit;

use crate::error::RasterError;
use crate::tiff::geokeys::{GeoKeyDirectory, PROJ_LINEAR_UNIT_SIZE, PROJ_LINEAR_UNITS};

/// GeoKey value of a CRS, datum, ellipsoid or unit defined by parameters.
pub const USER_DEFINED: u16 = 32767;

/// GeoKey value of an undefined code.
pub const UNDEFINED: u16 = 0;

/// EPSG angle unit codes.
const RADIAN: u16 = 9101;
const DEGREE: u16 = 9102;
const GRAD: u16 = 9105;
const DEGREE_SUPPLIER: u16 = 9122;

/// A key value that names an EPSG code: neither absent, undefined, nor
/// user-defined.
pub(super) fn epsg_code(value: Option<u16>) -> Option<u16> {
    value.filter(|v| *v != UNDEFINED && *v != USER_DEFINED)
}

/// Append formatted text. Writing to a `String` cannot fail.
pub(super) fn push(out: &mut String, args: core::fmt::Arguments<'_>) {
    let _ = out.write_fmt(args);
}

pub(super) fn unsupported(detail: String) -> RasterError {
    RasterError::UnsupportedUserCrs { detail }
}

/// An angle unit as degrees per unit.
#[derive(Debug, Clone, Copy)]
pub(super) struct AngleUnit(f64);

impl AngleUnit {
    /// The unit of `code_key`, or of `size_key` (radians per unit) when the
    /// code is user-defined. Degrees when absent.
    pub(super) fn of(
        keys: &GeoKeyDirectory,
        code_key: u16,
        size_key: u16,
    ) -> Result<Self, RasterError> {
        match keys.short(code_key) {
            None | Some(DEGREE) | Some(DEGREE_SUPPLIER) => Ok(AngleUnit(1.0)),
            Some(RADIAN) => Ok(AngleUnit(1.0_f64.to_degrees())),
            Some(GRAD) => Ok(AngleUnit(0.9)),
            Some(USER_DEFINED) => {
                let radians = keys.double(size_key).ok_or_else(|| {
                    unsupported(format!(
                        "GeoKey {code_key} is 32767 but GeoKey {size_key} is absent"
                    ))
                })?;
                Ok(AngleUnit(radians.to_degrees()))
            }
            Some(code) => Err(unsupported(format!(
                "angle unit {code} in GeoKey {code_key} is not supported; \
                 supported: degree (9102, 9122), radian (9101), grad (9105)"
            ))),
        }
    }

    pub(super) fn degrees(self, value: f64) -> f64 {
        value * self.0
    }
}

/// Metres per projected unit, from `ProjLinearUnitsGeoKey`. Metres when
/// absent.
pub(super) fn linear_unit(keys: &GeoKeyDirectory) -> Result<f64, RasterError> {
    match keys.short(PROJ_LINEAR_UNITS) {
        None => Ok(1.0),
        Some(USER_DEFINED) => keys.double(PROJ_LINEAR_UNIT_SIZE).ok_or_else(|| {
            unsupported(format!(
                "GeoKey {PROJ_LINEAR_UNITS} is 32767 but GeoKey {PROJ_LINEAR_UNIT_SIZE} is absent"
            ))
        }),
        Some(code) => LinearUnit::by_epsg_code(code)
            .map(|unit| unit.metres_per_unit)
            .ok_or_else(|| {
                unsupported(format!(
                    "length unit {code} is not supported; supported: metre (9001), \
                     foot (9002), US survey foot (9003), kilometre (9036)"
                ))
            }),
    }
}
