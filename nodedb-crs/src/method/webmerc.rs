// SPDX-License-Identifier: Apache-2.0

//! Popular Visualisation Pseudo-Mercator, PROJ `webmerc`, EPSG method 1024.
//!
//! Spherical Mercator formulas on the ellipsoid's semi-major axis, with the
//! geodetic latitude used directly (IOGP Guidance Note 7-2 §3.5.1.3, PROJ
//! 9.8.1 `src/projections/merc.cpp`). PROJ fixes k0 at 1.

use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Largest |φ| in radians: atan(sinh π) ≈ 85.0511°, where y reaches ±πa
/// and the map is square.
pub fn webmerc_max_lat() -> f64 {
    core::f64::consts::PI.sinh().atan()
}

/// Slack in radians on the inverse latitude limit, so a point projected at
/// the limit unprojects without a rounding error tripping the check.
const LIMIT_SLACK: f64 = 1e-12;

/// Defining parameters of one Pseudo-Mercator projection.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct WebMercParams {
    /// Longitude of natural origin λ0, degrees.
    pub lon0_deg: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl WebMercParams {
    /// Read from PROJ-string parameters. PROJ ignores `+k` for webmerc.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        Ok(Self {
            lon0_deg: p.number_or("lon_0", 0.0)?,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append ` +proj=webmerc` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=webmerc +lon_0={} +x_0={} +y_0={}",
            self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// A Pseudo-Mercator projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct WebMercator {
    /// λ0 in radians.
    lon0: f64,
    /// Semi-major axis a, metres.
    a: f64,
    /// Latitude limit in radians.
    max_lat: f64,
    false_easting: f64,
    false_northing: f64,
}

impl WebMercator {
    pub fn new(p: &WebMercParams, ell: &Ellipsoid) -> Self {
        Self {
            lon0: p.lon0_deg.to_radians(),
            a: ell.a,
            max_lat: webmerc_max_lat(),
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres. GN 7-2 §3.5.1.3:
    /// E = FE + a(λ − λ0), N = FN + a ln tan(π/4 + φ/2).
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        if phi.abs() > self.max_lat {
            return Err(DomainReason::LatitudeLimit);
        }
        Ok((
            self.false_easting + self.a * wrap_pi(lam - self.lon0),
            self.false_northing + self.a * phi.tan().asinh(),
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let phi = ((y - self.false_northing) / self.a).sinh().atan();
        if phi.abs() > self.max_lat + LIMIT_SLACK {
            return Err(DomainReason::LatitudeLimit);
        }
        let lam = (x - self.false_easting) / self.a;
        Ok((wrap_pi(self.lon0 + lam), phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    /// Inverse tolerance in degrees.
    const DEG_TOL: f64 = 1e-9;

    fn epsg_3857() -> WebMercator {
        let p =
            WebMercParams::from_proj(&ProjParams::tokenize("+proj=webmerc +datum=WGS84").unwrap())
                .unwrap();
        WebMercator::new(&p, &Ellipsoid::WGS84)
    }

    fn check(m: &WebMercator, lon: f64, lat: f64, want: [f64; 2]) {
        let (x, y) = m.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!(
            (x - want[0]).abs() < MM && (y - want[1]).abs() < MM,
            "({lon}, {lat}): got ({x}, {y}), want {want:?}"
        );
        // The references carry 0.1 mm, so the inverse starts from (x, y).
        let (lam, phi) = m.inverse(x, y).unwrap();
        assert!(
            (lam.to_degrees() - lon).abs() < DEG_TOL && (phi.to_degrees() - lat).abs() < DEG_TOL,
            "inverse ({lon}, {lat})"
        );
    }

    /// GN 7-2 §3.5.1.3 example, EPSG 1024: WGS 84 / Pseudo-Mercator, point
    /// 24°22'54.433"N, 100°20'00.000"W. GN 7-2 prints E = −11169055.58 m,
    /// N = 2800000.00 m.
    #[test]
    fn gn_1024_pseudo_mercator() {
        let m = epsg_3857();
        let lon: f64 = -(100.0 + 20.0 / 60.0);
        let lat: f64 = 24.0 + 22.0 / 60.0 + 54.433 / 3600.0;
        let (x, y) = m.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!((x + 11_169_055.58).abs() < 0.005 && (y - 2_800_000.00).abs() < 0.005);
        check(&m, lon, lat, [-11_169_055.576_3, 2_800_000.003_1]);
    }

    /// pyproj 3.8.0 / PROJ 9.8.1, "+proj=webmerc +datum=WGS84".
    #[test]
    fn proj_reference_points() {
        let m = epsg_3857();
        check(&m, 101.7117, 3.1579, [11_322_494.651_7, 351_713.934_7]);
        check(&m, 179.9, 85.0, [20_026_376.393_7, 19_971_868.880_4]);
    }

    #[test]
    fn beyond_limit_is_refused() {
        let m = epsg_3857();
        assert_eq!(
            m.forward(0.0, 85.06f64.to_radians()),
            Err(DomainReason::LatitudeLimit)
        );
        assert_eq!(m.inverse(0.0, 2.1e7), Err(DomainReason::LatitudeLimit));
        let edge = webmerc_max_lat();
        let (x, y) = m.forward(0.0, edge).unwrap();
        assert!(m.inverse(x, y).is_ok());
    }

    #[test]
    fn round_trip_writer() {
        let p = WebMercParams {
            lon0_deg: 10.0,
            false_easting: 1.5,
            false_northing: -2.0,
        };
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            WebMercParams::from_proj(&ProjParams::tokenize(&out).unwrap()),
            Ok(p)
        );
    }
}
