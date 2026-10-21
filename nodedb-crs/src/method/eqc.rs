// SPDX-License-Identifier: Apache-2.0

//! Equidistant Cylindrical, PROJ `eqc`.
//!
//! Covers EPSG methods 1028 (ellipsoidal) and 1029 (spherical). Follows
//! PROJ 9.8.1 `src/projections/eqc.cpp` and IOGP Guidance Note 7-2 §3.2.5:
//! E = FE + ν1 cos φ1 (λ − λ0), N = FN + M(φ) − M(φ0). PROJ picks the
//! spherical form when the ellipsoid is a sphere.

use core::f64::consts::FRAC_PI_2;
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::meridian::MeridianArc;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Largest absolute latitude, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Defining parameters of one Equidistant Cylindrical projection.
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
pub struct EqcParams {
    /// Latitude of the standard parallel φ1, degrees.
    pub lat_ts_deg: f64,
    /// Latitude of natural origin φ0, degrees.
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees.
    pub lon0_deg: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl EqcParams {
    /// Read from PROJ-string parameters. PROJ defaults `+lat_ts` and
    /// `+lat_0` to 0 and ignores `+k`.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat_ts_deg = p.number_or("lat_ts", 0.0)?;
        if lat_ts_deg.abs() >= MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_ts", &lat_ts_deg.to_string()));
        }
        let lat0_deg = p.number_or("lat_0", 0.0)?;
        if lat0_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_0", &lat0_deg.to_string()));
        }
        Ok(Self {
            lat_ts_deg,
            lat0_deg,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append ` +proj=eqc` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=eqc +lat_ts={} +lat_0={} +lon_0={} +x_0={} +y_0={}",
            self.lat_ts_deg, self.lat0_deg, self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// An Equidistant Cylindrical projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct EquidistantCylindrical {
    /// λ0 in radians.
    lon0: f64,
    /// φ0 in radians.
    phi0: f64,
    /// Semi-major axis a, metres.
    a: f64,
    /// ν1 cos φ1 over a, or cos φ1 on a sphere.
    rc: f64,
    /// Meridian arc on an ellipsoid, `None` on a sphere.
    arc: Option<MeridianArc>,
    /// M(φ0) over a.
    m0: f64,
    false_easting: f64,
    false_northing: f64,
}

impl EquidistantCylindrical {
    pub fn new(p: &EqcParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let (sin1, cos1) = p.lat_ts_deg.to_radians().sin_cos();
        let (rc, arc, m0) = if ell.is_sphere() {
            (cos1, None, 0.0)
        } else {
            let e2 = ell.e2();
            let arc = MeridianArc::new(e2);
            let m0 = arc.arc(phi0, phi0.sin(), phi0.cos());
            (cos1 / (1.0 - e2 * sin1 * sin1).sqrt(), Some(arc), m0)
        };
        Self {
            lon0: p.lon0_deg.to_radians(),
            phi0,
            a: ell.a,
            rc,
            arc,
            m0,
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let x = self.rc * wrap_pi(lam - self.lon0);
        let y = match &self.arc {
            Some(arc) => arc.arc(phi, phi.sin(), phi.cos()) - self.m0,
            None => phi - self.phi0,
        };
        Ok((
            self.false_easting + self.a * x,
            self.false_northing + self.a * y,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let dy = (y - self.false_northing) / self.a;
        let phi = match &self.arc {
            Some(arc) => arc.inverse(dy + self.m0)?,
            None => dy + self.phi0,
        };
        if phi.abs() > FRAC_PI_2 {
            return Err(DomainReason::OutsideProjection);
        }
        let lam = (x - self.false_easting) / (self.a * self.rc);
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

    fn build(text: &str, ell: Ellipsoid) -> EquidistantCylindrical {
        let p = EqcParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        EquidistantCylindrical::new(&p, &ell)
    }

    fn check(m: &EquidistantCylindrical, lon: f64, lat: f64, want: [f64; 2]) {
        let (x, y) = m.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!(
            (x - want[0]).abs() < MM && (y - want[1]).abs() < MM,
            "({lon}, {lat}): got ({x}, {y}), want {want:?}"
        );
        // The references carry 0.1 mm, so the inverse starts from (x, y).
        let (lam, phi) = m.inverse(x, y).unwrap();
        assert!(
            (lam.to_degrees() - lon).abs() < DEG_TOL && (phi.to_degrees() - lat).abs() < DEG_TOL,
            "inverse ({lon}, {lat}): got ({}, {})",
            lam.to_degrees(),
            phi.to_degrees()
        );
    }

    /// GN 7-2 §3.2.5 example, EPSG 1028: WGS 84 / World Equidistant
    /// Cylindrical (EPSG:4087), point 55°N, 10°E. GN 7-2 prints
    /// E = 1113194.91 m, N = 6097230.31 m. The other points are pyproj
    /// 3.8.0 / PROJ 9.8.1 on the same string.
    #[test]
    fn gn_1028_world_equidistant_cylindrical() {
        let m = build(
            "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0",
            Ellipsoid::WGS84,
        );
        let (x, y) = m.forward(10f64.to_radians(), 55f64.to_radians()).unwrap();
        assert!((x - 1_113_194.91).abs() < 0.005 && (y - 6_097_230.31).abs() < 0.005);
        check(&m, 10.0, 55.0, [1_113_194.907_9, 6_097_230.313_1]);
        check(&m, -120.0, -30.0, [-13_358_338.895_2, -3_320_113.397_9]);
        check(&m, 170.0, 89.0, [18_924_313.434_9, 9_890_271.864_4]);
    }

    /// pyproj, "+proj=eqc +lat_ts=30 +lat_0=10 +lon_0=5 +x_0=100 +y_0=200
    /// +ellps=intl".
    #[test]
    fn ellipsoidal_with_offsets() {
        let m = build(
            "+proj=eqc +lat_ts=30 +lat_0=10 +lon_0=5 +x_0=100 +y_0=200",
            Ellipsoid::INTERNATIONAL_1924,
        );
        check(&m, 20.0, 40.0, [1_447_456.286_2, 3_323_937.630_0]);
        check(&m, -10.0, -20.0, [-1_447_256.286_2, -3_318_061.358_9]);
    }

    /// EPSG 1029 form. pyproj, "+proj=eqc +lat_ts=30 +lat_0=10 +lon_0=5
    /// +R=6371007".
    #[test]
    fn spherical() {
        let m = build(
            "+proj=eqc +lat_ts=30 +lat_0=10 +lon_0=5",
            Ellipsoid::sphere(6_371_007.0),
        );
        check(&m, 20.0, 40.0, [1_444_466.055_8, 3_335_851.464_5]);
        check(&m, -10.0, -20.0, [-1_444_466.055_8, -3_335_851.464_5]);
    }

    #[test]
    fn beyond_the_pole_is_outside() {
        let m = build("+proj=eqc", Ellipsoid::WGS84);
        assert_eq!(m.inverse(0.0, 1.1e7), Err(DomainReason::OutsideProjection));
    }

    #[test]
    fn parse_and_round_trip() {
        let p = EqcParams::from_proj(
            &ProjParams::tokenize("+proj=eqc +lat_ts=30 +lat_0=10 +lon_0=5 +x_0=1").unwrap(),
        )
        .unwrap();
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            EqcParams::from_proj(&ProjParams::tokenize(&out).unwrap()),
            Ok(p),
            "{out}"
        );
        assert!(
            EqcParams::from_proj(&ProjParams::tokenize("+proj=eqc +lat_ts=90").unwrap()).is_err()
        );
    }
}
