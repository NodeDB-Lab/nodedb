// SPDX-License-Identifier: Apache-2.0

//! Lambert Cylindrical Equal Area, PROJ `cea`.
//!
//! Covers EPSG methods 9835 (ellipsoidal) and 9834 (spherical). Follows
//! PROJ 9.8.1 `src/projections/cea.cpp`, IOGP Guidance Note 7-2 §3.5.5 and
//! Snyder 1987 eqs. 10-15 to 10-19.

use core::f64::consts::FRAC_PI_2;
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::authalic::Authalic;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `cea.cpp` EPS: slack on |sin φ| ≤ 1 in the spherical inverse.
const SPHERE_EPS: f64 = 1e-10;

/// Largest absolute standard parallel, degrees, exclusive.
const MAX_LAT_TS_DEG: f64 = 90.0;

/// Defining parameters of one Lambert Cylindrical Equal Area projection.
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
pub struct CeaParams {
    /// Longitude of natural origin λ0, degrees.
    pub lon0_deg: f64,
    /// Latitude of the standard parallel φ1, degrees. When absent, `k0`
    /// sets the scale on the equator, as PROJ reads `+k_0`.
    pub lat_ts_deg: Option<f64>,
    /// Scale factor on the equator. Ignored when `lat_ts_deg` is set.
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl CeaParams {
    /// Read from PROJ-string parameters.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat_ts_deg = p.number("lat_ts")?;
        if let Some(lat_ts) = lat_ts_deg
            && lat_ts.abs() >= MAX_LAT_TS_DEG
        {
            return Err(CrsParseError::invalid("lat_ts", &lat_ts.to_string()));
        }
        let k0 = p.number_any(&["k_0", "k"])?.unwrap_or(1.0);
        if k0 <= 0.0 {
            return Err(CrsParseError::invalid("k_0", &k0.to_string()));
        }
        Ok(Self {
            lon0_deg: p.number_or("lon_0", 0.0)?,
            lat_ts_deg,
            k0,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append ` +proj=cea` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        out.push_str(" +proj=cea");
        let _ = match self.lat_ts_deg {
            Some(lat_ts) => write!(out, " +lat_ts={lat_ts}"),
            None => write!(out, " +k_0={}", self.k0),
        };
        let _ = write!(
            out,
            " +lon_0={} +x_0={} +y_0={}",
            self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// A Lambert Cylindrical Equal Area projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct CylindricalEqualArea {
    /// λ0 in radians.
    lon0: f64,
    /// Semi-major axis a, metres.
    a: f64,
    /// Scale factor on the equator.
    k0: f64,
    /// Authalic latitude conversion. `None` on a sphere.
    authalic: Option<Authalic>,
    false_easting: f64,
    false_northing: f64,
}

impl CylindricalEqualArea {
    pub fn new(p: &CeaParams, ell: &Ellipsoid) -> Self {
        let sphere = ell.is_sphere();
        // GN 7-2 §3.5.5: k0 = cos φ1 / sqrt(1 − e² sin² φ1).
        let k0 = match p.lat_ts_deg {
            Some(lat_ts) => {
                let (s, c) = lat_ts.to_radians().sin_cos();
                if sphere {
                    c
                } else {
                    c / (1.0 - ell.e2() * s * s).sqrt()
                }
            }
            None => p.k0,
        };
        Self {
            lon0: p.lon0_deg.to_radians(),
            a: ell.a,
            k0,
            authalic: if sphere {
                None
            } else {
                Some(Authalic::new(ell.e2()))
            },
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres. GN 7-2 §3.5.5:
    /// E = FE + a k0 (λ − λ0), N = FN + a q / (2 k0).
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let x = self.k0 * wrap_pi(lam - self.lon0);
        let y = match &self.authalic {
            Some(auth) => 0.5 * auth.q(phi.sin()) / self.k0,
            None => phi.sin() / self.k0,
        };
        Ok((
            self.false_easting + self.a * x,
            self.false_northing + self.a * y,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let yk = (y - self.false_northing) / self.a * self.k0;
        let phi = match &self.authalic {
            // β = asin(2 N k0 / (a qp)), PROJ cea_e_inverse.
            Some(auth) => {
                let ratio = 2.0 * yk / auth.qp();
                if ratio.abs() > 1.0 {
                    return Err(DomainReason::OutsideProjection);
                }
                auth.from_authalic(ratio.asin())
            }
            None => {
                let t = yk.abs();
                if t - SPHERE_EPS > 1.0 {
                    return Err(DomainReason::OutsideProjection);
                }
                if t >= 1.0 {
                    FRAC_PI_2.copysign(yk)
                } else {
                    yk.asin()
                }
            }
        };
        let lam = (x - self.false_easting) / (self.a * self.k0);
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

    fn build(text: &str, ell: Ellipsoid) -> CylindricalEqualArea {
        let p = CeaParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        CylindricalEqualArea::new(&p, &ell)
    }

    fn check(m: &CylindricalEqualArea, lon: f64, lat: f64, want: [f64; 2]) {
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

    /// EPSG 9835, WGS 84 / NSIDC EASE-Grid 2.0 Global (EPSG:6933). pyproj
    /// 3.8.0 / PROJ 9.8.1, "+proj=cea +lat_ts=30 +lon_0=0 +datum=WGS84".
    #[test]
    fn ease_grid_2_ellipsoidal() {
        let m = build(
            "+proj=cea +lat_ts=30 +lon_0=0 +x_0=0 +y_0=0",
            Ellipsoid::WGS84,
        );
        check(&m, 10.0, 5.0, [964_862.802_5, 637_078.744_0]);
        check(&m, -120.0, 60.0, [-11_578_353.630_1, 6_351_419.997_3]);
        check(&m, 170.0, -80.0, [16_402_667.642_7, -7_229_704.847_6]);
    }

    /// EPSG 9834, NSIDC EASE-Grid Global (EPSG:3410). pyproj, "+proj=cea
    /// +lat_ts=30 +lon_0=0 +R=6371228".
    #[test]
    fn ease_grid_spherical() {
        let m = build(
            "+proj=cea +lat_ts=30 +lon_0=0 +x_0=0 +y_0=0",
            Ellipsoid::sphere(6_371_228.0),
        );
        check(&m, 10.0, 5.0, [963_010.774_6, 641_192.632_6]);
        check(&m, -120.0, 60.0, [-11_556_129.295_8, 6_371_228.000_0]);
        check(&m, 170.0, -80.0, [16_371_183.169_0, -7_245_093.161_5]);
    }

    #[test]
    fn beyond_the_pole_is_outside() {
        let m = build("+proj=cea +lat_ts=30", Ellipsoid::WGS84);
        assert_eq!(m.inverse(0.0, 1e7), Err(DomainReason::OutsideProjection));
        let s = build("+proj=cea +lat_ts=30", Ellipsoid::sphere(6_371_228.0));
        assert_eq!(s.inverse(0.0, 1e7), Err(DomainReason::OutsideProjection));
    }

    #[test]
    fn parse_and_round_trip() {
        for text in [
            "+proj=cea +lat_ts=30 +lon_0=-5 +x_0=1",
            "+proj=cea +k_0=0.9 +y_0=7",
        ] {
            let p = CeaParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
            let mut out = String::new();
            p.write_proj(&mut out);
            assert_eq!(
                CeaParams::from_proj(&ProjParams::tokenize(&out).unwrap()),
                Ok(p),
                "{out}"
            );
        }
        assert!(
            CeaParams::from_proj(&ProjParams::tokenize("+proj=cea +lat_ts=95").unwrap()).is_err()
        );
    }
}
