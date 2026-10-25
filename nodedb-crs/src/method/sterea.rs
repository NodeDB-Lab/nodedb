// SPDX-License-Identifier: Apache-2.0

//! Oblique Stereographic, PROJ `sterea`, EPSG method 9809.
//!
//! A Gauss conformal mapping of the ellipsoid onto a sphere, then the
//! spherical oblique stereographic. Follows PROJ 9.8.1
//! `src/projections/sterea.cpp` and `src/gauss.cpp`, which match IOGP
//! Guidance Note 7-2 §3.3.1.

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `gauss.cpp` MAX_ITER: inverse Gauss iterations allowed.
const GAUSS_MAX_ITERS: usize = 20;

/// PROJ `gauss.cpp` DEL_TOL: inverse Gauss convergence on φ, radians.
const GAUSS_TOL: f64 = 1e-14;

/// PROJ `gauss.cpp`: φ0/2 + π/4 below which K uses the south-pole form.
const SOUTH_POLE_EPS: f64 = 1e-10;

/// Largest absolute latitude, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Defining parameters of one Oblique Stereographic projection.
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
pub struct StereaParams {
    /// Latitude of natural origin φ0, degrees.
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees.
    pub lon0_deg: f64,
    /// Scale factor at the natural origin.
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl StereaParams {
    /// Read from PROJ-string parameters.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat0_deg = p.number_or("lat_0", 0.0)?;
        if lat0_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_0", &lat0_deg.to_string()));
        }
        let k0 = p.number_any(&["k_0", "k"])?.unwrap_or(1.0);
        if k0 <= 0.0 {
            return Err(CrsParseError::invalid("k_0", &k0.to_string()));
        }
        Ok(Self {
            lat0_deg,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            k0,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append ` +proj=sterea` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=sterea +lat_0={} +lon_0={} +k_0={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.k0, self.false_easting, self.false_northing
        );
    }
}

/// PROJ `srat`: ((1 − e sin φ)/(1 + e sin φ))^ratexp.
fn srat(esinp: f64, ratexp: f64) -> f64 {
    ((1.0 - esinp) / (1.0 + esinp)).powf(ratexp)
}

/// The Gauss conformal sphere of one ellipsoid at φ0 (PROJ `gauss.cpp`,
/// GN 7-2 §3.3.1: n = C, c = K, χ0 = phic0).
#[derive(Debug, Clone, PartialEq)]
struct Gauss {
    c: f64,
    k: f64,
    e: f64,
    ratexp: f64,
}

impl Gauss {
    /// PROJ `pj_gauss_ini`. Returns the sphere, χ0 and the conformal sphere
    /// radius over a, R = sqrt(1 − e²) / (1 − e² sin² φ0).
    fn new(e: f64, phi0: f64) -> (Self, f64, f64) {
        let es = e * e;
        let sphi = phi0.sin();
        let cphi2 = phi0.cos().powi(2);
        let rc = (1.0 - es).sqrt() / (1.0 - es * sphi * sphi);
        let c = (1.0 + es * cphi2 * cphi2 / (1.0 - es)).sqrt();
        let chi = (sphi / c).asin();
        let ratexp = 0.5 * c * e;
        let srat_val = srat(e * sphi, ratexp);
        let k = if 0.5 * phi0 + FRAC_PI_4 < SOUTH_POLE_EPS {
            1.0 / srat_val
        } else {
            (0.5 * chi + FRAC_PI_4).tan() / ((0.5 * phi0 + FRAC_PI_4).tan().powf(c) * srat_val)
        };
        (Self { c, k, e, ratexp }, chi, rc)
    }

    /// Ellipsoid → sphere, PROJ `pj_gauss`.
    fn forward(&self, lam: f64, phi: f64) -> (f64, f64) {
        let chi = 2.0
            * (self.k
                * (0.5 * phi + FRAC_PI_4).tan().powf(self.c)
                * srat(self.e * phi.sin(), self.ratexp))
            .atan()
            - FRAC_PI_2;
        (self.c * lam, chi)
    }

    /// Sphere → ellipsoid, PROJ `pj_inv_gauss`.
    fn inverse(&self, lam: f64, chi: f64) -> Result<(f64, f64), DomainReason> {
        let num = ((0.5 * chi + FRAC_PI_4).tan() / self.k).powf(1.0 / self.c);
        let mut prev = chi;
        for _ in 0..GAUSS_MAX_ITERS {
            let phi = 2.0 * (num * srat(self.e * prev.sin(), -0.5 * self.e)).atan() - FRAC_PI_2;
            if (phi - prev).abs() < GAUSS_TOL {
                return Ok((lam / self.c, phi));
            }
            prev = phi;
        }
        Err(DomainReason::NoConvergence)
    }
}

/// An Oblique Stereographic projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct ObliqueStereographic {
    gauss: Gauss,
    /// χ0, the conformal latitude of the origin.
    phic0: f64,
    sinc0: f64,
    cosc0: f64,
    /// 2 R / a, the sphere diameter over a.
    r2: f64,
    /// a · k0.
    ak0: f64,
    /// λ0 in radians.
    lon0: f64,
    false_easting: f64,
    false_northing: f64,
}

impl ObliqueStereographic {
    pub fn new(p: &StereaParams, ell: &Ellipsoid) -> Self {
        let (gauss, phic0, rc) = Gauss::new(ell.e(), p.lat0_deg.to_radians());
        let (sinc0, cosc0) = phic0.sin_cos();
        Self {
            gauss,
            phic0,
            sinc0,
            cosc0,
            r2: 2.0 * rc,
            ak0: ell.a * p.k0,
            lon0: p.lon0_deg.to_radians(),
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let (lam_s, chi) = self.gauss.forward(wrap_pi(lam - self.lon0), phi);
        let (sinc, cosc) = chi.sin_cos();
        let (sinl, cosl) = lam_s.sin_cos();
        let denom = 1.0 + self.sinc0 * sinc + self.cosc0 * cosc * cosl;
        if denom == 0.0 {
            return Err(DomainReason::Antipode);
        }
        let k = self.ak0 * self.r2 / denom;
        Ok((
            self.false_easting + k * cosc * sinl,
            self.false_northing + k * (self.cosc0 * sinc - self.sinc0 * cosc * cosl),
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.false_easting) / self.ak0;
        let y = (y - self.false_northing) / self.ak0;
        let rho = x.hypot(y);
        let (lam_s, chi) = if rho != 0.0 {
            let (sinc, cosc) = (2.0 * rho.atan2(self.r2)).sin_cos();
            (
                (x * sinc).atan2(rho * self.cosc0 * cosc - y * self.sinc0 * sinc),
                (cosc * self.sinc0 + y * sinc * self.cosc0 / rho).asin(),
            )
        } else {
            (0.0, self.phic0)
        };
        let (dlam, phi) = self.gauss.inverse(lam_s, chi)?;
        Ok((wrap_pi(self.lon0 + dlam), phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    /// Inverse tolerance in degrees.
    const DEG_TOL: f64 = 1e-9;

    fn build(text: &str, ell: Ellipsoid) -> ObliqueStereographic {
        let p = StereaParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        ObliqueStereographic::new(&p, &ell)
    }

    fn check(m: &ObliqueStereographic, lon: f64, lat: f64, want: [f64; 2]) {
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

    /// GN 7-2 §3.3.1 example, EPSG 9809: Amersfoort / RD New, point 53°N,
    /// 6°E. GN 7-2 prints E = 196105.283 m, N = 557057.739 m. The other
    /// points are pyproj 3.8.0 / PROJ 9.8.1 on the same string.
    #[test]
    fn gn_9809_rd_new() {
        let m = build(
            "+proj=sterea +lat_0=52.1561605555556 +lon_0=5.38763888888889 +k=0.9999079 \
             +x_0=155000 +y_0=463000",
            Ellipsoid::BESSEL_1841,
        );
        let (x, y) = m.forward(6f64.to_radians(), 53f64.to_radians()).unwrap();
        assert!((x - 196_105.283).abs() < 0.0005 && (y - 557_057.739).abs() < 0.0005);
        check(&m, 6.0, 53.0, [196_105.283_0, 557_057.739_4]);
        check(&m, 4.9, 52.37, [121_793.919_4, 486_901.274_2]);
        check(&m, 3.4, 51.4, [16_710.909_4, 380_768.054_2]);
    }

    /// pyproj, "+proj=sterea +lat_0=46 +lon_0=25 +k=0.99975 +x_0=500000
    /// +y_0=500000 +ellps=krass" (Stereo 70).
    #[test]
    fn stereo_70() {
        let m = build(
            "+proj=sterea +lat_0=46 +lon_0=25 +k=0.99975 +x_0=500000 +y_0=500000",
            Ellipsoid::KRASSOVSKY,
        );
        check(&m, 26.1, 44.43, [587_581.703_1, 326_143.042_5]);
        check(&m, 21.2, 45.75, [204_443.178_2, 479_257.212_9]);
    }

    /// pyproj, the same string on "+R=6371000".
    #[test]
    fn sphere() {
        let m = build(
            "+proj=sterea +lat_0=46 +lon_0=25 +k=0.99975 +x_0=500000 +y_0=500000",
            Ellipsoid::sphere(6_371_000.0),
        );
        check(&m, 26.1, 44.43, [587_338.675_0, 326_051.814_3]);
        check(&m, 21.2, 45.75, [205_286.441_3, 479_226.094_8]);
    }

    #[test]
    fn origin_maps_to_false_origin() {
        let m = build(
            "+proj=sterea +lat_0=46 +lon_0=25 +k=0.99975 +x_0=500000 +y_0=500000",
            Ellipsoid::KRASSOVSKY,
        );
        let (x, y) = m.forward(25f64.to_radians(), 46f64.to_radians()).unwrap();
        assert!((x - 500_000.0).abs() < 1e-6 && (y - 500_000.0).abs() < 1e-6);
        let (lam, phi) = m.inverse(500_000.0, 500_000.0).unwrap();
        assert!((lam.to_degrees() - 25.0).abs() < 1e-12 && (phi.to_degrees() - 46.0).abs() < 1e-12);
    }

    #[test]
    fn parse_and_round_trip() {
        let p = StereaParams::from_proj(
            &ProjParams::tokenize(
                "+proj=sterea +lat_0=46 +lon_0=25 +k=0.99975 +x_0=500000 +y_0=500000",
            )
            .unwrap(),
        )
        .unwrap();
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            StereaParams::from_proj(&ProjParams::tokenize(&out).unwrap()),
            Ok(p),
            "{out}"
        );
        assert!(
            StereaParams::from_proj(&ProjParams::tokenize("+proj=sterea +k=-1").unwrap()).is_err()
        );
    }
}
