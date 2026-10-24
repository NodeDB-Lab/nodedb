// SPDX-License-Identifier: Apache-2.0

//! American Polyconic, EPSG method 9818, PROJ `poly`.
//!
//! Follows PROJ 9.8.1 `src/projections/poly.cpp` (Snyder 1987 pp. 124–133,
//! eqs. 18-1 to 18-21; IOGP Guidance Note 7-2 §3.5.1), ellipsoidal and
//! spherical, with Newton–Raphson inverses.

use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::meridian::MeridianArc;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// |φ| at or below which a point counts as on the equator, PROJ `TOL`.
const TOL: f64 = 1e-10;

/// Spherical inverse convergence, PROJ `CONV`.
const CONV: f64 = 1e-10;

/// Spherical inverse iterations, PROJ `N_ITER`.
const N_ITER: usize = 10;

/// Ellipsoidal inverse iterations, PROJ `I_ITER`.
const I_ITER: usize = 20;

/// Ellipsoidal inverse convergence and pole guard, PROJ `ITOL`.
const ITOL: f64 = 1e-12;

/// Largest absolute latitude of origin, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Defining parameters of one American Polyconic projection.
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
pub struct PolyParams {
    /// Latitude of natural origin φ0, degrees (`+lat_0`).
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// False easting, metres (`+x_0`).
    pub false_easting: f64,
    /// False northing, metres (`+y_0`).
    pub false_northing: f64,
}

impl PolyParams {
    /// Read from PROJ-string parameters. Every key defaults to 0 as in PROJ.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat0_deg = p.number_or("lat_0", 0.0)?;
        if lat0_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_0", &lat0_deg.to_string()));
        }
        Ok(Self {
            lat0_deg,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append the defining parameters as PROJ-string tokens.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=poly +lat_0={} +lon_0={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// American Polyconic with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Polyconic {
    params: PolyParams,
    a: f64,
    es: f64,
    lon0: f64,
    phi0: f64,
    /// Meridian arc; `None` on a sphere.
    arc: Option<MeridianArc>,
    /// Ellipsoid: arc to φ0 in units of a. Sphere: −φ0.
    ml0: f64,
}

impl Polyconic {
    pub fn new(p: &PolyParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let es = ell.e2();
        let (arc, ml0) = if ell.is_sphere() {
            (None, -phi0)
        } else {
            let arc = MeridianArc::new(es);
            let ml0 = arc.arc(phi0, phi0.sin(), phi0.cos());
            (Some(arc), ml0)
        };
        Self {
            params: *p,
            a: ell.a,
            es,
            lon0: p.lon0_deg.to_radians(),
            phi0,
            arc,
            ml0,
        }
    }

    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let (x, y) = match &self.arc {
            Some(arc) => self.e_forward(arc, lam, phi),
            None => self.s_forward(lam, phi),
        };
        Ok((
            self.params.false_easting + self.a * x,
            self.params.false_northing + self.a * y,
        ))
    }

    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        let (lam, phi) = match &self.arc {
            Some(arc) => self.e_inverse(arc, x, y)?,
            None => self.s_inverse(x, y)?,
        };
        Ok((wrap_pi(lam + self.lon0), phi))
    }

    /// PROJ `poly_e_forward` (Snyder eqs. 18-2, 18-3).
    fn e_forward(&self, arc: &MeridianArc, lam: f64, phi: f64) -> (f64, f64) {
        if phi.abs() <= TOL {
            return (lam, -self.ml0);
        }
        let (sp, cp) = phi.sin_cos();
        let ms = if cp.abs() > TOL {
            cp / (1.0 - self.es * sp * sp).sqrt() / sp
        } else {
            0.0
        };
        let e = lam * sp;
        let (sin_e, cos_e) = e.sin_cos();
        (
            ms * sin_e,
            (arc.arc(phi, sp, cp) - self.ml0) + ms * (1.0 - cos_e),
        )
    }

    /// PROJ `poly_s_forward` (Snyder eqs. 18-1).
    fn s_forward(&self, lam: f64, phi: f64) -> (f64, f64) {
        if phi.abs() <= TOL {
            return (lam, self.ml0);
        }
        let cot = 1.0 / phi.tan();
        let e = lam * phi.sin();
        (e.sin() * cot, phi - self.phi0 + cot * (1.0 - e.cos()))
    }

    /// PROJ `poly_e_inverse` (Snyder eqs. 18-17 to 18-21).
    fn e_inverse(&self, arc: &MeridianArc, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let y = y + self.ml0;
        if y.abs() <= TOL {
            return Ok((x, 0.0));
        }
        let r = y * y + x * x;
        let mut phi = y;
        let mut converged = false;
        for _ in 0..I_ITER {
            let (sp, cp) = phi.sin_cos();
            let s2ph = sp * cp;
            if cp.abs() < ITOL {
                return Err(DomainReason::OutsideProjection);
            }
            let mlp_root = (1.0 - self.es * sp * sp).sqrt();
            let c = sp * mlp_root / cp;
            let ml = arc.arc(phi, sp, cp);
            let mlb = ml * ml + r;
            let mlp = (1.0 - self.es) / (mlp_root * mlp_root * mlp_root);
            let dphi = (ml + ml + c * mlb - 2.0 * y * (c * ml + 1.0))
                / (self.es * s2ph * (mlb - 2.0 * y * ml) / c
                    + 2.0 * (y - ml) * (c * mlp - 1.0 / s2ph)
                    - mlp
                    - mlp);
            phi += dphi;
            if dphi.abs() <= ITOL {
                converged = true;
                break;
            }
        }
        if !converged {
            return Err(DomainReason::NoConvergence);
        }
        let sp = phi.sin();
        let arg = x * phi.tan() * (1.0 - self.es * sp * sp).sqrt();
        if arg.abs() > 1.0 {
            return Err(DomainReason::OutsideProjection);
        }
        Ok((arg.asin() / sp, phi))
    }

    /// PROJ `poly_s_inverse` (Snyder eqs. 18-9 to 18-14).
    fn s_inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let y = self.phi0 + y;
        if y.abs() <= TOL {
            return Ok((x, 0.0));
        }
        let mut phi = y;
        let b = x * x + y * y;
        let mut converged = false;
        for _ in 0..N_ITER {
            let tp = phi.tan();
            let dphi =
                (y * (phi * tp + 1.0) - phi - 0.5 * (phi * phi + b) * tp) / ((phi - y) / tp - 1.0);
            phi -= dphi;
            if dphi.is_nan() || dphi.abs() <= CONV {
                converged = true;
                break;
            }
        }
        if !converged {
            return Err(DomainReason::NoConvergence);
        }
        let arg = x * phi.tan();
        if arg.abs() > 1.0 {
            return Err(DomainReason::OutsideProjection);
        }
        Ok((arg.asin() / phi.sin(), phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn params(lat0_deg: f64, lon0_deg: f64, false_easting: f64, false_northing: f64) -> PolyParams {
        PolyParams {
            lat0_deg,
            lon0_deg,
            false_easting,
            false_northing,
        }
    }

    fn check(p: &PolyParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let poly = Polyconic::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = poly.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = poly.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    /// The GN 7-2 §3.5.1 parameter set, SAD69 / Brazil Polyconic
    /// (EPSG:29101) without the datum shift. PROJ 9.8.1 values via pyproj
    /// 3.8.0 for `+proj=poly +lat_0=0 +lon_0=-54 +x_0=5000000 +y_0=10000000
    /// +ellps=aust_SA`.
    #[test]
    fn brazil_polyconic() {
        check(
            &params(0.0, -54.0, 5_000_000.0, 10_000_000.0),
            &Ellipsoid::AUSTRALIAN_SA,
            BRAZIL,
        );
    }

    const BRAZIL: &[[f64; 4]] = &[
        [-45.0, -6.0, 5996382.3037087675, 9328347.629558984],
        [-60.0, -20.0, 4372249.445673196, 7776383.064872811],
        [-54.0, 0.0, 5000000.0, 10000000.0],
        [-40.0, 5.0, 6552470.132945759, 10569418.81021988],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:5472 Panama Polyconic in metres:
    /// `+proj=poly +lat_0=8.25 +lon_0=-81 +x_0=914391.7962
    /// +y_0=999404.721715486 +ellps=clrk66`.
    #[test]
    fn panama_polyconic() {
        check(
            &params(8.25, -81.0, 914_391.796_2, 999_404.721_715_486),
            &Ellipsoid::CLARKE_1866,
            PANAMA,
        );
    }

    const PANAMA: &[[f64; 4]] = &[
        [-80.0, 9.0, 1024350.9263183008, 1082499.190405663],
        [-82.5, 8.3, 749148.4770122074, 1005246.4922782447],
        [-79.0, 7.5, 1135140.4370462974, 916966.3856309258],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=poly +lat_0=30 +lon_0=-96 +R=6370997`.
    #[test]
    fn sphere() {
        check(
            &params(30.0, -96.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_370_997.0),
            SPHERE_PTS,
        );
    }

    const SPHERE_PTS: &[[f64; 4]] = &[
        [-100.0, 35.0, -364244.67434682354, 563268.1052725625],
        [-80.0, 25.0, 1608688.0876570228, -460937.5409812575],
        [-96.0, 30.0, 0.0, 0.0],
    ];

    #[test]
    fn equator_maps_to_straight_line() {
        let poly = Polyconic::new(&params(0.0, 0.0, 0.0, 0.0), &Ellipsoid::GRS80);
        let (x, y) = poly.forward(0.1, 0.0).unwrap();
        assert!((x - 0.1 * Ellipsoid::GRS80.a).abs() < 1e-6 && y.abs() < 1e-9);
        let (lam, phi) = poly.inverse(x, y).unwrap();
        assert!((lam - 0.1).abs() < 1e-15 && phi == 0.0);
    }

    #[test]
    fn parses_and_writes() {
        let p = ProjParams::tokenize("+proj=poly +lat_0=0 +lon_0=-54 +x_0=5000000 +y_0=10000000")
            .unwrap();
        let q = PolyParams::from_proj(&p).unwrap();
        let mut s = String::new();
        q.write_proj(&mut s);
        assert_eq!(
            s,
            " +proj=poly +lat_0=0 +lon_0=-54 +x_0=5000000 +y_0=10000000"
        );
    }
}
