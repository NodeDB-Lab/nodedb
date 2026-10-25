// SPDX-License-Identifier: Apache-2.0

//! Equal Earth, EPSG method 1078, PROJ `eqearth`.
//!
//! Follows PROJ 9.8.1 `src/projections/eqearth.cpp` (B. Šavrič,
//! T. Patterson, B. Jenny, "The Equal Earth map projection", IJGIS 33
//! (2019), eqs. 1–2; IOGP Guidance Note 7-2 §3.12.3). The ellipsoidal form
//! applies the spherical formulas to the authalic latitude and scales by
//! the authalic radius.

use core::f64::consts::PI;
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::authalic::Authalic;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Polynomial coefficients A1 to A4, Šavrič et al. 2019 eq. 1.
const A1: f64 = 1.340_264;
const A2: f64 = -0.081_106;
const A3: f64 = 0.000_893;
const A4: f64 = 0.003_796;

/// y at 90° latitude on the unit sphere, PROJ `MAX_Y`.
const MAX_Y: f64 = 1.317_362_759_157_4;

/// Newton step below which the inverse stops, PROJ `EPS`.
const EPS: f64 = 1e-11;

/// Newton iterations of the inverse, PROJ `MAX_ITER`.
const MAX_ITER: usize = 12;

/// |λ − λ0| beyond π by more than this is outside the map.
const LON_TOL: f64 = 1e-10;

/// M = √3 / 2, Šavrič et al. 2019 eq. 1.
fn m() -> f64 {
    3f64.sqrt() / 2.0
}

/// Defining parameters of one Equal Earth projection.
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
pub struct EqEarthParams {
    /// Longitude of natural origin λ0, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// False easting, metres (`+x_0`).
    pub false_easting: f64,
    /// False northing, metres (`+y_0`).
    pub false_northing: f64,
}

impl EqEarthParams {
    /// Read from PROJ-string parameters. Every key defaults to 0 as in PROJ.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        Ok(Self {
            lon0_deg: p.number_or("lon_0", 0.0)?,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append the defining parameters as PROJ-string tokens.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=eqearth +lon_0={} +x_0={} +y_0={}",
            self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// Equal Earth with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct EqualEarth {
    params: EqEarthParams,
    a: f64,
    lon0: f64,
    /// Authalic conversion; `None` on a sphere.
    auth: Option<Authalic>,
    /// Authalic radius over a, √(qp / 2); 1 on a sphere.
    rqda: f64,
}

impl EqualEarth {
    pub fn new(p: &EqEarthParams, ell: &Ellipsoid) -> Self {
        let (auth, rqda) = if ell.is_sphere() {
            (None, 1.0)
        } else {
            let a = Authalic::new(ell.e2());
            let rqda = (0.5 * a.qp()).sqrt();
            (Some(a), rqda)
        };
        Self {
            params: *p,
            a: ell.a,
            lon0: p.lon0_deg.to_radians(),
            auth,
            rqda,
        }
    }

    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let mut sbeta = phi.sin();
        if let Some(a) = &self.auth {
            // sin β = q / qp; rounding past ±1 clamps.
            sbeta = (a.q(sbeta) / a.qp()).clamp(-1.0, 1.0);
        }
        let m = m();
        let psi = (m * sbeta).asin();
        let psi2 = psi * psi;
        let psi6 = psi2 * psi2 * psi2;
        let x =
            lam * psi.cos() / (m * (A1 + 3.0 * A2 * psi2 + psi6 * (7.0 * A3 + 9.0 * A4 * psi2)));
        let y = psi * (A1 + A2 * psi2 + psi6 * (A3 + A4 * psi2));
        Ok((
            self.params.false_easting + self.a * x * self.rqda,
            self.params.false_northing + self.a * y * self.rqda,
        ))
    }

    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a / self.rqda;
        let y = ((y - self.params.false_northing) / self.a / self.rqda).clamp(-MAX_Y, MAX_Y);
        let mut yc = y;
        let mut converged = false;
        for _ in 0..MAX_ITER {
            let y2 = yc * yc;
            let y6 = y2 * y2 * y2;
            let f = yc * (A1 + A2 * y2 + y6 * (A3 + A4 * y2)) - y;
            let fder = A1 + 3.0 * A2 * y2 + y6 * (7.0 * A3 + 9.0 * A4 * y2);
            let tol = f / fder;
            yc -= tol;
            if tol.abs() < EPS {
                converged = true;
                break;
            }
        }
        if !converged {
            return Err(DomainReason::NoConvergence);
        }
        let m = m();
        let y2 = yc * yc;
        let y6 = y2 * y2 * y2;
        let lam = m * x * (A1 + 3.0 * A2 * y2 + y6 * (7.0 * A3 + 9.0 * A4 * y2)) / yc.cos();
        if lam.abs() > PI + LON_TOL {
            return Err(DomainReason::OutsideProjection);
        }
        let beta = (yc.sin() / m).asin();
        let phi = match &self.auth {
            Some(a) => a.from_authalic(beta),
            None => beta,
        };
        Ok((wrap_pi(lam + self.lon0), phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn params(lon0_deg: f64) -> EqEarthParams {
        EqEarthParams {
            lon0_deg,
            false_easting: 0.0,
            false_northing: 0.0,
        }
    }

    fn check(p: &EqEarthParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let ee = EqualEarth::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = ee.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = ee.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    /// EPSG:8857 WGS 84 / Equal Earth Greenwich, PROJ 9.8.1 via pyproj
    /// 3.8.0, `+proj=eqearth +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84`.
    #[test]
    fn greenwich() {
        check(&params(0.0), &Ellipsoid::WGS84, GREENWICH);
    }

    const GREENWICH: &[[f64; 4]] = &[
        [0.0, 0.0, 0.0, 0.0],
        [10.0, 52.0, 778005.2988841359, 6184897.936534862],
        [-120.0, -35.0, -10496394.36359727, -4352308.175397291],
        [179.0, 80.0, 10585770.166707683, 8205603.096488446],
    ];

    /// EPSG:8859 WGS 84 / Equal Earth Asia-Pacific, PROJ 9.8.1 via pyproj
    /// 3.8.0, `+proj=eqearth +lon_0=150 +x_0=0 +y_0=0 +datum=WGS84`.
    #[test]
    fn asia_pacific() {
        check(&params(150.0), &Ellipsoid::WGS84, ASIA_PACIFIC);
    }

    const ASIA_PACIFIC: &[[f64; 4]] = &[
        [-170.0, 10.0, 3804392.9698225083, 1281605.5100848149],
        [100.0, -40.0, -4248749.153614973, -4921020.061780324],
        [150.0, 0.0, 0.0, 0.0],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=eqearth +lon_0=0 +R=6371008.7714`.
    #[test]
    fn sphere() {
        check(
            &params(0.0),
            &Ellipsoid::sphere(6_371_008.771_4),
            SPHERE_PTS,
        );
    }

    const SPHERE_PTS: &[[f64; 4]] = &[
        [30.0, 45.0, 2463147.729110954, 5480539.791291671],
        [-150.0, -70.0, -9751649.563443083, -7692885.362673924],
    ];

    #[test]
    fn parses_and_writes() {
        let p = ProjParams::tokenize("+proj=eqearth +lon_0=-90 +x_0=0 +y_0=0").unwrap();
        let q = EqEarthParams::from_proj(&p).unwrap();
        let mut s = String::new();
        q.write_proj(&mut s);
        assert_eq!(s, " +proj=eqearth +lon_0=-90 +x_0=0 +y_0=0");
    }
}
