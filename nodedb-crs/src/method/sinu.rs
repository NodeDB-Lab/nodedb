// SPDX-License-Identifier: Apache-2.0

//! Sinusoidal (Sanson-Flamsteed), PROJ `sinu`.
//!
//! Follows PROJ 9.8.1 `src/projections/gn_sinu.cpp` (Snyder 1987
//! pp. 243–248, eqs. 30-1 to 30-11), spherical and ellipsoidal. NodeDB
//! SRID 990001 is the MODIS sinusoidal grid on a sphere of radius
//! 6 371 007.181 m.

use core::f64::consts::{FRAC_PI_2, PI};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::meridian::MeridianArc;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `EPS10`: an inverse latitude this far past ±90° rounds to the pole.
const EPS10: f64 = 1e-10;

/// Defining parameters of one Sinusoidal projection.
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
pub struct SinuParams {
    /// Central meridian λ0, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// False easting, metres (`+x_0`).
    pub false_easting: f64,
    /// False northing, metres (`+y_0`).
    pub false_northing: f64,
}

impl SinuParams {
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
            " +proj=sinu +lon_0={} +x_0={} +y_0={}",
            self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// Sinusoidal with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Sinusoidal {
    params: SinuParams,
    a: f64,
    es: f64,
    lon0: f64,
    /// Meridian arc; `None` on a sphere.
    arc: Option<MeridianArc>,
}

impl Sinusoidal {
    pub fn new(p: &SinuParams, ell: &Ellipsoid) -> Self {
        let es = ell.e2();
        Self {
            params: *p,
            a: ell.a,
            es,
            lon0: p.lon0_deg.to_radians(),
            arc: if ell.is_sphere() {
                None
            } else {
                Some(MeridianArc::new(es))
            },
        }
    }

    /// Ellipsoid: x = λ cos φ / √(1 − e² sin² φ), y = M(φ), Snyder eqs.
    /// 30-8, 30-9. Sphere: x = λ cos φ, y = φ, Snyder eqs. 30-1, 30-2.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let (s, c) = phi.sin_cos();
        let (x, y) = match &self.arc {
            Some(arc) => (lam * c / (1.0 - self.es * s * s).sqrt(), arc.arc(phi, s, c)),
            None => (lam * c, phi),
        };
        Ok((
            self.params.false_easting + self.a * x,
            self.params.false_northing + self.a * y,
        ))
    }

    /// PROJ `gn_sinu_e_inverse` and `gn_sinu_s_inverse`. A point past the
    /// map's outline (|λ − λ0| > 180°) is outside the projection.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        let (lam, phi) = match &self.arc {
            Some(arc) => {
                let phi = arc.inverse(y)?;
                let s = phi.abs();
                if s < FRAC_PI_2 {
                    let sp = phi.sin();
                    (x * (1.0 - self.es * sp * sp).sqrt() / phi.cos(), phi)
                } else if s - EPS10 < FRAC_PI_2 {
                    (0.0, phi.clamp(-FRAC_PI_2, FRAC_PI_2))
                } else {
                    return Err(DomainReason::OutsideProjection);
                }
            }
            None => {
                if y.abs() > FRAC_PI_2 {
                    return Err(DomainReason::OutsideProjection);
                }
                (x / y.cos(), y)
            }
        };
        if lam.abs() > PI + EPS10 {
            return Err(DomainReason::OutsideProjection);
        }
        Ok((wrap_pi(lam + self.lon0), phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn check(p: &SinuParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let sinu = Sinusoidal::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = sinu.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = sinu.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    fn modis() -> SinuParams {
        SinuParams {
            lon0_deg: 0.0,
            false_easting: 0.0,
            false_northing: 0.0,
        }
    }

    /// NodeDB SRID 990001 rows of `tests/fixtures/proj_reference.tsv`
    /// (PROJ 9.8.1 via pyproj 3.8.0), `+proj=sinu +lon_0=0 +x_0=0 +y_0=0
    /// +R=6371007.181`. (lon, lat, x, y).
    #[test]
    fn modis_sinusoidal() {
        check(
            &modis(),
            &Ellipsoid::sphere(6_371_007.181),
            &[
                [
                    -170.0,
                    -60.0,
                    -9_451_579.418_015_448,
                    -6_671_703.118_599_138,
                ],
                [
                    -120.0,
                    -30.0,
                    -11_555_728.774_429_437,
                    -3_335_851.559_299_569,
                ],
                [-60.0, 0.5, -6_671_449.080_587_624, 55_597.525_988_326_16],
                [0.0, 0.0, 0.0, 0.0],
                [30.0, 45.0, 2_358_803.258_612_444, 5_003_777.338_949_354],
            ],
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=sinu +lon_0=-90 +x_0=100
    /// +y_0=-200 +ellps=WGS84`.
    #[test]
    fn ellipsoid() {
        let p = SinuParams {
            lon0_deg: -90.0,
            false_easting: 100.0,
            false_northing: -200.0,
        };
        check(&p, &Ellipsoid::WGS84, ELLIPSOID_PTS);
    }

    const ELLIPSOID_PTS: &[[f64; 4]] = &[
        [-100.0, 40.0, -853838.5695861845, 4429329.030350515],
        [-60.0, -30.0, 2894688.407526896, -3320313.3979403824],
        [0.0, 75.0, 2601280.5228825966, 8326737.587280346],
        [-90.0, 0.0, 100.0, -200.0],
    ];

    #[test]
    fn outside_outline_is_out_of_domain() {
        let sinu = Sinusoidal::new(&modis(), &Ellipsoid::sphere(1.0));
        assert_eq!(sinu.inverse(3.5, 0.0), Err(DomainReason::OutsideProjection));
        assert_eq!(sinu.inverse(0.0, 1.6), Err(DomainReason::OutsideProjection));
    }

    #[test]
    fn parses_and_writes() {
        let p = ProjParams::tokenize(
            "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs",
        )
        .unwrap();
        let q = SinuParams::from_proj(&p).unwrap();
        assert_eq!(q, modis());
        let mut s = String::new();
        q.write_proj(&mut s);
        assert_eq!(s, " +proj=sinu +lon_0=0 +x_0=0 +y_0=0");
    }
}
