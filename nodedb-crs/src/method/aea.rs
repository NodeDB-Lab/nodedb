// SPDX-License-Identifier: Apache-2.0

//! Albers Equal Area, EPSG method 9822, PROJ `aea`.
//!
//! Follows PROJ 9.8.1 `src/projections/aea.cpp` (Snyder 1987 pp. 98–103,
//! eqs. 14-1 to 14-21; IOGP Guidance Note 7-2 §3.4.1), ellipsoidal and
//! spherical.

use core::f64::consts::FRAC_PI_2;
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::authalic::Authalic;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `EPS10`: |φ1 + φ2| below this, radians, has no cone.
const EPS10: f64 = 1e-10;

/// PROJ `TOL7`: |qp − |q|| below this puts the inverse on a pole.
const TOL7: f64 = 1e-7;

/// Largest absolute latitude, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Defining parameters of one Albers Equal Area projection.
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
pub struct AeaParams {
    /// Latitude of false origin φF, degrees (`+lat_0`).
    pub lat0_deg: f64,
    /// Longitude of false origin λF, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// Latitude of first standard parallel φ1, degrees (`+lat_1`).
    pub lat1_deg: f64,
    /// Latitude of second standard parallel φ2, degrees (`+lat_2`).
    pub lat2_deg: f64,
    /// Easting at false origin, metres (`+x_0`).
    pub false_easting: f64,
    /// Northing at false origin, metres (`+y_0`).
    pub false_northing: f64,
}

impl AeaParams {
    /// Read from PROJ-string parameters. `+lat_1` is required; `+lat_2`,
    /// `+lat_0`, `+lon_0`, `+x_0`, `+y_0` default to 0 as in PROJ.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat1_deg = p.number("lat_1")?.ok_or(CrsParseError::Missing {
            key: "lat_1",
            method: "aea",
        })?;
        let lat2_deg = p.number_or("lat_2", 0.0)?;
        if lat1_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_1", &lat1_deg.to_string()));
        }
        if lat2_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_2", &lat2_deg.to_string()));
        }
        // PROJ: |lat_1 + lat_2| must exceed EPS10, else the cone is a plane.
        if (lat1_deg + lat2_deg).to_radians().abs() < EPS10 {
            return Err(CrsParseError::invalid("lat_2", &lat2_deg.to_string()));
        }
        Ok(Self {
            lat0_deg: p.number_or("lat_0", 0.0)?,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            lat1_deg,
            lat2_deg,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append the defining parameters as PROJ-string tokens.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=aea +lat_0={} +lon_0={} +lat_1={} +lat_2={} +x_0={} +y_0={}",
            self.lat0_deg,
            self.lon0_deg,
            self.lat1_deg,
            self.lat2_deg,
            self.false_easting,
            self.false_northing
        );
    }
}

/// Albers Equal Area with its cone constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Albers {
    params: AeaParams,
    a: f64,
    lon0: f64,
    /// Authalic conversion; `None` on a sphere.
    auth: Option<Authalic>,
    /// Cone constant n.
    n: f64,
    /// 2n, used on the sphere.
    n2: f64,
    /// C, Snyder eq. 14-3 (ellipsoid) or 14-13 (sphere).
    c: f64,
    /// 1 / n.
    dd: f64,
    /// ρ0 / a.
    rho0: f64,
}

impl Albers {
    pub fn new(p: &AeaParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let phi1 = p.lat1_deg.to_radians();
        let phi2 = p.lat2_deg.to_radians();
        let secant = (phi1 - phi2).abs() >= EPS10;
        let (sinphi1, cosphi1) = phi1.sin_cos();
        let mut n = sinphi1;
        let (auth, n2, c, rho0);
        if ell.is_sphere() {
            if secant {
                n = 0.5 * (n + phi2.sin());
            }
            n2 = n + n;
            c = cosphi1 * cosphi1 + n2 * sinphi1;
            rho0 = (c - n2 * phi0.sin()).sqrt() / n;
            auth = None;
        } else {
            let es = ell.e2();
            let a = Authalic::new(es);
            let m1 = msfn(sinphi1, cosphi1, es);
            let ml1 = a.q(sinphi1);
            if secant {
                let (sinphi2, cosphi2) = phi2.sin_cos();
                let m2 = msfn(sinphi2, cosphi2, es);
                let ml2 = a.q(sinphi2);
                n = (m1 * m1 - m2 * m2) / (ml2 - ml1);
            }
            n2 = n + n;
            c = m1 * m1 + n * ml1;
            rho0 = (c - n * a.q(phi0.sin())).sqrt() / n;
            auth = Some(a);
        }
        Self {
            params: *p,
            a: ell.a,
            lon0: p.lon0_deg.to_radians(),
            auth,
            n,
            n2,
            c,
            dd: 1.0 / n,
            rho0,
        }
    }

    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let sinphi = phi.sin();
        let rho2 = self.c
            - match &self.auth {
                Some(a) => self.n * a.q(sinphi),
                None => self.n2 * sinphi,
            };
        if rho2 < 0.0 {
            return Err(DomainReason::OutsideProjection);
        }
        let rho = self.dd * rho2.sqrt();
        let (s, c) = (lam * self.n).sin_cos();
        Ok((
            self.params.false_easting + self.a * rho * s,
            self.params.false_northing + self.a * (self.rho0 - rho * c),
        ))
    }

    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let mut x = (x - self.params.false_easting) / self.a;
        let mut y = self.rho0 - (y - self.params.false_northing) / self.a;
        let mut rho = x.hypot(y);
        if rho == 0.0 {
            let phi = if self.n > 0.0 { FRAC_PI_2 } else { -FRAC_PI_2 };
            return Ok((self.lon0, phi));
        }
        if self.n < 0.0 {
            rho = -rho;
            x = -x;
            y = -y;
        }
        let r = rho / self.dd;
        let phi = match &self.auth {
            Some(a) => {
                let qs = (self.c - r * r) / self.n;
                let qp = a.qp();
                if (qp - qs.abs()).abs() > TOL7 {
                    // PROJ fails |qs| > 2; asin also needs |qs| ≤ qp.
                    if qs.abs() > qp {
                        return Err(DomainReason::OutsideProjection);
                    }
                    a.from_authalic((qs / qp).asin())
                } else if qs < 0.0 {
                    -FRAC_PI_2
                } else {
                    FRAC_PI_2
                }
            }
            None => {
                let qs_div_2 = (self.c - r * r) / self.n2;
                if qs_div_2.abs() <= 1.0 {
                    qs_div_2.asin()
                } else if qs_div_2 < 0.0 {
                    -FRAC_PI_2
                } else {
                    FRAC_PI_2
                }
            }
        };
        let lam = x.atan2(y) / self.n;
        Ok((wrap_pi(lam + self.lon0), phi))
    }
}

/// m = cos φ / √(1 − e² sin² φ), Snyder eq. 14-15, PROJ `pj_msfn`.
fn msfn(sinphi: f64, cosphi: f64, es: f64) -> f64 {
    cosphi / (1.0 - es * sinphi * sinphi).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn check(p: &AeaParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let aea = Albers::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = aea.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = aea.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    /// The GN 7-2 §3.4.1 parameter set (Clarke 1866, φF = 0°, λF = 132°E,
    /// φ1 = 18°S, φ2 = 36°S). PROJ 9.8.1 values via pyproj 3.8.0 for
    /// `+proj=aea +lat_0=0 +lon_0=132 +lat_1=-18 +lat_2=-36 +ellps=clrk66`.
    #[test]
    fn gn7_2_parameters() {
        let p = AeaParams {
            lat0_deg: 0.0,
            lon0_deg: 132.0,
            lat1_deg: -18.0,
            lat2_deg: -36.0,
            false_easting: 0.0,
            false_northing: 0.0,
        };
        check(
            &p,
            &Ellipsoid::CLARKE_1866,
            &[[
                140.0,
                -20.0,
                832_812.274_151_099_5,
                -2_170_059.232_002_873_4,
            ]],
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:3577 GDA94 / Australian Albers:
    /// `+proj=aea +lat_0=0 +lon_0=132 +lat_1=-18 +lat_2=-36 +x_0=0 +y_0=0
    /// +ellps=GRS80`.
    #[test]
    fn australian_albers() {
        let p = AeaParams {
            lat0_deg: 0.0,
            lon0_deg: 132.0,
            lat1_deg: -18.0,
            lat2_deg: -36.0,
            false_easting: 0.0,
            false_northing: 0.0,
        };
        check(&p, &Ellipsoid::GRS80, AU_ALBERS);
    }

    /// (lon, lat, x, y).
    const AU_ALBERS: &[[f64; 4]] = &[
        [147.0, -35.0, 1362365.5637248761, -3902190.4297908046],
        [115.86, -31.95, -1508261.2956576233, -3577137.7414186457],
        [130.84, -12.46, -128368.21938505347, -1314806.1986117077],
        [153.0, -27.5, 2040429.2689670695, -3150988.844969211],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0: `+proj=aea +lat_0=40 +lon_0=-96
    /// +lat_1=20 +lat_2=60 +R=6370997`.
    #[test]
    fn sphere() {
        let p = AeaParams {
            lat0_deg: 40.0,
            lon0_deg: -96.0,
            lat1_deg: 20.0,
            lat2_deg: 60.0,
            false_easting: 0.0,
            false_northing: 0.0,
        };
        check(&p, &Ellipsoid::sphere(6_370_997.0), SPHERE_PTS);
    }

    /// (lon, lat, x, y).
    const SPHERE_PTS: &[[f64; 4]] = &[
        [-120.0, 35.0, -2050766.2665282101, -328037.05230039987],
        [-75.0, 45.0, 1539757.8618297249, 762574.6118662546],
        [-96.0, 40.0, 0.0, 0.0],
        [-100.0, 60.0, -222323.84540856257, 2334542.866764218],
    ];

    #[test]
    fn parses_and_writes() {
        let p = ProjParams::tokenize(
            "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0",
        )
        .unwrap();
        let a = AeaParams::from_proj(&p).unwrap();
        assert_eq!(a.lat1_deg, 29.5);
        let mut s = String::new();
        a.write_proj(&mut s);
        assert_eq!(
            s,
            " +proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0"
        );
        let bad = ProjParams::tokenize("+proj=aea +lat_1=10 +lat_2=-10").unwrap();
        assert!(AeaParams::from_proj(&bad).is_err());
        let missing = ProjParams::tokenize("+proj=aea +lat_2=10").unwrap();
        assert_eq!(
            AeaParams::from_proj(&missing),
            Err(CrsParseError::Missing {
                key: "lat_1",
                method: "aea"
            })
        );
    }
}
