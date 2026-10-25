// SPDX-License-Identifier: Apache-2.0

//! Lambert Azimuthal Equal Area, EPSG methods 9820 (ellipsoidal) and 1027
//! (spherical), PROJ `laea`.
//!
//! Follows PROJ 9.8.1 `src/projections/laea.cpp` (Snyder 1987 pp. 182–190,
//! eqs. 24-2 to 24-29; IOGP Guidance Note 7-2 §3.6.2), with its polar,
//! equatorial and oblique aspects.

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::authalic::Authalic;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `EPS10`, radians.
const EPS10: f64 = 1e-10;

/// q below which a polar forward lands on the origin, PROJ `laea_e_forward`.
const POLAR_Q_MIN: f64 = 1e-15;

/// Largest absolute latitude, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Defining parameters of one Lambert Azimuthal Equal Area projection.
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
pub struct LaeaParams {
    /// Latitude of natural origin φ0, degrees (`+lat_0`).
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// False easting, metres (`+x_0`).
    pub false_easting: f64,
    /// False northing, metres (`+y_0`).
    pub false_northing: f64,
}

impl LaeaParams {
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
            " +proj=laea +lat_0={} +lon_0={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// Aspect of the projection, from φ0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    NorthPole,
    SouthPole,
    Equatorial,
    Oblique,
}

/// Lambert Azimuthal Equal Area with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct LambertAzimuthalEqualArea {
    params: LaeaParams,
    a: f64,
    lon0: f64,
    phi0: f64,
    mode: Mode,
    /// Authalic conversion; `None` on a sphere.
    auth: Option<Authalic>,
    /// sin and cos of the authalic (sphere: geodetic) origin latitude.
    sinb1: f64,
    cosb1: f64,
    xmf: f64,
    ymf: f64,
    /// Authalic radius over a, Rq.
    rq: f64,
    /// D, Snyder eq. 24-20.
    dd: f64,
}

impl LambertAzimuthalEqualArea {
    pub fn new(p: &LaeaParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let t = phi0.abs();
        let mode = if (t - FRAC_PI_2).abs() < EPS10 {
            if phi0 < 0.0 {
                Mode::SouthPole
            } else {
                Mode::NorthPole
            }
        } else if t < EPS10 {
            Mode::Equatorial
        } else {
            Mode::Oblique
        };
        let mut out = Self {
            params: *p,
            a: ell.a,
            lon0: p.lon0_deg.to_radians(),
            phi0,
            mode,
            auth: None,
            sinb1: 0.0,
            cosb1: 0.0,
            xmf: 0.0,
            ymf: 0.0,
            rq: 0.0,
            dd: 0.0,
        };
        if ell.is_sphere() {
            if mode == Mode::Oblique {
                (out.sinb1, out.cosb1) = phi0.sin_cos();
            }
            return out;
        }
        let es = ell.e2();
        let auth = Authalic::new(es);
        let qp = auth.qp();
        match mode {
            Mode::NorthPole | Mode::SouthPole => out.dd = 1.0,
            Mode::Equatorial => {
                out.rq = (0.5 * qp).sqrt();
                out.dd = 1.0 / out.rq;
                out.xmf = 1.0;
                out.ymf = 0.5 * qp;
            }
            Mode::Oblique => {
                out.rq = (0.5 * qp).sqrt();
                let (sinphi, cosphi) = phi0.sin_cos();
                (out.sinb1, out.cosb1) = auth.to_authalic(phi0).sin_cos();
                out.dd = cosphi / ((1.0 - es * sinphi * sinphi).sqrt() * out.rq * out.cosb1);
                out.ymf = out.rq / out.dd;
                out.xmf = out.rq * out.dd;
            }
        }
        out.auth = Some(auth);
        out
    }

    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let (x, y) = match &self.auth {
            Some(a) => self.e_forward(a, lam, phi)?,
            None => self.s_forward(lam, phi)?,
        };
        Ok((
            self.params.false_easting + self.a * x,
            self.params.false_northing + self.a * y,
        ))
    }

    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        let (lam, phi) = match &self.auth {
            Some(a) => self.e_inverse(a, x, y)?,
            None => self.s_inverse(x, y)?,
        };
        Ok((wrap_pi(lam + self.lon0), phi))
    }

    /// Ellipsoidal forward, PROJ `laea_e_forward`.
    fn e_forward(&self, auth: &Authalic, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let (sinlam, coslam) = lam.sin_cos();
        let xi = auth.to_authalic(phi);
        let (sinb, cosb) = xi.sin_cos();
        let qp = auth.qp();
        let mut q = sinb * qp;
        let b = match self.mode {
            Mode::Oblique => 1.0 + self.sinb1 * sinb + self.cosb1 * cosb * coslam,
            Mode::Equatorial => 1.0 + cosb * coslam,
            Mode::NorthPole => {
                q = qp - q;
                FRAC_PI_2 + phi
            }
            Mode::SouthPole => {
                q += qp;
                phi - FRAC_PI_2
            }
        };
        if b.abs() < EPS10 {
            return Err(DomainReason::Antipode);
        }
        Ok(match self.mode {
            Mode::Oblique => {
                let b = (2.0 / b).sqrt();
                (
                    self.xmf * b * cosb * sinlam,
                    self.ymf * b * (self.cosb1 * sinb - self.sinb1 * cosb * coslam),
                )
            }
            Mode::Equatorial => {
                let b = (2.0 / (1.0 + cosb * coslam)).sqrt();
                (self.xmf * b * cosb * sinlam, b * sinb * self.ymf)
            }
            Mode::NorthPole | Mode::SouthPole => {
                if q >= POLAR_Q_MIN {
                    let b = q.sqrt();
                    let y = if self.mode == Mode::SouthPole { b } else { -b };
                    (b * sinlam, coslam * y)
                } else {
                    (0.0, 0.0)
                }
            }
        })
    }

    /// Spherical forward, PROJ `laea_s_forward`.
    fn s_forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let (sinphi, cosphi) = phi.sin_cos();
        let (sinlam, mut coslam) = lam.sin_cos();
        match self.mode {
            Mode::Equatorial | Mode::Oblique => {
                let denom = if self.mode == Mode::Equatorial {
                    1.0 + cosphi * coslam
                } else {
                    1.0 + self.sinb1 * sinphi + self.cosb1 * cosphi * coslam
                };
                if denom <= EPS10 {
                    return Err(DomainReason::Antipode);
                }
                let k = (2.0 / denom).sqrt();
                let y = if self.mode == Mode::Equatorial {
                    sinphi
                } else {
                    self.cosb1 * sinphi - self.sinb1 * cosphi * coslam
                };
                Ok((k * cosphi * sinlam, k * y))
            }
            Mode::NorthPole | Mode::SouthPole => {
                if self.mode == Mode::NorthPole {
                    coslam = -coslam;
                }
                if (phi + self.phi0).abs() < EPS10 {
                    return Err(DomainReason::Antipode);
                }
                let t = FRAC_PI_4 - phi * 0.5;
                let r = 2.0
                    * if self.mode == Mode::SouthPole {
                        t.cos()
                    } else {
                        t.sin()
                    };
                Ok((r * sinlam, r * coslam))
            }
        }
    }

    /// Ellipsoidal inverse, PROJ `laea_e_inverse`.
    fn e_inverse(&self, auth: &Authalic, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let qp = auth.qp();
        let (mut x, mut y) = (x, y);
        let ab = match self.mode {
            Mode::Equatorial | Mode::Oblique => {
                x /= self.dd;
                y *= self.dd;
                let rho = x.hypot(y);
                if rho < EPS10 {
                    return Ok((0.0, self.phi0));
                }
                let asin_arg = 0.5 * rho / self.rq;
                if asin_arg > 1.0 {
                    return Err(DomainReason::OutsideProjection);
                }
                let (s_ce, c_ce) = (2.0 * asin_arg.asin()).sin_cos();
                x *= s_ce;
                if self.mode == Mode::Oblique {
                    let ab = c_ce * self.sinb1 + y * s_ce * self.cosb1 / rho;
                    y = rho * self.cosb1 * c_ce - y * self.sinb1 * s_ce;
                    ab
                } else {
                    let ab = y * s_ce / rho;
                    y = rho * c_ce;
                    ab
                }
            }
            Mode::NorthPole | Mode::SouthPole => {
                if self.mode == Mode::NorthPole {
                    y = -y;
                }
                let q = x * x + y * y;
                if q == 0.0 {
                    return Ok((0.0, self.phi0));
                }
                let ab = 1.0 - q / qp;
                if self.mode == Mode::SouthPole {
                    -ab
                } else {
                    ab
                }
            }
        };
        if ab.abs() > 1.0 {
            return Err(DomainReason::OutsideProjection);
        }
        Ok((x.atan2(y), auth.from_authalic(ab.asin())))
    }

    /// Spherical inverse, PROJ `laea_s_inverse`.
    fn s_inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let (mut x, mut y) = (x, y);
        let rh = x.hypot(y);
        let half = rh * 0.5;
        if half > 1.0 {
            return Err(DomainReason::OutsideProjection);
        }
        let z = 2.0 * half.asin();
        let (sinz, cosz) = z.sin_cos();
        let phi = match self.mode {
            Mode::Equatorial => {
                let phi = if rh.abs() <= EPS10 {
                    0.0
                } else {
                    (y * sinz / rh).asin()
                };
                x *= sinz;
                y = cosz * rh;
                phi
            }
            Mode::Oblique => {
                let phi = if rh.abs() <= EPS10 {
                    self.phi0
                } else {
                    (cosz * self.sinb1 + y * sinz * self.cosb1 / rh).asin()
                };
                x *= sinz * self.cosb1;
                y = (cosz - phi.sin() * self.sinb1) * rh;
                phi
            }
            Mode::NorthPole => {
                y = -y;
                FRAC_PI_2 - z
            }
            Mode::SouthPole => z - FRAC_PI_2,
        };
        let lam = if y == 0.0 && matches!(self.mode, Mode::Equatorial | Mode::Oblique) {
            0.0
        } else {
            x.atan2(y)
        };
        Ok((lam, phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn params(lat0_deg: f64, lon0_deg: f64, false_easting: f64, false_northing: f64) -> LaeaParams {
        LaeaParams {
            lat0_deg,
            lon0_deg,
            false_easting,
            false_northing,
        }
    }

    fn check(p: &LaeaParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let laea = LambertAzimuthalEqualArea::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = laea.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = laea.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    /// GN 7-2 §3.6.2 example, ETRS89 / LAEA Europe (EPSG:3035):
    /// φ = 50°N, λ = 5°E → E = 3 962 799.45 m, N = 2 999 718.85 m.
    #[test]
    fn gn7_2_etrs89_laea() {
        let laea = LambertAzimuthalEqualArea::new(
            &params(52.0, 10.0, 4_321_000.0, 3_210_000.0),
            &Ellipsoid::GRS80,
        );
        let (x, y) = laea.forward(5f64.to_radians(), 50f64.to_radians()).unwrap();
        assert!(
            (x - 3_962_799.45).abs() < 0.005 && (y - 2_999_718.85).abs() < 0.005,
            "{x} {y}"
        );
        let (lam, phi) = laea.inverse(3_962_799.45, 2_999_718.85).unwrap();
        assert!((lam.to_degrees() - 5.0).abs() < 1e-7 && (phi.to_degrees() - 50.0).abs() < 1e-7);
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:3035
    /// `+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80`.
    #[test]
    fn oblique_ellipsoid() {
        check(
            &params(52.0, 10.0, 4_321_000.0, 3_210_000.0),
            &Ellipsoid::GRS80,
            OBLIQUE,
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:6931
    /// `+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84`.
    #[test]
    fn north_polar_ellipsoid() {
        check(&params(90.0, 0.0, 0.0, 0.0), &Ellipsoid::WGS84, NORTH_POLAR);
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:6932
    /// `+proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84`.
    #[test]
    fn south_polar_ellipsoid() {
        check(
            &params(-90.0, 0.0, 0.0, 0.0),
            &Ellipsoid::WGS84,
            SOUTH_POLAR,
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=laea +lat_0=0 +lon_0=20 +ellps=WGS84`.
    #[test]
    fn equatorial_ellipsoid() {
        check(&params(0.0, 20.0, 0.0, 0.0), &Ellipsoid::WGS84, EQUATORIAL);
    }

    /// EPSG 1027 (spherical), PROJ 9.8.1 via pyproj 3.8.0, EPSG:3408
    /// `+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +R=6371228`.
    #[test]
    fn sphere_north_polar() {
        check(
            &params(90.0, 0.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_371_228.0),
            SPHERE_NORTH,
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:3409
    /// `+proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +R=6371228`.
    #[test]
    fn sphere_south_polar() {
        check(
            &params(-90.0, 0.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_371_228.0),
            SPHERE_SOUTH,
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=laea +lat_0=45 +lon_0=-100 +R=6370997`.
    #[test]
    fn sphere_oblique() {
        check(
            &params(45.0, -100.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_370_997.0),
            SPHERE_OBLIQUE,
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=laea +lat_0=0 +lon_0=0 +R=6370997`.
    #[test]
    fn sphere_equatorial() {
        check(
            &params(0.0, 0.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_370_997.0),
            SPHERE_EQUATORIAL,
        );
    }

    // (lon, lat, x, y) reference points.
    const OBLIQUE: &[[f64; 4]] = &[
        [5.0, 50.0, 3962799.4509550678, 2999718.853159564],
        [-9.0, 38.7, 2676732.112534873, 1941329.0843069698],
        [30.0, 60.0, 5422492.77496154, 4256803.184504329],
        [10.0, 52.0, 4321000.0, 3210000.0],
        [25.0, 35.0, 5695390.183788649, 1461721.553816132],
    ];
    const NORTH_POLAR: &[[f64; 4]] = &[
        [0.0, 90.0, 0.0, 0.0],
        [45.0, 60.0, 2340395.8490303955, -2340395.849030396],
        [-120.0, 75.0, -1446478.9424851332, 835125.0068209164],
        [170.0, 10.0, 1422897.3773335486, 8069652.027253477],
    ];
    const SOUTH_POLAR: &[[f64; 4]] = &[
        [45.0, -60.0, 2340395.8490303955, 2340395.849030396],
        [-120.0, -75.0, -1446478.9424851332, -835125.0068209164],
        [170.0, -10.0, 1422897.3773335486, -8069652.027253477],
    ];
    const EQUATORIAL: &[[f64; 4]] = &[
        [20.0, 0.0, 0.0, 0.0],
        [50.0, 30.0, 2955093.6416210765, 3389407.875813428],
        [-30.0, -45.0, -4058883.8196492754, -5263034.2804662995],
        [100.0, 10.0, 8085103.626887971, 1437923.67613473],
    ];
    const SPHERE_NORTH: &[[f64; 4]] = &[
        [45.0, 60.0, 2332031.3013027217, -2332031.3013027217],
        [-120.0, 75.0, -1440394.4623998066, 831612.1306057742],
        [170.0, 10.0, 1422298.8844147702, 8066257.80524404],
    ];
    const SPHERE_SOUTH: &[[f64; 4]] = &[
        [45.0, -60.0, 2332031.301302723, 2332031.301302723],
        [-120.0, -75.0, -1440394.462399806, -831612.1306057739],
    ];
    const SPHERE_OBLIQUE: &[[f64; 4]] = &[
        [-120.0, 35.0, -1807736.3959958896, -895049.1532893647],
        [-75.0, 45.0, 1926581.688418818, 302014.4119897752],
        [10.0, 10.0, 8865066.189534225, 3457815.856604664],
    ];
    const SPHERE_EQUATORIAL: &[[f64; 4]] = &[
        [0.0, 0.0, 0.0, 0.0],
        [30.0, 30.0, 2949198.539095313, 3405441.1408806597],
        [-120.0, -45.0, -6862331.619017425, -7923938.015016379],
        [89.0, 1.0, 8929633.072795173, 155891.06803553604],
    ];

    #[test]
    fn antipode_is_out_of_domain() {
        let laea = LambertAzimuthalEqualArea::new(&params(90.0, 0.0, 0.0, 0.0), &Ellipsoid::WGS84);
        assert_eq!(laea.forward(0.0, -FRAC_PI_2), Err(DomainReason::Antipode));
        let laea =
            LambertAzimuthalEqualArea::new(&params(0.0, 0.0, 0.0, 0.0), &Ellipsoid::sphere(1.0));
        assert_eq!(
            laea.forward(core::f64::consts::PI, 0.0),
            Err(DomainReason::Antipode)
        );
        assert_eq!(laea.inverse(3.0, 0.0), Err(DomainReason::OutsideProjection));
    }

    #[test]
    fn parses_and_writes() {
        let p = ProjParams::tokenize("+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000")
            .unwrap();
        let l = LaeaParams::from_proj(&p).unwrap();
        let mut s = String::new();
        l.write_proj(&mut s);
        assert_eq!(
            s,
            " +proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000"
        );
        let bad = ProjParams::tokenize("+proj=laea +lat_0=91").unwrap();
        assert!(LaeaParams::from_proj(&bad).is_err());
    }
}
