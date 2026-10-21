// SPDX-License-Identifier: Apache-2.0

//! Azimuthal Equidistant, EPSG method 1125 (and 9831 Guam), PROJ `aeqd`.
//!
//! Follows PROJ 9.8.1 `src/projections/aeqd.cpp`. The ellipsoidal oblique
//! and equatorial aspects solve the geodesic problem exactly (Karney 2013,
//! [`Geodesic`]); the polar aspects use the meridian arc (Snyder 1987
//! eq. 25-14); `+guam` selects the Guam series (Snyder eqs. 25-15 to
//! 25-17, IOGP Guidance Note 7-2 §3.9.2); spheres use Snyder eqs. 25-2 to
//! 25-5.

use core::f64::consts::{FRAC_PI_2, PI};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::geodesic::Geodesic;
use super::meridian::MeridianArc;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `EPS10`, radians.
const EPS10: f64 = 1e-10;

/// PROJ `TOL`: |cos c| this close to 1 counts as the centre or antipode.
const TOL: f64 = 1e-14;

/// PROJ `aasin` tolerance: |v| up to this rounds to ±1.
const ONE_TOL: f64 = 1.000_000_000_000_01;

/// Fixed-point iterations of the Guam inverse, PROJ `e_guam_inv`.
const GUAM_ITERS: usize = 3;

/// Radians per degree, PROJ `DEG_TO_RAD`.
const DEG_TO_RAD: f64 = PI / 180.0;

/// Largest absolute latitude of origin, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Defining parameters of one Azimuthal Equidistant projection.
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
pub struct AeqdParams {
    /// Latitude of natural origin φ0, degrees (`+lat_0`).
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// False easting, metres (`+x_0`).
    pub false_easting: f64,
    /// False northing, metres (`+y_0`).
    pub false_northing: f64,
    /// PROJ `+guam`: the Guam series, EPSG 9831. Ellipsoid only.
    pub guam: bool,
}

impl AeqdParams {
    /// Read from PROJ-string parameters. Every number defaults to 0 as in
    /// PROJ.
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
            guam: p.has("guam"),
        })
    }

    /// Append the defining parameters as PROJ-string tokens.
    pub fn write_proj(&self, out: &mut String) {
        out.push_str(" +proj=aeqd");
        if self.guam {
            out.push_str(" +guam");
        }
        let _ = write!(
            out,
            " +lat_0={} +lon_0={} +x_0={} +y_0={}",
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

/// Which formulas apply.
#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Sphere,
    /// Geodesic (oblique, equatorial) or meridian arc (polar).
    Ellipsoid(MeridianArc),
    /// The Guam series, with the arc to φ0 in units of a.
    Guam {
        arc: MeridianArc,
        m1: f64,
    },
}

/// Azimuthal Equidistant with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct AzimuthalEquidistant {
    params: AeqdParams,
    a: f64,
    e: f64,
    lon0: f64,
    phi0: f64,
    mode: Mode,
    sinph0: f64,
    cosph0: f64,
    kind: Kind,
    /// Geodesic on the ellipsoid scaled to a = 1, PROJ `geod_init(1, f)`.
    geod: Geodesic,
    /// Polar: meridian arc to the pole, units of a.
    mp: f64,
}

impl AzimuthalEquidistant {
    pub fn new(p: &AeqdParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let (mode, sinph0, cosph0) = if (phi0.abs() - FRAC_PI_2).abs() < EPS10 {
            if phi0 < 0.0 {
                (Mode::SouthPole, -1.0, 0.0)
            } else {
                (Mode::NorthPole, 1.0, 0.0)
            }
        } else if phi0.abs() < EPS10 {
            (Mode::Equatorial, 0.0, 1.0)
        } else {
            let (s, c) = phi0.sin_cos();
            (Mode::Oblique, s, c)
        };
        let es = ell.e2();
        let mut mp = 0.0;
        let kind = if ell.is_sphere() {
            Kind::Sphere
        } else {
            let arc = MeridianArc::new(es);
            if p.guam {
                let m1 = arc.arc(phi0, sinph0, cosph0);
                Kind::Guam { arc, m1 }
            } else {
                match mode {
                    Mode::NorthPole => mp = arc.arc(FRAC_PI_2, 1.0, 0.0),
                    Mode::SouthPole => mp = arc.arc(-FRAC_PI_2, -1.0, 0.0),
                    Mode::Equatorial | Mode::Oblique => {}
                }
                Kind::Ellipsoid(arc)
            }
        };
        Self {
            params: *p,
            a: ell.a,
            e: ell.e(),
            lon0: p.lon0_deg.to_radians(),
            phi0,
            mode,
            sinph0,
            cosph0,
            kind,
            geod: Geodesic::new(1.0, ell.f()),
            mp,
        }
    }

    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let (x, y) = match &self.kind {
            Kind::Sphere => self.s_forward(lam, phi)?,
            Kind::Ellipsoid(arc) => self.e_forward(arc, lam, phi),
            Kind::Guam { arc, m1 } => {
                let (sinphi, cosphi) = phi.sin_cos();
                let t = 1.0 / (1.0 - self.e * self.e * sinphi * sinphi).sqrt();
                (
                    lam * cosphi * t,
                    arc.arc(phi, sinphi, cosphi) - m1 + 0.5 * lam * lam * cosphi * sinphi * t,
                )
            }
        };
        Ok((
            self.params.false_easting + self.a * x,
            self.params.false_northing + self.a * y,
        ))
    }

    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        let (lam, phi) = match &self.kind {
            Kind::Sphere => self.s_inverse(x, y)?,
            Kind::Ellipsoid(arc) => self.e_inverse(arc, x, y)?,
            Kind::Guam { arc, m1 } => {
                let x2 = 0.5 * x * x;
                let mut phi = self.phi0;
                let mut t = 0.0;
                for _ in 0..GUAM_ITERS {
                    let es = self.e * phi.sin();
                    t = (1.0 - es * es).sqrt();
                    phi = arc.inverse(m1 + y - x2 * phi.tan() * t)?;
                }
                (x * t / phi.cos(), phi)
            }
        };
        if phi.abs() > FRAC_PI_2 {
            return Err(DomainReason::OutsideProjection);
        }
        Ok((wrap_pi(lam + self.lon0), phi))
    }

    /// PROJ `aeqd_e_forward`.
    fn e_forward(&self, arc: &MeridianArc, lam: f64, phi: f64) -> (f64, f64) {
        match self.mode {
            Mode::NorthPole | Mode::SouthPole => {
                let (sinlam, coslam) = lam.sin_cos();
                let coslam = if self.mode == Mode::NorthPole {
                    -coslam
                } else {
                    coslam
                };
                let rho = (self.mp - arc.arc(phi, phi.sin(), phi.cos())).abs();
                (rho * sinlam, rho * coslam)
            }
            Mode::Equatorial | Mode::Oblique => self.geodesic_forward(lam, phi),
        }
    }

    /// Distance and azimuth from the centre by the inverse geodesic
    /// problem, PROJ `aeqd_e_forward`.
    fn geodesic_forward(&self, lam: f64, phi: f64) -> (f64, f64) {
        if lam.abs() < EPS10 && (phi - self.phi0).abs() < EPS10 {
            return (0.0, 0.0);
        }
        let (s12, azi1, _) = self.geod.inverse(
            self.phi0 / DEG_TO_RAD,
            0.0,
            phi / DEG_TO_RAD,
            lam / DEG_TO_RAD,
        );
        let (s, c) = (azi1 * DEG_TO_RAD).sin_cos();
        (s12 * s, s12 * c)
    }

    /// PROJ `aeqd_s_forward` (Snyder eqs. 25-2 to 25-5).
    fn s_forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let (sinphi, cosphi) = phi.sin_cos();
        let (sinlam, coslam) = lam.sin_cos();
        match self.mode {
            Mode::Equatorial | Mode::Oblique => {
                let cosphi_coslam = cosphi * coslam;
                let cos_c = if self.mode == Mode::Equatorial {
                    cosphi_coslam
                } else {
                    self.sinph0 * sinphi + self.cosph0 * cosphi_coslam
                };
                if (cos_c.abs() - 1.0).abs() < TOL {
                    if cos_c < 0.0 {
                        return Err(DomainReason::Antipode);
                    }
                    return Ok(self.geodesic_forward(lam, phi));
                }
                let c = cos_c.acos();
                let k = c / c.sin();
                let y = if self.mode == Mode::Equatorial {
                    sinphi
                } else {
                    self.cosph0 * sinphi - self.sinph0 * cosphi_coslam
                };
                Ok((k * cosphi * sinlam, k * y))
            }
            Mode::NorthPole | Mode::SouthPole => {
                let (phi, coslam) = if self.mode == Mode::NorthPole {
                    (-phi, -coslam)
                } else {
                    (phi, coslam)
                };
                if (phi - FRAC_PI_2).abs() < EPS10 {
                    return Err(DomainReason::Antipode);
                }
                let r = FRAC_PI_2 + phi;
                Ok((r * sinlam, r * coslam))
            }
        }
    }

    /// PROJ `aeqd_e_inverse`.
    fn e_inverse(&self, arc: &MeridianArc, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let s12 = x.hypot(y);
        if s12 < EPS10 {
            return Ok((0.0, self.phi0));
        }
        match self.mode {
            Mode::Equatorial | Mode::Oblique => {
                let azi1 = x.atan2(y) / DEG_TO_RAD;
                let (lat2, lon2, _) = self.geod.direct(self.phi0 / DEG_TO_RAD, 0.0, azi1, s12);
                Ok((lon2 * DEG_TO_RAD, lat2 * DEG_TO_RAD))
            }
            Mode::NorthPole => Ok(((x).atan2(-y), arc.inverse(self.mp - s12)?)),
            Mode::SouthPole => Ok((x.atan2(y), arc.inverse(self.mp + s12)?)),
        }
    }

    /// PROJ `aeqd_s_inverse`.
    fn s_inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let (mut x, mut y) = (x, y);
        let mut c_rh = x.hypot(y);
        if c_rh > PI {
            if c_rh - EPS10 > PI {
                return Err(DomainReason::OutsideProjection);
            }
            c_rh = PI;
        } else if c_rh < EPS10 {
            return Ok((0.0, self.phi0));
        }
        match self.mode {
            Mode::Equatorial | Mode::Oblique => {
                let (sinc, cosc) = c_rh.sin_cos();
                let phi;
                if self.mode == Mode::Equatorial {
                    phi = aasin(y * sinc / c_rh)?;
                    x *= sinc;
                    y = cosc * c_rh;
                } else {
                    phi = aasin(cosc * self.sinph0 + y * sinc * self.cosph0 / c_rh)?;
                    y = (cosc - self.sinph0 * phi.sin()) * c_rh;
                    x *= sinc * self.cosph0;
                }
                let lam = if y == 0.0 { 0.0 } else { x.atan2(y) };
                Ok((lam, phi))
            }
            Mode::NorthPole => Ok((x.atan2(-y), FRAC_PI_2 - c_rh)),
            Mode::SouthPole => Ok((x.atan2(y), c_rh - FRAC_PI_2)),
        }
    }
}

/// asin that rounds |v| ≤ [`ONE_TOL`] to ±π/2, PROJ `aasin`.
fn aasin(v: f64) -> Result<f64, DomainReason> {
    if v.abs() >= 1.0 {
        if v.abs() > ONE_TOL {
            return Err(DomainReason::OutsideProjection);
        }
        return Ok(FRAC_PI_2.copysign(v));
    }
    Ok(v.asin())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn params(lat0_deg: f64, lon0_deg: f64, false_easting: f64, false_northing: f64) -> AeqdParams {
        AeqdParams {
            lat0_deg,
            lon0_deg,
            false_easting,
            false_northing,
            guam: false,
        }
    }

    fn check(p: &AeqdParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let aeqd = AzimuthalEquidistant::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = aeqd.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = aeqd.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    fn dms(d: f64, m: f64, s: f64) -> f64 {
        d + m / 60.0 + s / 3600.0
    }

    /// GN 7-2 §3.9.2 example, Guam 1963 / Yap Islands (EPSG:3295), Clarke
    /// 1866, φ0 = 9°32′48.15″N, λ0 = 138°10′07.48″E, FE = 40 000 m,
    /// FN = 60 000 m. φ = 9°35′47.493″N, λ = 138°11′34.908″E →
    /// E = 42 665.90 m, N = 65 509.82 m (Modified Azimuthal Equidistant;
    /// PROJ's exact geodesic agrees to 1 mm).
    #[test]
    fn gn7_2_yap_islands() {
        let p = params(
            dms(9.0, 32.0, 48.15),
            dms(138.0, 10.0, 7.48),
            40_000.0,
            60_000.0,
        );
        let aeqd = AzimuthalEquidistant::new(&p, &Ellipsoid::CLARKE_1866);
        let lon = dms(138.0, 11.0, 34.908);
        let lat = dms(9.0, 35.0, 47.493);
        let (x, y) = aeqd.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!(
            (x - 42_665.90).abs() < 0.005 && (y - 65_509.82).abs() < 0.005,
            "{x} {y}"
        );
        let (lam, phi) = aeqd.inverse(42_665.90, 65_509.82).unwrap();
        assert!((lam.to_degrees() - lon).abs() < 1e-7 && (phi.to_degrees() - lat).abs() < 1e-7);
        // PROJ 9.8.1 via pyproj 3.8.0, EPSG:3295 PROJ string.
        check(
            &p,
            &Ellipsoid::CLARKE_1866,
            &[[lon, lat, 42_665.904_012_062_79, 65_509.822_493_638_305]],
        );
    }

    /// GN 7-2 §3.9.2 with PROJ's `+guam` series: PROJ 9.8.1 via pyproj
    /// 3.8.0 gives E = 42 665.903 680 579 635, N = 65 509.822 816 551 77.
    #[test]
    fn guam_series() {
        let mut p = params(
            dms(9.0, 32.0, 48.15),
            dms(138.0, 10.0, 7.48),
            40_000.0,
            60_000.0,
        );
        p.guam = true;
        let lon = dms(138.0, 11.0, 34.908);
        let lat = dms(9.0, 35.0, 47.493);
        check(
            &p,
            &Ellipsoid::CLARKE_1866,
            &[[lon, lat, 42_665.903_680_579_635, 65_509.822_816_551_77]],
        );
    }

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:27704 WGS 84 / Equi7 Europe:
    /// `+proj=aeqd +lat_0=53 +lon_0=24 +x_0=5837287.82 +y_0=2121415.696
    /// +datum=WGS84`.
    #[test]
    fn equi7_europe() {
        check(
            &params(53.0, 24.0, 5_837_287.82, 2_121_415.696),
            &Ellipsoid::WGS84,
            EQUI7_EUROPE,
        );
    }

    const EQUI7_EUROPE: &[[f64; 4]] = &[
        [24.0, 53.0, 5837287.82, 2121415.696],
        [0.0, 40.0, 3802691.4752739277, 1001915.8778322181],
        [-20.0, 65.0, 3899068.361612137, 4116725.3950709505],
        [40.0, 35.0, 7312517.666956626, 273807.5616523754],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, EPSG:27702 WGS 84 / Equi7 Antarctica:
    /// `+proj=aeqd +lat_0=-90 +lon_0=0 +x_0=3714266.977 +y_0=3402016.506
    /// +datum=WGS84`.
    #[test]
    fn equi7_antarctica() {
        check(
            &params(-90.0, 0.0, 3_714_266.977, 3_402_016.506),
            &Ellipsoid::WGS84,
            EQUI7_ANTARCTICA,
        );
    }

    const EQUI7_ANTARCTICA: &[[f64; 4]] = &[
        [0.0, -90.0, 3714266.977, 3402016.506],
        [60.0, -70.0, 5648088.7146054655, 4518509.0067712655],
        [-150.0, -80.0, 3155854.0483120745, 2434816.9419091768],
        [170.0, -60.0, 4295622.4798146635, 104985.61215248471],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=aeqd +lat_0=90 +lon_0=0 +ellps=WGS84`.
    #[test]
    fn north_polar_ellipsoid() {
        check(&params(90.0, 0.0, 0.0, 0.0), &Ellipsoid::WGS84, NORTH_POLAR);
    }

    const NORTH_POLAR: &[[f64; 4]] = &[
        [45.0, 60.0, 2367317.7792216484, -2367317.779221649],
        [-120.0, 75.0, -1450616.9230538863, 837514.0710161875],
        [170.0, 10.0, 1544793.4454269276, 8760958.982114341],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=aeqd +lat_0=0 +lon_0=0 +ellps=WGS84`.
    #[test]
    fn equatorial_ellipsoid() {
        check(&params(0.0, 0.0, 0.0, 0.0), &Ellipsoid::WGS84, EQUATORIAL);
    }

    const EQUATORIAL: &[[f64; 4]] = &[
        [10.0, 10.0, 1101896.9072405538, 1111480.8582554795],
        [-60.0, 30.0, -5962674.986276318, 3954631.424151443],
        [120.0, -40.0, 8995111.25339604, -8710143.057967292],
        [0.0, 0.0, 0.0, 0.0],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=aeqd +lat_0=40 +lon_0=-100 +R=6370997`.
    #[test]
    fn sphere_oblique() {
        check(
            &params(40.0, -100.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_370_997.0),
            SPHERE_OBLIQUE,
        );
    }

    const SPHERE_OBLIQUE: &[[f64; 4]] = &[
        [-120.0, 35.0, -1810140.3394241622, -357945.7072990199],
        [-75.0, 45.0, 1939281.2513566797, 841944.9606152419],
        [10.0, 10.0, 10237736.198016878, 3866778.9215264153],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=aeqd +lat_0=-90 +lon_0=0 +R=6370997`.
    #[test]
    fn sphere_south_polar() {
        check(
            &params(-90.0, 0.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_370_997.0),
            SPHERE_SOUTH,
        );
    }

    const SPHERE_SOUTH: &[[f64; 4]] = &[
        [45.0, -60.0, 2358799.4891965115, 2358799.4891965115],
        [-120.0, -75.0, -1444463.7885172633, -833961.5571351082],
        [170.0, 10.0, 1930878.7285438292, -10950557.429077182],
    ];

    #[test]
    fn antipode_is_out_of_domain() {
        let aeqd = AzimuthalEquidistant::new(&params(30.0, 0.0, 0.0, 0.0), &Ellipsoid::sphere(1.0));
        assert_eq!(
            aeqd.forward(PI, -30f64.to_radians()),
            Err(DomainReason::Antipode)
        );
        assert_eq!(aeqd.inverse(4.0, 0.0), Err(DomainReason::OutsideProjection));
    }

    #[test]
    fn parses_and_writes() {
        let p =
            ProjParams::tokenize("+proj=aeqd +lat_0=53 +lon_0=24 +x_0=5837287.82 +y_0=2121415.696")
                .unwrap();
        let q = AeqdParams::from_proj(&p).unwrap();
        assert!(!q.guam);
        let mut s = String::new();
        q.write_proj(&mut s);
        assert_eq!(
            s,
            " +proj=aeqd +lat_0=53 +lon_0=24 +x_0=5837287.82 +y_0=2121415.696"
        );
    }
}
