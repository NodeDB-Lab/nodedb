// SPDX-License-Identifier: Apache-2.0

//! Stereographic, PROJ `stere` and `ups`.
//!
//! Covers Polar Stereographic EPSG methods 9810 (variant A, `+k_0`), 9829
//! (variant B, `+lat_ts`) and 9830 (variant C, written by PROJ as variant B
//! with a shifted false northing), plus the oblique and equatorial aspects
//! of PROJ `stere`. Follows PROJ 9.8.1 `src/projections/stere.cpp`, IOGP
//! Guidance Note 7-2 §3.3.2 and Snyder 1987 eqs. 21-1 to 21-44.

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::conformal::tsfn;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `stere.cpp` EPS10: aspect and degenerate-value tolerance.
const EPS10: f64 = 1e-10;

/// PROJ `stere.cpp` TOL: spherical tolerance at the opposite pole.
const SPHERE_POLE_TOL: f64 = 1e-8;

/// PROJ `stere.cpp`: |φ − 90°| below which the pole maps to x = 0.
const POLE_EXACT_TOL: f64 = 1e-15;

/// PROJ `stere.cpp` NITER: inverse iterations allowed.
const MAX_ITERS: usize = 8;

/// PROJ `stere.cpp` CONV: inverse convergence on φ, radians.
const CONV: f64 = 1e-10;

/// Largest absolute latitude, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// UPS scale factor at the pole (PROJ `ups`).
const UPS_K0: f64 = 0.994;

/// UPS false easting and northing, metres (PROJ `ups`).
const UPS_FALSE_ORIGIN: f64 = 2_000_000.0;

/// Defining parameters of one Stereographic projection.
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
pub struct StereParams {
    /// Latitude of natural origin φ0, degrees. ±90 selects the polar aspect.
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees.
    pub lon0_deg: f64,
    /// Latitude of true scale φc, degrees. Used by the polar aspect only.
    /// When set (and not ±90), PROJ ignores `k0` on the polar aspect.
    pub lat_ts_deg: Option<f64>,
    /// Scale factor at the natural origin.
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl StereParams {
    /// Read `+proj=stere` parameters.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat0_deg = p.number_or("lat_0", 0.0)?;
        if lat0_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_0", &lat0_deg.to_string()));
        }
        let lat_ts_deg = p.number("lat_ts")?;
        if let Some(lat_ts) = lat_ts_deg
            && lat_ts.abs() > MAX_LAT_DEG
        {
            return Err(CrsParseError::invalid("lat_ts", &lat_ts.to_string()));
        }
        let k0 = p.number_any(&["k_0", "k"])?.unwrap_or(1.0);
        if k0 <= 0.0 {
            return Err(CrsParseError::invalid("k_0", &k0.to_string()));
        }
        Ok(Self {
            lat0_deg,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            lat_ts_deg,
            k0,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Read `+proj=ups` parameters. PROJ fixes every value except the
    /// hemisphere (`+south`). PROJ refuses `ups` on a sphere, a check the
    /// caller makes with the ellipsoid in hand.
    pub fn from_proj_ups(p: &ProjParams<'_>) -> Self {
        Self {
            lat0_deg: if p.has("south") {
                -MAX_LAT_DEG
            } else {
                MAX_LAT_DEG
            },
            lon0_deg: 0.0,
            lat_ts_deg: None,
            k0: UPS_K0,
            false_easting: UPS_FALSE_ORIGIN,
            false_northing: UPS_FALSE_ORIGIN,
        }
    }

    /// Append ` +proj=stere` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(out, " +proj=stere +lat_0={}", self.lat0_deg);
        if let Some(lat_ts) = self.lat_ts_deg {
            let _ = write!(out, " +lat_ts={lat_ts}");
        }
        let _ = write!(
            out,
            " +lon_0={} +k_0={} +x_0={} +y_0={}",
            self.lon0_deg, self.k0, self.false_easting, self.false_northing
        );
    }
}

/// Aspect of the projection, PROJ `stere.cpp` Mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Aspect {
    SouthPole,
    NorthPole,
    Oblique,
    Equatorial,
}

/// A Stereographic projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Stereographic {
    aspect: Aspect,
    sphere: bool,
    e: f64,
    /// Semi-major axis a, metres.
    a: f64,
    /// PROJ `akm1`, the radius scale over a.
    akm1: f64,
    /// sin and cos of the conformal latitude of φ0 (of φ0 on a sphere).
    sin_x1: f64,
    cos_x1: f64,
    /// φ0 in radians.
    phi0: f64,
    /// λ0 in radians.
    lon0: f64,
    false_easting: f64,
    false_northing: f64,
}

/// PROJ `ssfn_`: tan((π/2 + φ)/2) · ((1 − e sin φ)/(1 + e sin φ))^(e/2),
/// so 2 atan(ssfn) − π/2 is the conformal latitude (Snyder 3-1).
fn ssfn(phi: f64, sinphi: f64, e: f64) -> f64 {
    let es = e * sinphi;
    (0.5 * (FRAC_PI_2 + phi)).tan() * ((1.0 - es) / (1.0 + es)).powf(0.5 * e)
}

/// Conformal latitude χ of φ (Snyder 3-1).
fn conformal_lat(phi: f64, sinphi: f64, e: f64) -> f64 {
    2.0 * ssfn(phi, sinphi, e).atan() - FRAC_PI_2
}

impl Stereographic {
    pub fn new(p: &StereParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let aspect = if (phi0.abs() - FRAC_PI_2).abs() < EPS10 {
            if phi0 < 0.0 {
                Aspect::SouthPole
            } else {
                Aspect::NorthPole
            }
        } else if phi0.abs() > EPS10 {
            Aspect::Oblique
        } else {
            Aspect::Equatorial
        };
        let phits = p.lat_ts_deg.map_or(FRAC_PI_2, |d| d.to_radians().abs());
        let ts_at_pole = (phits - FRAC_PI_2).abs() < EPS10;
        let sphere = ell.is_sphere();
        let e = ell.e();
        let (mut sin_x1, mut cos_x1) = (0.0, 0.0);
        let akm1 = match (sphere, aspect) {
            (false, Aspect::NorthPole | Aspect::SouthPole) => {
                if ts_at_pole {
                    // GN 7-2 §3.3.2.1: ρ = 2 a k0 t / sqrt((1+e)^(1+e) (1−e)^(1−e)).
                    2.0 * p.k0 / ((1.0 + e).powf(1.0 + e) * (1.0 - e).powf(1.0 - e)).sqrt()
                } else {
                    // GN 7-2 §3.3.2.2: mc / tc.
                    let t = phits.sin();
                    let et = e * t;
                    phits.cos() / tsfn(phits, t, e) / (1.0 - et * et).sqrt()
                }
            }
            (false, Aspect::Oblique | Aspect::Equatorial) => {
                let t = phi0.sin();
                let x1 = conformal_lat(phi0, t, e);
                (sin_x1, cos_x1) = x1.sin_cos();
                let et = e * t;
                2.0 * p.k0 * phi0.cos() / (1.0 - et * et).sqrt()
            }
            (true, Aspect::Oblique | Aspect::Equatorial) => {
                if aspect == Aspect::Oblique {
                    (sin_x1, cos_x1) = phi0.sin_cos();
                }
                2.0 * p.k0
            }
            (true, Aspect::NorthPole | Aspect::SouthPole) => {
                if ts_at_pole {
                    2.0 * p.k0
                } else {
                    phits.cos() / (FRAC_PI_4 - 0.5 * phits).tan()
                }
            }
        };
        Self {
            aspect,
            sphere,
            e,
            a: ell.a,
            akm1,
            sin_x1,
            cos_x1,
            phi0,
            lon0: p.lon0_deg.to_radians(),
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dlam = wrap_pi(lam - self.lon0);
        let (x, y) = if self.sphere {
            self.forward_sphere(dlam, phi)?
        } else {
            self.forward_ellipsoid(dlam, phi)?
        };
        Ok((
            self.false_easting + self.a * x,
            self.false_northing + self.a * y,
        ))
    }

    /// PROJ `stere_e_forward`, in units of a.
    fn forward_ellipsoid(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let (sinlam, mut coslam) = lam.sin_cos();
        let mut sinphi = phi.sin();
        let mut phi = phi;
        let (x, y) = match self.aspect {
            Aspect::Oblique | Aspect::Equatorial => {
                let (sin_x, cos_x) = conformal_lat(phi, sinphi, self.e).sin_cos();
                if self.aspect == Aspect::Oblique {
                    let denom =
                        self.cos_x1 * (1.0 + self.sin_x1 * sin_x + self.cos_x1 * cos_x * coslam);
                    if denom == 0.0 {
                        return Err(DomainReason::Antipode);
                    }
                    let a = self.akm1 / denom;
                    (
                        a * cos_x,
                        a * (self.cos_x1 * sin_x - self.sin_x1 * cos_x * coslam),
                    )
                } else {
                    let denom = 1.0 + cos_x * coslam;
                    if denom == 0.0 {
                        return Err(DomainReason::Antipode);
                    }
                    let a = self.akm1 / denom;
                    (a * cos_x, a * sin_x)
                }
            }
            Aspect::NorthPole | Aspect::SouthPole => {
                if self.aspect == Aspect::SouthPole {
                    phi = -phi;
                    coslam = -coslam;
                    sinphi = -sinphi;
                }
                if (phi + FRAC_PI_2).abs() < EPS10 {
                    return Err(DomainReason::Singularity);
                }
                // GN 7-2 §3.3.2.1: ρ = akm1 · t(φ).
                let rho = if (phi - FRAC_PI_2).abs() < POLE_EXACT_TOL {
                    0.0
                } else {
                    self.akm1 * tsfn(phi, sinphi, self.e)
                };
                (rho, -rho * coslam)
            }
        };
        Ok((x * sinlam, y))
    }

    /// PROJ `stere_s_forward`, in units of a.
    fn forward_sphere(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let (sinphi, cosphi) = phi.sin_cos();
        let (sinlam, mut coslam) = lam.sin_cos();
        match self.aspect {
            Aspect::Oblique | Aspect::Equatorial => {
                let oblique = self.aspect == Aspect::Oblique;
                let d = if oblique {
                    1.0 + self.sin_x1 * sinphi + self.cos_x1 * cosphi * coslam
                } else {
                    1.0 + cosphi * coslam
                };
                if d <= EPS10 {
                    return Err(DomainReason::Antipode);
                }
                let a = self.akm1 / d;
                let y = if oblique {
                    self.cos_x1 * sinphi - self.sin_x1 * cosphi * coslam
                } else {
                    sinphi
                };
                Ok((a * cosphi * sinlam, a * y))
            }
            Aspect::NorthPole | Aspect::SouthPole => {
                let mut phi = phi;
                if self.aspect == Aspect::NorthPole {
                    coslam = -coslam;
                    phi = -phi;
                }
                if (phi - FRAC_PI_2).abs() < SPHERE_POLE_TOL {
                    return Err(DomainReason::Singularity);
                }
                let rho = self.akm1 * (FRAC_PI_4 + 0.5 * phi).tan();
                Ok((sinlam * rho, rho * coslam))
            }
        }
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.false_easting) / self.a;
        let y = (y - self.false_northing) / self.a;
        let (lam, phi) = if self.sphere {
            self.inverse_sphere(x, y)
        } else {
            self.inverse_ellipsoid(x, y)?
        };
        Ok((wrap_pi(self.lon0 + lam), phi))
    }

    /// PROJ `stere_e_inverse`, in units of a.
    fn inverse_ellipsoid(&self, mut x: f64, mut y: f64) -> Result<(f64, f64), DomainReason> {
        let rho = x.hypot(y);
        let (tp, mut phi_l, halfpi, halfe) = match self.aspect {
            Aspect::Oblique | Aspect::Equatorial => {
                let (sin_c, cos_c) = (2.0 * (rho * self.cos_x1).atan2(self.akm1)).sin_cos();
                let phi_l = if rho == 0.0 {
                    (cos_c * self.sin_x1).asin()
                } else {
                    (cos_c * self.sin_x1 + y * sin_c * self.cos_x1 / rho).asin()
                };
                x *= sin_c;
                y = rho * self.cos_x1 * cos_c - y * self.sin_x1 * sin_c;
                (
                    (0.5 * (FRAC_PI_2 + phi_l)).tan(),
                    phi_l,
                    FRAC_PI_2,
                    0.5 * self.e,
                )
            }
            Aspect::NorthPole | Aspect::SouthPole => {
                if self.aspect == Aspect::NorthPole {
                    y = -y;
                }
                let tp = -rho / self.akm1;
                (tp, FRAC_PI_2 - 2.0 * tp.atan(), -FRAC_PI_2, -0.5 * self.e)
            }
        };
        // Snyder 7-9 fixed-point iteration on φ.
        for _ in 0..MAX_ITERS {
            let es = self.e * phi_l.sin();
            let phi = 2.0 * (tp * ((1.0 + es) / (1.0 - es)).powf(halfe)).atan() - halfpi;
            if (phi_l - phi).abs() < CONV {
                let phi = if self.aspect == Aspect::SouthPole {
                    -phi
                } else {
                    phi
                };
                let lam = if x == 0.0 && y == 0.0 {
                    0.0
                } else {
                    x.atan2(y)
                };
                return Ok((lam, phi));
            }
            phi_l = phi;
        }
        Err(DomainReason::NoConvergence)
    }

    /// PROJ `stere_s_inverse`, in units of a.
    fn inverse_sphere(&self, x: f64, mut y: f64) -> (f64, f64) {
        let rh = x.hypot(y);
        let (sinc, cosc) = (2.0 * (rh / self.akm1).atan()).sin_cos();
        match self.aspect {
            Aspect::Equatorial => {
                let phi = if rh <= EPS10 {
                    0.0
                } else {
                    (y * sinc / rh).asin()
                };
                let lam = if cosc != 0.0 || x != 0.0 {
                    (x * sinc).atan2(cosc * rh)
                } else {
                    0.0
                };
                (lam, phi)
            }
            Aspect::Oblique => {
                let phi = if rh <= EPS10 {
                    self.phi0
                } else {
                    (cosc * self.sin_x1 + y * sinc * self.cos_x1 / rh).asin()
                };
                let c = cosc - self.sin_x1 * phi.sin();
                let lam = if c != 0.0 || x != 0.0 {
                    (x * sinc * self.cos_x1).atan2(c * rh)
                } else {
                    0.0
                };
                (lam, phi)
            }
            Aspect::NorthPole | Aspect::SouthPole => {
                let south = self.aspect == Aspect::SouthPole;
                if !south {
                    y = -y;
                }
                let phi = if rh <= EPS10 {
                    self.phi0
                } else if south {
                    (-cosc).asin()
                } else {
                    cosc.asin()
                };
                let lam = if x == 0.0 && y == 0.0 {
                    0.0
                } else {
                    x.atan2(y)
                };
                (lam, phi)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    /// Inverse tolerance in degrees.
    const DEG_TOL: f64 = 1e-9;

    fn build(text: &str, ell: Ellipsoid) -> Stereographic {
        let p = StereParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        Stereographic::new(&p, &ell)
    }

    fn dms(d: f64, m: f64, s: f64) -> f64 {
        d + m / 60.0 + s / 3600.0
    }

    fn check(m: &Stereographic, lon: f64, lat: f64, want: [f64; 2]) {
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

    fn assert_gn(m: &Stereographic, lon: f64, lat: f64, want: [f64; 2]) {
        let (x, y) = m.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!(
            (x - want[0]).abs() < 0.005 && (y - want[1]).abs() < 0.005,
            "got ({x}, {y}), GN 7-2 prints {want:?}"
        );
    }

    /// GN 7-2 §3.3.2.1 example, EPSG 9810: WGS 84 / UPS North, point 73°N,
    /// 44°E. GN 7-2 prints E = 3320416.75 m, N = 632668.43 m.
    #[test]
    fn gn_9810_ups_north() {
        let m = build(
            "+proj=stere +lat_0=90 +lon_0=0 +k=0.994 +x_0=2000000 +y_0=2000000",
            Ellipsoid::WGS84,
        );
        assert_gn(&m, 44.0, 73.0, [3_320_416.75, 632_668.43]);
        check(&m, 44.0, 73.0, [3_320_416.747_4, 632_668.431_3]);
        check(&m, -150.0, 85.0, [1_722_271.304_3, 2_481_040.211_7]);
    }

    /// GN 7-2 §3.3.2.2 example, EPSG 9829: WGS 84 / Australian Antarctic
    /// Polar Stereographic, point 75°S, 120°E. GN 7-2 prints
    /// E = 7255380.79 m, N = 7053389.56 m.
    #[test]
    fn gn_9829_australian_antarctic() {
        let m = build(
            "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=70 +x_0=6000000 +y_0=6000000",
            Ellipsoid::WGS84,
        );
        assert_gn(&m, 120.0, -75.0, [7_255_380.79, 7_053_389.56]);
        check(&m, 120.0, -75.0, [7_255_380.793_3, 7_053_389.560_6]);
        check(&m, 60.0, -65.0, [5_520_991.162_9, 8_716_594.109_1]);
    }

    /// GN 7-2 §3.3.2.3 example, EPSG 9830: Petrels 1972 / Terre Adelie
    /// Polar Stereographic, point 66°36'18.820"S, 140°04'17.040"E, in the
    /// variant B form PROJ writes. GN 7-2 prints E = 303169.52 m,
    /// N = 244055.72 m.
    #[test]
    fn gn_9830_terre_adelie() {
        let m = build(
            "+proj=stere +lat_0=-90 +lat_ts=-67 +lon_0=140 +x_0=300000 +y_0=-2299363.4878305923",
            Ellipsoid::INTERNATIONAL_1924,
        );
        let (lon, lat) = (dms(140.0, 4.0, 17.040), -dms(66.0, 36.0, 18.820));
        assert_gn(&m, lon, lat, [303_169.52, 244_055.72]);
        check(&m, lon, lat, [303_169.521_9, 244_055.720_5]);
    }

    /// pyproj 3.8.0 / PROJ 9.8.1, "+proj=stere +lat_0=90 +lat_ts=70
    /// +lon_0=-45 +a=6378273 +b=6356889.449" (EPSG:3411).
    #[test]
    fn nsidc_north() {
        let ell = Ellipsoid {
            a: 6_378_273.0,
            inv_f: 6_378_273.0 / (6_378_273.0 - 6_356_889.449),
        };
        let m = build(
            "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +x_0=0 +y_0=0",
            ell,
        );
        check(&m, -30.0, 75.0, [422_888.049_4, -1_578_239.686_2]);
        check(&m, 100.0, 65.0, [1_577_510.572_2, 2_252_918.579_3]);
    }

    /// pyproj, "+proj=ups +south +datum=WGS84".
    #[test]
    fn ups_south() {
        let p = StereParams::from_proj_ups(&ProjParams::tokenize("+proj=ups +south").unwrap());
        let m = Stereographic::new(&p, &Ellipsoid::WGS84);
        check(&m, 44.0, -73.0, [3_320_416.747_4, 3_367_331.568_7]);
        check(&m, -150.0, -85.0, [1_722_271.304_3, 1_518_959.788_3]);
    }

    /// pyproj, "+proj=stere +lat_0=40 +lon_0=-100 +k=0.9999 +x_0=1000
    /// +y_0=2000 +ellps=GRS80" and "+proj=stere +lat_0=0 +lon_0=10 +k=1
    /// +ellps=GRS80".
    #[test]
    fn ellipsoidal_oblique_and_equatorial() {
        let m = build(
            "+proj=stere +lat_0=40 +lon_0=-100 +k=0.9999 +x_0=1000 +y_0=2000",
            Ellipsoid::GRS80,
        );
        check(&m, -90.0, 45.0, [790_389.133_4, 604_325.093_6]);
        check(&m, -110.0, 30.0, [-970_617.194_7, -1_061_081.187_5]);
        let m = build("+proj=stere +lat_0=0 +lon_0=10 +k=1", Ellipsoid::GRS80);
        check(&m, 30.0, 20.0, [2_178_140.976_1, 2_302_414.126_0]);
        check(&m, -20.0, -10.0, [-3_390_375.947_5, -1_187_625.777_8]);
    }

    /// pyproj on R = 6371000: "+proj=stere +lat_0=40 +lon_0=-100 +k=0.9999",
    /// "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0" and
    /// "+proj=stere +lat_0=90 +lon_0=0 +k=0.994".
    #[test]
    fn spherical_aspects() {
        let r = Ellipsoid::sphere(6_371_000.0);
        let m = build("+proj=stere +lat_0=40 +lon_0=-100 +k=0.9999", r);
        check(&m, -90.0, 45.0, [786_938.100_1, 602_829.558_7]);
        check(&m, -110.0, 30.0, [-970_258.655_7, -1_065_794.116_1]);
        let m = build("+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0", r);
        check(&m, 30.0, -75.0, [815_909.695_2, 1_413_197.046_4]);
        check(&m, -100.0, -60.0, [-3_270_746.597_5, -576_720.872_2]);
        let m = build("+proj=stere +lat_0=90 +lon_0=0 +k=0.994", r);
        check(&m, 30.0, 75.0, [833_725.513_8, -1_444_054.949_4]);
        check(&m, -100.0, 60.0, [-3_342_165.074_8, 589_313.876_7]);
    }

    #[test]
    fn opposite_pole_is_singular() {
        let m = build("+proj=stere +lat_0=90 +lat_ts=70", Ellipsoid::WGS84);
        assert_eq!(m.forward(0.0, -FRAC_PI_2), Err(DomainReason::Singularity));
        let s = build("+proj=stere +lat_0=-90", Ellipsoid::sphere(6_371_000.0));
        assert_eq!(s.forward(0.0, FRAC_PI_2), Err(DomainReason::Singularity));
    }

    #[test]
    fn pole_maps_to_false_origin() {
        let m = build(
            "+proj=stere +lat_0=-90 +lat_ts=-71 +x_0=5 +y_0=7",
            Ellipsoid::WGS84,
        );
        let (x, y) = m.forward(0.3, -FRAC_PI_2).unwrap();
        assert!((x - 5.0).abs() < 1e-9 && (y - 7.0).abs() < 1e-9);
        let (_, phi) = m.inverse(5.0, 7.0).unwrap();
        assert!((phi + FRAC_PI_2).abs() < 1e-12);
    }

    #[test]
    fn parse_and_round_trip() {
        for text in [
            "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=70 +x_0=6000000 +y_0=6000000",
            "+proj=stere +lat_0=90 +lon_0=-33 +k=0.994 +x_0=2000000 +y_0=2000000",
        ] {
            let p = StereParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
            let mut out = String::new();
            p.write_proj(&mut out);
            assert_eq!(
                StereParams::from_proj(&ProjParams::tokenize(&out).unwrap()),
                Ok(p),
                "{out}"
            );
        }
        assert!(
            StereParams::from_proj(&ProjParams::tokenize("+proj=stere +lat_0=91").unwrap())
                .is_err()
        );
    }
}
