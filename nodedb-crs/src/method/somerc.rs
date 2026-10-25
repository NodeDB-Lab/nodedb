// SPDX-License-Identifier: Apache-2.0

//! Swiss Oblique Mercator, by PROJ `src/projections/somerc.cpp`.
//!
//! A conformal projection of the ellipsoid onto the Gauss sphere, then an
//! oblique Mercator with the initial line at azimuth 90° through the
//! projection centre (swisstopo, "Formulas and constants for the
//! calculation of the Swiss conformal cylindrical projection", 2016). EPSG
//! lists these CRSs under method 9815, Hotine Oblique Mercator (variant B).

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Newton step on the latitude below which the inverse stops (PROJ `EPS`).
const LAT_TOLERANCE: f64 = 1e-10;

/// Newton iterations allowed (PROJ `NITER`).
const LAT_MAX_ITERS: usize = 6;

/// |sin| beyond 1 that PROJ `aasin` still rounds to ±π/2 (PROJ `ONE_TOL`).
const ASIN_ONE_TOL: f64 = 1.000_000_000_000_01;

/// cos of the oblique latitude below which the point is on the oblique pole.
const OBLIQUE_POLE_COS_EPS: f64 = 1e-15;

/// Largest |φ0| in degrees: the Gauss sphere needs cos φ0 > 0.
const MAX_LAT0_DEG: f64 = 90.0;

/// Defining parameters of one Swiss Oblique Mercator projection.
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
pub struct SomercParams {
    /// Latitude of the projection centre φ0, degrees.
    pub lat0_deg: f64,
    /// Longitude of the projection centre λ0, degrees.
    pub lon0_deg: f64,
    /// Scale factor at the projection centre.
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl SomercParams {
    /// Read from PROJ-string parameters. Every parameter has a PROJ
    /// default: φ0 = λ0 = 0, k0 = 1, no false origin.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat0_deg = p.number_or("lat_0", 0.0)?;
        if lat0_deg.abs() >= MAX_LAT0_DEG {
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

    /// Append the PROJ form of the defining parameters.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=somerc +lat_0={} +lon_0={} +k_0={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.k0, self.false_easting, self.false_northing
        );
    }
}

/// A Swiss Oblique Mercator projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct SwissObliqueMercator {
    params: SomercParams,
    /// λ0, radians.
    lon0: f64,
    e: f64,
    /// 1 / (1 − e²).
    rone_es: f64,
    /// e / 2.
    hlf_e: f64,
    /// Gauss sphere exponent α (PROJ `c`).
    c: f64,
    /// Gauss sphere constant K.
    k: f64,
    /// k0 · R / a, the sphere radius in units of a (PROJ `kR`).
    k_r: f64,
    /// sin and cos of the centre latitude on the sphere b0.
    sinp0: f64,
    cosp0: f64,
    /// Semi-major axis, metres.
    a: f64,
}

impl SwissObliqueMercator {
    pub fn new(p: &SomercParams, ell: &Ellipsoid) -> Self {
        let e2 = ell.e2();
        let e = ell.e();
        let hlf_e = 0.5 * e;
        let phi0 = p.lat0_deg.to_radians();
        let (sin_p0, cos_p0) = phi0.sin_cos();
        let rone_es = 1.0 / (1.0 - e2);
        // Gauss sphere constants, PROJ somerc.cpp setup.
        let c = (1.0 + e2 * cos_p0.powi(4) * rone_es).sqrt();
        let sinp0 = sin_p0 / c;
        let phip0 = sinp0.asin();
        let es0 = e * sin_p0;
        let k = (FRAC_PI_4 + 0.5 * phip0).tan().ln()
            - c * ((FRAC_PI_4 + 0.5 * phi0).tan().ln() - hlf_e * ((1.0 + es0) / (1.0 - es0)).ln());
        let k_r = p.k0 * (1.0 - e2).sqrt() / (1.0 - es0 * es0);
        Self {
            params: *p,
            lon0: p.lon0_deg.to_radians(),
            e,
            rone_es,
            hlf_e,
            c,
            k,
            k_r,
            sinp0,
            cosp0: phip0.cos(),
            a: ell.a,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dlam = wrap_pi(lam - self.lon0);
        let sp = self.e * phi.sin();
        // Latitude and longitude on the Gauss sphere.
        let phip = 2.0
            * (self.c
                * ((FRAC_PI_4 + 0.5 * phi).tan().ln()
                    - self.hlf_e * ((1.0 + sp) / (1.0 - sp)).ln())
                + self.k)
                .exp()
                .atan()
            - FRAC_PI_2;
        let lamp = self.c * dlam;
        let cp = phip.cos();
        // Rotation to the oblique sphere.
        let phipp = aasin(self.cosp0 * phip.sin() - self.sinp0 * cp * lamp.cos())?;
        let cos_phipp = phipp.cos();
        if cos_phipp < OBLIQUE_POLE_COS_EPS {
            return Err(DomainReason::Singularity);
        }
        let lampp = aasin(cp * lamp.sin() / cos_phipp)?;
        let x = self.k_r * lampp;
        let y = self.k_r * (FRAC_PI_4 + 0.5 * phipp).tan().ln();
        Ok((
            self.a * x + self.params.false_easting,
            self.a * y + self.params.false_northing,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        let phipp = 2.0 * ((y / self.k_r).exp().atan() - FRAC_PI_4);
        let lampp = x / self.k_r;
        let cp = phipp.cos();
        let mut phip = aasin(self.cosp0 * phipp.sin() + self.sinp0 * cp * lampp.cos())?;
        let lamp = aasin(cp * lampp.sin() / phip.cos())?;
        let con = (self.k - (FRAC_PI_4 + 0.5 * phip).tan().ln()) / self.c;
        // Newton iteration from the sphere latitude to the ellipsoid latitude.
        for _ in 0..LAT_MAX_ITERS {
            let esp = self.e * phip.sin();
            let delp = (con + (FRAC_PI_4 + 0.5 * phip).tan().ln()
                - self.hlf_e * ((1.0 + esp) / (1.0 - esp)).ln())
                * (1.0 - esp * esp)
                * phip.cos()
                * self.rone_es;
            phip -= delp;
            if delp.abs() < LAT_TOLERANCE {
                return Ok((self.lon0 + lamp / self.c, phip));
            }
        }
        Err(DomainReason::NoConvergence)
    }
}

/// asin that rounds an argument just past ±1 to ±π/2 and refuses one
/// further out, as PROJ `aasin` does.
fn aasin(v: f64) -> Result<f64, DomainReason> {
    if v.abs() < 1.0 {
        Ok(v.asin())
    } else if v.abs() <= ASIN_ONE_TOL {
        Ok(FRAC_PI_2.copysign(v))
    } else {
        Err(DomainReason::OutsideProjection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for PROJ reference values.
    const MM: f64 = 1e-3;

    /// Inverse tolerance, degrees.
    const DEG_TOL: f64 = 1e-9;

    /// EPSG:2056 CH1903+ / LV95.
    const LV95: SomercParams = SomercParams {
        lat0_deg: 46.952_405_555_555_6,
        lon0_deg: 7.439_583_333_333_33,
        k0: 1.0,
        false_easting: 2_600_000.0,
        false_northing: 1_200_000.0,
    };

    /// EPSG:23700 HD72 / EOV.
    const EOV: SomercParams = SomercParams {
        lat0_deg: 47.144_393_722_222_2,
        lon0_deg: 19.048_571_777_777_8,
        k0: 0.99993,
        false_easting: 650_000.0,
        false_northing: 200_000.0,
    };

    /// PROJ 9.8.1 (pyproj 3.8.0): `+proj=somerc +lat_0=46.9524055555556
    /// +lon_0=7.43958333333333 +k_0=1 +x_0=2600000 +y_0=1200000
    /// +ellps=bessel`.
    const PROJ_LV95: [([f64; 2], [f64; 2]); 4] = [
        (
            [8.5417, 47.3769],
            [2_683_220.754_828_522_4, 1_247_772.848_570_866_7],
        ),
        (
            [6.1432, 46.2044],
            [2_499_956.536_283_736, 1_117_683.860_689_401_9],
        ),
        (
            [9.8, 46.5],
            [2_781_147.794_173_625, 1_152_440.159_175_334_7],
        ),
        (
            [7.0, 45.9],
            [2_565_885.713_119_430_5, 1_083_117.132_843_990_6],
        ),
    ];

    /// PROJ 9.8.1: `+proj=somerc +lat_0=47.1443937222222
    /// +lon_0=19.0485717777778 +k_0=0.99993 +x_0=650000 +y_0=200000
    /// +ellps=GRS67`.
    const PROJ_EOV: [([f64; 2], [f64; 2]); 3] = [
        (
            [19.0402, 47.4979],
            [649_369.245_120_057_7, 239_299.475_175_236_32],
        ),
        (
            [21.6, 46.3],
            [846_544.024_531_169_1, 109_343.850_162_499_37],
        ),
        ([16.5, 47.7], [458_749.226_525_301_1, 264_886.966_444_310_9]),
    ];

    fn check(p: SomercParams, ell: Ellipsoid, refs: &[([f64; 2], [f64; 2])]) {
        let s = SwissObliqueMercator::new(&p, &ell);
        for (ll, xy) in refs {
            let (x, y) = s.forward(ll[0].to_radians(), ll[1].to_radians()).unwrap();
            assert!(
                (x - xy[0]).abs() < MM && (y - xy[1]).abs() < MM,
                "{ll:?}: got {x}, {y}"
            );
            let (lam, phi) = s.inverse(xy[0], xy[1]).unwrap();
            assert!((lam.to_degrees() - ll[0]).abs() < DEG_TOL, "{ll:?}");
            assert!((phi.to_degrees() - ll[1]).abs() < DEG_TOL, "{ll:?}");
        }
    }

    #[test]
    fn matches_proj_lv95() {
        check(LV95, Ellipsoid::BESSEL_1841, &PROJ_LV95);
    }

    #[test]
    fn matches_proj_eov() {
        check(EOV, Ellipsoid::GRS67, &PROJ_EOV);
    }

    /// The projection centre maps to the false origin.
    #[test]
    fn centre_maps_to_false_origin() {
        let s = SwissObliqueMercator::new(&LV95, &Ellipsoid::BESSEL_1841);
        let (x, y) = s
            .forward(LV95.lon0_deg.to_radians(), LV95.lat0_deg.to_radians())
            .unwrap();
        assert!((x - LV95.false_easting).abs() < MM && (y - LV95.false_northing).abs() < MM);
    }

    #[test]
    fn parses_and_writes_proj() {
        let text = "+proj=somerc +lat_0=46.9524055555556 +lon_0=7.43958333333333 +k_0=1 \
                    +x_0=2600000 +y_0=1200000 +ellps=bessel +units=m +no_defs";
        let p = SomercParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        assert_eq!(p, LV95);
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            SomercParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap(),
            p
        );
        assert!(
            SomercParams::from_proj(&ProjParams::tokenize("+proj=somerc +lat_0=90").unwrap())
                .is_err()
        );
    }
}
