// SPDX-License-Identifier: Apache-2.0

//! New Zealand Map Grid (EPSG 9811), by PROJ `src/projections/nzmg.cpp`.
//!
//! A complex-polynomial conformal projection fitted to the International
//! 1924 ellipsoid (Stirling, Technical Circular 1973/32; IOGP Guidance Note
//! 7-2 §3.4.1). PROJ fixes a = 6378388 m, φ0 = 41°S, λ0 = 173°E, FE =
//! 2510000 m and FN = 6023150 m, ignoring the PROJ-string values, and so
//! does this file.

use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// International 1924 semi-major axis, metres, as PROJ fixes it.
const NZMG_A: f64 = 6_378_388.0;

/// Latitude of origin φ0, degrees.
pub const NZMG_LAT0_DEG: f64 = -41.0;

/// Longitude of origin λ0, degrees.
pub const NZMG_LON0_DEG: f64 = 173.0;

/// False easting, metres.
pub const NZMG_FALSE_EASTING: f64 = 2_510_000.0;

/// False northing, metres.
pub const NZMG_FALSE_NORTHING: f64 = 6_023_150.0;

/// Arc-seconds in one radian.
const ARCSEC_PER_RAD: f64 = 180.0 * 3600.0 / core::f64::consts::PI;

/// Arc-seconds in one series unit: the series take δφ in 10⁵ arc-seconds.
const ARCSEC_PER_SERIES_UNIT: f64 = 1e5;

/// Radians to series units (PROJ `RAD_TO_SEC5`).
const RAD_TO_SEC5: f64 = ARCSEC_PER_RAD / ARCSEC_PER_SERIES_UNIT;

/// Series units to radians (PROJ `SEC5_TO_RAD`).
const SEC5_TO_RAD: f64 = ARCSEC_PER_SERIES_UNIT / ARCSEC_PER_RAD;

/// Complex coefficients B1..B6 of the ζ → z polynomial (PROJ `bf`).
const BF: [(f64, f64); 6] = [
    (0.755_785_322_8, 0.0),
    (0.249_204_646, 0.003_371_507),
    (-0.001_541_739, 0.041_058_560),
    (-0.101_629_07, 0.017_276_09),
    (-0.266_234_89, -0.362_492_18),
    (-0.687_098_3, -1.165_196_7),
];

/// Coefficients of the latitude → isometric latitude series (PROJ `tpsi`).
const TPSI: [f64; 10] = [
    0.639_917_507_3,
    -0.135_879_761_3,
    0.063_294_409,
    -0.025_268_53,
    0.011_787_9,
    -0.005_516_1,
    0.002_690_6,
    -0.001_333,
    0.000_67,
    -0.000_34,
];

/// Coefficients of the isometric latitude → latitude series (PROJ `tphi`).
const TPHI: [f64; 9] = [
    1.562_701_424_3,
    0.518_540_639_8,
    -0.033_330_98,
    -0.105_290_6,
    -0.036_859_4,
    0.007_317,
    0.012_20,
    0.003_94,
    -0.001_3,
];

/// Newton step size, |Δre| + |Δim| in units of a, below which the inverse
/// stops (PROJ `EPSLN`).
const NEWTON_TOLERANCE: f64 = 1e-10;

/// Newton iterations allowed (PROJ).
const NEWTON_MAX_ITERS: usize = 20;

/// Defining parameters of the New Zealand Map Grid. [`NzmgParams::from_proj`]
/// always yields the fixed NZMG values, as PROJ does.
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
pub struct NzmgParams {
    /// Latitude of origin φ0, degrees.
    pub lat0_deg: f64,
    /// Longitude of origin λ0, degrees.
    pub lon0_deg: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl NzmgParams {
    /// The fixed NZMG parameters.
    pub const NZMG: NzmgParams = NzmgParams {
        lat0_deg: NZMG_LAT0_DEG,
        lon0_deg: NZMG_LON0_DEG,
        false_easting: NZMG_FALSE_EASTING,
        false_northing: NZMG_FALSE_NORTHING,
    };

    /// Read from PROJ-string parameters. PROJ overrides `+lat_0`, `+lon_0`,
    /// `+x_0` and `+y_0` with the NZMG values, so this returns
    /// [`NzmgParams::NZMG`] for any input.
    pub fn from_proj(_p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        Ok(Self::NZMG)
    }

    /// Append the PROJ form of the defining parameters.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=nzmg +lat_0={} +lon_0={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// The New Zealand Map Grid with its origin prepared.
#[derive(Debug, Clone, PartialEq)]
pub struct NewZealandMapGrid {
    params: NzmgParams,
    /// φ0, radians.
    lat0: f64,
    /// λ0, radians.
    lon0: f64,
}

impl NewZealandMapGrid {
    /// Prepare the origin. The ellipsoid is ignored: PROJ fixes a = 6378388 m.
    pub fn new(p: &NzmgParams, _ell: &Ellipsoid) -> Self {
        Self {
            params: *p,
            lat0: p.lat0_deg.to_radians(),
            lon0: p.lon0_deg.to_radians(),
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dphi = (phi - self.lat0) * RAD_TO_SEC5;
        // δψ = δφ Σ tpsi[k] δφ^k, Horner form.
        let dpsi = dphi * TPSI.iter().rev().fold(0.0, |acc, c| c + dphi * acc);
        let zeta = (dpsi, wrap_pi(lam - self.lon0));
        let (re, im) = zpoly(zeta);
        Ok((
            NZMG_A * im + self.params.false_easting,
            NZMG_A * re + self.params.false_northing,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let target = (
            (y - self.params.false_northing) / NZMG_A,
            (x - self.params.false_easting) / NZMG_A,
        );
        // Newton's method on f(ζ) = z, started at ζ = z.
        let mut zeta = target;
        for _ in 0..NEWTON_MAX_ITERS {
            let (f, fp) = zpoly_with_derivative(zeta);
            let resid = (f.0 - target.0, f.1 - target.1);
            let den = fp.0 * fp.0 + fp.1 * fp.1;
            let step = (
                -(resid.0 * fp.0 + resid.1 * fp.1) / den,
                -(resid.1 * fp.0 - resid.0 * fp.1) / den,
            );
            zeta = (zeta.0 + step.0, zeta.1 + step.1);
            if step.0.abs() + step.1.abs() <= NEWTON_TOLERANCE {
                let dpsi = zeta.0;
                let series = TPHI.iter().rev().fold(0.0, |acc, c| c + dpsi * acc);
                let phi = self.lat0 + dpsi * series * SEC5_TO_RAD;
                return Ok((self.lon0 + zeta.1, phi));
            }
        }
        Err(DomainReason::NoConvergence)
    }
}

/// Complex product.
fn cmul(a: (f64, f64), b: (f64, f64)) -> (f64, f64) {
    (a.0 * b.0 - a.1 * b.1, a.0 * b.1 + a.1 * b.0)
}

/// z = Σ BF[k] ζ^(k+1), Horner form (PROJ `pj_zpoly1`).
fn zpoly(zeta: (f64, f64)) -> (f64, f64) {
    let inner = BF.iter().rev().fold((0.0, 0.0), |acc, c| {
        let m = cmul(acc, zeta);
        (c.0 + m.0, c.1 + m.1)
    });
    cmul(inner, zeta)
}

/// z and dz/dζ = Σ (k+1) BF[k] ζ^k (PROJ `pj_zpolyd1`).
fn zpoly_with_derivative(zeta: (f64, f64)) -> ((f64, f64), (f64, f64)) {
    let deriv = BF.iter().enumerate().rev().fold((0.0, 0.0), |acc, (k, c)| {
        let m = cmul(acc, zeta);
        let scale = (k + 1) as f64;
        (scale * c.0 + m.0, scale * c.1 + m.1)
    });
    (zpoly(zeta), deriv)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for PROJ reference values.
    const MM: f64 = 1e-3;

    /// Inverse tolerance, degrees.
    const DEG_TOL: f64 = 1e-9;

    /// PROJ 9.8.1 (pyproj 3.8.0): `+proj=nzmg +lat_0=-41 +lon_0=173
    /// +x_0=2510000 +y_0=6023150 +ellps=intl`. GN 7-2 §3.4.1 prints no
    /// worked example for this method.
    const PROJ_NZMG: [([f64; 2], [f64; 2]); 4] = [
        (
            [174.7633, -36.8485],
            [2_667_665.932_422_868, 6_482_380.313_793_957],
        ),
        (
            [172.6362, -43.5321],
            [2_480_614.526_881_576, 5_741_827.025_504_868_5],
        ),
        (
            [168.35, -46.41],
            [2_152_383.175_675_591_5, 5_412_054.517_847_946],
        ),
        (
            [178.0, -38.6],
            [2_945_419.582_789_707_5, 6_277_359.260_284_454],
        ),
    ];

    #[test]
    fn matches_proj() {
        let g = NewZealandMapGrid::new(&NzmgParams::NZMG, &Ellipsoid::INTERNATIONAL_1924);
        for (ll, xy) in PROJ_NZMG {
            let (x, y) = g.forward(ll[0].to_radians(), ll[1].to_radians()).unwrap();
            assert!(
                (x - xy[0]).abs() < MM && (y - xy[1]).abs() < MM,
                "{ll:?}: got {x}, {y}"
            );
            let (lam, phi) = g.inverse(xy[0], xy[1]).unwrap();
            assert!((lam.to_degrees() - ll[0]).abs() < DEG_TOL, "{ll:?}");
            assert!((phi.to_degrees() - ll[1]).abs() < DEG_TOL, "{ll:?}");
        }
    }

    #[test]
    fn origin_maps_to_false_origin() {
        let g = NewZealandMapGrid::new(&NzmgParams::NZMG, &Ellipsoid::INTERNATIONAL_1924);
        let (x, y) = g
            .forward(NZMG_LON0_DEG.to_radians(), NZMG_LAT0_DEG.to_radians())
            .unwrap();
        assert!((x - NZMG_FALSE_EASTING).abs() < MM && (y - NZMG_FALSE_NORTHING).abs() < MM);
    }

    #[test]
    fn parses_and_writes_proj() {
        let text = "+proj=nzmg +lat_0=-41 +lon_0=173 +x_0=2510000 +y_0=6023150 +ellps=intl";
        let p = NzmgParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        assert_eq!(p, NzmgParams::NZMG);
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            NzmgParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap(),
            p
        );
    }
}
