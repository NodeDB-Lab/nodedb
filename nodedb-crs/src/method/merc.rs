// SPDX-License-Identifier: Apache-2.0

//! Mercator, PROJ `merc`.
//!
//! Covers EPSG methods 9804 (variant A, scale factor k0) and 9805 (variant
//! B, standard parallel). Follows PROJ 9.8.1 `src/projections/merc.cpp` and
//! IOGP Guidance Note 7-2 §3.5.1, with the spherical branch PROJ uses for
//! `+R` definitions.

use core::f64::consts::FRAC_PI_2;
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::conformal::{msfn, tanphi_from_sinhpsi};
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Tolerance in radians for a latitude at a pole, where y is infinite.
const POLE_EPS: f64 = 1e-10;

/// Largest absolute standard parallel, degrees, exclusive (PROJ merc.cpp).
const MAX_LAT_TS_DEG: f64 = 90.0;

/// Defining parameters of one Mercator projection.
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
pub struct MercParams {
    /// Longitude of natural origin λ0, degrees.
    pub lon0_deg: f64,
    /// Scale factor on the equator (variant A). Ignored when `lat_ts_deg`
    /// is set, as in PROJ.
    pub k0: f64,
    /// Latitude of the standard parallel φ1, degrees (variant B). The scale
    /// factor then follows from it.
    pub lat_ts_deg: Option<f64>,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl MercParams {
    /// Read from PROJ-string parameters.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat_ts_deg = p.number("lat_ts")?;
        if let Some(lat_ts) = lat_ts_deg
            && lat_ts.abs() >= MAX_LAT_TS_DEG
        {
            return Err(CrsParseError::invalid("lat_ts", &lat_ts.to_string()));
        }
        let k0 = p.number_any(&["k_0", "k"])?.unwrap_or(1.0);
        if k0 <= 0.0 {
            return Err(CrsParseError::invalid("k_0", &k0.to_string()));
        }
        Ok(Self {
            lon0_deg: p.number_or("lon_0", 0.0)?,
            k0,
            lat_ts_deg,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append ` +proj=merc` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        out.push_str(" +proj=merc");
        let _ = match self.lat_ts_deg {
            Some(lat_ts) => write!(out, " +lat_ts={lat_ts}"),
            None => write!(out, " +k_0={}", self.k0),
        };
        let _ = write!(
            out,
            " +lon_0={} +x_0={} +y_0={}",
            self.lon0_deg, self.false_easting, self.false_northing
        );
    }
}

/// A Mercator projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Mercator {
    /// λ0 in radians.
    lon0: f64,
    e: f64,
    sphere: bool,
    /// a · k0.
    ak0: f64,
    false_easting: f64,
    false_northing: f64,
}

impl Mercator {
    pub fn new(p: &MercParams, ell: &Ellipsoid) -> Self {
        let sphere = ell.is_sphere();
        // GN 7-2 §3.5.1.2: k0 = cos φ1 / sqrt(1 − e² sin² φ1) for variant B.
        let k0 = match p.lat_ts_deg {
            Some(lat_ts) => {
                let (s, c) = lat_ts.abs().to_radians().sin_cos();
                if sphere { c } else { msfn(s, c, ell.e2()) }
            }
            None => p.k0,
        };
        Self {
            lon0: p.lon0_deg.to_radians(),
            e: ell.e(),
            sphere,
            ak0: ell.a * k0,
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        if (phi.abs() - FRAC_PI_2).abs() < POLE_EPS {
            return Err(DomainReason::Singularity);
        }
        let x = self.ak0 * wrap_pi(lam - self.lon0);
        // ψ = asinh(tan φ) − e atanh(e sin φ), PROJ merc_e_forward.
        let psi = if self.sphere {
            phi.tan().asinh()
        } else {
            let (s, c) = phi.sin_cos();
            (s / c).asinh() - self.e * (self.e * s).atanh()
        };
        Ok((self.false_easting + x, self.false_northing + self.ak0 * psi))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let sinh_psi = ((y - self.false_northing) / self.ak0).sinh();
        let phi = if self.sphere {
            sinh_psi.atan()
        } else {
            tanphi_from_sinhpsi(sinh_psi, self.e)?.atan()
        };
        let lam = (x - self.false_easting) / self.ak0;
        Ok((wrap_pi(self.lon0 + lam), phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    /// Inverse tolerance in degrees.
    const DEG_TOL: f64 = 1e-9;

    fn build(text: &str, ell: Ellipsoid) -> Mercator {
        let p = MercParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        Mercator::new(&p, &ell)
    }

    fn check(m: &Mercator, lon: f64, lat: f64, want: [f64; 2]) {
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

    /// GN 7-2 §3.5.1.1 example, EPSG 9804: Batavia / NEIEZ. GN 7-2 prints
    /// E = 5009726.58 m, N = 569150.82 m.
    #[test]
    fn gn_9804_batavia() {
        let m = build(
            "+proj=merc +lon_0=110 +k=0.997 +x_0=3900000 +y_0=900000",
            Ellipsoid::BESSEL_1841,
        );
        let (x, y) = m
            .forward(120f64.to_radians(), (-3f64).to_radians())
            .unwrap();
        assert!((x - 5_009_726.58).abs() < 0.005 && (y - 569_150.82).abs() < 0.005);
        check(&m, 120.0, -3.0, [5_009_726.583_3, 569_150.818_6]);
        check(&m, 100.0, 5.0, [2_790_273.416_7, 1_451_870.161_7]);
    }

    /// GN 7-2 §3.5.1.2 example, EPSG 9805: Pulkovo 1942 / Caspian Sea
    /// Mercator. GN 7-2 prints E = 165704.29 m, N = 5171848.07 m.
    #[test]
    fn gn_9805_caspian() {
        let m = build(
            "+proj=merc +lat_ts=42 +lon_0=51 +x_0=0 +y_0=0",
            Ellipsoid::KRASSOVSKY,
        );
        let (x, y) = m.forward(53f64.to_radians(), 53f64.to_radians()).unwrap();
        assert!((x - 165_704.29).abs() < 0.005 && (y - 5_171_848.07).abs() < 0.005);
        check(&m, 53.0, 53.0, [165_704.293_3, 5_171_848.072_9]);
        check(&m, 47.0, 38.0, [-331_408.586_6, 3_388_766.544_7]);
    }

    /// pyproj 3.8.0 / PROJ 9.8.1, "+proj=merc +lat_ts=-41 +lon_0=100
    /// +datum=WGS84" (EPSG:3994). Longitudes cross the antimeridian.
    #[test]
    fn southern_standard_parallel_across_antimeridian() {
        let m = build(
            "+proj=merc +lat_ts=-41 +lon_0=100 +x_0=0 +y_0=0",
            Ellipsoid::WGS84,
        );
        check(&m, 175.0, -41.0, [6_310_138.883_6, -3_767_131.992_2]);
        check(&m, -170.0, -30.0, [7_572_166.660_3, -2_631_835.819_7]);
    }

    /// pyproj, "+proj=merc +lon_0=0 +k=1 +datum=WGS84" (EPSG:3395).
    #[test]
    fn world_mercator() {
        let m = build("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0", Ellipsoid::WGS84);
        check(&m, 101.7117, 3.1579, [11_322_494.651_7, 349_361.795_7]);
        check(&m, -60.0, -75.0, [-6_679_169.447_6, -12_890_914.137_3]);
    }

    /// pyproj, "+proj=merc +lat_ts=20 +lon_0=10 +R=6371000".
    #[test]
    fn sphere_with_standard_parallel() {
        let m = build(
            "+proj=merc +lat_ts=20 +lon_0=10",
            Ellipsoid::sphere(6_371_000.0),
        );
        check(&m, 30.0, 60.0, [2_089_781.040_7, 7_884_339.419_9]);
        check(&m, -10.0, -40.0, [-2_089_781.040_7, -4_567_373.533_8]);
    }

    #[test]
    fn pole_is_singular() {
        let m = build("+proj=merc", Ellipsoid::WGS84);
        assert_eq!(m.forward(0.0, FRAC_PI_2), Err(DomainReason::Singularity));
    }

    #[test]
    fn parse_and_round_trip() {
        for text in [
            "+proj=merc +lat_ts=-41 +lon_0=100",
            "+proj=merc +lon_0=110 +k=0.997 +x_0=3900000",
        ] {
            let p = MercParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
            let mut out = String::new();
            p.write_proj(&mut out);
            let again = MercParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap();
            assert_eq!(again, p, "{out}");
        }
        assert!(
            MercParams::from_proj(&ProjParams::tokenize("+proj=merc +lat_ts=90").unwrap()).is_err()
        );
        assert!(MercParams::from_proj(&ProjParams::tokenize("+proj=merc +k=0").unwrap()).is_err());
    }
}
