// SPDX-License-Identifier: Apache-2.0

//! Lambert Conformal Conic, PROJ `lcc`.
//!
//! Covers EPSG methods 9801 (1SP), 9802 (2SP), 9803 (2SP Belgium) and 9826
//! (West Orientated, which differs only in axis order). Follows PROJ 9.8.1
//! `src/projections/lcc.cpp` and IOGP Guidance Note 7-2 §3.2.1. PROJ
//! expresses 9803 through adjusted standard parallels, so it needs no
//! special case here.

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::conformal::{msfn, phi_from_ts, tsfn};
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// PROJ `lcc.cpp` EPS10: tolerance in radians for poles and degenerate
/// parallels.
const EPS10: f64 = 1e-10;

/// Largest absolute latitude, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Method name in parse errors.
const METHOD: &str = "lcc";

/// Defining parameters of one Lambert Conformal Conic projection. The 1SP
/// form has `lat2_deg == lat1_deg`.
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
pub struct LccParams {
    /// Latitude of the false origin φ0, degrees.
    pub lat0_deg: f64,
    /// Longitude of the false origin λ0, degrees.
    pub lon0_deg: f64,
    /// First standard parallel φ1, degrees.
    pub lat1_deg: f64,
    /// Second standard parallel φ2, degrees. Equal to φ1 for the 1SP form.
    pub lat2_deg: f64,
    /// Scale factor k0 (on φ1 for the 1SP form).
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl LccParams {
    /// Read from PROJ-string parameters. Without `+lat_2` the cone is
    /// tangent at `+lat_1`, and `+lat_0` defaults to `+lat_1`, as in PROJ.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat1 = p.number("lat_1")?;
        let lat2 = p.number("lat_2")?;
        let (lat1_deg, lat2_deg, lat0_default) = match (lat1, lat2) {
            (None, None) => {
                return Err(CrsParseError::Missing {
                    key: "lat_1",
                    method: METHOD,
                });
            }
            (l1, Some(l2)) => (l1.unwrap_or(0.0), l2, 0.0),
            (Some(l1), None) => (l1, l1, l1),
        };
        let lat0_deg = p.number_or("lat_0", lat0_default)?;
        if lat1_deg.abs() >= MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_1", &lat1_deg.to_string()));
        }
        if lat2_deg.abs() >= MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_2", &lat2_deg.to_string()));
        }
        if (lat1_deg + lat2_deg).to_radians().abs() < EPS10 {
            return Err(CrsParseError::invalid("lat_2", &lat2_deg.to_string()));
        }
        if lat0_deg.abs() > MAX_LAT_DEG {
            return Err(CrsParseError::invalid("lat_0", &lat0_deg.to_string()));
        }
        let k0 = p.number_any(&["k_0", "k"])?.unwrap_or(1.0);
        if k0 <= 0.0 {
            return Err(CrsParseError::invalid("k_0", &k0.to_string()));
        }
        Ok(Self {
            lat0_deg,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            lat1_deg,
            lat2_deg,
            k0,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append ` +proj=lcc` and every defining parameter.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=lcc +lat_0={} +lon_0={} +lat_1={} +lat_2={} +k_0={} +x_0={} +y_0={}",
            self.lat0_deg,
            self.lon0_deg,
            self.lat1_deg,
            self.lat2_deg,
            self.k0,
            self.false_easting,
            self.false_northing
        );
    }
}

/// A Lambert Conformal Conic projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct LambertConformalConic {
    /// λ0 in radians.
    lon0: f64,
    e: f64,
    sphere: bool,
    /// Cone constant n.
    n: f64,
    /// a · k0 · F, the radius scale of the cone.
    c: f64,
    /// a · k0 · ρ0, the radius at the false origin.
    rho0: f64,
    false_easting: f64,
    false_northing: f64,
}

impl LambertConformalConic {
    pub fn new(p: &LccParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let phi1 = p.lat1_deg.to_radians();
        let phi2 = p.lat2_deg.to_radians();
        let (sin1, cos1) = phi1.sin_cos();
        let secant = (phi1 - phi2).abs() >= EPS10;
        let at_pole = (phi0.abs() - FRAC_PI_2).abs() < EPS10;
        let sphere = ell.is_sphere();
        let (e, e2) = (ell.e(), ell.e2());
        let (n, c, rho0) = if sphere {
            // PROJ lcc.cpp spherical set-up, Snyder 15-3 and 15-2.
            let n = if secant {
                (cos1 / phi2.cos()).ln()
                    / ((FRAC_PI_4 + 0.5 * phi2).tan() / (FRAC_PI_4 + 0.5 * phi1).tan()).ln()
            } else {
                sin1
            };
            let c = cos1 * (FRAC_PI_4 + 0.5 * phi1).tan().powf(n) / n;
            let rho0 = if at_pole {
                0.0
            } else {
                c * (FRAC_PI_4 + 0.5 * phi0).tan().powf(-n)
            };
            (n, c, rho0)
        } else {
            // GN 7-2 §3.2.1.1, Snyder 15-8 to 15-10.
            let m1 = msfn(sin1, cos1, e2);
            let t1 = tsfn(phi1, sin1, e);
            let n = if secant {
                let sin2 = phi2.sin();
                (m1 / msfn(sin2, phi2.cos(), e2)).ln() / (t1 / tsfn(phi2, sin2, e)).ln()
            } else {
                sin1
            };
            let c = m1 * t1.powf(-n) / n;
            let rho0 = if at_pole {
                0.0
            } else {
                c * tsfn(phi0, phi0.sin(), e).powf(n)
            };
            (n, c, rho0)
        };
        let scale = ell.a * p.k0;
        Self {
            lon0: p.lon0_deg.to_radians(),
            e,
            sphere,
            n,
            c: c * scale,
            rho0: rho0 * scale,
            false_easting: p.false_easting,
            false_northing: p.false_northing,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let rho = if (phi.abs() - FRAC_PI_2).abs() < EPS10 {
            // The pole opposite the cone apex maps to infinity.
            if phi * self.n <= 0.0 {
                return Err(DomainReason::Singularity);
            }
            0.0
        } else if self.sphere {
            self.c * (FRAC_PI_4 + 0.5 * phi).tan().powf(-self.n)
        } else {
            self.c * tsfn(phi, phi.sin(), self.e).powf(self.n)
        };
        let theta = wrap_pi(lam - self.lon0) * self.n;
        let (sin_t, cos_t) = theta.sin_cos();
        Ok((
            self.false_easting + rho * sin_t,
            self.false_northing + self.rho0 - rho * cos_t,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let mut dx = x - self.false_easting;
        let mut dy = self.rho0 - (y - self.false_northing);
        let mut rho = dx.hypot(dy);
        if rho == 0.0 {
            let phi = if self.n > 0.0 { FRAC_PI_2 } else { -FRAC_PI_2 };
            return Ok((wrap_pi(self.lon0), phi));
        }
        if self.n < 0.0 {
            rho = -rho;
            dx = -dx;
            dy = -dy;
        }
        let phi = if self.sphere {
            2.0 * (self.c / rho).powf(1.0 / self.n).atan() - FRAC_PI_2
        } else {
            phi_from_ts((rho / self.c).powf(1.0 / self.n), self.e)?
        };
        let lam = dx.atan2(dy) / self.n;
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

    /// Metres per US survey foot.
    const US_FT: f64 = 1200.0 / 3937.0;

    fn build(text: &str, ell: Ellipsoid) -> LambertConformalConic {
        let p = LccParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        LambertConformalConic::new(&p, &ell)
    }

    fn check(lcc: &LambertConformalConic, lon: f64, lat: f64, want: [f64; 2]) {
        let (x, y) = lcc.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!(
            (x - want[0]).abs() < MM && (y - want[1]).abs() < MM,
            "({lon}, {lat}): got ({x}, {y}), want {want:?}"
        );
        // The references carry 0.1 mm, so the inverse starts from (x, y).
        let (lam, phi) = lcc.inverse(x, y).unwrap();
        assert!(
            (lam.to_degrees() - lon).abs() < DEG_TOL && (phi.to_degrees() - lat).abs() < DEG_TOL,
            "inverse ({lon}, {lat}): got ({}, {})",
            lam.to_degrees(),
            phi.to_degrees()
        );
    }

    fn dms(d: f64, m: f64, s: f64) -> f64 {
        d + m / 60.0 + s / 3600.0
    }

    /// GN 7-2 §3.2.1.1 example, EPSG 9801: JAD69 / Jamaica National Grid.
    /// GN 7-2 prints E = 255966.58 m, N = 142493.51 m.
    #[test]
    fn gn_9801_jamaica() {
        let lcc = build(
            "+proj=lcc +lat_1=18 +lat_0=18 +lon_0=-77 +k_0=1 +x_0=250000 +y_0=150000",
            Ellipsoid::CLARKE_1866,
        );
        let lon = -dms(76.0, 56.0, 37.26);
        let lat = dms(17.0, 55.0, 55.80);
        let (x, y) = lcc.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!((x - 255_966.58).abs() < 0.005 && (y - 142_493.51).abs() < 0.005);
        check(&lcc, lon, lat, [255_966.581_8, 142_493.511_0]);
    }

    /// GN 7-2 §3.2.1.1 example, EPSG 9802: NAD27 / Texas South Central.
    /// GN 7-2 prints E = 2963503.91 ftUS, N = 254759.80 ftUS.
    #[test]
    fn gn_9802_texas() {
        let lcc = build(
            "+proj=lcc +lat_0=27.8333333333333 +lon_0=-99 +lat_1=28.3833333333333 \
             +lat_2=30.2833333333333 +x_0=609601.218950438 +y_0=0",
            Ellipsoid::CLARKE_1866,
        );
        let (x, y) = lcc
            .forward((-96f64).to_radians(), 28.5f64.to_radians())
            .unwrap();
        assert!((x / US_FT - 2_963_503.91).abs() < 0.01);
        assert!((y / US_FT - 254_759.80).abs() < 0.01);
        check(&lcc, -96.0, 28.5, [903_277.798_9, 77_650.942_5]);
    }

    /// EPSG 9803, Belgian Lambert 72 (EPSG:31370) as PROJ writes it. GN 7-2
    /// §3.2.1.2 prints E = 251763.20, N = 153034.13 for this point. PROJ's
    /// adjusted parallels give N 4.6 cm higher, and the reference is PROJ.
    #[test]
    fn belgium_72_matches_proj() {
        let lcc = build(
            "+proj=lcc +lat_0=90 +lon_0=4.36748666666667 +lat_1=51.1666672333333 \
             +lat_2=49.8333339 +x_0=150000.013 +y_0=5400088.438",
            Ellipsoid::INTERNATIONAL_1924,
        );
        check(
            &lcc,
            dms(5.0, 48.0, 26.533),
            dms(50.0, 40.0, 46.461),
            [251_763.205_0, 153_034.175_7],
        );
        check(&lcc, 4.3, 50.8, [145_242.297_7, 165_446.068_1]);
        check(&lcc, 2.6, 51.3, [26_730.420_1, 222_536.757_0]);
        check(&lcc, 6.3, 49.6, [289_697.742_3, 33_782.385_3]);
    }

    /// pyproj 3.8.0 / PROJ 9.8.1, "+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49
    /// +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80" (Lambert-93).
    #[test]
    fn lambert_93() {
        let lcc = build(
            "+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000",
            Ellipsoid::GRS80,
        );
        check(&lcc, 2.35, 48.85, [652_301.564_8, 6_861_302.725_9]);
        check(&lcc, -4.5, 48.4, [145_709.788_9, 6_837_422.082_6]);
        check(&lcc, 7.7, 43.7, [1_078_729.648_2, 6_300_271.150_7]);
    }

    /// pyproj, "+proj=lcc +lat_1=-30 +lat_0=-30 +lon_0=25 +k_0=0.9995
    /// +x_0=1000000 +y_0=2000000 +ellps=WGS84": a southern cone, n < 0.
    #[test]
    fn southern_1sp_with_scale() {
        let lcc = build(
            "+proj=lcc +lat_1=-30 +lat_0=-30 +lon_0=25 +k_0=0.9995 +x_0=1000000 +y_0=2000000",
            Ellipsoid::WGS84,
        );
        check(&lcc, 20.0, -25.0, [493_776.987_9, 2_543_428.162_0]);
        check(&lcc, 30.0, -35.0, [1_457_832.233_9, 1_435_097.123_5]);
    }

    /// pyproj, "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=23 +lon_0=-96
    /// +R=6370997".
    #[test]
    fn sphere_secant() {
        let lcc = build(
            "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0",
            Ellipsoid::sphere(6_370_997.0),
        );
        check(&lcc, -75.0, 40.0, [1_763_412.764_0, 2_107_212.051_0]);
        check(&lcc, -120.0, 35.0, [-2_154_255.795_6, 1_635_373.287_8]);
    }

    #[test]
    fn opposite_pole_is_singular() {
        let lcc = build(
            "+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5",
            Ellipsoid::GRS80,
        );
        assert_eq!(lcc.forward(0.0, -FRAC_PI_2), Err(DomainReason::Singularity));
        assert!(lcc.forward(0.0, FRAC_PI_2).is_ok());
    }

    #[test]
    fn parse_defaults_and_round_trip() {
        let p = LccParams::from_proj(&ProjParams::tokenize("+proj=lcc +lat_1=20 +k=0.99").unwrap())
            .unwrap();
        assert_eq!((p.lat0_deg, p.lat2_deg, p.k0), (20.0, 20.0, 0.99));
        let mut out = String::new();
        p.write_proj(&mut out);
        let again = LccParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap();
        assert_eq!(again, p, "{out}");
        assert_eq!(
            LccParams::from_proj(&ProjParams::tokenize("+proj=lcc +lat_0=10").unwrap()),
            Err(CrsParseError::Missing {
                key: "lat_1",
                method: "lcc"
            })
        );
        assert!(
            LccParams::from_proj(&ProjParams::tokenize("+proj=lcc +lat_1=10 +lat_2=-10").unwrap())
                .is_err()
        );
        assert!(
            LccParams::from_proj(&ProjParams::tokenize("+proj=lcc +lat_1=90").unwrap()).is_err()
        );
    }
}
