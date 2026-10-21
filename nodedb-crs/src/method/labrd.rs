// SPDX-License-Identifier: Apache-2.0

//! Laborde Oblique Mercator (EPSG 9813), by PROJ
//! `src/projections/labrd.cpp`.
//!
//! PROJ evaluates Laborde's series expansion of the method in the
//! longitude difference and a cubic complex correction for the azimuth,
//! where IOGP Guidance Note 7-2 §3.2.4.2 gives the closed form. The two
//! agree at the GN 7-2 worked example to within a millimetre.

use core::f64::consts::FRAC_PI_4;
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Latitude step below which the inverse iteration stops (PROJ `EPS`).
const LAT_TOLERANCE: f64 = 1e-10;

/// Latitude iterations (PROJ). PROJ keeps the last iterate when the cap is
/// reached, and so does this file.
const LAT_MAX_ITERS: usize = 20;

/// Largest |φc| in degrees.
const MAX_LAT0_DEG: f64 = 90.0;

/// Defining parameters of one Laborde Oblique Mercator projection.
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
pub struct LabrdParams {
    /// Latitude of the projection centre φc, degrees. Never zero.
    pub lat0_deg: f64,
    /// Longitude of the projection centre λc, degrees.
    pub lon0_deg: f64,
    /// Azimuth of the initial line αc, degrees (PROJ `+azi`).
    pub azi_deg: f64,
    /// Scale factor on the initial line kc.
    pub k0: f64,
    /// Easting at the projection centre, metres.
    pub false_easting: f64,
    /// Northing at the projection centre, metres.
    pub false_northing: f64,
}

impl LabrdParams {
    /// Read from PROJ-string parameters. `+lat_0` is required and nonzero,
    /// as in PROJ. `+azi` defaults to 0 and `+k` to 1.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let lat0_deg = p.number("lat_0")?.ok_or(CrsParseError::Missing {
            key: "lat_0",
            method: "labrd",
        })?;
        if lat0_deg == 0.0 || lat0_deg.abs() >= MAX_LAT0_DEG {
            return Err(CrsParseError::invalid("lat_0", &lat0_deg.to_string()));
        }
        let k0 = p.number_any(&["k", "k_0"])?.unwrap_or(1.0);
        if k0 <= 0.0 {
            return Err(CrsParseError::invalid("k", &k0.to_string()));
        }
        Ok(Self {
            lat0_deg,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            azi_deg: p.number_or("azi", 0.0)?,
            k0,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append the PROJ form of the defining parameters.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=labrd +lat_0={} +lon_0={} +azi={} +k={} +x_0={} +y_0={}",
            self.lat0_deg,
            self.lon0_deg,
            self.azi_deg,
            self.k0,
            self.false_easting,
            self.false_northing
        );
    }
}

/// A Laborde Oblique Mercator projection with its constants computed.
/// Lengths are in units of the semi-major axis a.
#[derive(Debug, Clone, PartialEq)]
pub struct LabordeObliqueMercator {
    params: LabrdParams,
    /// φc, radians.
    phi0: f64,
    /// λc, radians.
    lon0: f64,
    a: f64,
    e: f64,
    /// 1 − e².
    one_es: f64,
    /// k0 · √(N R), the Gauss sphere radius.
    k_rg: f64,
    /// Centre latitude on the Gauss sphere.
    p0s: f64,
    /// Gauss sphere exponent.
    big_a: f64,
    /// Gauss sphere constant.
    big_c: f64,
    /// Azimuth correction coefficients (PROJ `Ca`..`Cd`).
    ca: f64,
    cb: f64,
    cc: f64,
    cd: f64,
}

impl LabordeObliqueMercator {
    pub fn new(p: &LabrdParams, ell: &Ellipsoid) -> Self {
        let e2 = ell.e2();
        let e = ell.e();
        let one_es = 1.0 - e2;
        let phi0 = p.lat0_deg.to_radians();
        let az = p.azi_deg.to_radians();
        let sinp = phi0.sin();
        // Radii of curvature at φc in units of a, PROJ labrd.cpp setup.
        let t = 1.0 - e2 * sinp * sinp;
        let n = 1.0 / t.sqrt();
        let r = one_es * n / t;
        let k_rg = p.k0 * (n * r).sqrt();
        let p0s = ((r / n).sqrt() * phi0.tan()).atan();
        let big_a = sinp / p0s.sin();
        let es = e * sinp;
        let big_c = 0.5 * e * big_a * ((1.0 + es) / (1.0 - es)).ln()
            - big_a * (FRAC_PI_4 + 0.5 * phi0).tan().ln()
            + (FRAC_PI_4 + 0.5 * p0s).tan().ln();
        let cb0 = 1.0 / (12.0 * k_rg * k_rg);
        let ca = (1.0 - (2.0 * az).cos()) * cb0;
        let cb = cb0 * (2.0 * az).sin();
        Self {
            params: *p,
            phi0,
            lon0: p.lon0_deg.to_radians(),
            a: ell.a,
            e,
            one_es,
            k_rg,
            p0s,
            big_a,
            big_c,
            ca,
            cb,
            cc: 3.0 * (ca * ca - cb * cb),
            cd: 6.0 * ca * cb,
        }
    }

    /// Latitude on the Gauss sphere for geodetic latitude φ.
    fn sphere_lat(&self, phi: f64) -> f64 {
        let v1 = self.big_a * (FRAC_PI_4 + 0.5 * phi).tan().ln();
        let t = self.e * phi.sin();
        let v2 = 0.5 * self.e * self.big_a * ((1.0 + t) / (1.0 - t)).ln();
        2.0 * ((v1 - v2 + self.big_c).exp().atan() - FRAC_PI_4)
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dlam = wrap_pi(lam - self.lon0);
        let ps = self.sphere_lat(phi);
        let i1 = ps - self.p0s;
        let (sinps, cosps) = ps.sin_cos();
        let (sinps2, cosps2) = (sinps * sinps, cosps * cosps);
        let a2 = self.big_a * self.big_a;
        // Series terms I1..I6, PROJ labrd_e_forward.
        let i4 = self.big_a * cosps;
        let i2 = 0.5 * self.big_a * i4 * sinps;
        let i3 = i2 * a2 * (5.0 * cosps2 - sinps2) / 12.0;
        let i6_base = i4 * a2;
        let i5 = i6_base * (cosps2 - sinps2) / 6.0;
        let i6 = i6_base * a2 * (5.0 * cosps2 * cosps2 + sinps2 * (sinps2 - 18.0 * cosps2)) / 120.0;
        let t = dlam * dlam;
        let mut x = self.k_rg * dlam * (i4 + t * (i5 + t * i6));
        let mut y = self.k_rg * (i1 + t * (i2 + t * i3));
        // Cubic azimuth correction.
        let (x2, y2) = (x * x, y * y);
        let v1 = 3.0 * x * y2 - x * x2;
        let v2 = y * y2 - 3.0 * x2 * y;
        x += self.ca * v1 + self.cb * v2;
        y += self.ca * v2 - self.cb * v1;
        Ok((
            self.a * x + self.params.false_easting,
            self.a * y + self.params.false_northing,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let mut x = (x - self.params.false_easting) / self.a;
        let mut y = (y - self.params.false_northing) / self.a;
        // Remove the azimuth correction to fifth order.
        let (x2, y2) = (x * x, y * y);
        let v1 = 3.0 * x * y2 - x * x2;
        let v2 = y * y2 - 3.0 * x2 * y;
        let v3 = x * (5.0 * y2 * y2 + x2 * (-10.0 * y2 + x2));
        let v4 = y * (5.0 * x2 * x2 + y2 * (-10.0 * x2 + y2));
        x += -self.ca * v1 - self.cb * v2 + self.cc * v3 + self.cd * v4;
        y += self.cb * v1 - self.ca * v2 - self.cd * v3 + self.cc * v4;
        let ps = self.p0s + y / self.k_rg;
        // Geodetic latitude whose sphere latitude is ps, by fixed-point
        // iteration from ps shifted by φc − p0s.
        let mut pe = ps + self.phi0 - self.p0s;
        for _ in 0..LAT_MAX_ITERS {
            let step = ps - self.sphere_lat(pe);
            pe += step;
            if step.abs() < LAT_TOLERANCE {
                break;
            }
        }
        // Series terms I7..I11, PROJ labrd_e_inverse.
        let t = self.e * pe.sin();
        let t = 1.0 - t * t;
        let re = self.one_es / (t * t.sqrt());
        let tan_ps = ps.tan();
        let t2 = tan_ps * tan_ps;
        let s = self.k_rg * self.k_rg;
        let d = re * self.params.k0 * self.k_rg;
        let i7 = tan_ps / (2.0 * d);
        let i8 = tan_ps * (5.0 + 3.0 * t2) / (24.0 * d * s);
        let d = ps.cos() * self.k_rg * self.big_a;
        let i9 = 1.0 / d;
        let d = d * s;
        let i10 = (1.0 + 2.0 * t2) / (6.0 * d);
        let i11 = (5.0 + t2 * (28.0 + 24.0 * t2)) / (120.0 * d * s);
        let x2 = x * x;
        let phi = pe + x2 * (-i7 + i8 * x2);
        let dlam = x * (i9 + x2 * (-i10 + x2 * i11));
        Ok((self.lon0 + dlam, phi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for PROJ reference values.
    const MM: f64 = 1e-3;

    /// Inverse tolerance, degrees.
    const DEG_TOL: f64 = 1e-9;

    /// EPSG:8441 Tananarive / Laborde Grid, Greenwich λc.
    const TANANARIVE: LabrdParams = LabrdParams {
        lat0_deg: -18.9,
        lon0_deg: 46.437_229_166_666_7,
        azi_deg: 18.9,
        k0: 0.9995,
        false_easting: 400_000.0,
        false_northing: 800_000.0,
    };

    fn lab() -> LabordeObliqueMercator {
        LabordeObliqueMercator::new(&TANANARIVE, &Ellipsoid::INTERNATIONAL_1924)
    }

    /// GN 7-2 §3.2.4.2: 16°11'23.280"S, 44°27'27.260"E gives E =
    /// 188333.848 m, N = 1098841.091 m.
    #[test]
    fn gn7_2_example() {
        let lat: f64 = -(16.0 + 11.0 / 60.0 + 23.280 / 3600.0);
        let lon: f64 = 44.0 + 27.0 / 60.0 + 27.260 / 3600.0;
        let l = lab();
        let (x, y) = l.forward(lon.to_radians(), lat.to_radians()).unwrap();
        assert!(
            (x - 188_333.848).abs() < MM && (y - 1_098_841.091).abs() < MM,
            "got {x}, {y}"
        );
        let (lam, phi) = l.inverse(188_333.848, 1_098_841.091).unwrap();
        // GN 7-2 prints the reverse result to 0.001", about 5e-7°.
        assert!((lam.to_degrees() - lon).abs() < 1e-7 && (phi.to_degrees() - lat).abs() < 1e-7);
    }

    /// PROJ 9.8.1 (pyproj 3.8.0): `+proj=labrd +lat_0=-18.9
    /// +lon_0=46.4372291666667 +azi=18.9 +k=0.9995 +x_0=400000 +y_0=800000
    /// +ellps=intl`. Columns: input λ, φ; PROJ forward x, y; PROJ inverse
    /// of that x, y. The PROJ inverse series misses the input by up to
    /// 7e-8°, so the inverse check compares with PROJ's own inverse.
    const PROJ_TANANARIVE: [([f64; 2], [f64; 2], [f64; 2]); 4] = [
        (
            [47.5207, -18.9137],
            [514_091.950_795_276, 798_136.475_317_906],
            [47.520_700_000_000_8, -18.913_700_000_554_54],
        ),
        (
            [49.4, -12.3],
            [722_684.617_565_835_8, 1_527_659.685_043_107_7],
            [49.400_000_018_480_554, -12.299_999_931_815_185],
        ),
        (
            [43.7, -23.35],
            [120_070.082_881_767_13, 305_117.680_992_782_47],
            [43.699_999_994_561_25, -23.350_000_062_799_186],
        ),
        (
            [47.0, -25.0],
            [456_439.021_317_990_5, 124_525.325_313_277_43],
            [46.999_999_999_833_56, -24.999_999_985_024_647],
        ),
    ];

    #[test]
    fn matches_proj() {
        let l = lab();
        for (ll, xy, back) in PROJ_TANANARIVE {
            let (x, y) = l.forward(ll[0].to_radians(), ll[1].to_radians()).unwrap();
            assert!(
                (x - xy[0]).abs() < MM && (y - xy[1]).abs() < MM,
                "{ll:?}: got {x}, {y}"
            );
            let (lam, phi) = l.inverse(xy[0], xy[1]).unwrap();
            assert!((lam.to_degrees() - back[0]).abs() < DEG_TOL, "{ll:?}");
            assert!((phi.to_degrees() - back[1]).abs() < DEG_TOL, "{ll:?}");
        }
    }

    #[test]
    fn parses_and_writes_proj() {
        let text = "+proj=labrd +lat_0=-18.9 +lon_0=46.4372291666667 +azi=18.9 +k=0.9995 \
                    +x_0=400000 +y_0=800000 +ellps=intl +units=m +no_defs";
        let p = LabrdParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        assert_eq!(p, TANANARIVE);
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            LabrdParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap(),
            p
        );
        assert!(
            LabrdParams::from_proj(&ProjParams::tokenize("+proj=labrd +lat_0=0").unwrap()).is_err()
        );
        assert_eq!(
            LabrdParams::from_proj(&ProjParams::tokenize("+proj=labrd").unwrap()),
            Err(CrsParseError::Missing {
                key: "lat_0",
                method: "labrd"
            })
        );
    }
}
