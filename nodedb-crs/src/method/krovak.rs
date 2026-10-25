// SPDX-License-Identifier: Apache-2.0

//! Krovak and Krovak Modified, by IOGP Guidance Note 7-2 §3.1.2 and PROJ
//! `src/projections/krovak.cpp`.
//!
//! The native output follows PROJ: easting and northing (EPSG 1041 and
//! 1043, North Orientated) by default, or westing and southing with
//! `+czech`. EPSG 9819 and 1042 (south-orientated) rows carry
//! `+axis=swu`, which the transformer applies to this native output.
//!
//! PROJ fixes the ellipsoid to Bessel 1841, the cone-axis co-latitude αc to
//! 30°17'17.30311" and the pseudo standard parallel to 78°30'N. It ignores
//! `+ellps` and `+alpha` for this method, and so does this file.

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Bessel 1841 semi-major axis, metres, as PROJ `krovak.cpp` fixes it.
const BESSEL_A: f64 = 6_377_397.155;

/// Bessel 1841 e², as PROJ `krovak.cpp` fixes it.
const BESSEL_E2: f64 = 0.006_674_372_230_614;

/// Complement of αc, radians: 59°42'42.69689" (PROJ `UQ`).
const UQ: f64 = 1.042_168_563_804_74;

/// Latitude of the pseudo standard parallel φp, radians: 78°30'N (PROJ `S0`).
const S0: f64 = 1.370_083_462_815_55;

/// αc in degrees, the value PROJ uses in place of `+alpha`.
pub const KROVAK_ALPHA_DEG: f64 = 30.288_139_752_777_8;

/// Default latitude of the projection centre φc, degrees: 49°30'N.
pub const KROVAK_DEFAULT_LAT0_DEG: f64 = 49.5;

/// Longitude of origin east of Ferro, degrees: 42°30'.
const FERRO_LON0_DEG: f64 = 42.5;

/// Ferro meridian west of Greenwich, degrees: 17°40'.
const FERRO_WEST_OF_GREENWICH_DEG: f64 = 17.0 + 40.0 / 60.0;

/// Default longitude of origin λ0, degrees east of Greenwich (PROJ default).
pub const KROVAK_DEFAULT_LON0_DEG: f64 = FERRO_LON0_DEG - FERRO_WEST_OF_GREENWICH_DEG;

/// Default scale factor on the pseudo standard parallel.
pub const KROVAK_DEFAULT_K: f64 = 0.9999;

/// cos s below which PROJ maps the point to the cone apex.
const APEX_COS_EPS: f64 = 1e-12;

/// Latitude iteration step below which the inverse stops (PROJ `EPS`).
const LAT_TOLERANCE: f64 = 1e-15;

/// Latitude iterations allowed (PROJ `MAX_ITER`).
const LAT_MAX_ITERS: usize = 100;

/// Krovak Modified evaluation point X0 (southing), metres. GN 7-2 §3.1.2.3.
const MOD_X0: f64 = 1_089_000.0;

/// Krovak Modified evaluation point Y0 (westing), metres. GN 7-2 §3.1.2.3.
const MOD_Y0: f64 = 654_000.0;

/// Krovak Modified polynomial C1..C10. GN 7-2 §3.1.2.3.
const MOD_C: [f64; 10] = [
    2.946_529_277e-2,
    2.515_965_696e-2,
    1.193_845_912e-7,
    -4.668_270_147e-7,
    9.233_980_362e-12,
    1.523_735_715e-12,
    1.696_780_024e-18,
    4.408_314_235e-18,
    -8.331_083_518e-24,
    -3.689_471_323e-24,
];

/// Defining parameters of one Krovak projection.
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
pub struct KrovakParams {
    /// Latitude of the projection centre φc, degrees.
    pub lat0_deg: f64,
    /// Longitude of origin λ0, degrees.
    pub lon0_deg: f64,
    /// `+alpha`, degrees. Kept for writing only: the math uses
    /// [`KROVAK_ALPHA_DEG`], as PROJ does.
    pub alpha_deg: f64,
    /// Scale factor on the pseudo standard parallel kp.
    pub k: f64,
    /// False easting FE, metres. It offsets the westing.
    pub false_easting: f64,
    /// False northing FN, metres. It offsets the southing.
    pub false_northing: f64,
    /// PROJ `+czech`: output westing, southing instead of easting, northing.
    pub czech: bool,
    /// PROJ `mod_krovak` (EPSG 1042 / 1043): apply the GN 7-2 §3.1.2.3
    /// correction polynomial.
    pub modified: bool,
}

impl KrovakParams {
    /// Read from PROJ-string parameters. `+proj=mod_krovak` selects the
    /// modified method. Every parameter has a PROJ default.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let k = p.number_any(&["k", "k_0"])?.unwrap_or(KROVAK_DEFAULT_K);
        if k <= 0.0 {
            return Err(CrsParseError::invalid("k", &k.to_string()));
        }
        Ok(Self {
            lat0_deg: p.number_or("lat_0", KROVAK_DEFAULT_LAT0_DEG)?,
            lon0_deg: p.number_or("lon_0", KROVAK_DEFAULT_LON0_DEG)?,
            alpha_deg: p.number_or("alpha", KROVAK_ALPHA_DEG)?,
            k,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
            czech: p.has("czech"),
            modified: p.text("proj")? == Some("mod_krovak"),
        })
    }

    /// Append the PROJ form of the defining parameters.
    pub fn write_proj(&self, out: &mut String) {
        let name = if self.modified {
            "mod_krovak"
        } else {
            "krovak"
        };
        let _ = write!(
            out,
            " +proj={name} +lat_0={} +lon_0={} +alpha={} +k={} +x_0={} +y_0={}",
            self.lat0_deg,
            self.lon0_deg,
            self.alpha_deg,
            self.k,
            self.false_easting,
            self.false_northing
        );
        if self.czech {
            out.push_str(" +czech");
        }
    }
}

/// A Krovak projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Krovak {
    params: KrovakParams,
    /// λ0, radians.
    lon0: f64,
    /// e of the fixed Bessel ellipsoid.
    e: f64,
    /// GN 7-2 B (PROJ `alpha`).
    b: f64,
    /// GN 7-2 t0 (PROJ `k`).
    t0: f64,
    /// GN 7-2 n = sin φp.
    n: f64,
    /// GN 7-2 r0 in units of a.
    rho0: f64,
    /// rho0 · tan(φp/2 + π/4)^n, the forward r numerator.
    rho_num: f64,
    /// αc, radians.
    ad: f64,
}

impl Krovak {
    /// Prepare the constants. The ellipsoid is ignored: PROJ fixes Bessel.
    pub fn new(p: &KrovakParams, _ell: &Ellipsoid) -> Self {
        let e = BESSEL_E2.sqrt();
        let phi0 = p.lat0_deg.to_radians();
        let (sin_p0, cos_p0) = phi0.sin_cos();
        // GN 7-2 §3.1.2.1 projection constants.
        let b = (1.0 + BESSEL_E2 * cos_p0.powi(4) / (1.0 - BESSEL_E2)).sqrt();
        let u0 = (sin_p0 / b).asin();
        let g = ((1.0 + e * sin_p0) / (1.0 - e * sin_p0)).powf(b * e / 2.0);
        let t0 = (u0 / 2.0 + FRAC_PI_4).tan() / (phi0 / 2.0 + FRAC_PI_4).tan().powf(b) * g;
        let n0 = (1.0 - BESSEL_E2).sqrt() / (1.0 - BESSEL_E2 * sin_p0 * sin_p0);
        let n = S0.sin();
        let rho0 = p.k * n0 / S0.tan();
        Self {
            params: *p,
            lon0: p.lon0_deg.to_radians(),
            e,
            b,
            t0,
            n,
            rho0,
            rho_num: rho0 * (S0 / 2.0 + FRAC_PI_4).tan().powf(n),
            ad: FRAC_PI_2 - UQ,
        }
    }

    /// Project λ, φ radians to the native x, y metres (see the module doc).
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dlam = wrap_pi(lam - self.lon0);
        let es = self.e * phi.sin();
        let gfi = ((1.0 + es) / (1.0 - es)).powf(self.b * self.e / 2.0);
        let u =
            2.0 * ((self.t0 * (phi / 2.0 + FRAC_PI_4).tan().powf(self.b) / gfi).atan() - FRAC_PI_4);
        let dv = -dlam * self.b;
        let (sin_ad, cos_ad) = self.ad.sin_cos();
        let (sin_u, cos_u) = u.sin_cos();
        let s = clamped_asin(cos_ad * sin_u + sin_ad * cos_u * dv.cos());
        let cos_s = s.cos();
        // Southing Xp and westing Yp in units of a. The cone apex maps to 0.
        let (xp, yp) = if cos_s < APEX_COS_EPS {
            (0.0, 0.0)
        } else {
            let d = clamped_asin(cos_u * dv.sin() / cos_s);
            let eps = self.n * d;
            let rho = self.rho_num / (s / 2.0 + FRAC_PI_4).tan().powf(self.n);
            (rho * eps.cos(), rho * eps.sin())
        };
        let (mut south, mut west) = (xp * BESSEL_A, yp * BESSEL_A);
        if self.params.modified {
            let (dx, dy) = modified_correction(south - MOD_X0, west - MOD_Y0);
            south -= dx;
            west -= dy;
        }
        let south = south + self.params.false_northing;
        let west = west + self.params.false_easting;
        Ok(if self.params.czech {
            (west, south)
        } else {
            (-west, -south)
        })
    }

    /// Unproject native x, y metres to λ, φ radians.
    ///
    /// For north-orientated output PROJ 9.8.1 subtracts `+y_0` from the
    /// westing and `+x_0` from the southing, which inverts its own forward
    /// only when `+x_0 == +y_0`. This inverse follows GN 7-2 (westing − FE,
    /// southing − FN), so it agrees with PROJ on every EPSG row.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let (west, south) = if self.params.czech { (x, y) } else { (-x, -y) };
        let mut south = south - self.params.false_northing;
        let mut west = west - self.params.false_easting;
        if self.params.modified {
            let (dx, dy) = modified_correction(south - MOD_X0, west - MOD_Y0);
            south += dx;
            west += dy;
        }
        let (xp, yp) = (south / BESSEL_A, west / BESSEL_A);
        let rho = xp.hypot(yp);
        let eps = yp.atan2(xp);
        let d = eps / S0.sin();
        let s = if rho == 0.0 {
            FRAC_PI_2
        } else {
            2.0 * (((self.rho0 / rho).powf(1.0 / self.n) * (S0 / 2.0 + FRAC_PI_4).tan()).atan()
                - FRAC_PI_4)
        };
        let (sin_ad, cos_ad) = self.ad.sin_cos();
        let (sin_s, cos_s) = s.sin_cos();
        let u = clamped_asin(cos_ad * sin_s - sin_ad * cos_s * d.cos());
        let dv = clamped_asin(cos_s * d.sin() / u.cos());
        let lam = self.lon0 - dv / self.b;
        // GN 7-2 §3.1.2.1 latitude iteration, started at U'.
        let base = self.t0.powf(-1.0 / self.b) * (u / 2.0 + FRAC_PI_4).tan().powf(1.0 / self.b);
        let mut fi1 = u;
        for _ in 0..LAT_MAX_ITERS {
            let es = self.e * fi1.sin();
            let phi =
                2.0 * ((base * ((1.0 + es) / (1.0 - es)).powf(self.e / 2.0)).atan() - FRAC_PI_4);
            if (fi1 - phi).abs() < LAT_TOLERANCE {
                return Ok((lam, phi));
            }
            fi1 = phi;
        }
        Err(DomainReason::NoConvergence)
    }
}

/// asin with its argument clamped to [−1, 1], so rounding past ±1 at the
/// cone apex gives ±π/2 instead of NaN.
fn clamped_asin(v: f64) -> f64 {
    v.clamp(-1.0, 1.0).asin()
}

/// The Krovak Modified correction (dX, dY) in metres for the reduced
/// southing Xr and westing Yr. GN 7-2 §3.1.2.3.
fn modified_correction(xr: f64, yr: f64) -> (f64, f64) {
    let [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10] = MOD_C;
    let (xr2, yr2) = (xr * xr, yr * yr);
    let (xr4, yr4) = (xr2 * xr2, yr2 * yr2);
    let dx = c1 + c3 * xr - c4 * yr - 2.0 * c6 * xr * yr
        + c5 * (xr2 - yr2)
        + c7 * xr * (xr2 - 3.0 * yr2)
        - c8 * yr * (3.0 * xr2 - yr2)
        + 4.0 * c9 * xr * yr * (xr2 - yr2)
        + c10 * (xr4 + yr4 - 6.0 * xr2 * yr2);
    let dy = c2
        + c3 * yr
        + c4 * xr
        + 2.0 * c5 * xr * yr
        + c6 * (xr2 - yr2)
        + c8 * xr * (xr2 - 3.0 * yr2)
        + c7 * yr * (3.0 * xr2 - yr2)
        - 4.0 * c10 * xr * yr * (xr2 - yr2)
        + c9 * (xr4 + yr4 - 6.0 * xr2 * yr2);
    (dx, dy)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for PROJ reference values.
    const MM: f64 = 1e-3;

    /// GN 7-2 prints grid coordinates to the centimetre.
    const CM: f64 = 1e-2;

    /// Inverse tolerance, degrees.
    const DEG_TOL: f64 = 1e-9;

    /// Ferro meridian in degrees east of Greenwich.
    const FERRO_DEG: f64 = -FERRO_WEST_OF_GREENWICH_DEG;

    /// GN 7-2 §3.1.2.1 example point: 50°12'32.442"N, 16°50'59.179"E.
    const GN_LAT: f64 = 50.0 + 12.0 / 60.0 + 32.442 / 3600.0;
    const GN_LON_GREENWICH: f64 = 16.0 + 50.0 / 60.0 + 59.179 / 3600.0;

    /// EPSG:5514 S-JTSK / Krovak East North, Greenwich λ0.
    const EAST_NORTH: KrovakParams = KrovakParams {
        lat0_deg: 49.5,
        lon0_deg: 24.833_333_333_333_3,
        alpha_deg: KROVAK_ALPHA_DEG,
        k: 0.9999,
        false_easting: 0.0,
        false_northing: 0.0,
        czech: false,
        modified: false,
    };

    /// EPSG:5221 S-JTSK (Ferro) / Krovak East North: λ0 east of Ferro.
    const FERRO: KrovakParams = KrovakParams {
        lon0_deg: FERRO_LON0_DEG,
        ..EAST_NORTH
    };

    fn kro(p: KrovakParams) -> Krovak {
        Krovak::new(&p, &Ellipsoid::BESSEL_1841)
    }

    fn fwd(k: &Krovak, lon: f64, lat: f64) -> [f64; 2] {
        let (x, y) = k.forward(lon.to_radians(), lat.to_radians()).unwrap();
        [x, y]
    }

    fn inv(k: &Krovak, x: f64, y: f64) -> [f64; 2] {
        let (lam, phi) = k.inverse(x, y).unwrap();
        [lam.to_degrees(), phi.to_degrees()]
    }

    fn assert_xy(got: [f64; 2], want: [f64; 2], tol: f64) {
        assert!(
            (got[0] - want[0]).abs() < tol && (got[1] - want[1]).abs() < tol,
            "got {got:?}, want {want:?}"
        );
    }

    fn assert_ll(got: [f64; 2], want: [f64; 2]) {
        assert!(
            (got[0] - want[0]).abs() < DEG_TOL && (got[1] - want[1]).abs() < DEG_TOL,
            "got {got:?}, want {want:?}"
        );
    }

    /// GN 7-2 §3.1.2.1 and §3.1.2.2: southing 1050538.64, westing 568991.00
    /// on S-JTSK (Ferro); east-north is their negation.
    #[test]
    fn gn7_2_krovak_example() {
        let lon_ferro = GN_LON_GREENWICH - FERRO_DEG;
        let en = kro(FERRO);
        let got = fwd(&en, lon_ferro, GN_LAT);
        assert_xy(got, [-568_991.00, -1_050_538.64], CM);
        assert_ll(inv(&en, got[0], got[1]), [lon_ferro, GN_LAT]);
        let czech = kro(KrovakParams {
            czech: true,
            ..FERRO
        });
        let got = fwd(&czech, lon_ferro, GN_LAT);
        assert_xy(got, [568_991.00, 1_050_538.64], CM);
        assert_ll(inv(&czech, got[0], got[1]), [lon_ferro, GN_LAT]);
    }

    /// GN 7-2 §3.1.2.3: southing 6050538.71, westing 5568990.91.
    #[test]
    fn gn7_2_krovak_modified_example() {
        let lon_ferro = GN_LON_GREENWICH - FERRO_DEG;
        let m = kro(KrovakParams {
            alpha_deg: 30.288_139_722_222_2,
            false_easting: 5_000_000.0,
            false_northing: 5_000_000.0,
            czech: true,
            modified: true,
            ..FERRO
        });
        let got = fwd(&m, lon_ferro, GN_LAT);
        assert_xy(got, [5_568_990.91, 6_050_538.71], CM);
        assert_ll(inv(&m, got[0], got[1]), [lon_ferro, GN_LAT]);
    }

    /// PROJ 9.8.1 (pyproj 3.8.0): `+proj=krovak +lat_0=49.5
    /// +lon_0=24.8333333333333 +alpha=30.2881397527778 +k=0.9999 +x_0=0
    /// +y_0=0 +ellps=bessel`.
    const PROJ_EAST_NORTH: [([f64; 2], [f64; 2]); 4] = [
        (
            [14.42, 50.09],
            [-742_949.430_991_364, -1_042_796.966_162_558_8],
        ),
        (
            [17.25, 49.6],
            [-547_275.172_457_898_4, -1_120_838.490_282_355],
        ),
        (
            [21.26, 48.72],
            [-262_783.267_288_195_7, -1_239_668.514_791_595_3],
        ),
        (
            [12.5, 50.3],
            [-875_002.719_143_145_3, -999_278.244_061_490_6],
        ),
    ];

    #[test]
    fn matches_proj_east_north_and_czech() {
        let en = kro(EAST_NORTH);
        let czech = kro(KrovakParams {
            czech: true,
            ..EAST_NORTH
        });
        for (ll, xy) in PROJ_EAST_NORTH {
            assert_xy(fwd(&en, ll[0], ll[1]), xy, MM);
            assert_ll(inv(&en, xy[0], xy[1]), ll);
            assert_xy(fwd(&czech, ll[0], ll[1]), [-xy[0], -xy[1]], MM);
            assert_ll(inv(&czech, -xy[0], -xy[1]), ll);
        }
    }

    /// PROJ 9.8.1: the same with `+x_0=1000 +y_0=2000`. The false easting
    /// offsets the westing, so the easting moves by −x_0.
    #[test]
    fn matches_proj_false_origin() {
        let k = kro(KrovakParams {
            false_easting: 1000.0,
            false_northing: 2000.0,
            ..EAST_NORTH
        });
        let got = fwd(&k, 14.42, 50.09);
        assert_xy(got, [-743_949.430_991_364, -1_044_796.966_162_558_8], MM);
        assert_ll(inv(&k, got[0], got[1]), [14.42, 50.09]);
    }

    /// PROJ 9.8.1: `+proj=mod_krovak +lat_0=49.5 +lon_0=24.8333333333333
    /// +alpha=30.2881397222222 +k=0.9999 +x_0=5000000 +y_0=5000000
    /// +ellps=bessel` (EPSG:5516).
    #[test]
    fn matches_proj_modified_east_north() {
        let m = kro(KrovakParams {
            alpha_deg: 30.288_139_722_222_2,
            false_easting: 5_000_000.0,
            false_northing: 5_000_000.0,
            modified: true,
            ..EAST_NORTH
        });
        let refs = [
            (
                [14.42, 50.09],
                [-5_742_949.453_537_842, -6_042_796.939_892_596],
            ),
            (
                [17.25, 49.6],
                [-5_547_275.256_600_676, -6_120_838.599_420_415],
            ),
            (
                [21.26, 48.72],
                [-5_262_784.792_245_157, -6_239_670.199_519_085],
            ),
            (
                [12.5, 50.3],
                [-5_875_002.997_380_169, -5_999_278.420_538_127],
            ),
        ];
        for (ll, xy) in refs {
            assert_xy(fwd(&m, ll[0], ll[1]), xy, MM);
            assert_ll(inv(&m, xy[0], xy[1]), ll);
        }
    }

    #[test]
    fn parses_and_writes_proj() {
        let text = "+proj=krovak +axis=swu +lat_0=49.5 +lon_0=42.5 +alpha=30.2881397527778 \
                    +k=0.9999 +x_0=0 +y_0=0 +ellps=bessel +pm=-17.6666666666667";
        let p = KrovakParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        assert_eq!(p, FERRO);
        let mut out = String::new();
        p.write_proj(&mut out);
        let again = KrovakParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap();
        assert_eq!(again, p);
        let defaults =
            KrovakParams::from_proj(&ProjParams::tokenize("+proj=krovak").unwrap()).unwrap();
        assert_eq!(defaults.lat0_deg, KROVAK_DEFAULT_LAT0_DEG);
        assert!((defaults.lon0_deg - 24.833_333_333_333_3).abs() < 1e-12);
        assert_eq!(defaults.k, KROVAK_DEFAULT_K);
        let czech =
            KrovakParams::from_proj(&ProjParams::tokenize("+proj=mod_krovak +czech").unwrap())
                .unwrap();
        assert!(czech.czech && czech.modified);
        let mut out = String::new();
        czech.write_proj(&mut out);
        assert_eq!(
            KrovakParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap(),
            czech
        );
    }
}
