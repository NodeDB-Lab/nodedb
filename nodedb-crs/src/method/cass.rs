// SPDX-License-Identifier: Apache-2.0

//! Cassini-Soldner, EPSG method 9806 (and 9833 hyperbolic), PROJ `cass`.
//!
//! Follows PROJ 9.8.1 `src/projections/cass.cpp`: the ellipsoidal series
//! of Snyder 1987 pp. 92–95 (eqs. 13-5 to 13-11; IOGP Guidance Note 7-2
//! §3.2.2), refined on inverse by PROJ's 2-D Newton solver
//! (`src/generic_inverse.cpp`), and the exact spherical formulas.

use core::f64::consts::{FRAC_PI_2, PI};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use super::meridian::MeridianArc;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// Series coefficients 1/6, 1/120, 1/24, 1/3, 1/15 (Snyder eqs. 13-7,
/// 13-8, 13-10, 13-11).
const C1: f64 = 1.0 / 6.0;
const C2: f64 = 1.0 / 120.0;
const C3: f64 = 1.0 / 24.0;
const C4: f64 = 1.0 / 3.0;
const C5: f64 = 1.0 / 15.0;

/// Largest absolute latitude of origin, degrees.
const MAX_LAT_DEG: f64 = 90.0;

/// Newton iterations of PROJ `pj_generic_inverse_2d`.
const NEWTON_MAX_ITERS: usize = 15;

/// Residual in units of a at which the Newton inverse stops, PROJ
/// `cass_e_inverse` `deltaXYTolerance`.
const NEWTON_TOLERANCE: f64 = 1e-12;

/// Residual above which the Jacobian is recomputed, PROJ
/// `pj_generic_inverse_2d`.
const JACOBIAN_REFRESH: f64 = 1e-6;

/// Finite-difference step for the Jacobian, radians.
const JACOBIAN_STEP: f64 = 1e-6;

/// Largest Newton correction per step, radians.
const MAX_STEP: f64 = 0.3;

/// Defining parameters of one Cassini-Soldner projection.
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
pub struct CassParams {
    /// Latitude of natural origin φ0, degrees (`+lat_0`).
    pub lat0_deg: f64,
    /// Longitude of natural origin λ0, degrees (`+lon_0`).
    pub lon0_deg: f64,
    /// False easting, metres (`+x_0`).
    pub false_easting: f64,
    /// False northing, metres (`+y_0`).
    pub false_northing: f64,
    /// EPSG 9833 Hyperbolic Cassini-Soldner (`+hyperbolic`), the
    /// Vanua Levu Grid variant. Ellipsoid only.
    pub hyperbolic: bool,
}

impl CassParams {
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
            hyperbolic: p.has("hyperbolic"),
        })
    }

    /// Append the defining parameters as PROJ-string tokens.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=cass +lat_0={} +lon_0={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.false_easting, self.false_northing
        );
        if self.hyperbolic {
            out.push_str(" +hyperbolic");
        }
    }
}

/// Cassini-Soldner with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct Cassini {
    params: CassParams,
    a: f64,
    es: f64,
    lon0: f64,
    phi0: f64,
    /// Meridian arc; `None` on a sphere.
    arc: Option<MeridianArc>,
    /// Arc to the origin latitude, units of a.
    m0: f64,
}

impl Cassini {
    pub fn new(p: &CassParams, ell: &Ellipsoid) -> Self {
        let phi0 = p.lat0_deg.to_radians();
        let es = ell.e2();
        let (arc, m0) = if ell.is_sphere() {
            (None, 0.0)
        } else {
            let arc = MeridianArc::new(es);
            let m0 = arc.arc(phi0, phi0.sin(), phi0.cos());
            (Some(arc), m0)
        };
        Self {
            params: *p,
            a: ell.a,
            es,
            lon0: p.lon0_deg.to_radians(),
            phi0,
            arc,
            m0,
        }
    }

    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let lam = wrap_pi(lam - self.lon0);
        let (x, y) = match &self.arc {
            Some(arc) => self.e_forward(arc, lam, phi),
            None => (
                (phi.cos() * lam.sin()).asin(),
                phi.tan().atan2(lam.cos()) - self.phi0,
            ),
        };
        Ok((
            self.params.false_easting + self.a * x,
            self.params.false_northing + self.a * y,
        ))
    }

    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        let (lam, phi) = match &self.arc {
            Some(arc) => self.e_inverse(arc, x, y)?,
            None => {
                let dd = y + self.phi0;
                ((x.tan()).atan2(dd.cos()), (dd.sin() * x.cos()).asin())
            }
        };
        Ok((wrap_pi(lam + self.lon0), phi))
    }

    /// Ellipsoidal series, PROJ `cass_e_forward` (Snyder eqs. 13-5, 13-6).
    fn e_forward(&self, arc: &MeridianArc, lam: f64, phi: f64) -> (f64, f64) {
        let (sinphi, cosphi) = phi.sin_cos();
        let m = arc.arc(phi, sinphi, cosphi);
        let nu_square = 1.0 / (1.0 - self.es * sinphi * sinphi);
        let nu = nu_square.sqrt();
        let tanphi = phi.tan();
        let t = tanphi * tanphi;
        let a = lam * cosphi;
        let c = self.es * (cosphi * cosphi) / (1.0 - self.es);
        let a2 = a * a;
        let x = nu * a * (1.0 - a2 * t * (C1 + (8.0 - t + 8.0 * c) * a2 * C2));
        let mut y = m - self.m0 + nu * tanphi * a2 * (0.5 + (5.0 - t + 6.0 * c) * a2 * C3);
        if self.params.hyperbolic {
            // GN 7-2 §3.2.2 hyperbolic correction X − X³ / (6 ρ ν).
            let rho = nu_square * (1.0 - self.es) * nu;
            y -= y * y * y / (6.0 * rho * nu);
        }
        (x, y)
    }

    /// Ellipsoidal inverse, PROJ `cass_e_inverse`: Snyder eqs. 13-9 to
    /// 13-11 as the start of PROJ `pj_generic_inverse_2d`.
    fn e_inverse(&self, arc: &MeridianArc, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let phi1 = arc.inverse(self.m0 + y)?;
        let tanphi1 = phi1.tan();
        let t1 = tanphi1 * tanphi1;
        let sinphi1 = phi1.sin();
        let nu1_square = 1.0 / (1.0 - self.es * sinphi1 * sinphi1);
        let nu1 = nu1_square.sqrt();
        let rho1 = nu1_square * (1.0 - self.es) * nu1;
        let d = x / nu1;
        let d2 = d * d;
        let phi = phi1 - (nu1 * tanphi1 / rho1) * d2 * (0.5 - (1.0 + 3.0 * t1) * d2 * C3);
        let lam = d * (1.0 + t1 * d2 * (-C4 + (1.0 + 3.0 * t1) * d2 * C5)) / phi1.cos();
        self.newton(arc, x, y, lam, phi)
    }

    /// 2-D Newton refinement with a finite-difference Jacobian, PROJ
    /// `pj_generic_inverse_2d`.
    fn newton(
        &self,
        arc: &MeridianArc,
        x: f64,
        y: f64,
        lam: f64,
        phi: f64,
    ) -> Result<(f64, f64), DomainReason> {
        let (mut lam, mut phi) = (lam, phi);
        let (mut d_lam_x, mut d_lam_y, mut d_phi_x, mut d_phi_y) = (0.0, 0.0, 0.0, 0.0);
        for i in 0..NEWTON_MAX_ITERS {
            let (ax, ay) = self.e_forward(arc, lam, phi);
            let dx = ax - x;
            let dy = ay - y;
            if dx.abs() < NEWTON_TOLERANCE && dy.abs() < NEWTON_TOLERANCE {
                return Ok((lam, phi));
            }
            if i == 0 || dx.abs() > JACOBIAN_REFRESH || dy.abs() > JACOBIAN_REFRESH {
                let h_lam = if lam > 0.0 {
                    -JACOBIAN_STEP
                } else {
                    JACOBIAN_STEP
                };
                let (x2, y2) = self.e_forward(arc, lam + h_lam, phi);
                let dx_lam = (x2 - ax) / h_lam;
                let dy_lam = (y2 - ay) / h_lam;
                let h_phi = if phi > 0.0 {
                    -JACOBIAN_STEP
                } else {
                    JACOBIAN_STEP
                };
                let (x2, y2) = self.e_forward(arc, lam, phi + h_phi);
                let dx_phi = (x2 - ax) / h_phi;
                let dy_phi = (y2 - ay) / h_phi;
                let det = dx_lam * dy_phi - dx_phi * dy_lam;
                if det != 0.0 {
                    d_lam_x = dy_phi / det;
                    d_lam_y = -dx_phi / det;
                    d_phi_x = -dy_lam / det;
                    d_phi_y = dx_lam / det;
                }
            }
            let step_lam = (dx * d_lam_x + dy * d_lam_y).clamp(-MAX_STEP, MAX_STEP);
            lam = (lam - step_lam).clamp(-PI, PI);
            let step_phi = (dx * d_phi_x + dy * d_phi_y).clamp(-MAX_STEP, MAX_STEP);
            phi = (phi - step_phi).clamp(-FRAC_PI_2, FRAC_PI_2);
        }
        Err(DomainReason::NoConvergence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MM: f64 = 1e-3;
    const DEG_TOL: f64 = 1e-9;

    fn params(lat0_deg: f64, lon0_deg: f64, false_easting: f64, false_northing: f64) -> CassParams {
        CassParams {
            lat0_deg,
            lon0_deg,
            false_easting,
            false_northing,
            hyperbolic: false,
        }
    }

    fn check(p: &CassParams, ell: &Ellipsoid, pts: &[[f64; 4]]) {
        let cass = Cassini::new(p, ell);
        for [lon, lat, x, y] in pts.iter().copied() {
            let (gx, gy) = cass.forward(lon.to_radians(), lat.to_radians()).unwrap();
            assert!(
                (gx - x).abs() < MM && (gy - y).abs() < MM,
                "{lon} {lat}: {gx} {gy}"
            );
            let (glam, gphi) = cass.inverse(x, y).unwrap();
            assert!(
                (glam.to_degrees() - lon).abs() < DEG_TOL
                    && (gphi.to_degrees() - lat).abs() < DEG_TOL,
                "{x} {y}: {} {}",
                glam.to_degrees(),
                gphi.to_degrees()
            );
        }
    }

    /// GN 7-2 §3.2.2 example, Trinidad 1903 / Trinidad Grid, in Clarke's
    /// links: Clarke 1858 a = 31 706 587.88 links, 1/f = 294.2606764,
    /// φ0 = 10°26′30″N, λ0 = 61°20′00″W, FE = 430 000 links,
    /// FN = 325 000 links. φ = 10°N, λ = 62°W → E = 66 644.94 links,
    /// N = 82 536.22 links.
    #[test]
    fn gn7_2_trinidad() {
        let lat0 = 10.0 + 26.0 / 60.0 + 30.0 / 3600.0;
        let lon0 = -(61.0 + 20.0 / 60.0);
        let ell = Ellipsoid {
            a: 31_706_587.88,
            inv_f: 294.260_676_4,
        };
        let cass = Cassini::new(&params(lat0, lon0, 430_000.0, 325_000.0), &ell);
        let (x, y) = cass
            .forward((-62f64).to_radians(), 10f64.to_radians())
            .unwrap();
        assert!(
            (x - 66_644.94).abs() < 0.005 && (y - 82_536.22).abs() < 0.005,
            "{x} {y}"
        );
        let (lam, phi) = cass.inverse(66_644.94, 82_536.22).unwrap();
        assert!((lam.to_degrees() + 62.0).abs() < 1e-7 && (phi.to_degrees() - 10.0).abs() < 1e-7);
    }

    /// PROJ 9.8.1 via pyproj 3.8.0 for the GDM2000 state grids, EPSG
    /// 3377–3385, each `+proj=cass +lat_0=… +lon_0=… +x_0=… +y_0=…
    /// +ellps=GRS80` as in the EPSG table. Rows: (lat0, lon0, x0, y0,
    /// lon, lat, x, y).
    #[test]
    fn gdm2000_state_grids() {
        for row in GDM2000 {
            let [lat0, lon0, x0, y0, lon, lat, x, y] = *row;
            check(
                &params(lat0, lon0, x0, y0),
                &Ellipsoid::GRS80,
                &[[lon, lat, x, y]],
            );
        }
    }

    const GDM2000: &[[f64; 8]] = &[
        // EPSG:3377 GDM2000 / Johor Grid
        [
            2.12167974444444,
            103.427936236111,
            -14810.562,
            8758.32,
            103.52,
            2.08,
            -4568.778015175592,
            4149.848894692858,
        ],
        // EPSG:3377 GDM2000 / Johor Grid
        [
            2.12167974444444,
            103.427936236111,
            -14810.562,
            8758.32,
            102.54,
            2.85,
            -113533.72065212582,
            89331.37648655649,
        ],
        // EPSG:3378 GDM2000 / Sembilan and Melaka Grid
        [
            2.68234763611111,
            101.974905041667,
            3673.785,
            -4240.573,
            102.205,
            2.655,
            29260.52717390083,
            -7262.203986713459,
        ],
        // EPSG:3378 GDM2000 / Sembilan and Melaka Grid
        [
            2.68234763611111,
            101.974905041667,
            3673.785,
            -4240.573,
            101.8,
            3.18,
            -15766.774675108012,
            50790.071188531205,
        ],
        // EPSG:3379 GDM2000 / Pahang Grid
        [
            3.76938808888889,
            102.368298983333,
            -7368.228,
            6485.858,
            102.5,
            3.615,
            7263.685344680928,
            -10585.143806558506,
        ],
        // EPSG:3379 GDM2000 / Pahang Grid
        [
            3.76938808888889,
            102.368298983333,
            -7368.228,
            6485.858,
            101.43,
            4.68,
            -111473.23475130659,
            107251.17740146085,
        ],
        // EPSG:3380 GDM2000 / Selangor Grid
        [
            3.68464905,
            101.389107913889,
            -34836.161,
            56464.049,
            101.365,
            3.205,
            -37515.672158265,
            3425.3081720405753,
        ],
        // EPSG:3380 GDM2000 / Selangor Grid
        [
            3.68464905,
            101.389107913889,
            -34836.161,
            56464.049,
            100.86,
            3.77,
            -93609.57369879901,
            65919.91306226248,
        ],
        // EPSG:3381 GDM2000 / Terengganu Grid
        [
            4.9762852,
            103.070275625,
            19594.245,
            3371.895,
            103.05,
            4.895,
            17345.350054870152,
            -5616.791316690804,
        ],
        // EPSG:3381 GDM2000 / Terengganu Grid
        [
            4.9762852,
            103.070275625,
            19594.245,
            3371.895,
            102.48,
            5.8,
            -45780.77549408487,
            94495.6759236236,
        ],
        // EPSG:3382 GDM2000 / Pinang Grid
        [
            5.42151754166667,
            100.344376963889,
            -23.414,
            62.283,
            100.34,
            5.355,
            -508.54300377559804,
            -7293.495598518345,
        ],
        // EPSG:3382 GDM2000 / Pinang Grid
        [
            5.42151754166667,
            100.344376963889,
            -23.414,
            62.283,
            100.22,
            5.49,
            -13805.905549187662,
            7636.799863656235,
        ],
        // EPSG:3383 GDM2000 / Kedah and Perlis Grid
        [
            5.96467271388889,
            100.636371111111,
            0.0,
            0.0,
            100.355,
            5.9,
            -31157.270629031478,
            -7144.041483735553,
        ],
        // EPSG:3383 GDM2000 / Kedah and Perlis Grid
        [
            5.96467271388889,
            100.636371111111,
            0.0,
            0.0,
            99.69,
            6.62,
            -104651.73329365306,
            72570.73845025174,
        ],
        // EPSG:3384 GDM2000 / Perak Grid
        [
            4.85906302222222,
            100.815410586111,
            -1.769,
            133454.779,
            101.035,
            4.79,
            24358.006918962714,
            125821.54086360031,
        ],
        // EPSG:3384 GDM2000 / Perak Grid
        [
            4.85906302222222,
            100.815410586111,
            -1.769,
            133454.779,
            100.17,
            5.82,
            -71480.64811689194,
            239759.78035247474,
        ],
        // EPSG:3385 GDM2000 / Kelantan Grid
        [
            5.97254365833333,
            102.295241669444,
            13227.851,
            8739.894,
            102.0,
            5.415,
            -19492.60336433349,
            -52908.23524820403,
        ],
        // EPSG:3385 GDM2000 / Kelantan Grid
        [
            5.97254365833333,
            102.295241669444,
            13227.851,
            8739.894,
            101.43,
            6.19,
            -82532.54050499732,
            32865.65315694644,
        ],
    ];

    /// PROJ 9.8.1 via pyproj 3.8.0, `+proj=cass +lat_0=48 +lon_0=16 +R=6370997`.
    #[test]
    fn sphere() {
        check(
            &params(48.0, 16.0, 0.0, 0.0),
            &Ellipsoid::sphere(6_370_997.0),
            SPHERE_PTS,
        );
    }

    const SPHERE_PTS: &[[f64; 4]] = &[
        [17.0, 49.0, 72948.29160438642, 111675.35102138421],
        [14.0, 46.0, -154468.66333483803, -220449.84087588376],
        [16.0, 48.0, 0.0, 0.0],
    ];

    /// EPSG 9833, PROJ 9.8.1 via pyproj 3.8.0, EPSG:3139 Vanua Levu Grid
    /// in metres instead of links: `+proj=cass +hyperbolic +lat_0=-16.25
    /// +lon_0=179.333333333333 +x_0=251727.9155424 +y_0=334519.953768
    /// +a=6378306.3696 +b=6356571.996`.
    #[test]
    fn hyperbolic_vanua_levu() {
        let mut p = params(
            -16.25,
            179.333_333_333_333,
            251_727.915_542_4,
            334_519.953_768,
        );
        p.hyperbolic = true;
        let a = 6_378_306.369_6;
        let ell = Ellipsoid {
            a,
            inv_f: a / (a - 6_356_571.996),
        };
        check(&p, &ell, HYPERBOLIC_PTS);
    }

    const HYPERBOLIC_PTS: &[[f64; 4]] = &[
        [179.3, -16.4, 248167.17721095833, 317921.5387187793],
        [178.9, -16.6, 205485.89162969342, 295740.8647022293],
        [-179.9, -16.2, 333707.8206480166, 339899.5818423088],
    ];

    #[test]
    fn parses_and_writes() {
        let p = ProjParams::tokenize("+proj=cass +lat_0=2.12167974444444 +lon_0=103.427936236111 +x_0=-14810.562 +y_0=8758.32").unwrap();
        let c = CassParams::from_proj(&p).unwrap();
        assert!(!c.hyperbolic);
        let mut s = String::new();
        c.write_proj(&mut s);
        assert_eq!(
            s,
            " +proj=cass +lat_0=2.12167974444444 +lon_0=103.427936236111 +x_0=-14810.562 +y_0=8758.32"
        );
    }
}
