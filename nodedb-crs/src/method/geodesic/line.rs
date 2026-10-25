// SPDX-License-Identifier: Apache-2.0

//! The direct geodesic problem: a geodesic line from a point and azimuth,
//! ported from GeographicLib's C library (PROJ 9.8.1 `src/geodesic.c`,
//! `geod_lineinit_int`, `geod_genposition`, `geod_direct`).

use super::math::{DEGREE, ang_normalize, ang_round, atan2_d, lat_fix, norm2, sin_cos_d, sq};
use super::model::{Geodesic, tiny};
use super::series::{Coeffs, ORDER, a1m1f, a2m1f, a3f, c1f, c1pf, c2f, c3f, c4f, sin_cos_series};

/// |f| above which the reverted distance series takes one Newton step,
/// Karney 2013 §4.
const REVERTED_SERIES_MAX_F: f64 = 0.01;

/// Every output of a direct or inverse geodesic solution. Angles in
/// degrees, lengths in the units of the semi-major axis, area in their
/// square.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeodesicSolution {
    pub lat1: f64,
    pub lon1: f64,
    pub azi1: f64,
    pub lat2: f64,
    pub lon2: f64,
    pub azi2: f64,
    /// Distance s12.
    pub s12: f64,
    /// Arc length a12 on the auxiliary sphere, degrees.
    pub a12: f64,
    /// Reduced length m12.
    pub m12: f64,
    /// Geodesic scale of point 2 relative to point 1.
    pub big_m12: f64,
    /// Geodesic scale of point 1 relative to point 2.
    pub big_m21: f64,
    /// Area between the geodesic and the equator, S12.
    pub area_s12: f64,
}

/// A geodesic line starting at a point with a given azimuth.
#[derive(Debug, Clone, PartialEq)]
pub struct GeodesicLine {
    lat1: f64,
    lon1: f64,
    azi1: f64,
    a: f64,
    f: f64,
    b: f64,
    c2: f64,
    f1: f64,
    salp1: f64,
    calp1: f64,
    dn1: f64,
    salp0: f64,
    calp0: f64,
    ssig1: f64,
    csig1: f64,
    somg1: f64,
    comg1: f64,
    k2: f64,
    a1m1: f64,
    a2m1: f64,
    a3c: f64,
    b11: f64,
    b21: f64,
    b31: f64,
    a4: f64,
    b41: f64,
    stau1: f64,
    ctau1: f64,
    c1a: Coeffs,
    c1pa: Coeffs,
    c2a: Coeffs,
    c3a: Coeffs,
    c4a: Coeffs,
}

impl GeodesicLine {
    /// The line from (`lat1`, `lon1`) with azimuth `azi1`, degrees.
    pub fn new(g: &Geodesic, lat1: f64, lon1: f64, azi1: f64) -> Self {
        let azi1 = ang_normalize(azi1);
        // Rounding guards against underflow in salp0.
        let (salp1, calp1) = sin_cos_d(ang_round(azi1));
        Self::with_sin_cos(g, lat1, lon1, azi1, salp1, calp1)
    }

    /// The line from a point whose azimuth is given by its sine and cosine
    /// as well, GeographicLib `geod_lineinit_int` with every capability.
    pub(super) fn with_sin_cos(
        g: &Geodesic,
        lat1: f64,
        lon1: f64,
        azi1: f64,
        salp1: f64,
        calp1: f64,
    ) -> Self {
        let lat1 = lat_fix(lat1);
        let (sbet1, cbet1) = sin_cos_d(ang_round(lat1));
        let (sbet1, cbet1) = norm2(sbet1 * g.f1, cbet1);
        // cos β1 = +ε at the poles.
        let cbet1 = cbet1.max(tiny());
        let dn1 = (1.0 + g.ep2 * sq(sbet1)).sqrt();
        // sin α0 = sin α1 cos β1, α0 in [0, π/2 − |β1|].
        let salp0 = salp1 * cbet1;
        let calp0 = calp1.hypot(salp1 * sbet1);
        // tan β1 = tan σ1 cos α1 and tan ω1 = sin α0 tan σ1.
        let somg1 = salp0 * sbet1;
        let c = if sbet1 != 0.0 || calp1 != 0.0 {
            cbet1 * calp1
        } else {
            1.0
        };
        let (ssig1, csig1) = norm2(sbet1, c);
        let comg1 = c;
        let k2 = sq(calp0) * g.ep2;
        let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);

        let a1m1 = a1m1f(eps);
        let c1a = c1f(eps);
        let b11 = sin_cos_series(true, ssig1, csig1, &c1a, ORDER);
        let (s, c) = b11.sin_cos();
        // τ1 = σ1 + B11.
        let stau1 = ssig1 * c + csig1 * s;
        let ctau1 = csig1 * c - ssig1 * s;
        let c1pa = c1pf(eps);
        let a2m1 = a2m1f(eps);
        let c2a = c2f(eps);
        let b21 = sin_cos_series(true, ssig1, csig1, &c2a, ORDER);
        let c3a = c3f(&g.c3x, eps);
        let a3c = -g.f * salp0 * a3f(&g.a3x, eps);
        let b31 = sin_cos_series(true, ssig1, csig1, &c3a, ORDER - 1);
        let c4a = c4f(&g.c4x, eps);
        // a² e² cos α0 sin α0.
        let a4 = sq(g.a) * calp0 * salp0 * g.e2;
        let b41 = sin_cos_series(false, ssig1, csig1, &c4a, ORDER);
        Self {
            lat1,
            lon1,
            azi1,
            a: g.a,
            f: g.f,
            b: g.b,
            c2: g.c2,
            f1: g.f1,
            salp1,
            calp1,
            dn1,
            salp0,
            calp0,
            ssig1,
            csig1,
            somg1,
            comg1,
            k2,
            a1m1,
            a2m1,
            a3c,
            b11,
            b21,
            b31,
            a4,
            b41,
            stau1,
            ctau1,
            c1a,
            c1pa,
            c2a,
            c3a,
            c4a,
        }
    }

    /// The point at distance `s12` along the line. Longitude reduces to
    /// [−180°, 180°].
    pub fn position(&self, s12: f64) -> GeodesicSolution {
        self.gen_position(false, s12, false)
    }

    /// The point at `s12_a12` along the line: a distance, or an arc length
    /// in degrees when `arc_mode`. With `unroll`, the longitude counts
    /// whole turns instead of reducing to [−180°, 180°].
    pub fn gen_position(&self, arc_mode: bool, s12_a12: f64, unroll: bool) -> GeodesicSolution {
        let mut b12 = 0.0;
        let (mut sig12, mut ssig12, mut csig12);
        if arc_mode {
            sig12 = s12_a12 * DEGREE;
            (ssig12, csig12) = sin_cos_d(s12_a12);
        } else {
            let tau12 = s12_a12 / (self.b * (1.0 + self.a1m1));
            let (s, c) = tau12.sin_cos();
            // τ2 = τ1 + τ12.
            b12 = -sin_cos_series(
                true,
                self.stau1 * c + self.ctau1 * s,
                self.ctau1 * c - self.stau1 * s,
                &self.c1pa,
                ORDER,
            );
            sig12 = tau12 - (b12 - self.b11);
            (ssig12, csig12) = sig12.sin_cos();
            if self.f.abs() > REVERTED_SERIES_MAX_F {
                let ssig2 = self.ssig1 * csig12 + self.csig1 * ssig12;
                let csig2 = self.csig1 * csig12 - self.ssig1 * ssig12;
                b12 = sin_cos_series(true, ssig2, csig2, &self.c1a, ORDER);
                let serr = (1.0 + self.a1m1) * (sig12 + (b12 - self.b11)) - s12_a12 / self.b;
                sig12 -= serr / (1.0 + self.k2 * sq(ssig2)).sqrt();
                (ssig12, csig12) = sig12.sin_cos();
            }
        }

        // σ2 = σ1 + σ12.
        let ssig2 = self.ssig1 * csig12 + self.csig1 * ssig12;
        let mut csig2 = self.csig1 * csig12 - self.ssig1 * ssig12;
        let dn2 = (1.0 + self.k2 * sq(ssig2)).sqrt();
        if arc_mode || self.f.abs() > REVERTED_SERIES_MAX_F {
            b12 = sin_cos_series(true, ssig2, csig2, &self.c1a, ORDER);
        }
        let ab1 = (1.0 + self.a1m1) * (b12 - self.b11);
        // sin β2 = cos α0 sin σ2.
        let sbet2 = self.calp0 * ssig2;
        let mut cbet2 = self.salp0.hypot(self.calp0 * csig2);
        if cbet2 == 0.0 {
            // sin α0 = 0 and cos σ2 = 0: break the degeneracy.
            cbet2 = tiny();
            csig2 = tiny();
        }
        // tan α0 = cos σ2 tan α2.
        let salp2 = self.salp0;
        let calp2 = self.calp0 * csig2;

        let s12 = if arc_mode {
            self.b * ((1.0 + self.a1m1) * sig12 + ab1)
        } else {
            s12_a12
        };

        let e = 1.0f64.copysign(self.salp0);
        // tan ω2 = sin α0 tan σ2.
        let somg2 = self.salp0 * ssig2;
        let comg2 = csig2;
        let omg12 = if unroll {
            e * (sig12 - (ssig2.atan2(csig2) - self.ssig1.atan2(self.csig1))
                + ((e * somg2).atan2(comg2) - (e * self.somg1).atan2(self.comg1)))
        } else {
            (somg2 * self.comg1 - comg2 * self.somg1).atan2(comg2 * self.comg1 + somg2 * self.somg1)
        };
        let lam12 = omg12
            + self.a3c
                * (sig12 + (sin_cos_series(true, ssig2, csig2, &self.c3a, ORDER - 1) - self.b31));
        let lon12 = lam12 / DEGREE;
        let lon2 = if unroll {
            self.lon1 + lon12
        } else {
            ang_normalize(ang_normalize(self.lon1) + ang_normalize(lon12))
        };
        let lat2 = atan2_d(sbet2, self.f1 * cbet2);
        let azi2 = atan2_d(salp2, calp2);

        let b22 = sin_cos_series(true, ssig2, csig2, &self.c2a, ORDER);
        let ab2 = (1.0 + self.a2m1) * (b22 - self.b21);
        let j12 = (self.a1m1 - self.a2m1) * sig12 + (ab1 - ab2);
        // Parenthesised products keep the cancellation exact for
        // coincident points.
        let m12 = self.b
            * ((dn2 * (self.csig1 * ssig2) - self.dn1 * (self.ssig1 * csig2))
                - self.csig1 * csig2 * j12);
        let t = self.k2 * (ssig2 - self.ssig1) * (ssig2 + self.ssig1) / (self.dn1 + dn2);
        let big_m12 = csig12 + (t * ssig2 - csig2 * j12) * self.ssig1 / self.dn1;
        let big_m21 = csig12 - (t * self.ssig1 - self.csig1 * j12) * ssig2 / dn2;

        let b42 = sin_cos_series(false, ssig2, csig2, &self.c4a, ORDER);
        let (salp12, calp12) = if self.calp0 == 0.0 || self.salp0 == 0.0 {
            // α12 = α2 − α1, used in atan2 so no need to normalise.
            (
                salp2 * self.calp1 - calp2 * self.salp1,
                calp2 * self.calp1 + salp2 * self.salp1,
            )
        } else {
            // tan(α2 − α1) from tan α = tan α0 sec σ, Karney 2013 eq. 65.
            let num = if csig12 <= 0.0 {
                self.csig1 * (1.0 - csig12) + ssig12 * self.ssig1
            } else {
                ssig12 * (self.csig1 * ssig12 / (1.0 + csig12) + self.ssig1)
            };
            (
                self.calp0 * self.salp0 * num,
                sq(self.salp0) + sq(self.calp0) * self.csig1 * csig2,
            )
        };
        let area_s12 = self.c2 * salp12.atan2(calp12) + self.a4 * (b42 - self.b41);

        GeodesicSolution {
            lat1: self.lat1,
            lon1: self.lon1,
            azi1: self.azi1,
            lat2,
            lon2,
            azi2,
            s12,
            a12: if arc_mode { s12_a12 } else { sig12 / DEGREE },
            m12,
            big_m12,
            big_m21,
            area_s12,
        }
    }

    /// Semi-major axis of the line's ellipsoid.
    pub fn a(&self) -> f64 {
        self.a
    }
}

impl Geodesic {
    /// Solve the direct problem: from (`lat1`, `lon1`) along azimuth `azi1`
    /// for distance `s12`, return (lat2, lon2, azi2). Degrees and the units
    /// of the semi-major axis.
    pub fn direct(&self, lat1: f64, lon1: f64, azi1: f64, s12: f64) -> (f64, f64, f64) {
        let sol = self.direct_full(lat1, lon1, azi1, s12);
        (sol.lat2, sol.lon2, sol.azi2)
    }

    /// Solve the direct problem with every output.
    pub fn direct_full(&self, lat1: f64, lon1: f64, azi1: f64, s12: f64) -> GeodesicSolution {
        GeodesicLine::new(self, lat1, lon1, azi1).position(s12)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Latitude, longitude and azimuth tolerance against PROJ, degrees.
    const ANGLE_TOL_DEG: f64 = 1e-11;

    /// (lat1, lon1, azi1, s12, lat2, lon2, azi2) from GeographicLib as
    /// bundled in PROJ 9.8.1, via pyproj 3.8.0 `Geod(ellps="WGS84").fwd`.
    const DIRECT: &[[f64; 7]] = &[
        [
            40.6,
            -73.8,
            51.4,
            5_000_000.0,
            52.724_376_645_671_35,
            -8.361_446_565_212_93,
            101.745_597_701_112_46,
        ],
        [
            -30.0,
            0.0,
            179.0,
            19_000_000.0,
            20.939_069_684_509_27,
            179.823_126_134_612_55,
            0.927_634_299_928_172_4,
        ],
        [
            89.0,
            10.0,
            -120.0,
            500_000.0,
            84.948_710_822_340_83,
            -40.149_256_704_217_166,
            -170.115_400_931_919_4,
        ],
        [
            0.0,
            0.0,
            90.0,
            20_000_000.0,
            0.0,
            179.663_056_823_904_3,
            90.0,
        ],
    ];

    fn angle_close(got: f64, want: f64) -> bool {
        let d = (got - want).rem_euclid(360.0);
        d.min(360.0 - d) < ANGLE_TOL_DEG
    }

    #[test]
    fn direct_matches_geographiclib() {
        let g = Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        for row in DIRECT {
            let [lat1, lon1, azi1, s12, lat2, lon2, azi2] = *row;
            let (la, lo, az) = g.direct(lat1, lon1, azi1, s12);
            assert!((la - lat2).abs() < ANGLE_TOL_DEG, "{row:?}: lat2 {la}");
            assert!(angle_close(lo, lon2), "{row:?}: lon2 {lo}");
            assert!(angle_close(az, azi2), "{row:?}: azi2 {az}");
        }
    }

    #[test]
    fn arc_mode_and_unroll() {
        let g = Geodesic::new(1.0, 0.0);
        let line = GeodesicLine::new(&g, 0.0, 170.0, 90.0);
        let p = line.gen_position(true, 30.0, true);
        assert!((p.lon2 - 200.0).abs() < 1e-12 && (p.s12 - 30f64.to_radians()).abs() < 1e-15);
        let p = line.gen_position(true, 30.0, false);
        assert!((p.lon2 + 160.0).abs() < 1e-12);
        let (lat, lon, _) = g.direct(0.0, 0.0, 0.0, core::f64::consts::FRAC_PI_4);
        assert!((lat - 45.0).abs() < 1e-12 && lon == 0.0);
    }
}
