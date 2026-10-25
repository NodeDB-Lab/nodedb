// SPDX-License-Identifier: Apache-2.0

//! The inverse geodesic problem, ported from GeographicLib's C library
//! (PROJ 9.8.1 `src/geodesic.c`, `geod_geninverse_int`).

use super::line::GeodesicSolution;
use super::math::{
    DEGREE, HD, QD, ang_diff, ang_round, atan2_d, lat_fix, norm2, sin_cos_d, sin_cos_de, sq,
};
use super::model::{Arc, Geodesic, MAXIT1, MAXIT2, TOL0, TOLB, tiny, tol2};
use super::series::{ORDER, c4f, sin_cos_series};
use super::start::{Ends, Lambda};

/// Multiplier of tol0 in the Newton exit test after `tripn`.
const TRIPN_TOL_FACTOR: f64 = 8.0;

/// |v| below which Newton's quadratic convergence is trusted, in tol0.
const TRIPN_SET_FACTOR: f64 = 16.0;

/// cos ω12 above which the area uses the tan(Γ/2) formula: ω12 < 3π/4.
/// GeographicLib writes the threshold as −0.7071.
const AREA_MAX_COMG12: f64 = -core::f64::consts::FRAC_1_SQRT_2;

/// sin β2 − sin β1 below which the area uses the tan(Γ/2) formula.
const AREA_MAX_DSBET: f64 = 1.75;

/// Inverse solution in sine/cosine form, before conversion to degrees.
struct Raw {
    a12: f64,
    s12: f64,
    salp1: f64,
    calp1: f64,
    salp2: f64,
    calp2: f64,
    m12: f64,
    big_m12: f64,
    big_m21: f64,
    area_s12: f64,
}

impl Geodesic {
    /// Solve the inverse problem between (`lat1`, `lon1`) and (`lat2`,
    /// `lon2`): return (s12, azi1, azi2). Degrees and the units of the
    /// semi-major axis.
    pub fn inverse(&self, lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> (f64, f64, f64) {
        let sol = self.inverse_full(lat1, lon1, lat2, lon2);
        (sol.s12, sol.azi1, sol.azi2)
    }

    /// Solve the inverse problem with every output.
    pub fn inverse_full(&self, lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> GeodesicSolution {
        let r = self.gen_inverse(lat1, lon1, lat2, lon2);
        GeodesicSolution {
            lat1,
            lon1,
            azi1: atan2_d(r.salp1, r.calp1),
            lat2,
            lon2,
            azi2: atan2_d(r.salp2, r.calp2),
            s12: r.s12,
            a12: r.a12,
            m12: r.m12,
            big_m12: r.big_m12,
            big_m21: r.big_m21,
            area_s12: r.area_s12,
        }
    }

    fn gen_inverse(&self, lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> Raw {
        let tiny = tiny();
        let tol2 = tol2();
        // λ12 in [−180°, 180°], −180° only for west-going geodesics.
        let (mut lon12, mut lon12s) = ang_diff(lon1, lon2);
        let mut lonsign: f64 = if lon12.is_sign_negative() { -1.0 } else { 1.0 };
        lon12 *= lonsign;
        lon12s *= lonsign;
        let lam12 = lon12 * DEGREE;
        let (slam12, clam12) = sin_cos_de(lon12, lon12s);
        // The supplementary longitude difference.
        let lon12s = (HD - lon12) - lon12s;

        // Treat points very close to the equator as on it.
        let mut lat1 = ang_round(lat_fix(lat1));
        let mut lat2 = ang_round(lat_fix(lat2));
        // Point 1 has the larger |latitude|. A NaN latitude becomes lat1.
        let swapp: f64 = if lat1.abs() < lat2.abs() || lat2.is_nan() {
            -1.0
        } else {
            1.0
        };
        if swapp < 0.0 {
            lonsign = -lonsign;
            core::mem::swap(&mut lat1, &mut lat2);
        }
        // lat1 ≤ −0.
        let latsign: f64 = if lat1.is_sign_negative() { 1.0 } else { -1.0 };
        lat1 *= latsign;
        lat2 *= latsign;
        // Canonical form: 0 ≤ λ12 ≤ 180, −90 ≤ lat1 ≤ −0, lat1 ≤ lat2 ≤ −lat1.

        let (sbet1, cbet1) = sin_cos_d(lat1);
        let (sbet1, cbet1) = norm2(sbet1 * self.f1, cbet1);
        let cbet1 = cbet1.max(tiny);
        let (sbet2, cbet2) = sin_cos_d(lat2);
        let (mut sbet2, cbet2) = norm2(sbet2 * self.f1, cbet2);
        let mut cbet2 = cbet2.max(tiny);

        // Force β2 = ±β1 exactly when the sensitive measure vanishes.
        if cbet1 < -sbet1 {
            if cbet2 == cbet1 {
                sbet2 = sbet1.copysign(sbet2);
            }
        } else if sbet2.abs() == -sbet1 {
            cbet2 = cbet1;
        }

        let dn1 = (1.0 + self.ep2 * sq(sbet1)).sqrt();
        let dn2 = (1.0 + self.ep2 * sq(sbet2)).sqrt();
        let ends = Ends {
            sbet1,
            cbet1,
            dn1,
            sbet2,
            cbet2,
            dn2,
        };

        let mut a12 = 0.0;
        let mut sig12;
        let (mut salp1, mut calp1, mut salp2, mut calp2) = (0.0, 0.0, 0.0, 0.0);
        let (mut s12x, mut m12x) = (0.0, 0.0);
        let (mut big_m12, mut big_m21) = (0.0, 0.0);
        // somg12 = 2 marks "not computed yet".
        let (mut omg12, mut somg12, mut comg12) = (0.0, 2.0, 0.0);

        let mut meridian = lat1 == -QD || slam12 == 0.0;
        if meridian {
            // Both endpoints lie on one full meridian.
            calp1 = clam12;
            salp1 = slam12;
            calp2 = 1.0;
            salp2 = 0.0;
            let ssig1 = sbet1;
            let csig1 = calp1 * cbet1;
            let ssig2 = sbet2;
            let csig2 = calp2 * cbet2;
            sig12 = ((csig1 * ssig2 - ssig1 * csig2).max(0.0) + 0.0)
                .atan2(csig1 * csig2 + ssig1 * ssig2);
            let l = self.lengths(
                &Arc {
                    eps: self.n,
                    sig12,
                    ssig1,
                    csig1,
                    dn1,
                    ssig2,
                    csig2,
                    dn2,
                    cbet1,
                    cbet2,
                },
                true,
            );
            s12x = l.s12b;
            m12x = l.m12b;
            big_m12 = l.big_m12;
            big_m21 = l.big_m21;
            // Zero-length geodesics can give m12 < 0, hence the σ12 test.
            if sig12 < tol2 || m12x >= 0.0 {
                if sig12 < 3.0 * tiny || (sig12 < TOL0 && (s12x < 0.0 || m12x < 0.0)) {
                    sig12 = 0.0;
                    m12x = 0.0;
                    s12x = 0.0;
                }
                m12x *= self.b;
                s12x *= self.b;
                a12 = sig12 / DEGREE;
            } else {
                // m12 < 0: prolate and too close to antipodal.
                meridian = false;
            }
        }

        if !meridian && sbet1 == 0.0 && (self.f <= 0.0 || lon12s >= self.f * HD) {
            // The geodesic runs along the equator.
            calp1 = 0.0;
            calp2 = 0.0;
            salp1 = 1.0;
            salp2 = 1.0;
            s12x = self.a * lam12;
            sig12 = lam12 / self.f1;
            omg12 = sig12;
            m12x = self.b * sig12.sin();
            big_m12 = sig12.cos();
            big_m21 = big_m12;
            a12 = lon12 / self.f1;
        } else if !meridian {
            let start = self.inverse_start(&ends, lam12, slam12, clam12);
            sig12 = start.sig12;
            salp1 = start.salp1;
            calp1 = start.calp1;
            if sig12 >= 0.0 {
                // Short line: the starting guess is the solution.
                salp2 = start.salp2;
                calp2 = start.calp2;
                let dnm = start.dnm;
                s12x = sig12 * self.b * dnm;
                m12x = sq(dnm) * self.b * (sig12 / dnm).sin();
                big_m12 = (sig12 / dnm).cos();
                big_m21 = big_m12;
                a12 = sig12 / DEGREE;
                omg12 = lam12 / (self.f1 * dnm);
            } else {
                let lam = self.newton(&ends, &mut salp1, &mut calp1, slam12, clam12);
                salp2 = lam.salp2;
                calp2 = lam.calp2;
                sig12 = lam.arc.sig12;
                let l = self.lengths(&lam.arc, true);
                m12x = l.m12b * self.b;
                s12x = l.s12b * self.b;
                big_m12 = l.big_m12;
                big_m21 = l.big_m21;
                a12 = sig12 / DEGREE;
                // ω12 = λ12 − Δω12.
                let (sdomg12, cdomg12) = lam.domg12.sin_cos();
                somg12 = slam12 * cdomg12 - clam12 * sdomg12;
                comg12 = clam12 * cdomg12 + slam12 * sdomg12;
            }
        }

        // Convert −0 to 0.
        let s12 = 0.0 + s12x;
        let m12 = 0.0 + m12x;

        // Area, Karney 2013 §6.
        let salp0 = salp1 * cbet1;
        let calp0 = calp1.hypot(salp1 * sbet1);
        let mut area_s12 = if calp0 != 0.0 && salp0 != 0.0 {
            let (ssig1, csig1) = norm2(sbet1, calp1 * cbet1);
            let (ssig2, csig2) = norm2(sbet2, calp2 * cbet2);
            let k2 = sq(calp0) * self.ep2;
            let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);
            let a4 = sq(self.a) * calp0 * salp0 * self.e2;
            let c4a = c4f(&self.c4x, eps);
            let b41 = sin_cos_series(false, ssig1, csig1, &c4a, ORDER);
            let b42 = sin_cos_series(false, ssig2, csig2, &c4a, ORDER);
            a4 * (b42 - b41)
        } else {
            // σ1, σ2 are indeterminate on the equator.
            0.0
        };
        if !meridian && somg12 == 2.0 {
            (somg12, comg12) = omg12.sin_cos();
        }
        let alp12 = if !meridian && comg12 > AREA_MAX_COMG12 && sbet2 - sbet1 < AREA_MAX_DSBET {
            // tan(Γ/2) = tan(ω12/2) (tan(β1/2) + tan(β2/2)) / (1 + tan(β1/2) tan(β2/2)).
            let domg12 = 1.0 + comg12;
            let dbet1 = 1.0 + cbet1;
            let dbet2 = 1.0 + cbet2;
            2.0 * (somg12 * (sbet1 * dbet2 + sbet2 * dbet1))
                .atan2(domg12 * (sbet1 * sbet2 + dbet1 * dbet2))
        } else {
            let mut salp12 = salp2 * calp1 - calp2 * salp1;
            let mut calp12 = calp2 * calp1 + salp2 * salp1;
            // Attach the sign to zero so α12 = ±180° comes out right.
            if salp12 == 0.0 && calp12 < 0.0 {
                salp12 = tiny * calp1;
                calp12 = -1.0;
            }
            salp12.atan2(calp12)
        };
        area_s12 += self.c2 * alp12;
        area_s12 *= swapp * lonsign * latsign;
        area_s12 += 0.0;

        // Undo the canonical transformation.
        if swapp < 0.0 {
            core::mem::swap(&mut salp1, &mut salp2);
            core::mem::swap(&mut calp1, &mut calp2);
            core::mem::swap(&mut big_m12, &mut big_m21);
        }
        salp1 *= swapp * lonsign;
        calp1 *= swapp * latsign;
        salp2 *= swapp * lonsign;
        calp2 *= swapp * latsign;

        Raw {
            a12,
            s12,
            salp1,
            calp1,
            salp2,
            calp2,
            m12,
            big_m12,
            big_m21,
            area_s12,
        }
    }

    /// Solve λ12(α1) = λ12 by Newton's method with a bracketing fallback,
    /// Karney 2013 §5. Returns the state of the final evaluation.
    fn newton(
        &self,
        ends: &Ends,
        salp1: &mut f64,
        calp1: &mut f64,
        slam12: f64,
        clam12: f64,
    ) -> Lambda {
        let tiny = tiny();
        let (mut salp1a, mut calp1a, mut salp1b, mut calp1b) = (tiny, 1.0, tiny, -1.0);
        let mut tripn = false;
        let mut tripb = false;
        let mut numit = 0u32;
        loop {
            let lam = self.lambda12(ends, *salp1, *calp1, slam12, clam12, numit < MAXIT1);
            let v = lam.lam12;
            let dv = lam.dlam12;
            let tol = if tripn { TRIPN_TOL_FACTOR } else { 1.0 } * TOL0;
            // The reversed comparison lets NaN escape.
            if tripb || v.is_nan() || v.abs() < tol || numit == MAXIT2 {
                return lam;
            }
            // Shrink the bracket.
            if v > 0.0 && (numit > MAXIT1 || *calp1 / *salp1 > calp1b / salp1b) {
                salp1b = *salp1;
                calp1b = *calp1;
            } else if v < 0.0 && (numit > MAXIT1 || *calp1 / *salp1 < calp1a / salp1a) {
                salp1a = *salp1;
                calp1a = *calp1;
            }
            numit += 1;
            if numit - 1 < MAXIT1 && dv > 0.0 {
                let dalp1 = -v / dv;
                if dalp1.abs() < core::f64::consts::PI {
                    let (sdalp1, cdalp1) = dalp1.sin_cos();
                    let nsalp1 = *salp1 * cdalp1 + *calp1 * sdalp1;
                    if nsalp1 > 0.0 {
                        let ncalp1 = *calp1 * cdalp1 - *salp1 * sdalp1;
                        (*salp1, *calp1) = norm2(nsalp1, ncalp1);
                        // Slope → 0 in some regimes: test against ε, not √ε.
                        tripn = v.abs() <= TRIPN_SET_FACTOR * TOL0;
                        continue;
                    }
                }
            }
            // dv ≤ 0 or the step left (0, π): bisect the bracket.
            (*salp1, *calp1) = norm2((salp1a + salp1b) / 2.0, (calp1a + calp1b) / 2.0);
            tripn = false;
            tripb = (salp1a - *salp1).abs() + (calp1a - *calp1) < TOLB
                || (*salp1 - salp1b).abs() + (*calp1 - calp1b) < TOLB;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values: GeographicLib as bundled in PROJ 9.8.1, via pyproj
    // 3.8.0 `Geod(ellps="WGS84").inv`. pyproj returns the back azimuth at
    // point 2; the forward azimuth below is that value ± 180°.

    /// Distance tolerance against PROJ, 100 nm. GeographicLib's round-off
    /// is about 15 nm.
    const DIST_TOL_M: f64 = 1e-7;

    /// Azimuth tolerance against PROJ, degrees.
    const ANGLE_TOL_DEG: f64 = 1e-11;

    /// Area tolerance against PROJ, m².
    const AREA_TOL_M2: f64 = 1e-2;

    fn wgs84() -> Geodesic {
        Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563)
    }

    /// (lat1, lon1, lat2, lon2, s12, azi1, azi2).
    const INVERSE: &[[f64; 7]] = &[
        [
            40.6,
            -73.8,
            51.6,
            -0.5,
            5_551_759.400_318_678_5,
            51.198_882_845_579_83,
            107.821_776_735_514_26,
        ],
        [
            0.0,
            0.0,
            0.5,
            179.5,
            19_936_288.578_965_314,
            25.671_872_868_291_88,
            154.327_085_469_941_6,
        ],
        [
            -30.0,
            0.0,
            29.9,
            179.8,
            19_989_832.827_609_53,
            161.890_524_736_326_97,
            18.090_737_245_739_49,
        ],
        [
            10.0,
            20.0,
            10.0,
            20.000_000_1,
            0.010_963_936_534_940_925,
            89.999_999_991_317_6,
            90.000_000_008_682_4,
        ],
        [0.0, 0.0, 0.0, 90.0, 10_018_754.171_394_622, 90.0, 90.0],
        [-90.0, 0.0, 45.0, 30.0, 14_986_910.107_290_467, 30.0, 0.0],
        [20.0, 5.0, -40.0, 5.0, 6_641_895.284_522_149, 180.0, 180.0],
        [
            0.0,
            0.0,
            0.0,
            179.9,
            20_003_008.421_509_41,
            9.545_672_694_738_908,
            170.454_327_305_261_1,
        ],
        [
            52.0,
            24.0,
            -33.9,
            151.2,
            15_388_185.664_646_542,
            85.262_458_179_209_96,
            132.269_847_457_975_7,
        ],
    ];

    fn angle_close(got: f64, want: f64, tol: f64) -> bool {
        let d = (got - want).rem_euclid(360.0);
        d.min(360.0 - d) < tol
    }

    #[test]
    fn inverse_matches_geographiclib() {
        let g = wgs84();
        for row in INVERSE {
            let [lat1, lon1, lat2, lon2, s12, azi1, azi2] = *row;
            let (s, a1, a2) = g.inverse(lat1, lon1, lat2, lon2);
            assert!((s - s12).abs() < DIST_TOL_M, "{row:?}: s12 {s}");
            assert!(angle_close(a1, azi1, ANGLE_TOL_DEG), "{row:?}: azi1 {a1}");
            assert!(angle_close(a2, azi2, ANGLE_TOL_DEG), "{row:?}: azi2 {a2}");
        }
    }

    #[test]
    fn direct_inverts_inverse() {
        let g = wgs84();
        for row in INVERSE {
            let [lat1, lon1, lat2, lon2, ..] = *row;
            let (s, a1, _) = g.inverse(lat1, lon1, lat2, lon2);
            let (la, lo, _) = g.direct(lat1, lon1, a1, s);
            assert!((la - lat2).abs() < 1e-9, "{row:?}: {la}");
            assert!(
                lat2.abs() == 90.0 || angle_close(lo, lon2, 1e-9),
                "{row:?}: {lo}"
            );
        }
    }

    /// Σ S12 around a ring, reduced modulo the ellipsoid area 4π c² and
    /// negated to the counter-clockwise-positive sense, as GeographicLib
    /// `geod_polygon_compute` does for a ring that crosses no meridian ±180°.
    fn ring_area(g: &Geodesic, ring: &[[f64; 2]]) -> f64 {
        let mut sum = 0.0;
        for (i, p) in ring.iter().enumerate() {
            let q = ring[(i + 1) % ring.len()];
            sum += g.inverse_full(p[0], p[1], q[0], q[1]).area_s12;
        }
        let total = 4.0 * core::f64::consts::PI * g.c2;
        -(sum - (sum / total).round() * total)
    }

    /// pyproj `polygon_area_perimeter` of the octant (0,0)–(90,0)–(0,90)
    /// and of the 1° square at the origin, (lat, lon) vertices.
    #[test]
    fn area_matches_geographiclib() {
        let g = wgs84();
        let octant = ring_area(&g, &[[0.0, 0.0], [90.0, 0.0], [0.0, 90.0]]);
        assert!((octant - -63_758_202_715_511.055).abs() < 1.0, "{octant}");
        let square = ring_area(&g, &[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0]]);
        assert!(
            (square - 12_308_778_361.469_452).abs() < AREA_TOL_M2,
            "{square}"
        );
    }

    #[test]
    fn sphere_great_circle() {
        let g = Geodesic::new(1.0, 0.0);
        let (s, a1, a2) = g.inverse(0.0, 0.0, 0.0, 90.0);
        assert!((s - core::f64::consts::FRAC_PI_2).abs() < 1e-15);
        assert_eq!((a1, a2), (90.0, 90.0));
    }
}
