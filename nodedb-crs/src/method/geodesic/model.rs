// SPDX-License-Identifier: Apache-2.0

//! The geodesic model of one ellipsoid and the shared length integrals,
//! ported from GeographicLib's C library (PROJ 9.8.1 `src/geodesic.c`,
//! `geod_init` and `Lengths`).

use super::math::sq;
use super::series::{
    Coeffs, NC3X, NC4X, ORDER, a1m1f, a2m1f, a3_coeff, c1f, c2f, c3_coeff, c4_coeff, sin_cos_series,
};

/// Newton iterations before the inverse solver falls back to bisection.
pub(super) const MAXIT1: u32 = 20;

/// Total iteration cap of the inverse solver: MAXIT1 + mantissa digits + 10.
pub(super) const MAXIT2: u32 = MAXIT1 + f64::MANTISSA_DIGITS + 10;

/// √(smallest normal f64), keeps cos β above zero at the poles.
pub(super) fn tiny() -> f64 {
    f64::MIN_POSITIVE.sqrt()
}

/// Machine epsilon, GeographicLib `tol0`.
pub(super) const TOL0: f64 = f64::EPSILON;

/// GeographicLib `tol1` = 200 · tol0.
pub(super) const TOL1: f64 = 200.0 * TOL0;

/// GeographicLib `tol2` = √tol0.
pub(super) fn tol2() -> f64 {
    TOL0.sqrt()
}

/// Bisection interval check, GeographicLib `tolb`.
pub(super) const TOLB: f64 = TOL0;

/// GeographicLib `xthresh` = 1000 · tol2.
pub(super) fn xthresh() -> f64 {
    1000.0 * tol2()
}

/// Safety factor in the "really short line" threshold `etol2`.
const ETOL2_SAFETY: f64 = 0.1;

/// Floor on |f| in `etol2`, keeps it bounded for near-spheres.
const ETOL2_MIN_F: f64 = 0.001;

/// An ellipsoid prepared for geodesic calculations (Karney 2013).
#[derive(Debug, Clone, PartialEq)]
pub struct Geodesic {
    /// Semi-major axis.
    pub(super) a: f64,
    /// Flattening.
    pub(super) f: f64,
    /// 1 − f.
    pub(super) f1: f64,
    /// e².
    pub(super) e2: f64,
    /// e′² = e² / (1 − e²).
    pub(super) ep2: f64,
    /// Third flattening.
    pub(super) n: f64,
    /// Semi-minor axis.
    pub(super) b: f64,
    /// Authalic radius squared.
    pub(super) c2: f64,
    /// σ12 threshold for "really short" lines.
    pub(super) etol2: f64,
    pub(super) a3x: [f64; ORDER],
    pub(super) c3x: [f64; NC3X],
    pub(super) c4x: [f64; NC4X],
}

/// The distance and reduced-length integrals of [`Geodesic::lengths`].
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Lengths {
    /// s12 / b.
    pub(super) s12b: f64,
    /// m12 / b.
    pub(super) m12b: f64,
    /// Secular coefficient of the reduced length.
    pub(super) m0: f64,
    /// Geodesic scale M12.
    pub(super) big_m12: f64,
    /// Geodesic scale M21.
    pub(super) big_m21: f64,
}

/// The auxiliary-sphere state that [`Geodesic::lengths`] integrates over.
#[derive(Debug, Clone, Copy)]
pub(super) struct Arc {
    pub(super) eps: f64,
    pub(super) sig12: f64,
    pub(super) ssig1: f64,
    pub(super) csig1: f64,
    pub(super) dn1: f64,
    pub(super) ssig2: f64,
    pub(super) csig2: f64,
    pub(super) dn2: f64,
    pub(super) cbet1: f64,
    pub(super) cbet2: f64,
}

impl Geodesic {
    /// The geodesic model of the ellipsoid with semi-major axis `a` and
    /// flattening `f` (0 for a sphere, negative for a prolate spheroid).
    pub fn new(a: f64, f: f64) -> Self {
        let f1 = 1.0 - f;
        let e2 = f * (2.0 - f);
        let ep2 = e2 / sq(f1);
        let n = f / (2.0 - f);
        let b = a * f1;
        let c2 = (sq(a)
            + sq(b)
                * if e2 == 0.0 {
                    1.0
                } else {
                    (if e2 > 0.0 {
                        e2.sqrt().atanh()
                    } else {
                        (-e2).sqrt().atan()
                    }) / e2.abs().sqrt()
                })
            / 2.0;
        let etol2 = ETOL2_SAFETY * tol2()
            / (f.abs().max(ETOL2_MIN_F) * (1.0 - f / 2.0).min(1.0) / 2.0).sqrt();
        Self {
            a,
            f,
            f1,
            e2,
            ep2,
            n,
            b,
            c2,
            etol2,
            a3x: a3_coeff(n),
            c3x: c3_coeff(n),
            c4x: c4_coeff(n),
        }
    }

    /// Semi-major axis.
    pub fn a(&self) -> f64 {
        self.a
    }

    /// Flattening.
    pub fn f(&self) -> f64 {
        self.f
    }

    /// Distance s12/b, reduced length m12/b, m0 and the geodesic scales
    /// along an arc, GeographicLib `Lengths`. Without `with_distance`,
    /// `s12b` stays 0 and J12 comes from the combined C1/C2 series, as
    /// GeographicLib does when the distance is not requested.
    pub(super) fn lengths(&self, arc: &Arc, with_distance: bool) -> Lengths {
        let Arc {
            eps,
            sig12,
            ssig1,
            csig1,
            dn1,
            ssig2,
            csig2,
            dn2,
            cbet1,
            cbet2,
        } = *arc;
        let ca: Coeffs = c1f(eps);
        let mut cb: Coeffs = c2f(eps);
        let a1m1 = a1m1f(eps);
        let a2m1 = a2m1f(eps);
        let m0 = a1m1 - a2m1;
        let a1 = 1.0 + a1m1;
        let a2 = 1.0 + a2m1;
        let (s12b, j12) = if with_distance {
            let b1 = sin_cos_series(true, ssig2, csig2, &ca, ORDER)
                - sin_cos_series(true, ssig1, csig1, &ca, ORDER);
            let b2 = sin_cos_series(true, ssig2, csig2, &cb, ORDER)
                - sin_cos_series(true, ssig1, csig1, &cb, ORDER);
            (a1 * (sig12 + b1), m0 * sig12 + (a1 * b1 - a2 * b2))
        } else {
            for (c, a) in cb.iter_mut().zip(ca.iter()).skip(1) {
                *c = a1 * a - a2 * *c;
            }
            let j12 = m0 * sig12
                + (sin_cos_series(true, ssig2, csig2, &cb, ORDER)
                    - sin_cos_series(true, ssig1, csig1, &cb, ORDER));
            (0.0, j12)
        };
        // Parenthesised products keep the cancellation exact for
        // coincident points.
        let m12b = dn2 * (csig1 * ssig2) - dn1 * (ssig1 * csig2) - csig1 * csig2 * j12;
        let csig12 = csig1 * csig2 + ssig1 * ssig2;
        let t = self.ep2 * (cbet1 - cbet2) * (cbet1 + cbet2) / (dn1 + dn2);
        Lengths {
            s12b,
            m12b,
            m0,
            big_m12: csig12 + (t * ssig2 - csig2 * j12) * ssig1 / dn1,
            big_m21: csig12 - (t * ssig1 - csig1 * j12) * ssig2 / dn2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wgs84_constants() {
        let g = Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        assert!((g.b - 6_356_752.314_245).abs() < 1e-6);
        // Authalic radius of WGS 84, 6 371 007.181 m.
        assert!((g.c2.sqrt() - 6_371_007.180_9).abs() < 1e-3);
        assert_eq!(MAXIT2, 83);
    }
}
