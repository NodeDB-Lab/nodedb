// SPDX-License-Identifier: Apache-2.0

//! The starting guess and the λ12(α1) function of the inverse geodesic
//! problem, ported from GeographicLib's C library (PROJ 9.8.1
//! `src/geodesic.c`, `Astroid`, `InverseStart`, `Lambda12`).

use core::f64::consts::PI;

use super::math::{norm2, sq};
use super::model::{Arc, Geodesic, TOL1, tiny, xthresh};
use super::series::{ORDER, a3f, c3f, sin_cos_series};

/// |n| above which the astroid estimate is skipped, Karney 2013 §5.
const ASTROID_MAX_N: f64 = 0.1;

/// Multiplier of |n| π cos² β1 in the "no astroid needed" test.
const ASTROID_SIG_FACTOR: f64 = 6.0;

/// Threshold on sin β12, cos β2 · λ12 for a "short line", Karney 2013 §5.
const SHORT_LINE: f64 = 0.5;

/// x below which the f < 0 branch scales β by x itself.
const PROLATE_X_SCALE: f64 = -0.01;

/// Solve k⁴ + 2k³ − (x² + y² − 1)k² − 2y²k − y² = 0 for the positive root,
/// Karney 2013 eq. 55.
pub(super) fn astroid(x: f64, y: f64) -> f64 {
    let p = sq(x);
    let q = sq(y);
    let r = (p + q - 1.0) / 6.0;
    if q == 0.0 && r <= 0.0 {
        // y = 0 with |x| ≤ 1.
        return 0.0;
    }
    // S = r³ s and the discriminant of the quadratic for T³.
    let s = p * q / 4.0;
    let r2 = sq(r);
    let r3 = r * r2;
    let disc = s * (s + 2.0 * r3);
    let mut u = r;
    if disc >= 0.0 {
        let mut t3 = s + r3;
        // The sign of the root maximises |T³|, avoiding cancellation.
        t3 += if t3 < 0.0 { -disc.sqrt() } else { disc.sqrt() };
        let t = t3.cbrt();
        u += t + if t != 0.0 { r2 / t } else { 0.0 };
    } else {
        // T is complex, u stays real.
        let ang = (-disc).sqrt().atan2(-(s + r3));
        u += 2.0 * r * (ang / 3.0).cos();
    }
    let v = (sq(u) + q).sqrt();
    let uv = if u < 0.0 { q / (v - u) } else { u + v };
    let w = (uv - q) / (2.0 * v);
    uv / ((uv + sq(w)).sqrt() + w)
}

/// Reduced latitudes of the two endpoints, with dn = √(1 + e′² sin² β).
#[derive(Debug, Clone, Copy)]
pub(super) struct Ends {
    pub(super) sbet1: f64,
    pub(super) cbet1: f64,
    pub(super) dn1: f64,
    pub(super) sbet2: f64,
    pub(super) cbet2: f64,
    pub(super) dn2: f64,
}

/// The starting guess of [`Geodesic::inverse_start`].
#[derive(Debug, Clone, Copy)]
pub(super) struct Start {
    /// σ12 when the short-line solution is final, else negative.
    pub(super) sig12: f64,
    pub(super) salp1: f64,
    pub(super) calp1: f64,
    /// Only set when `sig12 >= 0`.
    pub(super) salp2: f64,
    pub(super) calp2: f64,
    /// Only set for short lines.
    pub(super) dnm: f64,
}

/// The state [`Geodesic::lambda12`] computes for one α1.
#[derive(Debug, Clone, Copy)]
pub(super) struct Lambda {
    pub(super) lam12: f64,
    pub(super) salp2: f64,
    pub(super) calp2: f64,
    pub(super) arc: Arc,
    pub(super) domg12: f64,
    /// dλ12/dα1, when requested.
    pub(super) dlam12: f64,
}

impl Geodesic {
    /// Starting guess for Newton's method, GeographicLib `InverseStart`.
    pub(super) fn inverse_start(&self, e: &Ends, lam12: f64, slam12: f64, clam12: f64) -> Start {
        let Ends {
            sbet1,
            cbet1,
            dn1,
            sbet2,
            cbet2,
            dn2,
        } = *e;
        let mut out = Start {
            sig12: -1.0,
            salp1: 0.0,
            calp1: 0.0,
            salp2: 0.0,
            calp2: 0.0,
            dnm: 0.0,
        };
        // β12 = β2 − β1 in [0, π), β12a = β2 + β1 in (−π, 0].
        let sbet12 = sbet2 * cbet1 - cbet2 * sbet1;
        let cbet12 = cbet2 * cbet1 + sbet2 * sbet1;
        let sbet12a = sbet2 * cbet1 + cbet2 * sbet1;
        let shortline = cbet12 >= 0.0 && sbet12 < SHORT_LINE && cbet2 * lam12 < SHORT_LINE;
        let (mut somg12, mut comg12);
        if shortline {
            let mut sbetm2 = sq(sbet1 + sbet2);
            sbetm2 /= sbetm2 + sq(cbet1 + cbet2);
            out.dnm = (1.0 + self.ep2 * sbetm2).sqrt();
            let omg12 = lam12 / (self.f1 * out.dnm);
            (somg12, comg12) = omg12.sin_cos();
        } else {
            (somg12, comg12) = (slam12, clam12);
        }

        let mut salp1 = cbet2 * somg12;
        let mut calp1 = if comg12 >= 0.0 {
            sbet12 + cbet2 * sbet1 * sq(somg12) / (1.0 + comg12)
        } else {
            sbet12a - cbet2 * sbet1 * sq(somg12) / (1.0 - comg12)
        };
        let ssig12 = salp1.hypot(calp1);
        let csig12 = sbet1 * sbet2 + cbet1 * cbet2 * comg12;

        if shortline && ssig12 < self.etol2 {
            // Really short line.
            let salp2 = cbet1 * somg12;
            let calp2 = sbet12
                - cbet1
                    * sbet2
                    * if comg12 >= 0.0 {
                        sq(somg12) / (1.0 + comg12)
                    } else {
                        1.0 - comg12
                    };
            (out.salp2, out.calp2) = norm2(salp2, calp2);
            out.sig12 = ssig12.atan2(csig12);
        } else if self.n.abs() > ASTROID_MAX_N
            || csig12 >= 0.0
            || ssig12 >= ASTROID_SIG_FACTOR * self.n.abs() * PI * sq(cbet1)
        {
            // The zeroth-order spherical approximation is good enough.
        } else {
            // Scale λ12 and β2 so the antipode is at the origin and the
            // singular point at y = 0, x = −1.
            let lam12x = (-slam12).atan2(-clam12);
            let (x, y, lamscale);
            if self.f >= 0.0 {
                let k2 = sq(sbet1) * self.ep2;
                let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);
                lamscale = self.f * cbet1 * a3f(&self.a3x, eps) * PI;
                let betscale = lamscale * cbet1;
                x = lam12x / lamscale;
                y = sbet12a / betscale;
            } else {
                let cbet12a = cbet2 * cbet1 - sbet2 * sbet1;
                let bet12a = sbet12a.atan2(cbet12a);
                let l = self.lengths(
                    &Arc {
                        eps: self.n,
                        sig12: PI + bet12a,
                        ssig1: sbet1,
                        csig1: -cbet1,
                        dn1,
                        ssig2: sbet2,
                        csig2: cbet2,
                        dn2,
                        cbet1,
                        cbet2,
                    },
                    false,
                );
                x = -1.0 + l.m12b / (cbet1 * cbet2 * l.m0 * PI);
                let betscale = if x < PROLATE_X_SCALE {
                    sbet12a / x
                } else {
                    -self.f * sq(cbet1) * PI
                };
                lamscale = betscale / cbet1;
                y = lam12x / lamscale;
            }

            if y > -TOL1 && x > -1.0 - xthresh() {
                // Strip near the cut.
                if self.f >= 0.0 {
                    salp1 = (-x).min(1.0);
                    calp1 = -(1.0 - sq(salp1)).sqrt();
                } else {
                    calp1 = x.max(if x > -TOL1 { 0.0 } else { -1.0 });
                    salp1 = (1.0 - sq(calp1)).sqrt();
                }
            } else {
                // Estimate ω12 from the astroid, then α1 from the spherical
                // formula, Karney 2013 eqs. 56–57.
                let k = astroid(x, y);
                let omg12a = lamscale
                    * if self.f >= 0.0 {
                        -x * k / (1.0 + k)
                    } else {
                        -y * (1.0 + k) / k
                    };
                somg12 = omg12a.sin();
                comg12 = -omg12a.cos();
                salp1 = cbet2 * somg12;
                calp1 = sbet12a - cbet2 * sbet1 * sq(somg12) / (1.0 - comg12);
            }
        }
        // The reversed test lets NaN through to norm2.
        if salp1 > 0.0 || salp1.is_nan() {
            (out.salp1, out.calp1) = norm2(salp1, calp1);
        } else {
            (out.salp1, out.calp1) = (1.0, 0.0);
        }
        out
    }

    /// λ12 for a trial α1, GeographicLib `Lambda12`.
    pub(super) fn lambda12(
        &self,
        e: &Ends,
        salp1: f64,
        calp1: f64,
        slam120: f64,
        clam120: f64,
        diffp: bool,
    ) -> Lambda {
        let Ends {
            sbet1,
            cbet1,
            dn1,
            sbet2,
            cbet2,
            dn2,
        } = *e;
        // Break the degeneracy of the equatorial line, handled elsewhere.
        let calp1 = if sbet1 == 0.0 && calp1 == 0.0 {
            -tiny()
        } else {
            calp1
        };

        // sin α1 cos β1 = sin α0.
        let salp0 = salp1 * cbet1;
        let calp0 = calp1.hypot(salp1 * sbet1);

        // tan β1 = tan σ1 cos α1, tan ω1 = sin α0 tan σ1.
        let somg1 = salp0 * sbet1;
        let comg1 = calp1 * cbet1;
        let (ssig1, csig1) = norm2(sbet1, comg1);

        // Enforce symmetry for |β2| = −β1: sin α2 cos β2 = sin α0.
        let salp2 = if cbet2 != cbet1 { salp0 / cbet2 } else { salp1 };
        let calp2 = if cbet2 != cbet1 || sbet2.abs() != -sbet1 {
            (sq(calp1 * cbet1)
                + if cbet1 < -sbet1 {
                    (cbet2 - cbet1) * (cbet1 + cbet2)
                } else {
                    (sbet1 - sbet2) * (sbet1 + sbet2)
                })
            .sqrt()
                / cbet2
        } else {
            calp1.abs()
        };
        // tan β2 = tan σ2 cos α2, tan ω2 = sin α0 tan σ2.
        let somg2 = salp0 * sbet2;
        let comg2 = calp2 * cbet2;
        let (ssig2, csig2) = norm2(sbet2, comg2);

        // σ12 = σ2 − σ1 in [0, π].
        let sig12 =
            ((csig1 * ssig2 - ssig1 * csig2).max(0.0) + 0.0).atan2(csig1 * csig2 + ssig1 * ssig2);
        // ω12 = ω2 − ω1 in [0, π].
        let somg12 = (comg1 * somg2 - somg1 * comg2).max(0.0) + 0.0;
        let comg12 = comg1 * comg2 + somg1 * somg2;
        // η = ω12 − λ120.
        let eta = (somg12 * clam120 - comg12 * slam120).atan2(comg12 * clam120 + somg12 * slam120);
        let k2 = sq(calp0) * self.ep2;
        let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);
        let c3a = c3f(&self.c3x, eps);
        let b312 = sin_cos_series(true, ssig2, csig2, &c3a, ORDER - 1)
            - sin_cos_series(true, ssig1, csig1, &c3a, ORDER - 1);
        let domg12 = -self.f * a3f(&self.a3x, eps) * salp0 * (sig12 + b312);
        let arc = Arc {
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
        };
        let dlam12 = if !diffp {
            0.0
        } else if calp2 == 0.0 {
            -2.0 * self.f1 * dn1 / sbet1
        } else {
            self.lengths(&arc, false).m12b * self.f1 / (calp2 * cbet2)
        };
        Lambda {
            lam12: eta + domg12,
            salp2,
            calp2,
            arc,
            domg12,
            dlam12,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn astroid_root_solves_quartic() {
        for (x, y) in [(-0.5, -0.3), (-1.2, -0.01), (-0.9, -2.0), (0.3, -0.4)] {
            let k: f64 = astroid(x, y);
            let r = k.powi(4) + 2.0 * k.powi(3)
                - (x * x + y * y - 1.0) * k * k
                - 2.0 * y * y * k
                - y * y;
            assert!(k > 0.0 && r.abs() < 1e-12, "{x} {y} {k} {r}");
        }
        assert_eq!(astroid(0.5, 0.0), 0.0);
    }
}
