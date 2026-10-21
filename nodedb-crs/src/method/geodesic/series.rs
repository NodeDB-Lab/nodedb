// SPDX-License-Identifier: Apache-2.0

//! Series coefficients of the geodesic integrals to order 6, ported from
//! GeographicLib's C library (PROJ 9.8.1 `src/geodesic.c`).
//!
//! C. F. F. Karney, "Algorithms for geodesics", J. Geodesy 87 (2013):
//! A1, C1 (eqs. 17–18), C1′ (eq. 21), A2, C2 (eqs. 42–43), A3, C3
//! (eqs. 24–25) and C4 (eq. 64). Each table stores a polynomial's
//! coefficients, highest power first, followed by its common denominator.

use super::math::{polyval, sq};

/// Series order, GeographicLib `GEOGRAPHICLIB_GEODESIC_ORDER`.
pub(super) const ORDER: usize = 6;

/// Length of a Fourier coefficient array, index 0 to [`ORDER`].
pub(super) const NC: usize = ORDER + 1;

/// Number of stored C3 coefficients, nC3 (nC3 − 1) / 2.
pub(super) const NC3X: usize = ORDER * (ORDER - 1) / 2;

/// Number of stored C4 coefficients, nC4 (nC4 + 1) / 2.
pub(super) const NC4X: usize = ORDER * (ORDER + 1) / 2;

/// A Fourier coefficient array. Index 0 is unused by the sine series.
pub(super) type Coeffs = [f64; NC];

/// (1 − ε) A1 − 1 as a polynomial in ε².
const A1M1_COEFF: [f64; 5] = [1.0, 4.0, 64.0, 0.0, 256.0];

/// C1[l] / ε^l as polynomials in ε².
const C1_COEFF: [f64; 18] = [
    -1.0, 6.0, -16.0, 32.0, -9.0, 64.0, -128.0, 2048.0, 9.0, -16.0, 768.0, 3.0, -5.0, 512.0, -7.0,
    1280.0, -7.0, 2048.0,
];

/// C1′[l] / ε^l as polynomials in ε².
const C1P_COEFF: [f64; 18] = [
    205.0, -432.0, 768.0, 1536.0, 4005.0, -4736.0, 3840.0, 12288.0, -225.0, 116.0, 384.0, -7173.0,
    2695.0, 7680.0, 3467.0, 7680.0, 38081.0, 61440.0,
];

/// (1 + ε) A2 − 1 as a polynomial in ε².
const A2M1_COEFF: [f64; 5] = [-11.0, -28.0, -192.0, 0.0, 256.0];

/// C2[l] / ε^l as polynomials in ε².
const C2_COEFF: [f64; 18] = [
    1.0, 2.0, 16.0, 32.0, 35.0, 64.0, 384.0, 2048.0, 15.0, 80.0, 768.0, 7.0, 35.0, 512.0, 63.0,
    1280.0, 77.0, 2048.0,
];

/// A3 coefficients of ε^5 … ε^0 as polynomials in n.
const A3_COEFF: [f64; 18] = [
    -3.0, 128.0, -2.0, -3.0, 64.0, -1.0, -3.0, -1.0, 16.0, 3.0, -1.0, -2.0, 8.0, 1.0, -1.0, 2.0,
    1.0, 1.0,
];

/// C3[l] coefficients of ε^j as polynomials in n.
const C3_COEFF: [f64; 45] = [
    3.0, 128.0, 2.0, 5.0, 128.0, -1.0, 3.0, 3.0, 64.0, -1.0, 0.0, 1.0, 8.0, -1.0, 1.0, 4.0, 5.0,
    256.0, 1.0, 3.0, 128.0, -3.0, -2.0, 3.0, 64.0, 1.0, -3.0, 2.0, 32.0, 7.0, 512.0, -10.0, 9.0,
    384.0, 5.0, -9.0, 5.0, 192.0, 7.0, 512.0, -14.0, 7.0, 512.0, 21.0, 2560.0,
];

/// C4[l] coefficients of ε^j as polynomials in n.
const C4_COEFF: [f64; 77] = [
    97.0, 15015.0, 1088.0, 156.0, 45045.0, -224.0, -4784.0, 1573.0, 45045.0, -10656.0, 14144.0,
    -4576.0, -858.0, 45045.0, 64.0, 624.0, -4576.0, 6864.0, -3003.0, 15015.0, 100.0, 208.0, 572.0,
    3432.0, -12012.0, 30030.0, 45045.0, 1.0, 9009.0, -2944.0, 468.0, 135_135.0, 5792.0, 1040.0,
    -1287.0, 135_135.0, 5952.0, -11648.0, 9152.0, -2574.0, 135_135.0, -64.0, -624.0, 4576.0,
    -6864.0, 3003.0, 135_135.0, 8.0, 10725.0, 1856.0, -936.0, 225_225.0, -8448.0, 4992.0, -1144.0,
    225_225.0, -1440.0, 4160.0, -4576.0, 1716.0, 225_225.0, -136.0, 63063.0, 1024.0, -208.0,
    105_105.0, 3584.0, -3328.0, 1144.0, 315_315.0, -128.0, 135_135.0, -2560.0, 832.0, 405_405.0,
    128.0, 99099.0,
];

/// Evaluate `table[o..=o+m]` at `x` and divide by `table[o+m+1]`.
fn ratio(table: &[f64], o: usize, m: usize, x: f64) -> f64 {
    polyval(&table[o..=o + m], x) / table[o + m + 1]
}

/// Fill c[1..=ORDER] with ε^l · P_l(ε²) / d_l from a C1/C1′/C2 table.
fn fourier_in_eps2(table: &[f64; 18], eps: f64) -> Coeffs {
    let eps2 = sq(eps);
    let mut c = [0.0; NC];
    let mut d = eps;
    let mut o = 0;
    for (l, slot) in c.iter_mut().enumerate().skip(1) {
        let m = (ORDER - l) / 2;
        *slot = d * ratio(table, o, m, eps2);
        o += m + 2;
        d *= eps;
    }
    c
}

/// A1 − 1, Karney 2013 eq. 17.
pub(super) fn a1m1f(eps: f64) -> f64 {
    let m = ORDER / 2;
    let t = ratio(&A1M1_COEFF, 0, m, sq(eps));
    (t + eps) / (1.0 - eps)
}

/// C1[l], Karney 2013 eq. 18.
pub(super) fn c1f(eps: f64) -> Coeffs {
    fourier_in_eps2(&C1_COEFF, eps)
}

/// C1′[l], Karney 2013 eq. 21.
pub(super) fn c1pf(eps: f64) -> Coeffs {
    fourier_in_eps2(&C1P_COEFF, eps)
}

/// A2 − 1, Karney 2013 eq. 42.
pub(super) fn a2m1f(eps: f64) -> f64 {
    let m = ORDER / 2;
    let t = ratio(&A2M1_COEFF, 0, m, sq(eps));
    (t - eps) / (1.0 + eps)
}

/// C2[l], Karney 2013 eq. 43.
pub(super) fn c2f(eps: f64) -> Coeffs {
    fourier_in_eps2(&C2_COEFF, eps)
}

/// The n-dependent A3 coefficients, highest power of ε first.
pub(super) fn a3_coeff(n: f64) -> [f64; ORDER] {
    let mut out = [0.0; ORDER];
    let mut o = 0;
    for (k, slot) in out.iter_mut().enumerate() {
        let j = ORDER - 1 - k;
        let m = (ORDER - j - 1).min(j);
        *slot = ratio(&A3_COEFF, o, m, n);
        o += m + 2;
    }
    out
}

/// The n-dependent C3 coefficients.
pub(super) fn c3_coeff(n: f64) -> [f64; NC3X] {
    let mut out = [0.0; NC3X];
    let (mut o, mut k) = (0, 0);
    for l in 1..ORDER {
        for j in (l..ORDER).rev() {
            let m = (ORDER - j - 1).min(j);
            out[k] = ratio(&C3_COEFF, o, m, n);
            k += 1;
            o += m + 2;
        }
    }
    out
}

/// The n-dependent C4 coefficients.
pub(super) fn c4_coeff(n: f64) -> [f64; NC4X] {
    let mut out = [0.0; NC4X];
    let (mut o, mut k) = (0, 0);
    for l in 0..ORDER {
        for j in (l..ORDER).rev() {
            let m = ORDER - j - 1;
            out[k] = ratio(&C4_COEFF, o, m, n);
            k += 1;
            o += m + 2;
        }
    }
    out
}

/// A3(ε) from the prepared coefficients.
pub(super) fn a3f(a3x: &[f64; ORDER], eps: f64) -> f64 {
    polyval(a3x, eps)
}

/// C3[1..ORDER) at ε from the prepared coefficients.
pub(super) fn c3f(c3x: &[f64; NC3X], eps: f64) -> Coeffs {
    let mut c = [0.0; NC];
    let mut mult = 1.0;
    let mut o = 0;
    for (l, slot) in c.iter_mut().enumerate().take(ORDER).skip(1) {
        let m = ORDER - l - 1;
        mult *= eps;
        *slot = mult * polyval(&c3x[o..=o + m], eps);
        o += m + 1;
    }
    c
}

/// C4[0..ORDER) at ε from the prepared coefficients.
pub(super) fn c4f(c4x: &[f64; NC4X], eps: f64) -> Coeffs {
    let mut c = [0.0; NC];
    let mut mult = 1.0;
    let mut o = 0;
    for (l, slot) in c.iter_mut().enumerate().take(ORDER) {
        let m = ORDER - l - 1;
        *slot = mult * polyval(&c4x[o..=o + m], eps);
        o += m + 1;
        mult *= eps;
    }
    c
}

/// Clenshaw sum: Σ c[i] sin(2i x), i = 1..=n, when `sinp`, else
/// Σ c[i] cos((2i + 1) x), i = 0..n.
pub(super) fn sin_cos_series(sinp: bool, sinx: f64, cosx: f64, c: &Coeffs, n: usize) -> f64 {
    let mut idx = n + usize::from(sinp);
    let ar = 2.0 * (cosx - sinx) * (cosx + sinx);
    let mut y0 = if n % 2 == 1 {
        idx -= 1;
        c[idx]
    } else {
        0.0
    };
    let mut y1 = 0.0;
    for _ in 0..n / 2 {
        idx -= 1;
        y1 = ar * y0 - y1 + c[idx];
        idx -= 1;
        y0 = ar * y1 - y0 + c[idx];
    }
    if sinp {
        2.0 * sinx * cosx * y0
    } else {
        cosx * (y0 - y1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_lengths_match_loops() {
        let n = 0.001_679_220_386_383_705;
        assert_eq!(a3_coeff(n)[ORDER - 1], 1.0);
        assert!((a3_coeff(n)[ORDER - 2] - (n - 1.0) / 2.0).abs() < 1e-17);
        let _ = c3_coeff(n);
        let _ = c4_coeff(n);
    }

    #[test]
    fn zero_eps_gives_zero_series() {
        assert_eq!(a1m1f(0.0), 0.0);
        assert_eq!(a2m1f(0.0), 0.0);
        assert!(c1f(0.0).iter().all(|c| *c == 0.0));
    }

    #[test]
    fn sine_series_single_term() {
        let mut c = [0.0; NC];
        c[1] = 1.0;
        let x: f64 = 0.3;
        let got = sin_cos_series(true, x.sin(), x.cos(), &c, ORDER);
        assert!((got - (2.0 * x).sin()).abs() < 1e-15);
    }
}
