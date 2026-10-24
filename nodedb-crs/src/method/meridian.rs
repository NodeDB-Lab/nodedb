// SPDX-License-Identifier: Apache-2.0

//! Meridian arc length and its inverse, as PROJ `pj_enfn` / `pj_mlfn` /
//! `pj_inv_mlfn` (PROJ 9.8.1 `src/mlfn.cpp`, `src/latitudes.cpp`).
//!
//! The arc is A · μ, with A the rectifying radius over a and μ the
//! rectifying latitude. Both conversions φ ↔ μ are Fourier series whose
//! coefficients are Taylor series in the third flattening n to order 6
//! (C. F. F. Karney, "On auxiliary latitudes", Survey Review 56 (2024),
//! eqs. A1–A28). Full double precision for |f| ≤ 1/150.

use crate::error::DomainReason;

/// Number of Fourier terms, PROJ `AuxLat::ORDER`.
pub(crate) const AUX_ORDER: usize = 6;

/// Karney 2024 matrix C[μ,φ] (φ → μ), even powers of n only, as PROJ
/// `pj_auxlat_coeffs`.
const C_MU_PHI: [f64; 12] = [
    -3.0 / 2.0,
    9.0 / 16.0,
    -3.0 / 32.0,
    15.0 / 16.0,
    -15.0 / 32.0,
    135.0 / 2048.0,
    -35.0 / 48.0,
    105.0 / 256.0,
    315.0 / 512.0,
    -189.0 / 512.0,
    -693.0 / 1280.0,
    1001.0 / 2048.0,
];

/// Karney 2024 matrix C[φ,μ] (μ → φ), even powers of n only.
const C_PHI_MU: [f64; 12] = [
    3.0 / 2.0,
    -27.0 / 32.0,
    269.0 / 512.0,
    21.0 / 16.0,
    -55.0 / 32.0,
    6759.0 / 4096.0,
    151.0 / 96.0,
    -417.0 / 128.0,
    1097.0 / 512.0,
    -15543.0 / 2560.0,
    8011.0 / 2560.0,
    293_393.0 / 61440.0,
];

/// Rectifying-radius series in n², ((2k − 3)!! / (2k)!!)², PROJ
/// `pj_rectifying_radius`.
const RECTIFYING_RADIUS: [f64; 4] = [1.0, 1.0 / 4.0, 1.0 / 64.0, 1.0 / 256.0];

/// Meridian arc length from the equator (PROJ pj_enfn / pj_mlfn / pj_inv_mlfn).
#[derive(Debug, Clone, PartialEq)]
pub struct MeridianArc {
    /// Rectifying radius over a.
    radius: f64,
    /// φ → μ Fourier coefficients.
    to_mu: [f64; AUX_ORDER],
    /// μ → φ Fourier coefficients.
    from_mu: [f64; AUX_ORDER],
}

impl MeridianArc {
    pub fn new(e2: f64) -> Self {
        let n = third_flattening(e2);
        Self {
            radius: polyval(n * n, &RECTIFYING_RADIUS) / (1.0 + n),
            to_mu: even_series(n, &C_MU_PHI),
            from_mu: even_series(n, &C_PHI_MU),
        }
    }

    /// Arc length in units of the semi-major axis.
    pub fn arc(&self, phi: f64, sinphi: f64, cosphi: f64) -> f64 {
        self.radius * (phi + clenshaw(sinphi, cosphi, &self.to_mu))
    }

    /// Latitude of an arc length. Err(NoConvergence) if it fails.
    ///
    /// The series inverse is closed-form, so the only failure is a
    /// non-finite input.
    pub fn inverse(&self, arc: f64) -> Result<f64, DomainReason> {
        let mu = arc / self.radius;
        let phi = mu + clenshaw(mu.sin(), mu.cos(), &self.from_mu);
        if phi.is_finite() {
            Ok(phi)
        } else {
            Err(DomainReason::NoConvergence)
        }
    }
}

/// Third flattening n = (a − b)/(a + b) from e², written e² / (1 + √(1 − e²))²
/// to keep precision for small e².
pub(crate) fn third_flattening(e2: f64) -> f64 {
    let s = 1.0 + (1.0 - e2).sqrt();
    e2 / (s * s)
}

/// Σ p[i] xⁱ by Horner's method, PROJ `pj_polyval`.
pub(crate) fn polyval(x: f64, p: &[f64]) -> f64 {
    match p.split_last() {
        None => 0.0,
        Some((last, rest)) => rest.iter().rev().fold(*last, |y, c| y * x + c),
    }
}

/// Fourier coefficients F[l] = n^(l+1) · P_l(n²) for a conversion among φ
/// and μ, PROJ `pj_auxlat_coeffs` (the n² branch).
fn even_series(n: f64, coeffs: &[f64; 12]) -> [f64; AUX_ORDER] {
    let n2 = n * n;
    let mut out = [0.0; AUX_ORDER];
    let mut d = n;
    let mut o = 0;
    for (l, slot) in out.iter_mut().enumerate() {
        let m = (AUX_ORDER - l - 1) / 2;
        *slot = d * polyval(n2, &coeffs[o..=o + m]);
        o += m + 1;
        d *= n;
    }
    out
}

/// Fourier coefficients F[l] = n^(l+1) · P_l(n) for a conversion that
/// involves the authalic or conformal latitude, PROJ `pj_auxlat_coeffs`.
pub(crate) fn full_series(n: f64, coeffs: &[f64; 21]) -> [f64; AUX_ORDER] {
    let mut out = [0.0; AUX_ORDER];
    let mut d = n;
    let mut o = 0;
    for (l, slot) in out.iter_mut().enumerate() {
        let m = AUX_ORDER - l - 1;
        *slot = d * polyval(n, &coeffs[o..=o + m]);
        o += m + 1;
        d *= n;
    }
    out
}

/// Σ F[k] sin((2k + 2) ζ) by Clenshaw summation, PROJ `pj_clenshaw`.
pub(crate) fn clenshaw(szeta: f64, czeta: f64, f: &[f64; AUX_ORDER]) -> f64 {
    let x = 2.0 * (czeta - szeta) * (czeta + szeta);
    let (mut u0, mut u1) = (0.0, 0.0);
    for c in f.iter().rev() {
        let t = x * u0 - u1 + c;
        u1 = u0;
        u0 = t;
    }
    2.0 * szeta * czeta * u0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::Ellipsoid;
    use core::f64::consts::FRAC_PI_2;

    /// GRS 80 quarter meridian, 10 001 965.729 m (Moritz, Geodetic Reference
    /// System 1980, Bulletin Géodésique 54 (1980)).
    const GRS80_QUARTER_MERIDIAN_M: f64 = 10_001_965.729_3;

    #[test]
    fn grs80_quarter_meridian() {
        let e = Ellipsoid::GRS80;
        let m = MeridianArc::new(e.e2());
        let q = m.arc(FRAC_PI_2, 1.0, 0.0) * e.a;
        assert!((q - GRS80_QUARTER_MERIDIAN_M).abs() < 1e-3, "{q}");
    }

    #[test]
    fn inverse_recovers_latitude() {
        let m = MeridianArc::new(Ellipsoid::WGS84.e2());
        for deg in [-89.9, -45.0, -3.0, 0.0, 2.121_679_744, 33.3, 80.0, 90.0] {
            let phi = f64::to_radians(deg);
            let s = m.arc(phi, phi.sin(), phi.cos());
            let back = m.inverse(s).unwrap();
            assert!((back - phi).abs() < 1e-15, "{deg}");
        }
    }

    #[test]
    fn sphere_arc_is_latitude() {
        let m = MeridianArc::new(0.0);
        assert_eq!(m.arc(0.3, 0.3f64.sin(), 0.3f64.cos()), 0.3);
        assert_eq!(m.inverse(0.3), Ok(0.3));
        assert_eq!(m.inverse(f64::NAN), Err(DomainReason::NoConvergence));
    }
}
