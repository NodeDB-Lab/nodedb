// SPDX-License-Identifier: Apache-2.0

//! Authalic latitude, as PROJ `pj_authalic_lat_q`, `pj_authalic_lat` and
//! `pj_authalic_lat_inverse` (PROJ 9.8.1 `src/latitudes.cpp`).
//!
//! For |n| < 0.01 both directions use Fourier series in n to order 6
//! (Karney, "On auxiliary latitudes", Survey Review 56 (2024)). For larger
//! |n| the forward uses β = asin(q / qp) (Snyder 1987 eqs. 3-11, 3-12) and
//! the inverse refines the series start by Newton's method.

use super::meridian::{AUX_ORDER, clenshaw, full_series, third_flattening};

/// Eccentricity below which q(φ) uses the spherical limit 2 sin φ, PROJ
/// `pj_authalic_lat_q`.
const SPHERE_ECCENTRICITY: f64 = 1e-7;

/// |n| below which the series are accurate to double precision, PROJ
/// `PROJ_AUTHALIC_SERIES_VALID`.
const SERIES_VALID_N: f64 = 0.01;

/// Newton iterations for the inverse at large |n|, PROJ
/// `pj_authalic_lat_inverse`.
const NEWTON_MAX_ITERS: usize = 10;

/// Newton step below which the inverse stops.
const NEWTON_TOLERANCE: f64 = 1e-15;

/// Karney 2024 matrix C[ξ,φ] (φ → ξ), as PROJ `pj_auxlat_coeffs`.
const C_XI_PHI: [f64; 21] = [
    -4.0 / 3.0,
    -4.0 / 45.0,
    88.0 / 315.0,
    538.0 / 4725.0,
    20824.0 / 467_775.0,
    -44732.0 / 2_837_835.0,
    34.0 / 45.0,
    8.0 / 105.0,
    -2482.0 / 14175.0,
    -37192.0 / 467_775.0,
    -12_467_764.0 / 212_837_625.0,
    -1532.0 / 2835.0,
    -898.0 / 14175.0,
    54968.0 / 467_775.0,
    100_320_856.0 / 1_915_538_625.0,
    6007.0 / 14175.0,
    24496.0 / 467_775.0,
    -5_884_124.0 / 70_945_875.0,
    -23356.0 / 66825.0,
    -839_792.0 / 19_348_875.0,
    570_284_222.0 / 1_915_538_625.0,
];

/// Karney 2024 matrix C[φ,ξ] (ξ → φ).
const C_PHI_XI: [f64; 21] = [
    4.0 / 3.0,
    4.0 / 45.0,
    -16.0 / 35.0,
    -2582.0 / 14175.0,
    60136.0 / 467_775.0,
    28_112_932.0 / 212_837_625.0,
    46.0 / 45.0,
    152.0 / 945.0,
    -11966.0 / 14175.0,
    -21016.0 / 51975.0,
    251_310_128.0 / 638_512_875.0,
    3044.0 / 2835.0,
    3802.0 / 14175.0,
    -94388.0 / 66825.0,
    -8_797_648.0 / 10_945_935.0,
    6059.0 / 4725.0,
    41072.0 / 93555.0,
    -1_472_637_812.0 / 638_512_875.0,
    768_272.0 / 467_775.0,
    455_935_736.0 / 638_512_875.0,
    4_210_684_958.0 / 1_915_538_625.0,
];

/// q(φ) (Snyder 3-12, PROJ pj_qsfn).
///
/// Returns +∞ where 1 − e² sin² φ is zero, as PROJ does.
pub fn qsfn(sinphi: f64, e: f64, one_es: f64) -> f64 {
    if e < SPHERE_ECCENTRICITY {
        return 2.0 * sinphi;
    }
    let e_sinphi = e * sinphi;
    let one_minus = 1.0 - e_sinphi * e_sinphi;
    if one_minus == 0.0 {
        return f64::INFINITY;
    }
    one_es * (sinphi / one_minus + e_sinphi.atanh() / e)
}

/// Geodetic ↔ authalic latitude conversion for one ellipsoid.
#[derive(Debug, Clone, PartialEq)]
pub struct Authalic {
    e: f64,
    es: f64,
    one_es: f64,
    /// Whether |n| is small enough for the series in both directions.
    series: bool,
    /// q at φ = 90°.
    qp: f64,
    /// φ → ξ Fourier coefficients.
    to_xi: [f64; AUX_ORDER],
    /// ξ → φ Fourier coefficients.
    from_xi: [f64; AUX_ORDER],
}

impl Authalic {
    pub fn new(e2: f64) -> Self {
        let n = third_flattening(e2);
        let e = e2.sqrt();
        let one_es = 1.0 - e2;
        Self {
            e,
            es: e2,
            one_es,
            series: n.abs() < SERIES_VALID_N,
            qp: qsfn(1.0, e, one_es),
            to_xi: full_series(n, &C_XI_PHI),
            from_xi: full_series(n, &C_PHI_XI),
        }
    }

    /// q at the pole, qp = q(90°).
    pub fn qp(&self) -> f64 {
        self.qp
    }

    /// q(φ) of this ellipsoid from sin φ.
    pub fn q(&self, sinphi: f64) -> f64 {
        qsfn(sinphi, self.e, self.one_es)
    }

    /// Geodetic → authalic latitude, PROJ `pj_authalic_lat`.
    pub fn to_authalic(&self, phi: f64) -> f64 {
        let (sinphi, cosphi) = phi.sin_cos();
        if self.series {
            return phi + clenshaw(sinphi, cosphi, &self.to_xi);
        }
        // q / qp is ill-conditioned near the poles. Rounding past ±1 clamps.
        (self.q(sinphi) / self.qp).clamp(-1.0, 1.0).asin()
    }

    /// Authalic → geodetic latitude (PROJ pj_authlat, `pj_authalic_lat_inverse`).
    pub fn from_authalic(&self, beta: f64) -> f64 {
        let mut phi = beta + clenshaw(beta.sin(), beta.cos(), &self.from_xi);
        if self.series {
            return phi;
        }
        // Newton on qp sin β / (1 − e²) − q(φ) / (1 − e²) = 0, PROJ
        // `pj_authalic_lat_inverse`.
        let q_div_one_es = beta.sin() * self.qp / self.one_es;
        for _ in 0..NEWTON_MAX_ITERS {
            let (sinphi, cosphi) = phi.sin_cos();
            let w = 1.0 - self.es * sinphi * sinphi;
            let dphi = w * w / (2.0 * cosphi)
                * (q_div_one_es - sinphi / w - (self.e * sinphi).atanh() / self.e);
            if dphi.is_nan() || dphi.abs() < NEWTON_TOLERANCE {
                break;
            }
            phi += dphi;
        }
        phi
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::Ellipsoid;

    #[test]
    fn round_trip_on_grs80() {
        let a = Authalic::new(Ellipsoid::GRS80.e2());
        for deg in [-90.0, -60.0, -1.0, 0.0, 10.5, 45.0, 89.999, 90.0] {
            let phi = f64::to_radians(deg);
            let back = a.from_authalic(a.to_authalic(phi));
            assert!((back - phi).abs() < 1e-14, "{deg}");
        }
    }

    #[test]
    fn series_matches_closed_form() {
        let e = Ellipsoid::WGS84;
        let a = Authalic::new(e.e2());
        for deg in [5.0, 30.0, 52.0, 75.0] {
            let phi = f64::to_radians(deg);
            let closed = (a.q(phi.sin()) / a.qp()).asin();
            assert!((a.to_authalic(phi) - closed).abs() < 1e-14, "{deg}");
        }
    }

    /// An ellipsoid with |n| ≥ 0.01 takes the closed form and Newton path.
    #[test]
    fn large_flattening_round_trips() {
        let a = Authalic::new(
            Ellipsoid {
                a: 1.0,
                inv_f: 30.0,
            }
            .e2(),
        );
        assert!(!a.series);
        for deg in [-80.0, -20.0, 0.0, 33.0, 70.0] {
            let phi = f64::to_radians(deg);
            let back = a.from_authalic(a.to_authalic(phi));
            assert!((back - phi).abs() < 1e-13, "{deg}");
        }
    }

    #[test]
    fn sphere_is_identity() {
        let a = Authalic::new(0.0);
        assert_eq!(a.qp(), 2.0);
        assert_eq!(a.to_authalic(0.7), 0.7);
        assert_eq!(a.from_authalic(0.7), 0.7);
        assert_eq!(qsfn(0.5, 0.0, 1.0), 1.0);
    }

    /// qp for GRS 80, Snyder 1987 eq. 3-12 at φ = 90°.
    #[test]
    fn grs80_qp() {
        let a = Authalic::new(Ellipsoid::GRS80.e2());
        assert!((a.qp() - 1.995_531_087_7).abs() < 1e-9, "{}", a.qp());
    }
}
