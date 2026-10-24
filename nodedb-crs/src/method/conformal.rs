// SPDX-License-Identifier: Apache-2.0

//! Conformal-latitude helpers shared by the conformal projections.
//!
//! Follows PROJ 9.8.1 `src/msfn.cpp`, `src/tsfn.cpp` and `src/phi2.cpp`,
//! and Snyder, "Map Projections: A Working Manual" (USGS PP 1395, 1987).

use crate::error::DomainReason;

/// Newton iterations allowed in [`tanphi_from_sinhpsi`]. PROJ `phi2.cpp`
/// uses 5. Two reach double precision for terrestrial flattening.
pub const SINHPSI_MAX_ITERS: usize = 5;

/// τ' above which the Newton start uses τ = exp(e·atanh e)·τ'. PROJ
/// `phi2.cpp`: 70 corresponds to χ = 89.18°.
const LARGE_TAUP: f64 = 70.0;

/// m(φ) = cos φ / sqrt(1 − e² sin² φ) (Snyder 14-15, PROJ `pj_msfn`).
pub fn msfn(sinphi: f64, cosphi: f64, e2: f64) -> f64 {
    cosphi / (1.0 - e2 * sinphi * sinphi).sqrt()
}

/// t(φ) = tan(π/4 − φ/2) / ((1 − e sin φ)/(1 + e sin φ))^(e/2) (Snyder
/// 15-9, PROJ `pj_tsfn`). Computed as exp(−ψ), ψ the isometric latitude,
/// in the form PROJ uses for full accuracy on both hemispheres.
pub fn tsfn(phi: f64, sinphi: f64, e: f64) -> f64 {
    let cosphi = phi.cos();
    let geo = if sinphi > 0.0 {
        cosphi / (1.0 + sinphi)
    } else {
        (1.0 - sinphi) / cosphi
    };
    (e * (e * sinphi).atanh()).exp() * geo
}

/// tan φ from τ' = sinh ψ = tan χ, by Newton's method (Karney 2011 eq. 7,
/// PROJ `pj_sinhpsi2tanphi`). Err(NoConvergence) when the iteration does
/// not settle.
pub fn tanphi_from_sinhpsi(taup: f64, e: f64) -> Result<f64, DomainReason> {
    let rooteps = f64::EPSILON.sqrt();
    let tol = rooteps / 10.0;
    let tmax = 2.0 / rooteps;
    let e2m = 1.0 - e * e;
    let stol = tol * taup.abs().max(1.0);
    let mut tau = if taup.abs() > LARGE_TAUP {
        taup * (e * e.atanh()).exp()
    } else {
        taup / e2m
    };
    // Covers ±∞, NaN and e = 1, as in PROJ.
    if tau.is_nan() || tau.abs() >= tmax {
        return Ok(tau);
    }
    for _ in 0..SINHPSI_MAX_ITERS {
        let tau1 = (1.0 + tau * tau).sqrt();
        let sig = (e * (e * tau / tau1).atanh()).sinh();
        let taupa = (1.0 + sig * sig).sqrt() * tau - sig * tau1;
        let dtau =
            (taup - taupa) * (1.0 + e2m * tau * tau) / (e2m * tau1 * (1.0 + taupa * taupa).sqrt());
        tau += dtau;
        if dtau.is_nan() || dtau.abs() < stol {
            return Ok(tau);
        }
    }
    Err(DomainReason::NoConvergence)
}

/// Inverse of [`tsfn`] (Snyder 7-9, PROJ `pj_phi2`): φ from t = exp(−ψ).
/// Err(NoConvergence) if the iteration fails.
pub fn phi_from_ts(ts: f64, e: f64) -> Result<f64, DomainReason> {
    Ok(tanphi_from_sinhpsi((1.0 / ts - ts) / 2.0, e)?.atan())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::Ellipsoid;

    #[test]
    fn ts_round_trips() {
        let e = Ellipsoid::WGS84.e();
        for deg in [-89.9, -60.0, -1.0, 0.0, 0.5, 45.0, 85.0, 89.99] {
            let phi = f64::to_radians(deg);
            let ts = tsfn(phi, phi.sin(), e);
            let back = phi_from_ts(ts, e).unwrap();
            assert!((back - phi).abs() < 1e-14, "{deg}: {back}");
        }
    }

    #[test]
    fn sphere_ts_is_tan_half_colatitude() {
        let phi = 0.7_f64;
        let want = (core::f64::consts::FRAC_PI_4 - phi / 2.0).tan();
        assert!((tsfn(phi, phi.sin(), 0.0) - want).abs() < 1e-15);
        assert!((phi_from_ts(want, 0.0).unwrap() - phi).abs() < 1e-15);
    }

    #[test]
    fn msfn_on_equator_is_one() {
        assert_eq!(msfn(0.0, 1.0, Ellipsoid::GRS80.e2()), 1.0);
        let phi = 0.5_f64;
        assert_eq!(msfn(phi.sin(), phi.cos(), 0.0), phi.cos());
    }

    #[test]
    fn pole_and_infinite_tau_pass_through() {
        let e = Ellipsoid::WGS84.e();
        assert_eq!(tanphi_from_sinhpsi(f64::INFINITY, e), Ok(f64::INFINITY));
        assert_eq!(tanphi_from_sinhpsi(0.0, e), Ok(0.0));
    }
}
