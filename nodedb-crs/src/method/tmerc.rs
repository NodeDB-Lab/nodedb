// SPDX-License-Identifier: Apache-2.0

//! Transverse Mercator, by the Krüger series to n⁶.
//!
//! Follows C. F. F. Karney, "Transverse Mercator with an accuracy of a few
//! nanometers", J. Geodesy 85 (2011), eqs. 7–9 and 35–36. Within 35° of the
//! central meridian the series errors stay far below a millimetre.

use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::DomainReason;

/// Largest |λ − λ0| in degrees that the projection accepts. Karney 2011
/// bounds the n⁶ series error below 5 nm inside 35° of the central meridian.
pub const TM_MAX_LON_OFFSET_DEG: f64 = 35.0;

/// Newton iterations allowed when inverting τ' → τ. Karney 2011 §2.2 reports
/// convergence to double precision in two. The cap only bounds bad input.
pub const TAU_NEWTON_MAX_ITERS: usize = 10;

/// Newton step on τ below which the inverse stops. A step this small moves
/// latitude by under 1e-14 rad, about 64 nm on the ground.
pub const TAU_TOLERANCE: f64 = 1e-14;

/// Defining parameters of one Transverse Mercator projection.
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
pub struct TmParams {
    /// Latitude of natural origin φ0, degrees.
    pub lat0_deg: f64,
    /// Central meridian λ0, degrees.
    pub lon0_deg: f64,
    /// Scale factor on the central meridian.
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl TmParams {
    /// Append the PROJ-string parameters, leading space included.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=tmerc +lat_0={} +lon_0={} +k={} +x_0={} +y_0={}",
            self.lat0_deg, self.lon0_deg, self.k0, self.false_easting, self.false_northing
        );
    }
}

/// A Transverse Mercator projection with its series coefficients computed.
#[derive(Debug, Clone, PartialEq)]
pub struct TransverseMercator {
    params: TmParams,
    /// λ0 in radians.
    lon0: f64,
    e: f64,
    e2: f64,
    /// k0 · A, where A is the rectifying radius.
    k0_a: f64,
    /// ξ at the natural origin, so φ0 maps to the false northing.
    xi0: f64,
    /// Krüger α coefficients (forward).
    alpha: [f64; 6],
    /// Krüger β coefficients (inverse).
    beta: [f64; 6],
}

impl TransverseMercator {
    pub fn new(params: &TmParams, ellipsoid: &Ellipsoid) -> Self {
        let n = ellipsoid.n();
        let n2 = n * n;
        let n3 = n2 * n;
        let n4 = n3 * n;
        let n5 = n4 * n;
        let n6 = n5 * n;
        // Rectifying radius A, Karney 2011 eq. 14.
        let a_rect = ellipsoid.a / (1.0 + n) * (1.0 + n2 / 4.0 + n4 / 64.0 + n6 / 256.0);
        // Karney 2011 eq. 35.
        let alpha = [
            n / 2.0 - 2.0 / 3.0 * n2 + 5.0 / 16.0 * n3 + 41.0 / 180.0 * n4 - 127.0 / 288.0 * n5
                + 7891.0 / 37800.0 * n6,
            13.0 / 48.0 * n2 - 3.0 / 5.0 * n3 + 557.0 / 1440.0 * n4 + 281.0 / 630.0 * n5
                - 1_983_433.0 / 1_935_360.0 * n6,
            61.0 / 240.0 * n3 - 103.0 / 140.0 * n4
                + 15061.0 / 26880.0 * n5
                + 167_603.0 / 181_440.0 * n6,
            49561.0 / 161_280.0 * n4 - 179.0 / 168.0 * n5 + 6_601_661.0 / 7_257_600.0 * n6,
            34729.0 / 80640.0 * n5 - 3_418_889.0 / 1_995_840.0 * n6,
            212_378_941.0 / 319_334_400.0 * n6,
        ];
        // Karney 2011 eq. 36.
        let beta = [
            n / 2.0 - 2.0 / 3.0 * n2 + 37.0 / 96.0 * n3 - 1.0 / 360.0 * n4 - 81.0 / 512.0 * n5
                + 96199.0 / 604_800.0 * n6,
            1.0 / 48.0 * n2 + 1.0 / 15.0 * n3 - 437.0 / 1440.0 * n4 + 46.0 / 105.0 * n5
                - 1_118_711.0 / 3_870_720.0 * n6,
            17.0 / 480.0 * n3 - 37.0 / 840.0 * n4 - 209.0 / 4480.0 * n5 + 5569.0 / 90720.0 * n6,
            4397.0 / 161_280.0 * n4 - 11.0 / 504.0 * n5 - 830_251.0 / 7_257_600.0 * n6,
            4583.0 / 161_280.0 * n5 - 108_847.0 / 3_991_680.0 * n6,
            20_648_693.0 / 638_668_800.0 * n6,
        ];
        let e = ellipsoid.e();
        // On the central meridian η' = 0, so ξ = ξ' + Σ αj sin(2j ξ').
        let xi0_p = conformal_tau(params.lat0_deg.to_radians().tan(), e).atan();
        let xi0 = xi0_p
            + alpha
                .iter()
                .enumerate()
                .map(|(j, a)| a * (2.0 * (j + 1) as f64 * xi0_p).sin())
                .sum::<f64>();
        Self {
            params: *params,
            lon0: params.lon0_deg.to_radians(),
            e,
            e2: ellipsoid.e2(),
            k0_a: params.k0 * a_rect,
            xi0,
            alpha,
            beta,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dlon = wrap_pi(lam - self.lon0);
        if dlon.abs() > TM_MAX_LON_OFFSET_DEG.to_radians() {
            return Err(DomainReason::CentralMeridianOffset);
        }
        let tau_p = conformal_tau(phi.tan(), self.e);
        let xi_p = tau_p.atan2(dlon.cos());
        let eta_p = (dlon.sin() / tau_p.hypot(dlon.cos())).asinh();
        let mut xi = xi_p;
        let mut eta = eta_p;
        for (j, a) in self.alpha.iter().enumerate() {
            let k = 2.0 * (j + 1) as f64;
            xi += a * (k * xi_p).sin() * (k * eta_p).cosh();
            eta += a * (k * xi_p).cos() * (k * eta_p).sinh();
        }
        Ok((
            self.params.false_easting + self.k0_a * eta,
            self.params.false_northing + self.k0_a * (xi - self.xi0),
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let xi = (y - self.params.false_northing) / self.k0_a + self.xi0;
        let eta = (x - self.params.false_easting) / self.k0_a;
        let mut xi_p = xi;
        let mut eta_p = eta;
        for (j, b) in self.beta.iter().enumerate() {
            let k = 2.0 * (j + 1) as f64;
            xi_p -= b * (k * xi).sin() * (k * eta).cosh();
            eta_p -= b * (k * xi).cos() * (k * eta).sinh();
        }
        let tau_p = xi_p.sin() / eta_p.sinh().hypot(xi_p.cos());
        let dlon = eta_p.sinh().atan2(xi_p.cos());
        if dlon.abs() > TM_MAX_LON_OFFSET_DEG.to_radians() {
            return Err(DomainReason::CentralMeridianOffset);
        }
        let tau = self.tau_from_conformal(tau_p);
        Ok((self.lon0 + dlon, tau.atan()))
    }

    /// Solve τ' = τ'(τ) for τ by Newton's method, Karney 2011 eqs. 19–21.
    fn tau_from_conformal(&self, tau_p: f64) -> f64 {
        let one_minus_e2 = 1.0 - self.e2;
        let mut tau = tau_p;
        for _ in 0..TAU_NEWTON_MAX_ITERS {
            let tau_i_p = conformal_tau(tau, self.e);
            let step = (tau_p - tau_i_p) / (1.0 + tau_i_p * tau_i_p).sqrt()
                * (1.0 + one_minus_e2 * tau * tau)
                / (one_minus_e2 * (1.0 + tau * tau).sqrt());
            tau += step;
            if step.abs() < TAU_TOLERANCE {
                break;
            }
        }
        tau
    }
}

/// Conformal-latitude tangent τ' from geodetic τ = tan φ, Karney 2011 eq. 7.
fn conformal_tau(tau: f64, e: f64) -> f64 {
    let tau1 = (1.0 + tau * tau).sqrt();
    let sigma = (e * (e * tau / tau1).atanh()).sinh();
    tau * (1.0 + sigma * sigma).sqrt() - sigma * tau1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    fn utm_north(lon0_deg: f64) -> TransverseMercator {
        let params = TmParams {
            lat0_deg: 0.0,
            lon0_deg,
            k0: 0.9996,
            false_easting: 500_000.0,
            false_northing: 0.0,
        };
        TransverseMercator::new(&params, &Ellipsoid::WGS84)
    }

    fn fwd(tm: &TransverseMercator, lon: f64, lat: f64) -> [f64; 2] {
        let (x, y) = tm.forward(lon.to_radians(), lat.to_radians()).unwrap();
        [x, y]
    }

    fn assert_close(got: [f64; 2], want: [f64; 2]) {
        assert!(
            (got[0] - want[0]).abs() < MM && (got[1] - want[1]).abs() < MM,
            "got {got:?}, want {want:?}"
        );
    }

    // Reference values: PROJ 9.8.1 via pyproj, EPSG:4326 → target, always_xy.

    #[test]
    fn utm47n_klcc() {
        assert_close(
            fwd(&utm_north(99.0), 101.7117, 3.1579),
            [801_401.794_9, 349_439.510_6],
        );
    }

    #[test]
    fn utm48n_klcc() {
        assert_close(
            fwd(&utm_north(105.0), 101.7117, 3.1579),
            [134_445.592_4, 349_624.727_8],
        );
    }

    #[test]
    fn utm48n_origin_and_five_north() {
        let tm = utm_north(105.0);
        assert_close(fwd(&tm, 105.0, 0.0), [500_000.0, 0.0]);
        assert_close(fwd(&tm, 105.0, 5.0), [500_000.0, 552_664.296_9]);
    }

    /// Landsat keeps north zones south of the equator, so northings go
    /// negative and still invert.
    #[test]
    fn north_zone_accepts_negative_northing() {
        let tm = utm_north(105.0);
        let [x, y] = fwd(&tm, 105.0, -5.0);
        assert_close([x, y], [500_000.0, -552_664.296_9]);
        let (lam, phi) = tm.inverse(x, y).unwrap();
        assert!((lam.to_degrees() - 105.0).abs() < 1e-9);
        assert!((phi.to_degrees() + 5.0).abs() < 1e-9);
    }

    #[test]
    fn origin_latitude_maps_to_false_northing() {
        let params = TmParams {
            lat0_deg: 4.0,
            lon0_deg: 102.0,
            k0: 0.99995,
            false_easting: 40_000.0,
            false_northing: 10_000.0,
        };
        let tm = TransverseMercator::new(&params, &Ellipsoid::GRS80);
        assert_close(fwd(&tm, 102.0, 4.0), [40_000.0, 10_000.0]);
        let (lam, phi) = tm.inverse(40_000.0, 10_000.0).unwrap();
        assert!((lam.to_degrees() - 102.0).abs() < 1e-12);
        assert!((phi.to_degrees() - 4.0).abs() < 1e-12);
    }

    #[test]
    fn inverse_recovers_input() {
        let tm = utm_north(105.0);
        for [lon, lat] in [
            [101.7117, 3.1579],
            [105.0, 0.0],
            [109.5, 6.2],
            [100.0, -2.0],
        ] {
            let [x, y] = fwd(&tm, lon, lat);
            let (lam, phi) = tm.inverse(x, y).unwrap();
            assert!(
                (lam.to_degrees() - lon).abs() < 1e-9 && (phi.to_degrees() - lat).abs() < 1e-9,
                "{lon} {lat}"
            );
        }
    }

    #[test]
    fn far_from_central_meridian_is_out_of_domain() {
        let err = utm_north(105.0).forward(150f64.to_radians(), 3f64.to_radians());
        assert_eq!(err, Err(DomainReason::CentralMeridianOffset));
    }

    #[test]
    fn sphere_is_accepted() {
        let params = TmParams {
            lat0_deg: 0.0,
            lon0_deg: 0.0,
            k0: 1.0,
            false_easting: 0.0,
            false_northing: 0.0,
        };
        let tm = TransverseMercator::new(&params, &Ellipsoid::sphere(6_371_000.0));
        let (x, y) = tm.forward(0.1, 0.2).unwrap();
        let (lam, phi) = tm.inverse(x, y).unwrap();
        assert!((lam - 0.1).abs() < 1e-12 && (phi - 0.2).abs() < 1e-12);
    }
}
