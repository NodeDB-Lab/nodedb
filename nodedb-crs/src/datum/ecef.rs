// SPDX-License-Identifier: Apache-2.0

//! Geodetic ↔ Earth-centred, Earth-fixed (ECEF) Cartesian coordinates.

use crate::def::Ellipsoid;

/// Iterations allowed for Bowring's latitude refinement. Each iteration
/// roughly triples the correct digits, so three reach double precision on
/// Earth-sized ellipsoids. The cap only bounds bad input.
const BOWRING_MAX_ITERS: usize = 8;

/// Latitude change in radians below which Bowring's iteration stops, about
/// 6 nm on the ground.
const LATITUDE_TOLERANCE_RAD: f64 = 1e-15;

/// λ, φ radians and h metres above `ellipsoid` to ECEF metres.
pub fn geodetic_to_ecef(lam: f64, phi: f64, h: f64, ellipsoid: &Ellipsoid) -> [f64; 3] {
    let e2 = ellipsoid.e2();
    let (sin_phi, cos_phi) = phi.sin_cos();
    let (sin_lam, cos_lam) = lam.sin_cos();
    let n = ellipsoid.a / (1.0 - e2 * sin_phi * sin_phi).sqrt();
    [
        (n + h) * cos_phi * cos_lam,
        (n + h) * cos_phi * sin_lam,
        (n * (1.0 - e2) + h) * sin_phi,
    ]
}

/// ECEF metres to λ, φ radians and h metres above `ellipsoid`, by Bowring's
/// iteration on the parametric latitude.
pub fn ecef_to_geodetic(xyz: [f64; 3], ellipsoid: &Ellipsoid) -> (f64, f64, f64) {
    let [x, y, z] = xyz;
    let a = ellipsoid.a;
    let b = ellipsoid.b();
    let e2 = ellipsoid.e2();
    let ep2 = e2 / (1.0 - e2);
    let p = x.hypot(y);
    let lam = y.atan2(x);

    let mut beta = (a * z).atan2(b * p);
    let mut phi = 0.0;
    for _ in 0..BOWRING_MAX_ITERS {
        let (sin_b, cos_b) = beta.sin_cos();
        let next = (z + ep2 * b * sin_b.powi(3)).atan2(p - e2 * a * cos_b.powi(3));
        let done = (next - phi).abs() < LATITUDE_TOLERANCE_RAD;
        phi = next;
        if done {
            break;
        }
        let (sin_p, cos_p) = phi.sin_cos();
        beta = (b * sin_p).atan2(a * cos_p);
    }
    let (sin_phi, cos_phi) = phi.sin_cos();
    let h = p * cos_phi + z * sin_phi - a * (1.0 - e2 * sin_phi * sin_phi).sqrt();
    (lam, phi, h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_below_a_micrometre() {
        for ell in [
            Ellipsoid::WGS84,
            Ellipsoid::EVEREST_1830_MODIFIED,
            Ellipsoid::sphere(6_371_007.181),
        ] {
            for (lon, lat, h) in [
                (101.7117, 3.1579, 45.0),
                (-75.0, -89.9, 1000.0),
                (179.9, 60.0, -100.0),
                (0.0, 0.0, 0.0),
                (12.0, 90.0, 10.0),
            ] {
                let xyz = geodetic_to_ecef(f64::to_radians(lon), f64::to_radians(lat), h, &ell);
                let (lam, phi, h2) = ecef_to_geodetic(xyz, &ell);
                let back = geodetic_to_ecef(lam, phi, h2, &ell);
                for (got, want) in back.iter().zip(xyz) {
                    assert!(
                        (got - want).abs() < 1e-6,
                        "{lon} {lat} {h}: {back:?} vs {xyz:?}"
                    );
                }
                assert!((h2 - h).abs() < 1e-6, "{h2} vs {h}");
            }
        }
    }

    /// The equator on the prime meridian lies at x = a.
    #[test]
    fn equator_point() {
        let xyz = geodetic_to_ecef(0.0, 0.0, 0.0, &Ellipsoid::WGS84);
        assert_eq!(xyz, [6_378_137.0, 0.0, 0.0]);
    }
}
