// SPDX-License-Identifier: Apache-2.0

//! Hotine Oblique Mercator, by IOGP Guidance Note 7-2 §3.2.4.
//!
//! Variant A (EPSG method 9812) measures u from the natural origin, the PROJ
//! `+no_uoff` form. Variant B (EPSG method 9815) measures u from the
//! projection centre, the PROJ default.

use core::f64::consts::{FRAC_PI_2, FRAC_PI_4};
use core::fmt::Write;

use serde::{Deserialize, Serialize};

use crate::def::Ellipsoid;
use crate::error::DomainReason;

/// |cos αc| below which the initial-line azimuth counts as 90°, where
/// GN7-2 replaces the uc formula with uc = A (λc − λ0).
const AZIMUTH_RIGHT_ANGLE_EPS: f64 = 1e-12;

/// Where u is measured from.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum HomVariant {
    /// EPSG 9812: from the natural origin. False easting and northing apply
    /// at the natural origin.
    A,
    /// EPSG 9815: from the projection centre. False easting and northing
    /// apply at the projection centre.
    B,
}

/// Defining parameters of one Hotine Oblique Mercator projection.
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
pub struct HomParams {
    pub variant: HomVariant,
    /// Latitude of the projection centre φc, degrees.
    pub lat_c_deg: f64,
    /// Longitude of the projection centre λc, degrees.
    pub lon_c_deg: f64,
    /// Azimuth of the initial line αc, degrees.
    pub alpha_c_deg: f64,
    /// Angle from the rectified grid to the skew grid γc, degrees.
    pub gamma_c_deg: f64,
    /// Scale factor on the initial line.
    pub k_c: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
    /// PROJ `+no_rot`: output the rectified (u, v) as (x, y) without the
    /// rotation by γc.
    pub no_rot: bool,
}

impl HomParams {
    /// Append the PROJ-string parameters, leading space included.
    pub fn write_proj(&self, out: &mut String) {
        out.push_str(" +proj=omerc");
        match self.variant {
            HomVariant::A => out.push_str(" +no_uoff"),
            HomVariant::B => {}
        }
        let _ = write!(
            out,
            " +lat_0={} +lonc={} +alpha={} +gamma={} +k={} +x_0={} +y_0={}",
            self.lat_c_deg,
            self.lon_c_deg,
            self.alpha_c_deg,
            self.gamma_c_deg,
            self.k_c,
            self.false_easting,
            self.false_northing
        );
        if self.no_rot {
            out.push_str(" +no_rot");
        }
    }
}

/// The GN7-2 constant D at the projection centre.
fn centre_d(lat_c_deg: f64, ellipsoid: &Ellipsoid) -> f64 {
    let e2 = ellipsoid.e2();
    let (sin_pc, cos_pc) = lat_c_deg.to_radians().sin_cos();
    let b = (1.0 + e2 * cos_pc.powi(4) / (1.0 - e2)).sqrt();
    b * (1.0 - e2).sqrt() / (cos_pc * (1.0 - e2 * sin_pc * sin_pc).sqrt())
}

/// γ0 from αc, degrees: asin(sin αc / D). PROJ uses it as γc when `+gamma`
/// is absent. NaN when no real angle exists.
pub fn gamma_from_alpha(lat_c_deg: f64, alpha_c_deg: f64, ellipsoid: &Ellipsoid) -> f64 {
    (alpha_c_deg.to_radians().sin() / centre_d(lat_c_deg, ellipsoid))
        .asin()
        .to_degrees()
}

/// αc from γ0, degrees: asin(D sin γ0). PROJ uses it when `+alpha` is
/// absent. NaN when no real angle exists.
pub fn alpha_from_gamma(lat_c_deg: f64, gamma_deg: f64, ellipsoid: &Ellipsoid) -> f64 {
    (centre_d(lat_c_deg, ellipsoid) * gamma_deg.to_radians().sin())
        .asin()
        .to_degrees()
}

/// A Hotine Oblique Mercator projection with its constants computed.
#[derive(Debug, Clone, PartialEq)]
pub struct HotineObliqueMercator {
    params: HomParams,
    e: f64,
    e2: f64,
    b: f64,
    a: f64,
    h: f64,
    sin_g0: f64,
    cos_g0: f64,
    sin_gc: f64,
    cos_gc: f64,
    /// λ0 in radians.
    lon0: f64,
    /// Offset of u for variant B: |uc| · sign(φc). Zero for variant A.
    u_offset: f64,
}

impl HotineObliqueMercator {
    pub fn new(params: &HomParams, ellipsoid: &Ellipsoid) -> Self {
        let (e, e2) = (ellipsoid.e(), ellipsoid.e2());
        let phi_c = params.lat_c_deg.to_radians();
        let lam_c = params.lon_c_deg.to_radians();
        let alpha_c = params.alpha_c_deg.to_radians();
        let gamma_c = params.gamma_c_deg.to_radians();
        let (sin_pc, cos_pc) = phi_c.sin_cos();
        let sign_pc = if phi_c < 0.0 { -1.0 } else { 1.0 };

        let b = (1.0 + e2 * cos_pc.powi(4) / (1.0 - e2)).sqrt();
        let a = ellipsoid.a * b * params.k_c * (1.0 - e2).sqrt() / (1.0 - e2 * sin_pc * sin_pc);
        let t0 = isometric_t(phi_c, e);
        let d = centre_d(params.lat_c_deg, ellipsoid);
        // GN7-2: when D < 1, take D² = 1 so F stays real.
        let d2 = (d * d).max(1.0);
        let f = d + (d2 - 1.0).sqrt() * sign_pc;
        let h = f * t0.powf(b);
        let g = (f - 1.0 / f) / 2.0;
        let gamma0 = (alpha_c.sin() / d).asin();
        let lon0 = lam_c - (g * gamma0.tan()).asin() / b;
        let u_offset = match params.variant {
            HomVariant::A => 0.0,
            HomVariant::B => {
                let uc = if alpha_c.cos().abs() < AZIMUTH_RIGHT_ANGLE_EPS {
                    a * (lam_c - lon0)
                } else {
                    a / b * ((d2 - 1.0).sqrt() / alpha_c.cos()).atan() * sign_pc
                };
                uc.abs() * sign_pc
            }
        };
        let (sin_g0, cos_g0) = gamma0.sin_cos();
        let (sin_gc, cos_gc) = gamma_c.sin_cos();
        Self {
            params: *params,
            e,
            e2,
            b,
            a,
            h,
            sin_g0,
            cos_g0,
            sin_gc,
            cos_gc,
            lon0,
            u_offset,
        }
    }

    /// Project λ, φ radians to easting, northing metres.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let t = isometric_t(phi, self.e);
        let q = self.h / t.powf(self.b);
        let s = (q - 1.0 / q) / 2.0;
        let tt = (q + 1.0 / q) / 2.0;
        let (v_sin, b_dlam_cos) = (self.b * (lam - self.lon0)).sin_cos();
        let u_big = (-v_sin * self.cos_g0 + s * self.sin_g0) / tt;
        if !u_big.is_finite() || u_big.abs() >= 1.0 {
            return Err(DomainReason::ObliqueSingularity);
        }
        let v = self.a * ((1.0 - u_big) / (1.0 + u_big)).ln() / (2.0 * self.b);
        let u = self.a * (s * self.cos_g0 + v_sin * self.sin_g0).atan2(b_dlam_cos) / self.b
            - self.u_offset;
        let (x, y) = if self.params.no_rot {
            (u, v)
        } else {
            (
                v * self.cos_gc + u * self.sin_gc,
                u * self.cos_gc - v * self.sin_gc,
            )
        };
        Ok((
            x + self.params.false_easting,
            y + self.params.false_northing,
        ))
    }

    /// Unproject easting, northing metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let de = x - self.params.false_easting;
        let dn = y - self.params.false_northing;
        let (v, u_rect) = if self.params.no_rot {
            (dn, de)
        } else {
            (
                de * self.cos_gc - dn * self.sin_gc,
                dn * self.cos_gc + de * self.sin_gc,
            )
        };
        let u = u_rect + self.u_offset;
        let q = (-self.b * v / self.a).exp();
        let s = (q - 1.0 / q) / 2.0;
        let tt = (q + 1.0 / q) / 2.0;
        let (v_sin, bu_cos) = (self.b * u / self.a).sin_cos();
        let u_big = (v_sin * self.cos_g0 + s * self.sin_g0) / tt;
        if !u_big.is_finite() || u_big.abs() >= 1.0 {
            return Err(DomainReason::ObliqueSingularity);
        }
        let t = (self.h / ((1.0 + u_big) / (1.0 - u_big)).sqrt()).powf(1.0 / self.b);
        let chi = FRAC_PI_2 - 2.0 * t.atan();
        let phi = latitude_from_conformal(chi, self.e2);
        let lam = self.lon0 - (s * self.cos_g0 - v_sin * self.sin_g0).atan2(bu_cos) / self.b;
        Ok((lam, phi))
    }
}

/// The GN7-2 t function: tan(π/4 − φ/2) / [(1 − e sin φ)/(1 + e sin φ)]^(e/2).
fn isometric_t(phi: f64, e: f64) -> f64 {
    let es = e * phi.sin();
    (FRAC_PI_4 - phi / 2.0).tan() / ((1.0 - es) / (1.0 + es)).powf(e / 2.0)
}

/// Geodetic latitude from conformal latitude χ, the GN7-2 series in e².
/// The first omitted term is of order e¹⁰ ≈ 1.4e-11 rad, under 0.1 mm.
fn latitude_from_conformal(chi: f64, e2: f64) -> f64 {
    let e4 = e2 * e2;
    let e6 = e4 * e2;
    let e8 = e6 * e2;
    chi + (2.0 * chi).sin() * (e2 / 2.0 + 5.0 * e4 / 24.0 + e6 / 12.0 + 13.0 * e8 / 360.0)
        + (4.0 * chi).sin() * (7.0 * e4 / 48.0 + 29.0 * e6 / 240.0 + 811.0 * e8 / 11520.0)
        + (6.0 * chi).sin() * (7.0 * e6 / 120.0 + 81.0 * e8 / 1120.0)
        + (8.0 * chi).sin() * (4279.0 * e8 / 161_280.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    /// RSO scale factor on the initial line, both Malaysian RSO grids.
    const RSO_K: f64 = 0.99984;

    /// EPSG:3375, GDM2000 Peninsula RSO.
    const PENINSULAR_RSO: HomParams = HomParams {
        variant: HomVariant::A,
        lat_c_deg: 4.0,
        lon_c_deg: 102.25,
        alpha_c_deg: 323.025_796_466_667,
        gamma_c_deg: 323.130_102_361_111,
        k_c: RSO_K,
        false_easting: 804_671.0,
        false_northing: 0.0,
        no_rot: false,
    };

    /// EPSG:3376, GDM2000 East Malaysia (Borneo) RSO.
    const BORNEO_RSO: HomParams = HomParams {
        variant: HomVariant::A,
        lat_c_deg: 4.0,
        lon_c_deg: 115.0,
        alpha_c_deg: 53.315_809_95,
        gamma_c_deg: 53.130_102_361_111_1,
        k_c: RSO_K,
        false_easting: 0.0,
        false_northing: 0.0,
        no_rot: false,
    };

    fn hom(params: HomParams) -> HotineObliqueMercator {
        HotineObliqueMercator::new(&params, &Ellipsoid::GRS80)
    }

    fn fwd(h: &HotineObliqueMercator, lon: f64, lat: f64) -> Result<[f64; 2], DomainReason> {
        h.forward(lon.to_radians(), lat.to_radians())
            .map(|(x, y)| [x, y])
    }

    fn assert_close(got: [f64; 2], want: [f64; 2]) {
        assert!(
            (got[0] - want[0]).abs() < MM && (got[1] - want[1]).abs() < MM,
            "got {got:?}, want {want:?}"
        );
    }

    // Reference values: PROJ 9.8.1 via pyproj, EPSG:4326 → target, always_xy.

    #[test]
    fn peninsular_rso_klcc() {
        let got = fwd(&hom(PENINSULAR_RSO), 101.7117, 3.1579).unwrap();
        assert_close(got, [412_834.468_1, 349_472.917_7]);
    }

    #[test]
    fn peninsular_rso_105_5() {
        let got = fwd(&hom(PENINSULAR_RSO), 105.0, 5.0).unwrap();
        assert_close(got, [778_104.319_5, 553_022.237_0]);
    }

    #[test]
    fn borneo_rso_kota_kinabalu() {
        let got = fwd(&hom(BORNEO_RSO), 116.0735, 5.9804).unwrap();
        assert_close(got, [708_697.150_6, 662_326.916_8]);
    }

    #[test]
    fn borneo_rso_105_5() {
        let got = fwd(&hom(BORNEO_RSO), 105.0, 5.0).unwrap();
        assert_close(got, [-521_596.204_7, 554_604.058_6]);
    }

    #[test]
    fn inverse_recovers_input_to_a_millimetre() {
        for h in [hom(PENINSULAR_RSO), hom(BORNEO_RSO)] {
            for [lon, lat] in [[101.7117, 3.1579], [116.0735, 5.9804], [105.0, 5.0]] {
                let projected = fwd(&h, lon, lat).unwrap();
                let (lam, phi) = h.inverse(projected[0], projected[1]).unwrap();
                let again = h.forward(lam, phi).map(|(x, y)| [x, y]).unwrap();
                assert_close(again, projected);
            }
        }
    }

    /// Variant B moves u to the projection centre: the centre itself maps to
    /// the false origin.
    #[test]
    fn variant_b_maps_centre_to_false_origin() {
        let params = HomParams {
            variant: HomVariant::B,
            false_easting: 0.0,
            false_northing: 0.0,
            ..PENINSULAR_RSO
        };
        let h = hom(params);
        let centre = fwd(&h, params.lon_c_deg, params.lat_c_deg).unwrap();
        assert_close(centre, [0.0, 0.0]);
        let (lam, phi) = h.inverse(centre[0], centre[1]).unwrap();
        assert!((lam.to_degrees() - params.lon_c_deg).abs() < 1e-9);
        assert!((phi.to_degrees() - params.lat_c_deg).abs() < 1e-9);
    }

    #[test]
    fn no_rot_inverts() {
        let h = hom(HomParams {
            no_rot: true,
            ..BORNEO_RSO
        });
        let p = fwd(&h, 116.0735, 5.9804).unwrap();
        let (lam, phi) = h.inverse(p[0], p[1]).unwrap();
        assert!((lam.to_degrees() - 116.0735).abs() < 1e-9);
        assert!((phi.to_degrees() - 5.9804).abs() < 1e-9);
    }

    #[test]
    fn pole_is_out_of_domain() {
        assert!(fwd(&hom(PENINSULAR_RSO), 102.0, 90.0).is_err());
    }

    /// The RSO γc is the rectified-grid angle of a square grid, close to γ0.
    #[test]
    fn gamma_and_alpha_invert_each_other() {
        let g = gamma_from_alpha(4.0, 53.315_809_95, &Ellipsoid::GRS80);
        let a = alpha_from_gamma(4.0, g, &Ellipsoid::GRS80);
        assert!((a - 53.315_809_95).abs() < 1e-9);
    }
}
