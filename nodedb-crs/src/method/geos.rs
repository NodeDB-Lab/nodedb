// SPDX-License-Identifier: Apache-2.0

//! Geostationary Satellite View, by PROJ `src/projections/geos.cpp`.
//!
//! x and y are the satellite's scanning angles times its height above the
//! ellipsoid. `+sweep=y` (the default, Meteosat convention) scans y first;
//! `+sweep=x` (GOES convention) scans x first.

use core::fmt::Write;

use serde::{Deserialize, Serialize};

use super::angle::wrap_pi;
use crate::def::Ellipsoid;
use crate::error::{CrsParseError, DomainReason};
use crate::parse::tokens::ProjParams;

/// The scanning axis that sweeps first.
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
pub enum SweepAxis {
    /// `+sweep=x`, GOES.
    X,
    /// `+sweep=y`, Meteosat and Himawari. The PROJ default.
    Y,
}

/// Defining parameters of one geostationary projection.
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
pub struct GeosParams {
    /// Satellite height above the ellipsoid, metres.
    pub h: f64,
    /// Sub-satellite longitude λ0, degrees.
    pub lon0_deg: f64,
    pub sweep: SweepAxis,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
}

impl GeosParams {
    /// Read from PROJ-string parameters. `+h` is required.
    pub fn from_proj(p: &ProjParams<'_>) -> Result<Self, CrsParseError> {
        let h = p.number("h")?.ok_or(CrsParseError::Missing {
            key: "h",
            method: "geos",
        })?;
        if h <= 0.0 {
            return Err(CrsParseError::invalid("h", &h.to_string()));
        }
        let sweep = match p.text("sweep")? {
            None | Some("y") => SweepAxis::Y,
            Some("x") => SweepAxis::X,
            Some(other) => return Err(CrsParseError::invalid("sweep", other)),
        };
        Ok(Self {
            h,
            lon0_deg: p.number_or("lon_0", 0.0)?,
            sweep,
            false_easting: p.number_or("x_0", 0.0)?,
            false_northing: p.number_or("y_0", 0.0)?,
        })
    }

    /// Append the PROJ form of the defining parameters.
    pub fn write_proj(&self, out: &mut String) {
        let _ = write!(
            out,
            " +proj=geos +h={} +lon_0={} +x_0={} +y_0={}",
            self.h, self.lon0_deg, self.false_easting, self.false_northing
        );
        match self.sweep {
            SweepAxis::X => out.push_str(" +sweep=x"),
            SweepAxis::Y => {}
        }
    }
}

/// A geostationary projection with its constants computed. Lengths are in
/// units of the semi-major axis a.
#[derive(Debug, Clone, PartialEq)]
pub struct GeostationarySatellite {
    params: GeosParams,
    /// λ0, radians.
    lon0: f64,
    /// a, metres.
    a: f64,
    /// Polar radius b/a (1 on a sphere).
    radius_p: f64,
    /// (b/a)².
    radius_p2: f64,
    /// (a/b)².
    radius_p_inv2: f64,
    /// Distance from the Earth centre to the satellite, 1 + h/a.
    radius_g: f64,
    /// h/a.
    radius_g_1: f64,
    /// radius_g² − 1.
    c: f64,
}

impl GeostationarySatellite {
    pub fn new(p: &GeosParams, ell: &Ellipsoid) -> Self {
        let radius_g_1 = p.h / ell.a;
        let radius_g = 1.0 + radius_g_1;
        let one_es = 1.0 - ell.e2();
        Self {
            params: *p,
            lon0: p.lon0_deg.to_radians(),
            a: ell.a,
            radius_p: one_es.sqrt(),
            radius_p2: one_es,
            radius_p_inv2: 1.0 / one_es,
            radius_g,
            radius_g_1,
            c: radius_g * radius_g - 1.0,
        }
    }

    /// Project λ, φ radians to x, y metres. A point the satellite does not
    /// see returns `NotVisible`.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        let dlam = wrap_pi(lam - self.lon0);
        // Geocentric latitude and the geocentric radius, in units of a.
        let phi_c = (self.radius_p2 * phi.tan()).atan();
        let (sin_c, cos_c) = phi_c.sin_cos();
        let r = self.radius_p / (self.radius_p * cos_c).hypot(sin_c);
        let vx = r * dlam.cos() * cos_c;
        let vy = r * dlam.sin() * cos_c;
        let vz = r * sin_c;
        // PROJ geos.cpp visibility test. PROJ applies it on the ellipsoid
        // only; on a sphere it reduces to the same test with b = a.
        if (self.radius_g - vx) * vx - vy * vy - vz * vz * self.radius_p_inv2 < 0.0 {
            return Err(DomainReason::NotVisible);
        }
        let tmp = self.radius_g - vx;
        let (x, y) = match self.params.sweep {
            SweepAxis::X => (
                self.radius_g_1 * (vy / vz.hypot(tmp)).atan(),
                self.radius_g_1 * (vz / tmp).atan(),
            ),
            SweepAxis::Y => (
                self.radius_g_1 * (vy / tmp).atan(),
                self.radius_g_1 * (vz / vy.hypot(tmp)).atan(),
            ),
        };
        Ok((
            self.a * x + self.params.false_easting,
            self.a * y + self.params.false_northing,
        ))
    }

    /// Unproject x, y metres to λ, φ radians. A ray that misses the
    /// ellipsoid returns `OutsideProjection`.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        let x = (x - self.params.false_easting) / self.a;
        let y = (y - self.params.false_northing) / self.a;
        // Direction of the ray from the satellite.
        let vx = -1.0;
        let (vy, vz) = match self.params.sweep {
            SweepAxis::X => {
                let vz = (y / self.radius_g_1).tan();
                ((x / self.radius_g_1).tan() * 1f64.hypot(vz), vz)
            }
            SweepAxis::Y => {
                let vy = (x / self.radius_g_1).tan();
                (vy, (y / self.radius_g_1).tan() * 1f64.hypot(vy))
            }
        };
        // Nearest intersection of the ray with the ellipsoid.
        let vz_scaled = vz / self.radius_p;
        let qa = vy * vy + vz_scaled * vz_scaled + vx * vx;
        let qb = 2.0 * self.radius_g * vx;
        let det = qb * qb - 4.0 * qa * self.c;
        if det < 0.0 {
            return Err(DomainReason::OutsideProjection);
        }
        let k = (-qb - det.sqrt()) / (2.0 * qa);
        let px = self.radius_g + k * vx;
        let py = k * vy;
        let pz = k * vz;
        let lam = py.atan2(px);
        let phi = (self.radius_p_inv2 * pz / px.hypot(py)).atan();
        Ok((adjlon(self.lon0 + lam), phi))
    }
}

/// Longitude past ±π by more than this wraps into [−π, π] (PROJ `adjlon`).
const ADJLON_SLACK: f64 = 1e-12;

/// Wrap λ into [−π, π] unless it is within [`ADJLON_SLACK`] of the range,
/// as PROJ `adjlon` does on the inverse output.
fn adjlon(lam: f64) -> f64 {
    if lam.abs() < core::f64::consts::PI + ADJLON_SLACK {
        lam
    } else {
        wrap_pi(lam)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One millimetre, the tolerance for PROJ reference values.
    const MM: f64 = 1e-3;

    /// Inverse tolerance, degrees.
    const DEG_TOL: f64 = 1e-9;

    /// NodeDB 990002, Himawari: `+proj=geos +h=35785863 +lon_0=140.7
    /// +ellps=WGS84`.
    const HIMAWARI: GeosParams = GeosParams {
        h: 35_785_863.0,
        lon0_deg: 140.7,
        sweep: SweepAxis::Y,
        false_easting: 0.0,
        false_northing: 0.0,
    };

    /// Input points shared by the reference sets.
    const POINTS: [[f64; 2]; 4] = [[100.0, -30.0], [180.0, 40.0], [120.0, 60.0], [150.0, 10.0]];

    /// PROJ 9.8.1 (pyproj 3.8.0): HIMAWARI.
    const PROJ_SWEEP_Y: [[f64; 2]; 4] = [
        [-3_387_194.248_597_037, -2_967_586.956_076_014_3],
        [2_883_394.569_923_46, 3_776_151.720_974_31],
        [-1_032_087.201_384_981_2, 4_990_147.993_494_502],
        [1_009_858.536_176_618_2, 1_094_000.736_002_136_3],
    ];

    /// PROJ 9.8.1: HIMAWARI with `+sweep=x`.
    const PROJ_SWEEP_X: [[f64; 2]; 4] = [
        [-3_375_519.754_397_492_4, -2_980_868.497_043_092],
        [2_867_322.184_380_393_5, 3_788_351.068_362_223_5],
        [-1_022_066.338_940_185_6, 4_992_197.247_323_077],
        [1_009_386.555_539_076_1, 1_094_436.206_817_044_6],
    ];

    /// PROJ 9.8.1: `+proj=geos +h=35785863 +lon_0=140.7 +R=6378137`.
    const PROJ_SPHERE: [[f64; 2]; 4] = [
        [-3_384_064.730_060_460_5, -2_984_755.273_870_914_2],
        [2_879_029.962_482_346_7, 3_795_711.914_534_45],
        [-1_029_297.017_993_590_4, 5_009_940.871_180_367],
        [1_009_739.101_464_030_2, 1_101_238.970_240_290_5],
    ];

    fn check(p: GeosParams, ell: Ellipsoid, refs: &[[f64; 2]; 4]) {
        let g = GeostationarySatellite::new(&p, &ell);
        for (ll, xy) in POINTS.iter().zip(refs) {
            let (x, y) = g.forward(ll[0].to_radians(), ll[1].to_radians()).unwrap();
            assert!(
                (x - xy[0]).abs() < MM && (y - xy[1]).abs() < MM,
                "{ll:?}: got {x}, {y}"
            );
            let (lam, phi) = g.inverse(xy[0], xy[1]).unwrap();
            assert!((lam.to_degrees() - ll[0]).abs() < DEG_TOL, "{ll:?}");
            assert!((phi.to_degrees() - ll[1]).abs() < DEG_TOL, "{ll:?}");
        }
    }

    #[test]
    fn matches_proj_sweep_y() {
        check(HIMAWARI, Ellipsoid::WGS84, &PROJ_SWEEP_Y);
    }

    #[test]
    fn matches_proj_sweep_x() {
        check(
            GeosParams {
                sweep: SweepAxis::X,
                ..HIMAWARI
            },
            Ellipsoid::WGS84,
            &PROJ_SWEEP_X,
        );
    }

    #[test]
    fn matches_proj_sphere() {
        check(HIMAWARI, Ellipsoid::sphere(6_378_137.0), &PROJ_SPHERE);
    }

    #[test]
    fn sub_satellite_point_is_origin() {
        let g = GeostationarySatellite::new(&HIMAWARI, &Ellipsoid::WGS84);
        let (x, y) = g.forward(140.7f64.to_radians(), 0.0).unwrap();
        assert!(x.abs() < MM && y.abs() < MM);
    }

    #[test]
    fn far_side_and_off_disk_are_out_of_domain() {
        for ell in [Ellipsoid::WGS84, Ellipsoid::sphere(6_378_137.0)] {
            let g = GeostationarySatellite::new(&HIMAWARI, &ell);
            assert_eq!(
                g.forward((-39.3f64).to_radians(), 0.0),
                Err(DomainReason::NotVisible)
            );
            assert_eq!(
                g.forward(140.7f64.to_radians(), 85f64.to_radians()),
                Err(DomainReason::NotVisible)
            );
            assert_eq!(
                g.inverse(6_000_000.0, 0.0),
                Err(DomainReason::OutsideProjection)
            );
        }
    }

    /// PROJ 9.8.1 wraps the inverse longitude: 140.7° + 79.3° comes back
    /// as −140° (pyproj gives −140.0000000000027).
    #[test]
    fn inverse_wraps_longitude_like_proj() {
        let g = GeostationarySatellite::new(&HIMAWARI, &Ellipsoid::WGS84);
        let (x, y) = g
            .forward((-140f64).to_radians(), 10f64.to_radians())
            .unwrap();
        assert!((x - 5_347_786.938_853_634).abs() < MM && (y - 949_453.684_845_179_2).abs() < MM);
        let (lam, phi) = g.inverse(x, y).unwrap();
        assert!(
            (lam.to_degrees() + 140.0).abs() < DEG_TOL && (phi.to_degrees() - 10.0).abs() < DEG_TOL
        );
    }

    #[test]
    fn parses_and_writes_proj() {
        let text = "+proj=geos +h=35785863 +lon_0=140.7 +ellps=WGS84 +units=m +no_defs";
        let p = GeosParams::from_proj(&ProjParams::tokenize(text).unwrap()).unwrap();
        assert_eq!(p, HIMAWARI);
        let mut out = String::new();
        p.write_proj(&mut out);
        assert_eq!(
            GeosParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap(),
            p
        );
        let x = GeosParams::from_proj(&ProjParams::tokenize("+proj=geos +h=1 +sweep=x").unwrap())
            .unwrap();
        assert_eq!(x.sweep, SweepAxis::X);
        let mut out = String::new();
        x.write_proj(&mut out);
        assert_eq!(
            GeosParams::from_proj(&ProjParams::tokenize(&out).unwrap()).unwrap(),
            x
        );
        assert!(
            GeosParams::from_proj(&ProjParams::tokenize("+proj=geos +h=1 +sweep=z").unwrap())
                .is_err()
        );
        assert_eq!(
            GeosParams::from_proj(&ProjParams::tokenize("+proj=geos +lon_0=0").unwrap()),
            Err(CrsParseError::Missing {
                key: "h",
                method: "geos"
            })
        );
    }
}
