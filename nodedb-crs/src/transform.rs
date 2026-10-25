// SPDX-License-Identifier: Apache-2.0

//! Coordinate transformation between two CRS definitions.
//!
//! Pipeline: source axis order to native east, north, up → source units →
//! metres → inverse projection to (λ, φ, h) on the source ellipsoid (a
//! geographic source reads degrees directly) → prime meridian to Greenwich
//! → datum shift through ECEF and WGS 84 when both datums are known and
//! differ → target prime meridian → forward projection → target units →
//! target axis order.
//!
//! Two datums differ unless [`Datum::same_datum`] holds: equal EPSG datum
//! codes, or one ellipsoid with shifts of the same effect. A projected CRS
//! and its own geographic base share an EPSG datum code, so no shift runs
//! between them even when their `to_wgs84` differ, as in PROJ.
//!
//! A datum with no known shift to WGS 84 is passed through: its geodetic
//! coordinates carry over unchanged, as PROJ's ballpark transformation does.
//!
//! A time-dependent Helmert shift takes its parameters at the coordinate
//! epoch passed to [`Transformer::apply_at`]. [`Transformer::apply`] takes
//! each shift at its own parameter reference epoch, where only the static
//! parameters act.

use std::sync::Arc;

use crate::datum::{ecef_to_geodetic, geodetic_to_ecef};
use crate::def::{CoordinateEpoch, CrsDef, Datum, DatumShift};
use crate::error::{CrsError, DomainReason};
use crate::method::Projector;
use crate::method::angle::{wrap_lon_deg, wrap_pi};
use crate::srid::Srid;

/// Largest absolute latitude, degrees.
const MAX_LATITUDE_DEG: f64 = 90.0;

/// Which steps a transform runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Path {
    /// Source and target are the same CRS.
    Identity,
    /// One datum, or one side unknown: no ECEF step.
    SameDatum,
    /// Both datums are known and differ: shift through ECEF and WGS 84.
    DatumShift,
}

/// A prepared transform from one CRS to another.
#[derive(Debug, Clone)]
pub struct Transformer {
    source: Arc<CrsDef>,
    target: Arc<CrsDef>,
    path: Path,
    source_projector: Option<Projector>,
    target_projector: Option<Projector>,
}

/// One side of a transform. A side with no projector is geographic.
struct Side<'a> {
    def: &'a CrsDef,
    projector: Option<&'a Projector>,
}

impl Transformer {
    pub fn new(source: Arc<CrsDef>, target: Arc<CrsDef>) -> Self {
        let path = if Arc::ptr_eq(&source, &target) || *source == *target {
            Path::Identity
        } else if needs_shift(&source.datum, &target.datum) {
            Path::DatumShift
        } else {
            Path::SameDatum
        };
        let prepare = |def: &CrsDef| def.projection.map(|p| p.prepare(&def.datum.ellipsoid));
        Self {
            source_projector: prepare(&source),
            target_projector: prepare(&target),
            source,
            target,
            path,
        }
    }

    pub fn source(&self) -> &Arc<CrsDef> {
        &self.source
    }

    pub fn target(&self) -> &Arc<CrsDef> {
        &self.target
    }

    /// Map one coordinate. x, y are in source units (degrees for a
    /// geographic CRS), in the source axis order. z is ellipsoidal height
    /// in metres. 2D callers pass z = 0. The output follows the target
    /// axis order.
    ///
    /// A time-dependent datum shift runs at its parameter reference epoch.
    /// Use [`Transformer::apply_at`] for coordinates at another epoch.
    pub fn apply(&self, xyz: [f64; 3]) -> Result<[f64; 3], CrsError> {
        self.run(xyz, None)
    }

    /// Map one coordinate that refers to coordinate epoch `epoch`. Every
    /// time-dependent datum shift takes its parameters at `epoch`, IOGP
    /// GN 7-2 §4.2.5. Static shifts and same-datum paths ignore the epoch.
    /// Arguments and output are as for [`Transformer::apply`].
    pub fn apply_at(&self, xyz: [f64; 3], epoch: CoordinateEpoch) -> Result<[f64; 3], CrsError> {
        self.run(xyz, Some(epoch))
    }

    fn run(&self, xyz: [f64; 3], epoch: Option<CoordinateEpoch>) -> Result<[f64; 3], CrsError> {
        let [x, y, z] = xyz;
        let fail = |srid: Srid, reason: DomainReason| CrsError::OutOfDomain { srid, x, y, reason };
        if !(x.is_finite() && y.is_finite() && z.is_finite()) {
            return Err(fail(self.source.srid, DomainReason::NonFinite));
        }
        if self.path == Path::Identity {
            return Ok(xyz);
        }
        let source = Side {
            def: &self.source,
            projector: self.source_projector.as_ref(),
        };
        let target = Side {
            def: &self.target,
            projector: self.target_projector.as_ref(),
        };
        let [east, north, up] = self.source.axis.to_native(xyz);
        let (lam, phi) =
            to_geodetic(&source, east, north).map_err(|r| fail(self.source.srid, r))?;
        let (lam, phi, h) = match self.path {
            Path::Identity | Path::SameDatum => (lam, phi, up),
            Path::DatumShift => shift(
                &self.source.datum,
                &self.target.datum,
                epoch,
                [lam, phi, up],
            ),
        };
        let (out_x, out_y) =
            from_geodetic(&target, lam, phi).map_err(|r| fail(self.target.srid, r))?;
        Ok(self.target.axis.from_native([out_x, out_y, h]))
    }
}

/// Whether a transform between the two datums runs the ECEF step.
fn needs_shift(source: &Datum, target: &Datum) -> bool {
    source.is_known() && target.is_known() && !source.same_datum(target)
}

/// Source coordinates to λ (from Greenwich), φ radians.
fn to_geodetic(side: &Side<'_>, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
    let pm = side.def.datum.prime_meridian_deg;
    match side.projector {
        None => {
            if y.abs() > MAX_LATITUDE_DEG {
                return Err(DomainReason::LatitudeRange);
            }
            Ok(((x + pm).to_radians(), y.to_radians()))
        }
        Some(p) => {
            let m = side.def.unit.metres_per_unit;
            let (lam, phi) = p.inverse(x * m, y * m)?;
            Ok((lam + pm.to_radians(), phi))
        }
    }
}

/// λ (from Greenwich), φ radians to target coordinates.
fn from_geodetic(side: &Side<'_>, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
    let pm = side.def.datum.prime_meridian_deg;
    match side.projector {
        None => {
            let out = (wrap_lon_deg(lam.to_degrees() - pm), phi.to_degrees());
            if out.0.is_finite() && out.1.is_finite() {
                Ok(out)
            } else {
                Err(DomainReason::NonFinite)
            }
        }
        Some(p) => {
            let m = side.def.unit.metres_per_unit;
            // Oblique methods scale λ − λ0 by a constant, so λ must sit in
            // [−π, π] before it reaches them.
            let (x, y) = p.forward(wrap_pi(lam - pm.to_radians()), phi)?;
            Ok((x / m, y / m))
        }
    }
}

/// Shift λ, φ, h from the source datum to the target datum through ECEF.
/// A time-dependent shift takes its parameters at `epoch`, or at its own
/// reference epoch when `epoch` is `None`.
fn shift(
    source: &Datum,
    target: &Datum,
    epoch: Option<CoordinateEpoch>,
    [lam, phi, h]: [f64; 3],
) -> (f64, f64, f64) {
    let local = geodetic_to_ecef(lam, phi, h, &source.ellipsoid);
    let wgs84 = match &source.to_wgs84 {
        DatumShift::Identity => local,
        DatumShift::Helmert(t) => match epoch {
            None => t.forward(local),
            Some(e) => t.at_epoch(e.year()).forward(local),
        },
    };
    let target_xyz = match &target.to_wgs84 {
        DatumShift::Identity => wgs84,
        DatumShift::Helmert(t) => match epoch {
            None => t.inverse(wgs84),
            Some(e) => t.at_epoch(e.year()).inverse(wgs84),
        },
    };
    ecef_to_geodetic(target_xyz, &target.ellipsoid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_proj_string;

    /// One millimetre, the tolerance for PROJ reference values.
    const MM: f64 = 1e-3;

    /// Metres per degree of latitude, rounded up. Converts a metre
    /// tolerance to a conservative degree tolerance.
    const METRES_PER_DEGREE: f64 = 111_320.0;

    fn def(code: u32, proj: &str) -> Arc<CrsDef> {
        let srid = Srid::new(code).unwrap();
        Arc::new(parse_proj_string(srid, "t", proj).unwrap())
    }

    fn wgs84() -> Arc<CrsDef> {
        def(4326, "+proj=longlat +datum=WGS84 +no_defs")
    }

    fn utm48n() -> Arc<CrsDef> {
        def(32648, "+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs")
    }

    fn kertau_geographic() -> Arc<CrsDef> {
        def(
            4245,
            "+proj=longlat +ellps=evrst48 +towgs84=-11,851,5,0,0,0,0 +no_defs",
        )
    }

    #[test]
    fn wgs84_to_utm48n_and_back() {
        let fwd = Transformer::new(wgs84(), utm48n());
        let inv = Transformer::new(utm48n(), wgs84());
        for [lon, lat] in [
            [101.7117, 3.1579],
            [105.0, 0.0],
            [103.5, -1.5],
            [109.0, 6.0],
        ] {
            let p = fwd.apply([lon, lat, 0.0]).unwrap();
            let back = inv.apply(p).unwrap();
            let tol = MM / METRES_PER_DEGREE;
            assert!(
                (back[0] - lon).abs() < tol && (back[1] - lat).abs() < tol,
                "{lon} {lat} -> {back:?}"
            );
        }
        // PROJ 9.8.1 via pyproj, EPSG:4326 → EPSG:32648, always_xy.
        let klcc = fwd.apply([101.7117, 3.1579, 0.0]).unwrap();
        assert!((klcc[0] - 134_445.592_4).abs() < MM && (klcc[1] - 349_624.727_8).abs() < MM);
    }

    #[test]
    fn identity_returns_input() {
        let t = Transformer::new(utm48n(), utm48n());
        assert_eq!(t.apply([1.0, 2.0, 3.0]), Ok([1.0, 2.0, 3.0]));
    }

    #[test]
    fn helmert_datum_round_trip_below_a_micrometre() {
        let to_kertau = Transformer::new(wgs84(), kertau_geographic());
        let back = Transformer::new(kertau_geographic(), wgs84());
        assert_eq!(to_kertau.path, Path::DatumShift);
        let input = [101.7117, 3.1579, 30.0];
        let k = to_kertau.apply(input).unwrap();
        // The shift moves the point by hundreds of metres.
        assert!((k[0] - input[0]).abs() > 1e-4 || (k[1] - input[1]).abs() > 1e-4);
        let w = back.apply(k).unwrap();
        let tol = 1e-6 / METRES_PER_DEGREE;
        assert!(
            (w[0] - input[0]).abs() < tol && (w[1] - input[1]).abs() < tol,
            "{w:?}"
        );
        assert!((w[2] - input[2]).abs() < 1e-6, "{w:?}");
    }

    /// A translation-only shift between datums on one ellipsoid moves the
    /// ECEF point by exactly the translation.
    #[test]
    fn translation_moves_ecef_by_the_vector() {
        let src = def(910_001, "+proj=longlat +ellps=WGS84 +towgs84=100,-50,25");
        let t = Transformer::new(src, wgs84());
        let input = [10.0, 45.0, 0.0];
        let out = t.apply(input).unwrap();
        let ell = crate::def::Ellipsoid::WGS84;
        let a = geodetic_to_ecef(input[0].to_radians(), input[1].to_radians(), input[2], &ell);
        let b = geodetic_to_ecef(out[0].to_radians(), out[1].to_radians(), out[2], &ell);
        for (i, want) in [100.0, -50.0, 25.0].into_iter().enumerate() {
            assert!((b[i] - a[i] - want).abs() < 1e-6, "{i}: {}", b[i] - a[i]);
        }
    }

    /// An unknown datum (a sphere with no shift) passes geodetic
    /// coordinates through, as PROJ does for MODIS sinusoidal.
    #[test]
    fn unknown_datum_skips_the_shift() {
        let sphere = def(990_100, "+proj=longlat +R=6371007.181 +no_defs");
        let t = Transformer::new(wgs84(), sphere);
        assert_eq!(t.path, Path::SameDatum);
        let out = t.apply([101.5, 3.25, 7.0]).unwrap();
        assert!(
            (out[0] - 101.5).abs() < 1e-12 && (out[1] - 3.25).abs() < 1e-12,
            "{out:?}"
        );
        assert_eq!(out[2], 7.0);
    }

    #[test]
    fn units_and_prime_meridian() {
        let feet = def(910_002, "+proj=utm +zone=48 +datum=WGS84 +units=us-ft");
        let metres = Transformer::new(wgs84(), utm48n())
            .apply([104.0, 2.0, 0.0])
            .unwrap();
        let in_feet = Transformer::new(wgs84(), feet)
            .apply([104.0, 2.0, 0.0])
            .unwrap();
        let m = 1200.0 / 3937.0;
        assert!((in_feet[0] * m - metres[0]).abs() < 1e-6);
        assert!((in_feet[1] * m - metres[1]).abs() < 1e-6);
        let paris = def(910_003, "+proj=longlat +datum=WGS84 +pm=paris");
        let out = Transformer::new(wgs84(), paris)
            .apply([2.337_229_166_667, 48.0, 0.0])
            .unwrap();
        assert!(out[0].abs() < 1e-9, "{out:?}");
    }

    #[test]
    fn errors_name_the_failing_crs() {
        let t = Transformer::new(wgs84(), utm48n());
        let err = t.apply([179.0, 3.0, 0.0]).unwrap_err();
        assert_eq!(
            err,
            CrsError::OutOfDomain {
                srid: Srid::new(32648).unwrap(),
                x: 179.0,
                y: 3.0,
                reason: DomainReason::CentralMeridianOffset
            }
        );
        let err = t.apply([100.0, 95.0, 0.0]).unwrap_err();
        assert!(
            matches!(err, CrsError::OutOfDomain { srid, reason: DomainReason::LatitudeRange, .. } if srid == Srid::WGS84)
        );
        assert!(t.apply([f64::NAN, 0.0, 0.0]).is_err());
    }

    /// A GRS 1980 geographic CRS whose shift to WGS 84 is the EPSG 6276
    /// time-dependent Coordinate Frame rotation, IOGP GN 7-2 §4.2.5.
    fn time_dependent_geographic() -> Arc<CrsDef> {
        use crate::def::{Helmert, HelmertRates, RotationConvention};
        let mut d = (*def(910_005, "+proj=longlat +ellps=GRS80 +no_defs")).clone();
        let mut h = Helmert::position_vector([
            -0.084_68,
            -0.019_42,
            0.032_01,
            -0.000_425_4,
            0.002_257_8,
            0.002_401_5,
            0.009_71,
        ]);
        h.convention = RotationConvention::CoordinateFrame;
        h.rates = Some(HelmertRates::new(
            [
                0.001_42,
                0.001_34,
                0.000_90,
                0.001_546_1,
                0.001_182_0,
                0.001_155_1,
                0.000_109,
            ],
            1994.0,
        ));
        d.datum.to_wgs84 = DatumShift::Helmert(h);
        Arc::new(d)
    }

    /// PROJ 9.8.1 pipeline: unitconvert, cart GRS80, `+proj=helmert` with
    /// the EPSG 6276 parameters and `+t_epoch=1994
    /// +convention=coordinate_frame`, inverse cart WGS84, on (128, −16,
    /// 100 m), as (epoch, lon, lat, h).
    const PROJ_TIME_DEPENDENT: [[f64; 4]; 3] = [
        [
            2013.9,
            127.999_993_192_190_9,
            -16.000_010_577_429_9,
            100.103_043_247_014_28,
        ],
        [
            1994.0,
            127.999_999_906_555_56,
            -15.999_999_923_389_051,
            100.088_547_978_550_2,
        ],
        [
            2030.25,
            127.999_987_675_614_36,
            -16.000_019_330_875_343,
            100.114_952_898_584_3,
        ],
    ];

    /// Height agreement with PROJ, metres.
    const HEIGHT_TOL_M: f64 = 1e-6;

    #[test]
    fn apply_at_matches_proj_time_dependent_helmert() {
        let t = Transformer::new(time_dependent_geographic(), wgs84());
        let tol = 1e-6 / METRES_PER_DEGREE;
        for [epoch, lon, lat, h] in PROJ_TIME_DEPENDENT {
            let at = CoordinateEpoch::new(epoch).unwrap();
            let out = t.apply_at([128.0, -16.0, 100.0], at).unwrap();
            assert!(
                (out[0] - lon).abs() < tol && (out[1] - lat).abs() < tol,
                "{epoch}: {out:?}"
            );
            assert!((out[2] - h).abs() < HEIGHT_TOL_M, "{epoch}: {out:?}");
            let back = Transformer::new(wgs84(), time_dependent_geographic())
                .apply_at(out, at)
                .unwrap();
            assert!(
                (back[0] - 128.0).abs() < tol && (back[1] + 16.0).abs() < tol,
                "{epoch}: {back:?}"
            );
        }
    }

    /// Without an epoch the shift runs at its reference epoch, 1994.0.
    #[test]
    fn apply_uses_the_reference_epoch() {
        let t = Transformer::new(time_dependent_geographic(), wgs84());
        let input = [128.0, -16.0, 100.0];
        let at_reference = t
            .apply_at(input, CoordinateEpoch::new(1994.0).unwrap())
            .unwrap();
        assert_eq!(t.apply(input).unwrap(), at_reference);
        let [_, lon, lat, _] = PROJ_TIME_DEPENDENT[1];
        let out = t.apply(input).unwrap();
        let tol = 1e-6 / METRES_PER_DEGREE;
        assert!((out[0] - lon).abs() < tol && (out[1] - lat).abs() < tol);
    }

    /// A static shift gives the same result at every epoch.
    #[test]
    fn static_shift_ignores_the_epoch() {
        let t = Transformer::new(kertau_geographic(), wgs84());
        let input = [101.7117, 3.1579, 30.0];
        let at = CoordinateEpoch::new(2150.0).unwrap();
        assert_eq!(t.apply_at(input, at), t.apply(input));
    }

    /// EPSG:2046 Hartebeesthoek94 / Lo15 is west-south: x is westing and y
    /// southing. PROJ 9.8.1 reference row from the fixture.
    #[test]
    fn axis_order_applies_on_both_sides() {
        let lo15 = def(
            2046,
            "+proj=tmerc +axis=wsu +lat_0=0 +lon_0=15 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 \
             +towgs84=0,0,0 +units=m +no_defs",
        );
        let fwd = Transformer::new(wgs84(), Arc::clone(&lo15));
        let p = fwd.apply([14.475, -22.915, 0.0]).unwrap();
        assert!((p[0] - 53_858.502_884_905_3).abs() < MM, "{p:?}");
        assert!((p[1] - 2_535_220.466_812_430_4).abs() < MM, "{p:?}");
        let back = Transformer::new(lo15, wgs84()).apply(p).unwrap();
        let tol = MM / METRES_PER_DEGREE;
        assert!(
            (back[0] - 14.475).abs() < tol && (back[1] + 22.915).abs() < tol,
            "{back:?}"
        );
        let neu = def(910_004, "+proj=longlat +axis=neu +datum=WGS84");
        let swapped = Transformer::new(wgs84(), neu)
            .apply([101.5, 3.25, 7.0])
            .unwrap();
        assert_eq!(swapped, [3.25, 101.5, 7.0]);
    }

    /// Equal EPSG datum codes skip the shift although the `+towgs84`
    /// values differ. Different codes run it.
    #[test]
    fn epsg_datum_code_decides_the_path() {
        let coded = |code: u32, text: &str, datum: u32| {
            let mut d = (*def(code, text)).clone();
            d.datum.epsg_code = Some(datum);
            Arc::new(d)
        };
        let base = coded(
            4230,
            "+proj=longlat +ellps=intl +towgs84=-87,-98,-121",
            6230,
        );
        let grid = coded(
            23031,
            "+proj=utm +zone=31 +ellps=intl +towgs84=-84,-107,-120 +units=m",
            6230,
        );
        assert_eq!(
            Transformer::new(Arc::clone(&base), Arc::clone(&grid)).path,
            Path::SameDatum
        );
        let other = coded(
            4231,
            "+proj=longlat +ellps=intl +towgs84=-83,-96,-113",
            6231,
        );
        assert_eq!(Transformer::new(other, grid).path, Path::DatumShift);
        let wgs = coded(4326, "+proj=longlat +datum=WGS84", 6326);
        assert_eq!(Transformer::new(base, wgs).path, Path::DatumShift);
    }
}
