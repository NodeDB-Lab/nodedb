// SPDX-License-Identifier: Apache-2.0

//! Geodetic datums, their shift to WGS 84, and the PROJ `+datum` and `+pm`
//! names.

use serde::{Deserialize, Serialize};

use super::ellipsoid::Ellipsoid;
use super::helmert_rates::HelmertRates;

/// A geodetic datum: an ellipsoid, a prime meridian, and the shift that
/// takes its geocentric coordinates to WGS 84.
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
pub struct Datum {
    pub ellipsoid: Ellipsoid,
    /// Prime meridian, degrees east of Greenwich.
    pub prime_meridian_deg: f64,
    pub to_wgs84: DatumShift,
    /// EPSG code of the datum, when the CRS comes from the EPSG table.
    /// PROJ strings do not carry it.
    pub epsg_code: Option<u32>,
}

/// How a datum's geocentric coordinates map to WGS 84.
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
pub enum DatumShift {
    /// No shift is known. On the WGS 84 ellipsoid this is WGS 84 itself.
    /// On any other ellipsoid the datum is unknown, and a transform passes
    /// its geodetic coordinates through unchanged, as PROJ does.
    Identity,
    /// A 3-, 7- or 15-parameter Helmert transformation to WGS 84.
    Helmert(Helmert),
}

/// A Helmert transformation in the small-angle form PROJ `+towgs84` uses,
/// with optional yearly rates for the time-dependent form.
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
pub struct Helmert {
    pub tx_m: f64,
    pub ty_m: f64,
    pub tz_m: f64,
    pub rx_arcsec: f64,
    pub ry_arcsec: f64,
    pub rz_arcsec: f64,
    pub scale_ppm: f64,
    pub convention: RotationConvention,
    /// Published accuracy of the transformation, metres.
    pub accuracy_m: Option<f64>,
    /// Yearly rates and their reference epoch. `None` for a static
    /// transformation. The static parameters above hold at the reference
    /// epoch.
    pub rates: Option<HelmertRates>,
}

/// Sign convention of the Helmert rotations.
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
pub enum RotationConvention {
    /// EPSG method 1033 / 9606. PROJ `+towgs84` uses this convention.
    PositionVector,
    /// EPSG method 1032 / 9607. Rotations are the negated position-vector
    /// rotations.
    CoordinateFrame,
}

impl Helmert {
    /// A translation-only transformation.
    pub const fn translation(tx_m: f64, ty_m: f64, tz_m: f64) -> Helmert {
        Helmert::position_vector([tx_m, ty_m, tz_m, 0.0, 0.0, 0.0, 0.0])
    }

    /// A position-vector transformation from the seven `+towgs84` values:
    /// tx, ty, tz metres, rx, ry, rz arc-seconds, scale ppm.
    pub const fn position_vector(p: [f64; 7]) -> Helmert {
        Helmert {
            tx_m: p[0],
            ty_m: p[1],
            tz_m: p[2],
            rx_arcsec: p[3],
            ry_arcsec: p[4],
            rz_arcsec: p[5],
            scale_ppm: p[6],
            convention: RotationConvention::PositionVector,
            accuracy_m: None,
            rates: None,
        }
    }

    /// Rotations in the position-vector convention, arc-seconds.
    pub fn position_vector_rotations(&self) -> [f64; 3] {
        let r = [self.rx_arcsec, self.ry_arcsec, self.rz_arcsec];
        match self.convention {
            RotationConvention::PositionVector => r,
            // 0 − v keeps a zero rotation +0, so it never prints as "-0".
            RotationConvention::CoordinateFrame => r.map(|v| 0.0 - v),
        }
    }

    /// The seven `+towgs84` values, position-vector convention.
    pub fn to_towgs84(&self) -> [f64; 7] {
        let [rx, ry, rz] = self.position_vector_rotations();
        [self.tx_m, self.ty_m, self.tz_m, rx, ry, rz, self.scale_ppm]
    }

    /// The seven rates in `+towgs84` order and position-vector convention,
    /// per year, and the reference epoch. `None` for a static
    /// transformation.
    pub fn position_vector_rates(&self) -> Option<([f64; 7], f64)> {
        self.rates.map(|r| {
            let mut v = r.values();
            if self.convention == RotationConvention::CoordinateFrame {
                for rate in &mut v[3..6] {
                    // 0 − v keeps a zero rate +0.
                    *rate = 0.0 - *rate;
                }
            }
            (v, r.reference_epoch)
        })
    }

    /// Whether every parameter and every rate is zero.
    pub fn is_zero(&self) -> bool {
        self.to_towgs84().iter().all(|v| *v == 0.0)
            && self
                .rates
                .is_none_or(|r| r.values().iter().all(|v| *v == 0.0))
    }
}

impl DatumShift {
    /// Whether the shift moves no coordinate: `Identity` or an all-zero
    /// Helmert.
    pub fn is_null(&self) -> bool {
        match self {
            DatumShift::Identity => true,
            DatumShift::Helmert(h) => h.is_zero(),
        }
    }

    /// Whether two shifts move coordinates the same way.
    pub fn same_effect(&self, other: &DatumShift) -> bool {
        match (self, other) {
            (DatumShift::Helmert(a), DatumShift::Helmert(b)) => {
                a.to_towgs84() == b.to_towgs84()
                    && a.position_vector_rates() == b.position_vector_rates()
            }
            (DatumShift::Identity, DatumShift::Helmert(_))
            | (DatumShift::Helmert(_), DatumShift::Identity)
            | (DatumShift::Identity, DatumShift::Identity) => self.is_null() && other.is_null(),
        }
    }
}

impl Datum {
    /// The WGS 84 datum.
    pub const WGS84: Datum = Datum {
        ellipsoid: Ellipsoid::WGS84,
        prime_meridian_deg: 0.0,
        to_wgs84: DatumShift::Identity,
        epsg_code: None,
    };

    /// Whether the datum's relation to WGS 84 is known: it carries a
    /// Helmert shift, or it is WGS 84 itself.
    pub fn is_known(&self) -> bool {
        match self.to_wgs84 {
            DatumShift::Helmert(_) => true,
            DatumShift::Identity => self.ellipsoid == Ellipsoid::WGS84,
        }
    }

    /// Whether two datums are one datum, so a transform between them runs
    /// no shift.
    ///
    /// Equal EPSG codes name one datum, even when the two `to_wgs84`
    /// shifts differ: the EPSG table picks each CRS's best shift for its
    /// own area of use. PROJ, given the two EPSG codes, applies no shift
    /// between a projected CRS and its geographic base either. Without
    /// both codes, two datums are one when the ellipsoid matches and the
    /// shifts move coordinates the same way.
    pub fn same_datum(&self, other: &Datum) -> bool {
        match (self.epsg_code, other.epsg_code) {
            (Some(a), Some(b)) if a == b => true,
            (Some(_) | None, Some(_) | None) => {
                self.ellipsoid == other.ellipsoid && self.to_wgs84.same_effect(&other.to_wgs84)
            }
        }
    }
}

/// A PROJ `+datum` name: its ellipsoid and its `+towgs84` values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NamedDatum {
    pub ellipsoid: Ellipsoid,
    pub shift: DatumShift,
}

/// The datum PROJ calls `name` in `+datum`. Grid-based datums such as
/// NAD27 are absent: this crate applies no grid shifts.
pub fn datum_by_proj_name(name: &str) -> Option<NamedDatum> {
    let (ellipsoid, shift) = match name {
        "WGS84" => (Ellipsoid::WGS84, DatumShift::Identity),
        "NAD83" => (
            Ellipsoid::GRS80,
            DatumShift::Helmert(Helmert::translation(0.0, 0.0, 0.0)),
        ),
        "GGRS87" => (
            Ellipsoid::GRS80,
            DatumShift::Helmert(Helmert::translation(-199.87, 74.79, 246.62)),
        ),
        "potsdam" => (
            Ellipsoid::BESSEL_1841,
            DatumShift::Helmert(Helmert::position_vector([
                598.1, 73.7, 418.2, 0.202, 0.045, -2.455, 6.7,
            ])),
        ),
        "hermannskogel" => (
            Ellipsoid::BESSEL_1841,
            DatumShift::Helmert(Helmert::position_vector([
                577.326, 90.129, 463.919, 5.137, 1.474, 5.297, 2.4232,
            ])),
        ),
        "ire65" => (
            Ellipsoid::AIRY_MODIFIED,
            DatumShift::Helmert(Helmert::position_vector([
                482.530, -130.596, 564.557, -1.042, -0.214, -0.631, 8.15,
            ])),
        ),
        "nzgd49" => (
            Ellipsoid::INTERNATIONAL_1924,
            DatumShift::Helmert(Helmert::position_vector([
                59.47, -5.04, 187.44, 0.47, -0.1, 1.024, -4.5993,
            ])),
        ),
        "OSGB36" => (
            Ellipsoid::AIRY_1830,
            DatumShift::Helmert(Helmert::position_vector([
                446.448, -125.157, 542.060, 0.1502, 0.2470, 0.8421, -20.4894,
            ])),
        ),
        _ => return None,
    };
    Some(NamedDatum { ellipsoid, shift })
}

/// Seconds in a degree.
const SECONDS_PER_DEGREE: f64 = 3600.0;

/// Minutes in a degree.
const MINUTES_PER_DEGREE: f64 = 60.0;

/// Degrees from degrees, minutes and seconds. `east` false gives a west
/// (negative) longitude.
const fn dms(d: f64, m: f64, s: f64, east: bool) -> f64 {
    let deg = d + m / MINUTES_PER_DEGREE + s / SECONDS_PER_DEGREE;
    if east { deg } else { -deg }
}

/// PROJ `+pm` names, degrees east of Greenwich, from PROJ's DMS values.
const PRIME_MERIDIANS: &[(&str, f64)] = &[
    ("greenwich", 0.0),
    ("lisbon", dms(9.0, 7.0, 54.862, false)),
    ("paris", dms(2.0, 20.0, 14.025, true)),
    ("bogota", dms(74.0, 4.0, 51.3, false)),
    ("madrid", dms(3.0, 41.0, 16.58, false)),
    ("rome", dms(12.0, 27.0, 8.4, true)),
    ("bern", dms(7.0, 26.0, 22.5, true)),
    ("jakarta", dms(106.0, 48.0, 27.79, true)),
    ("ferro", dms(17.0, 40.0, 0.0, false)),
    ("brussels", dms(4.0, 22.0, 4.71, true)),
    ("stockholm", dms(18.0, 3.0, 29.8, true)),
    ("athens", dms(23.0, 42.0, 58.815, true)),
    ("oslo", dms(10.0, 43.0, 22.5, true)),
    ("copenhagen", dms(12.0, 34.0, 40.35, true)),
];

/// The prime meridian PROJ calls `name` in `+pm`, degrees east.
pub fn prime_meridian_by_proj_name(name: &str) -> Option<f64> {
    PRIME_MERIDIANS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, deg)| *deg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinate_frame_negates_rotations() {
        let mut h = Helmert::position_vector([1.0, 2.0, 3.0, 0.1, 0.2, 0.3, 4.0]);
        h.convention = RotationConvention::CoordinateFrame;
        assert_eq!(h.to_towgs84(), [1.0, 2.0, 3.0, -0.1, -0.2, -0.3, 4.0]);
    }

    #[test]
    fn at_epoch_adds_rate_times_elapsed_years() {
        let mut h = Helmert::position_vector([1.0, 2.0, 3.0, 0.1, 0.2, 0.3, 4.0]);
        h.rates = Some(HelmertRates::new(
            [0.5, -1.0, 0.25, 0.01, -0.02, 0.03, 0.5],
            2010.0,
        ));
        assert_eq!(h.at_epoch(2010.0).to_towgs84(), h.to_towgs84());
        let later = h.at_epoch(2012.0);
        assert_eq!(later.rates, None);
        let want = [2.0, 0.0, 3.5, 0.12, 0.16, 0.36, 5.0];
        for (got, want) in later.to_towgs84().iter().zip(want) {
            assert!((got - want).abs() < 1e-12, "{got} {want}");
        }
        let fixed = Helmert::translation(1.0, 2.0, 3.0);
        assert_eq!(fixed.at_epoch(1950.0), fixed);
    }

    #[test]
    fn rates_decide_null_and_same_effect() {
        let mut moving = Helmert::translation(0.0, 0.0, 0.0);
        moving.rates = Some(HelmertRates::new(
            [0.0, 0.0, 0.001, 0.0, 0.0, 0.0, 0.0],
            2000.0,
        ));
        assert!(!moving.is_zero());
        assert!(!DatumShift::Helmert(moving).same_effect(&DatumShift::Identity));
        let mut cf = moving;
        cf.convention = RotationConvention::CoordinateFrame;
        assert!(DatumShift::Helmert(moving).same_effect(&DatumShift::Helmert(cf)));
        let mut rotating = moving;
        rotating.rates = Some(HelmertRates::new(
            [0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 0.0],
            2000.0,
        ));
        let mut negated = rotating;
        negated.convention = RotationConvention::CoordinateFrame;
        negated.rates = Some(HelmertRates::new(
            [0.0, 0.0, 0.0, -0.1, 0.0, 0.0, 0.0],
            2000.0,
        ));
        assert!(DatumShift::Helmert(rotating).same_effect(&DatumShift::Helmert(negated)));
        assert!(!DatumShift::Helmert(rotating).same_effect(&DatumShift::Helmert(moving)));
    }

    #[test]
    fn null_shifts_have_the_same_effect() {
        let zero = DatumShift::Helmert(Helmert::translation(0.0, 0.0, 0.0));
        assert!(zero.same_effect(&DatumShift::Identity));
        let moved = DatumShift::Helmert(Helmert::translation(1.0, 0.0, 0.0));
        assert!(!moved.same_effect(&DatumShift::Identity));
    }

    #[test]
    fn known_datums() {
        assert!(Datum::WGS84.is_known());
        let sphere = Datum {
            ellipsoid: Ellipsoid::sphere(6_371_007.181),
            ..Datum::WGS84
        };
        assert!(!sphere.is_known());
    }

    #[test]
    fn epsg_code_decides_same_datum() {
        let shifted = |tx: f64, code: Option<u32>| Datum {
            ellipsoid: Ellipsoid::INTERNATIONAL_1924,
            prime_meridian_deg: 0.0,
            to_wgs84: DatumShift::Helmert(Helmert::translation(tx, 0.0, 0.0)),
            epsg_code: code,
        };
        assert!(shifted(-87.0, Some(6230)).same_datum(&shifted(-90.0, Some(6230))));
        assert!(!shifted(-87.0, Some(6230)).same_datum(&shifted(-90.0, Some(6231))));
        assert!(!shifted(-87.0, None).same_datum(&shifted(-90.0, Some(6230))));
        assert!(shifted(-87.0, Some(6230)).same_datum(&shifted(-87.0, None)));
        let wgs84_ellipsoid = Datum {
            ellipsoid: Ellipsoid::WGS84,
            ..shifted(-191.9, Some(6756))
        };
        assert!(!wgs84_ellipsoid.same_datum(&Datum::WGS84));
    }

    #[test]
    fn paris_meridian() {
        let paris = prime_meridian_by_proj_name("paris").unwrap();
        assert!((paris - 2.337_229_166_667).abs() < 1e-11);
        assert!(datum_by_proj_name("NAD27").is_none());
    }
}
