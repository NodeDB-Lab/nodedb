// SPDX-License-Identifier: Apache-2.0

//! Unit and ellipsoid names as WKT2 and PROJJSON write them.

use super::helmert_params::SECONDS_PER_YEAR;
use super::model::{ARC_SECOND_RAD, DEGREE_RAD, PPM};
use crate::def::Ellipsoid;

/// Linear units with metres per unit.
const LINEAR: &[(&str, f64)] = &[
    ("metre", 1.0),
    ("millimetre", 0.001),
    ("kilometre", 1000.0),
    ("foot", 0.3048),
    ("US survey foot", 1200.0 / 3937.0),
    ("Clarke's foot", 0.304_797_265_4),
    ("link", 0.201_168),
    ("chain", 20.1168),
    ("yard", 0.9144),
];

/// Angular units with radians per unit.
const ANGULAR: &[(&str, f64)] = &[
    ("degree", DEGREE_RAD),
    ("radian", 1.0),
    ("grad", core::f64::consts::PI / 200.0),
    ("arc-second", ARC_SECOND_RAD),
    ("milliarc-second", ARC_SECOND_RAD / 1000.0),
    ("arc-minute", DEGREE_RAD / 60.0),
    ("microradian", 1e-6),
];

/// Scale units as factors.
const SCALE: &[(&str, f64)] = &[
    ("unity", 1.0),
    ("parts per million", PPM),
    ("parts per billion", PPM / 1000.0),
];

/// Rate and time units of a time-dependent Helmert transformation, SI per
/// second for a rate and seconds for the year: EPSG units of measure 1042,
/// 1027, 1043, 1032, 1041, 1030 and 1029.
const RATE: &[(&str, f64)] = &[
    ("metres per year", 1.0 / SECONDS_PER_YEAR),
    ("millimetres per year", 0.001 / SECONDS_PER_YEAR),
    ("arc-seconds per year", ARC_SECOND_RAD / SECONDS_PER_YEAR),
    (
        "milliarc-seconds per year",
        ARC_SECOND_RAD / 1000.0 / SECONDS_PER_YEAR,
    ),
    ("parts per million per year", PPM / SECONDS_PER_YEAR),
    (
        "parts per billion per year",
        PPM / 1000.0 / SECONDS_PER_YEAR,
    ),
    ("year", SECONDS_PER_YEAR),
];

/// WKT names of the ellipsoids PROJ names, keyed by the PROJ name.
const ELLIPSOIDS: &[(&str, &str)] = &[
    ("WGS84", "WGS 84"),
    ("GRS80", "GRS 1980"),
    ("WGS72", "WGS 72"),
    ("intl", "International 1924"),
    ("bessel", "Bessel 1841"),
    ("clrk66", "Clarke 1866"),
    ("clrk80ign", "Clarke 1880 (IGN)"),
    ("krass", "Krassowsky 1940"),
    ("airy", "Airy 1830"),
    ("mod_airy", "Airy Modified 1849"),
    ("evrst30", "Everest 1830 (1937 Adjustment)"),
    ("evrstSS", "Everest 1830 (1967 Definition)"),
    ("evrst48", "Everest 1830 Modified"),
    ("evrst56", "Everest 1830 (1956)"),
    ("evrst69", "Everest 1830 (1962)"),
    ("aust_SA", "Australian National Spheroid"),
    ("GRS67", "GRS 1967"),
    ("helmert", "Helmert 1906"),
];

/// Metres per unit of the linear unit named `name`, compared without case.
pub(crate) fn linear_factor(name: &str) -> Option<f64> {
    lookup(LINEAR, name).or(match name.to_ascii_lowercase().as_str() {
        "meter" | "m" => Some(1.0),
        _ => None,
    })
}

/// Radians per unit of the angular unit named `name`.
pub(crate) fn angular_factor(name: &str) -> Option<f64> {
    lookup(ANGULAR, name)
}

/// The factor of the scale unit named `name`.
pub(crate) fn scale_factor(name: &str) -> Option<f64> {
    lookup(SCALE, name)
}

/// The factor of `name` in any unit table.
pub(crate) fn any_factor(name: &str) -> Option<f64> {
    linear_factor(name)
        .or_else(|| angular_factor(name))
        .or_else(|| scale_factor(name))
        .or_else(|| lookup(RATE, name))
}

fn lookup(table: &[(&str, f64)], name: &str) -> Option<f64> {
    table
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, f)| *f)
}

/// The name of the linear unit of exactly `metres_per_unit`, if it has one.
pub(crate) fn linear_name(metres_per_unit: f64) -> Option<&'static str> {
    LINEAR
        .iter()
        .find(|(_, f)| *f == metres_per_unit)
        .map(|(n, _)| *n)
}

/// The WKT name of `ellipsoid`, else `unknown`.
pub(crate) fn ellipsoid_name(ellipsoid: &Ellipsoid) -> &'static str {
    ellipsoid
        .proj_name()
        .and_then(|proj| ELLIPSOIDS.iter().find(|(p, _)| *p == proj))
        .map_or("unknown", |(_, wkt)| *wkt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_factors() {
        assert_eq!(linear_factor("US survey foot"), Some(1200.0 / 3937.0));
        assert_eq!(linear_factor("Metre"), Some(1.0));
        assert_eq!(linear_name(0.3048), Some("foot"));
        assert_eq!(angular_factor("degree"), Some(DEGREE_RAD));
        assert_eq!(scale_factor("parts per million"), Some(PPM));
        assert_eq!(any_factor("grad"), Some(core::f64::consts::PI / 200.0));
        // EPSG unit 1027 prints 3.16887651727315e-11 m/s.
        let mm_per_yr = any_factor("millimetres per year").unwrap();
        assert!((mm_per_yr / 3.168_876_517_273_15e-11 - 1.0).abs() < 1e-14);
        assert_eq!(any_factor("year"), Some(31_556_925.445));
        assert_eq!(ellipsoid_name(&Ellipsoid::WGS84), "WGS 84");
        assert_eq!(ellipsoid_name(&Ellipsoid::sphere(6_371_000.0)), "unknown");
    }
}
