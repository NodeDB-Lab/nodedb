// SPDX-License-Identifier: Apache-2.0

//! Linear units of projected coordinates and the PROJ `+units` names.

use serde::{Deserialize, Serialize};

/// A linear unit, as a length in metres.
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
pub struct LinearUnit {
    pub metres_per_unit: f64,
}

impl LinearUnit {
    /// The metre.
    pub const METRE: LinearUnit = LinearUnit {
        metres_per_unit: 1.0,
    };

    /// The unit PROJ calls `name` in `+units`.
    pub fn by_proj_name(name: &str) -> Option<LinearUnit> {
        NAMED
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, metres_per_unit)| LinearUnit {
                metres_per_unit: *metres_per_unit,
            })
    }

    /// The unit with EPSG code `code`, for the length units this crate
    /// names.
    pub fn by_epsg_code(code: u16) -> Option<LinearUnit> {
        BY_EPSG_CODE
            .iter()
            .find(|(c, _)| *c == code)
            .map(|(_, metres_per_unit)| LinearUnit {
                metres_per_unit: *metres_per_unit,
            })
    }

    /// The PROJ `+units` name of this exact unit, if it has one.
    pub fn proj_name(&self) -> Option<&'static str> {
        NAMED
            .iter()
            .find(|(_, m)| *m == self.metres_per_unit)
            .map(|(n, _)| *n)
    }
}

impl Default for LinearUnit {
    fn default() -> Self {
        LinearUnit::METRE
    }
}

/// Metres per US survey inch: 100/3937, the US survey foot divided by 12.
const US_INCH: f64 = 100.0 / 3937.0;

/// PROJ `+units` names with metres per unit.
const NAMED: &[(&str, f64)] = &[
    ("m", 1.0),
    ("km", 1000.0),
    ("dm", 0.1),
    ("cm", 0.01),
    ("mm", 0.001),
    ("kmi", 1852.0),
    ("in", 0.0254),
    ("ft", 0.3048),
    ("yd", 0.9144),
    ("mi", 1609.344),
    ("fath", 1.8288),
    ("ch", 20.1168),
    ("link", 0.201_168),
    ("us-in", US_INCH),
    ("us-ft", 1200.0 / 3937.0),
    ("us-yd", 36.0 * US_INCH),
    ("us-ch", 792.0 * US_INCH),
    ("us-mi", 63_360.0 * US_INCH),
    ("ind-yd", 0.914_395_23),
    ("ind-ft", 0.304_798_41),
    ("ind-ch", 20.116_695_06),
];

/// EPSG length unit codes with metres per unit: metre, foot, US survey
/// foot and kilometre.
const BY_EPSG_CODE: &[(u16, f64)] = &[
    (9001, 1.0),
    (9002, 0.3048),
    (9003, 1200.0 / 3937.0),
    (9036, 1000.0),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epsg_codes_name_their_length() {
        let metres = |code| LinearUnit::by_epsg_code(code).map(|u| u.metres_per_unit);
        assert_eq!(metres(9001), Some(1.0));
        assert_eq!(metres(9002), Some(0.3048));
        assert_eq!(metres(9003), Some(1200.0 / 3937.0));
        assert_eq!(metres(9036), Some(1000.0));
        assert_eq!(metres(9999), None);
    }

    #[test]
    fn us_survey_foot_is_1200_over_3937() {
        let ft = LinearUnit::by_proj_name("us-ft").map(|u| u.metres_per_unit);
        assert!(ft.is_some_and(|m| (m - 1200.0 / 3937.0).abs() < 1e-15));
    }

    #[test]
    fn names_round_trip() {
        for (name, m) in NAMED {
            let unit = LinearUnit {
                metres_per_unit: *m,
            };
            assert_eq!(LinearUnit::by_proj_name(name), Some(unit));
            assert_eq!(unit.proj_name(), Some(*name));
        }
    }
}
