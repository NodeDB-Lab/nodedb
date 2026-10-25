// SPDX-License-Identifier: Apache-2.0

//! Reference ellipsoids and the PROJ `+ellps` names.

use serde::{Deserialize, Serialize};

/// An ellipsoid of revolution, defined by its semi-major axis and inverse
/// flattening. `inv_f == 0` marks a sphere.
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
pub struct Ellipsoid {
    /// Semi-major axis in metres.
    pub a: f64,
    /// Inverse flattening 1/f, or 0 for a sphere.
    pub inv_f: f64,
}

/// Build an ellipsoid from a and b, the form PROJ uses for some names.
const fn from_a_b(a: f64, b: f64) -> Ellipsoid {
    if a == b {
        Ellipsoid { a, inv_f: 0.0 }
    } else {
        Ellipsoid {
            a,
            inv_f: a / (a - b),
        }
    }
}

impl Ellipsoid {
    /// WGS 84 (EPSG:7030).
    pub const WGS84: Ellipsoid = Ellipsoid {
        a: 6_378_137.0,
        inv_f: 298.257_223_563,
    };
    /// GRS 1980 (EPSG:7019).
    pub const GRS80: Ellipsoid = Ellipsoid {
        a: 6_378_137.0,
        inv_f: 298.257_222_101,
    };
    /// WGS 72 (EPSG:7043).
    pub const WGS72: Ellipsoid = Ellipsoid {
        a: 6_378_135.0,
        inv_f: 298.26,
    };
    /// Everest 1830 (1937 adjustment), PROJ `evrst30`.
    pub const EVEREST_1830: Ellipsoid = Ellipsoid {
        a: 6_377_276.345,
        inv_f: 300.8017,
    };
    /// Everest (Sabah and Sarawak), PROJ `evrstSS`.
    pub const EVEREST_SABAH_SARAWAK: Ellipsoid = Ellipsoid {
        a: 6_377_298.556,
        inv_f: 300.8017,
    };
    /// Everest 1830 Modified, the Kertau ellipsoid (EPSG:7018), PROJ `evrst48`.
    pub const EVEREST_1830_MODIFIED: Ellipsoid = Ellipsoid {
        a: 6_377_304.063,
        inv_f: 300.8017,
    };
    /// Everest 1956, PROJ `evrst56`.
    pub const EVEREST_1956: Ellipsoid = Ellipsoid {
        a: 6_377_301.243,
        inv_f: 300.8017,
    };
    /// Everest 1969, PROJ `evrst69`.
    pub const EVEREST_1969: Ellipsoid = Ellipsoid {
        a: 6_377_295.664,
        inv_f: 300.8017,
    };
    /// International 1924, PROJ `intl`.
    pub const INTERNATIONAL_1924: Ellipsoid = Ellipsoid {
        a: 6_378_388.0,
        inv_f: 297.0,
    };
    /// Clarke 1866, PROJ `clrk66`.
    pub const CLARKE_1866: Ellipsoid = from_a_b(6_378_206.4, 6_356_583.8);
    /// Clarke 1880 (modified), PROJ `clrk80`.
    pub const CLARKE_1880: Ellipsoid = Ellipsoid {
        a: 6_378_249.145,
        inv_f: 293.4663,
    };
    /// Clarke 1880 (IGN), PROJ `clrk80ign`.
    pub const CLARKE_1880_IGN: Ellipsoid = Ellipsoid {
        a: 6_378_249.2,
        inv_f: 293.466_021_293_626_9,
    };
    /// Bessel 1841, PROJ `bessel`.
    pub const BESSEL_1841: Ellipsoid = Ellipsoid {
        a: 6_377_397.155,
        inv_f: 299.152_812_8,
    };
    /// Bessel 1841 (Namibia), PROJ `bess_nam`.
    pub const BESSEL_NAMIBIA: Ellipsoid = Ellipsoid {
        a: 6_377_483.865,
        inv_f: 299.152_812_8,
    };
    /// Krassovsky 1940, PROJ `krass`.
    pub const KRASSOVSKY: Ellipsoid = Ellipsoid {
        a: 6_378_245.0,
        inv_f: 298.3,
    };
    /// Airy 1830, PROJ `airy`.
    pub const AIRY_1830: Ellipsoid = Ellipsoid {
        a: 6_377_563.396,
        inv_f: 299.324_964_6,
    };
    /// Airy Modified 1849, PROJ `mod_airy`.
    pub const AIRY_MODIFIED: Ellipsoid = from_a_b(6_377_340.189, 6_356_034.446);
    /// Australian National and South American 1969, PROJ `aust_SA`.
    pub const AUSTRALIAN_SA: Ellipsoid = Ellipsoid {
        a: 6_378_160.0,
        inv_f: 298.25,
    };
    /// Helmert 1906, PROJ `helmert`.
    pub const HELMERT_1906: Ellipsoid = Ellipsoid {
        a: 6_378_200.0,
        inv_f: 298.3,
    };
    /// GRS 1967, PROJ `GRS67`.
    pub const GRS67: Ellipsoid = Ellipsoid {
        a: 6_378_160.0,
        inv_f: 298.247_167_427,
    };
    /// WGS 66, PROJ `WGS66`.
    pub const WGS66: Ellipsoid = Ellipsoid {
        a: 6_378_145.0,
        inv_f: 298.25,
    };
    /// IAU 1976, PROJ `IAU76`.
    pub const IAU76: Ellipsoid = Ellipsoid {
        a: 6_378_140.0,
        inv_f: 298.257,
    };
    /// PZ-90, PROJ `PZ90`.
    pub const PZ90: Ellipsoid = Ellipsoid {
        a: 6_378_136.0,
        inv_f: 298.257_84,
    };
    /// GSK-2011, PROJ `GSK2011`.
    pub const GSK2011: Ellipsoid = Ellipsoid {
        a: 6_378_136.5,
        inv_f: 298.256_415_1,
    };
    /// Fischer (Mercury Datum) 1960 modified, PROJ `fschr60m`.
    pub const FISCHER_1960_MODIFIED: Ellipsoid = Ellipsoid {
        a: 6_378_155.0,
        inv_f: 298.3,
    };
    /// Naval Weapons Lab 1965, PROJ `NWL9D`. Same values as WGS 66.
    pub const NWL9D: Ellipsoid = Ellipsoid {
        a: 6_378_145.0,
        inv_f: 298.25,
    };
    /// Hough, PROJ `hough`.
    pub const HOUGH: Ellipsoid = Ellipsoid {
        a: 6_378_270.0,
        inv_f: 297.0,
    };
    /// Andrae 1876, PROJ `andrae`.
    pub const ANDRAE: Ellipsoid = Ellipsoid {
        a: 6_377_104.43,
        inv_f: 300.0,
    };
    /// New International 1967, PROJ `new_intl`.
    pub const NEW_INTERNATIONAL_1967: Ellipsoid = from_a_b(6_378_157.5, 6_356_772.2);
    /// Southeast Asia, PROJ `SEasia`.
    pub const SOUTHEAST_ASIA: Ellipsoid = from_a_b(6_378_155.0, 6_356_773.320_5);
    /// Walbeck, PROJ `walbeck`.
    pub const WALBECK: Ellipsoid = from_a_b(6_376_896.0, 6_355_834.846_7);
    /// The PROJ `sphere`, radius 6 370 997 m.
    pub const SPHERE: Ellipsoid = Ellipsoid {
        a: 6_370_997.0,
        inv_f: 0.0,
    };

    /// A sphere of radius `r` metres.
    pub const fn sphere(r: f64) -> Ellipsoid {
        Ellipsoid { a: r, inv_f: 0.0 }
    }

    /// Whether this is a sphere.
    pub fn is_sphere(&self) -> bool {
        self.inv_f == 0.0
    }

    /// Flattening f, 0 for a sphere.
    pub fn f(&self) -> f64 {
        if self.is_sphere() {
            0.0
        } else {
            1.0 / self.inv_f
        }
    }

    /// Semi-minor axis b = a(1 − f).
    pub fn b(&self) -> f64 {
        self.a * (1.0 - self.f())
    }

    /// First eccentricity squared, e² = f(2 − f).
    pub fn e2(&self) -> f64 {
        let f = self.f();
        f * (2.0 - f)
    }

    /// First eccentricity e.
    pub fn e(&self) -> f64 {
        self.e2().sqrt()
    }

    /// Third flattening n = f / (2 − f).
    pub fn n(&self) -> f64 {
        let f = self.f();
        f / (2.0 - f)
    }

    /// The ellipsoid PROJ calls `name` in `+ellps`.
    pub fn by_proj_name(name: &str) -> Option<Ellipsoid> {
        NAMED
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, ellipsoid)| *ellipsoid)
    }

    /// The ellipsoid with EPSG code `code`, for the ellipsoids this crate
    /// names.
    pub fn by_epsg_code(code: u16) -> Option<Ellipsoid> {
        BY_EPSG_CODE
            .iter()
            .find(|(c, _)| *c == code)
            .map(|(_, ellipsoid)| *ellipsoid)
    }

    /// A PROJ `+ellps` name of this exact ellipsoid, if it has one. Where
    /// two names share values, the first in the table wins.
    pub fn proj_name(&self) -> Option<&'static str> {
        NAMED
            .iter()
            .find(|(_, ellipsoid)| ellipsoid == self)
            .map(|(n, _)| *n)
    }
}

/// EPSG ellipsoid codes of the named ellipsoids.
const BY_EPSG_CODE: &[(u16, Ellipsoid)] = &[
    (7001, Ellipsoid::AIRY_1830),
    (7003, Ellipsoid::AUSTRALIAN_SA),
    (7004, Ellipsoid::BESSEL_1841),
    (7008, Ellipsoid::CLARKE_1866),
    (7015, Ellipsoid::EVEREST_1830),
    (7016, Ellipsoid::EVEREST_SABAH_SARAWAK),
    (7018, Ellipsoid::EVEREST_1830_MODIFIED),
    (7019, Ellipsoid::GRS80),
    (7022, Ellipsoid::INTERNATIONAL_1924),
    (7024, Ellipsoid::KRASSOVSKY),
    (7030, Ellipsoid::WGS84),
    (7036, Ellipsoid::GRS67),
    (7043, Ellipsoid::WGS72),
    (7044, Ellipsoid::EVEREST_1956),
    (7056, Ellipsoid::EVEREST_1969),
];

/// PROJ `+ellps` names with their exact defining values.
const NAMED: &[(&str, Ellipsoid)] = &[
    ("WGS84", Ellipsoid::WGS84),
    ("GRS80", Ellipsoid::GRS80),
    ("WGS72", Ellipsoid::WGS72),
    ("evrst30", Ellipsoid::EVEREST_1830),
    ("evrstSS", Ellipsoid::EVEREST_SABAH_SARAWAK),
    ("evrst48", Ellipsoid::EVEREST_1830_MODIFIED),
    ("evrst56", Ellipsoid::EVEREST_1956),
    ("evrst69", Ellipsoid::EVEREST_1969),
    ("intl", Ellipsoid::INTERNATIONAL_1924),
    ("clrk66", Ellipsoid::CLARKE_1866),
    ("clrk80", Ellipsoid::CLARKE_1880),
    ("clrk80ign", Ellipsoid::CLARKE_1880_IGN),
    ("bessel", Ellipsoid::BESSEL_1841),
    ("bess_nam", Ellipsoid::BESSEL_NAMIBIA),
    ("krass", Ellipsoid::KRASSOVSKY),
    ("airy", Ellipsoid::AIRY_1830),
    ("mod_airy", Ellipsoid::AIRY_MODIFIED),
    ("aust_SA", Ellipsoid::AUSTRALIAN_SA),
    ("helmert", Ellipsoid::HELMERT_1906),
    ("GRS67", Ellipsoid::GRS67),
    ("WGS66", Ellipsoid::WGS66),
    ("IAU76", Ellipsoid::IAU76),
    ("PZ90", Ellipsoid::PZ90),
    ("GSK2011", Ellipsoid::GSK2011),
    ("NWL9D", Ellipsoid::NWL9D),
    ("fschr60m", Ellipsoid::FISCHER_1960_MODIFIED),
    ("hough", Ellipsoid::HOUGH),
    ("andrae", Ellipsoid::ANDRAE),
    ("new_intl", Ellipsoid::NEW_INTERNATIONAL_1967),
    ("SEasia", Ellipsoid::SOUTHEAST_ASIA),
    ("walbeck", Ellipsoid::WALBECK),
    ("sphere", Ellipsoid::SPHERE),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grs80_and_wgs84_differ_only_in_flattening() {
        assert_eq!(Ellipsoid::GRS80.a, Ellipsoid::WGS84.a);
        assert!((Ellipsoid::GRS80.e2() - 0.006_694_380_022_90).abs() < 1e-14);
        assert!((Ellipsoid::WGS84.e2() - 0.006_694_379_990_14).abs() < 1e-14);
    }

    #[test]
    fn sphere_has_no_flattening() {
        let s = Ellipsoid::sphere(6_371_007.181);
        assert_eq!(s.f(), 0.0);
        assert_eq!(s.e(), 0.0);
        assert_eq!(s.b(), s.a);
    }

    #[test]
    fn names_round_trip() {
        for (name, ellipsoid) in NAMED {
            assert_eq!(Ellipsoid::by_proj_name(name), Some(*ellipsoid));
            let canonical = ellipsoid.proj_name().and_then(Ellipsoid::by_proj_name);
            assert_eq!(canonical, Some(*ellipsoid));
        }
        assert_eq!(Ellipsoid::by_proj_name("nope"), None);
    }

    #[test]
    fn epsg_codes() {
        assert_eq!(Ellipsoid::by_epsg_code(7030), Some(Ellipsoid::WGS84));
        assert_eq!(Ellipsoid::by_epsg_code(7019), Some(Ellipsoid::GRS80));
        assert_eq!(Ellipsoid::by_epsg_code(7000), None);
    }

    #[test]
    fn a_b_form_keeps_b() {
        let c = Ellipsoid::CLARKE_1866;
        assert!((c.b() - 6_356_583.8).abs() < 1e-6);
    }
}
