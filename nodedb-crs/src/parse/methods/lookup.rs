// SPDX-License-Identifier: Apache-2.0

//! Method lookup: readers by EPSG code or name, writers by projection
//! shape.

use super::super::model::normalize_name;
use super::azimuthal::{AEQD, GEOS_X, GEOS_Y, GUAM, LAEA, POLAR_A, POLAR_B, STERE, STEREA};
use super::conic::{AEA, KROVAK, KROVAK_MOD, KROVAK_MOD_NORTH, KROVAK_NORTH, LCC_2SP, POLY};
use super::cylindrical::{CASS, CEA, EQC, EQEARTH, HYPERBOLIC_CASS, SINU};
use super::mercator::{HOM_A, HOM_B, LABRD, MERC_A, MERC_B, NZMG, SOMERC, TMERC, WEBMERC};
use super::spec::MethodSpec;
use super::{azimuthal, conic, cylindrical, mercator};
use crate::method::{HomVariant, ProjectionDef, SweepAxis};

/// Every method a writer picks.
const WRITERS: &[MethodSpec] = &[
    TMERC,
    HOM_A,
    HOM_B,
    LCC_2SP,
    MERC_A,
    MERC_B,
    WEBMERC,
    CEA,
    EQC,
    POLAR_A,
    POLAR_B,
    STERE,
    STEREA,
    AEA,
    LAEA,
    CASS,
    HYPERBOLIC_CASS,
    POLY,
    AEQD,
    GUAM,
    SINU,
    EQEARTH,
    KROVAK,
    KROVAK_NORTH,
    KROVAK_MOD,
    KROVAK_MOD_NORTH,
    SOMERC,
    NZMG,
    GEOS_Y,
    GEOS_X,
    LABRD,
];

/// The read-only aliases of every family.
const READ_ONLY: &[&[MethodSpec]] = &[
    mercator::READ_ONLY,
    conic::READ_ONLY,
    cylindrical::READ_ONLY,
    azimuthal::READ_ONLY,
];

/// Every method: the writer methods, then the read-only aliases.
fn all_methods() -> impl Iterator<Item = &'static MethodSpec> {
    WRITERS
        .iter()
        .chain(READ_ONLY.iter().flat_map(|family| family.iter()))
}

/// The method with EPSG `code`, else the one named `name`.
pub(crate) fn method_by(code: Option<u32>, name: &str) -> Option<&'static MethodSpec> {
    if let Some(code) = code
        && let Some(found) = all_methods().find(|m| m.code == Some(code))
    {
        return Some(found);
    }
    let normalized = normalize_name(name);
    all_methods().find(|m| normalize_name(m.name) == normalized)
}

/// The method a writer names `projection` by.
pub(crate) fn writer_spec(projection: &ProjectionDef) -> &'static MethodSpec {
    match projection {
        ProjectionDef::TransverseMercator(_) => &TMERC,
        ProjectionDef::HotineObliqueMercator(p) => match p.variant {
            HomVariant::A => &HOM_A,
            HomVariant::B => &HOM_B,
        },
        ProjectionDef::LambertConicConformal(_) => &LCC_2SP,
        ProjectionDef::Mercator(p) => match p.lat_ts_deg {
            Some(_) => &MERC_B,
            None => &MERC_A,
        },
        ProjectionDef::PseudoMercator(_) => &WEBMERC,
        ProjectionDef::LambertCylindricalEqualArea(_) => &CEA,
        ProjectionDef::EquidistantCylindrical(_) => &EQC,
        ProjectionDef::Stereographic(p) => match (p.lat0_deg.abs() == 90.0, p.lat_ts_deg) {
            (true, None) => &POLAR_A,
            (true, Some(_)) => &POLAR_B,
            (false, _) => &STERE,
        },
        ProjectionDef::ObliqueStereographic(_) => &STEREA,
        ProjectionDef::AlbersEqualArea(_) => &AEA,
        ProjectionDef::LambertAzimuthalEqualArea(_) => &LAEA,
        ProjectionDef::CassiniSoldner(p) => {
            if p.hyperbolic {
                &HYPERBOLIC_CASS
            } else {
                &CASS
            }
        }
        ProjectionDef::AmericanPolyconic(_) => &POLY,
        ProjectionDef::AzimuthalEquidistant(p) => {
            if p.guam {
                &GUAM
            } else {
                &AEQD
            }
        }
        ProjectionDef::Sinusoidal(_) => &SINU,
        ProjectionDef::EqualEarth(_) => &EQEARTH,
        ProjectionDef::Krovak(p) => match (p.modified, p.czech) {
            (false, true) => &KROVAK,
            (false, false) => &KROVAK_NORTH,
            (true, true) => &KROVAK_MOD,
            (true, false) => &KROVAK_MOD_NORTH,
        },
        ProjectionDef::SwissObliqueMercator(_) => &SOMERC,
        ProjectionDef::NewZealandMapGrid(_) => &NZMG,
        ProjectionDef::GeostationarySatellite(p) => match p.sweep {
            SweepAxis::X => &GEOS_X,
            SweepAxis::Y => &GEOS_Y,
        },
        ProjectionDef::LabordeObliqueMercator(_) => &LABRD,
    }
}

#[cfg(test)]
mod tests {
    use super::super::spec::Target;
    use super::*;

    #[test]
    fn codes_and_names_are_unique() {
        let all: Vec<&MethodSpec> = all_methods().collect();
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(normalize_name(a.name), normalize_name(b.name), "{}", a.name);
                if a.code.is_some() {
                    assert_ne!(a.code, b.code, "{}", a.name);
                }
            }
        }
    }

    #[test]
    fn lookup_by_code_then_name() {
        assert_eq!(
            method_by(Some(9807), "anything").map(|m| m.proj),
            Some("tmerc")
        );
        assert_eq!(
            method_by(None, "hotine_oblique_mercator (variant b)").map(|m| m.code),
            Some(Some(9815))
        );
        assert!(method_by(Some(1), "Bonne").is_none());
        let hom = method_by(Some(9812), "").unwrap();
        assert_eq!(
            hom.param(None, "Azimuth at projection centre")
                .map(|p| p.target),
            Some(Target::Keys(&["alpha"]))
        );
    }
}
