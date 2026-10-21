// SPDX-License-Identifier: Apache-2.0

//! The defining parameters of every implemented projection method.
//!
//! A new method adds one file under `method/`, one variant here and one in
//! [`Projector`], and one arm in each exhaustive match.

use serde::{Deserialize, Serialize};

use super::aea::{AeaParams, Albers};
use super::aeqd::{AeqdParams, AzimuthalEquidistant};
use super::cass::{CassParams, Cassini};
use super::cea::{CeaParams, CylindricalEqualArea};
use super::eqc::{EqcParams, EquidistantCylindrical};
use super::eqearth::{EqEarthParams, EqualEarth};
use super::geos::{GeosParams, GeostationarySatellite};
use super::krovak::{Krovak, KrovakParams};
use super::labrd::{LabordeObliqueMercator, LabrdParams};
use super::laea::{LaeaParams, LambertAzimuthalEqualArea};
use super::lcc::{LambertConformalConic, LccParams};
use super::merc::{MercParams, Mercator};
use super::nzmg::{NewZealandMapGrid, NzmgParams};
use super::omerc::{HomParams, HotineObliqueMercator};
use super::poly::{PolyParams, Polyconic};
use super::projector::Projector;
use super::sinu::{SinuParams, Sinusoidal};
use super::somerc::{SomercParams, SwissObliqueMercator};
use super::stere::{StereParams, Stereographic};
use super::sterea::{ObliqueStereographic, StereaParams};
use super::tmerc::{TmParams, TransverseMercator};
use super::webmerc::{WebMercParams, WebMercator};
use crate::def::Ellipsoid;
use crate::error::DomainReason;

/// A map projection with its defining parameters.
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
pub enum ProjectionDef {
    /// EPSG 9807 and 9808 (South Orientated, through `+axis=wsu`), PROJ
    /// `tmerc` and `utm`.
    TransverseMercator(TmParams),
    /// EPSG 9812 / 9815, PROJ `omerc`.
    HotineObliqueMercator(HomParams),
    /// EPSG 9801, 9802, 9803, 9826 (West Orientated, through `+axis=wnu`),
    /// 1051 and 1102, PROJ `lcc`.
    LambertConicConformal(LccParams),
    /// EPSG 9804, 9805 and 1026, PROJ `merc`.
    Mercator(MercParams),
    /// EPSG 1024, PROJ `webmerc`.
    PseudoMercator(WebMercParams),
    /// EPSG 9834 and 9835, PROJ `cea`.
    LambertCylindricalEqualArea(CeaParams),
    /// EPSG 1028 and 1029, PROJ `eqc`.
    EquidistantCylindrical(EqcParams),
    /// EPSG 9810, 9829 and 9830 plus the oblique and equatorial aspects,
    /// PROJ `stere` and `ups`.
    Stereographic(StereParams),
    /// EPSG 9809, PROJ `sterea`.
    ObliqueStereographic(StereaParams),
    /// EPSG 9822, PROJ `aea`.
    AlbersEqualArea(AeaParams),
    /// EPSG 9820 and 1027, PROJ `laea`.
    LambertAzimuthalEqualArea(LaeaParams),
    /// EPSG 9806 and 9833 (hyperbolic), PROJ `cass`.
    CassiniSoldner(CassParams),
    /// EPSG 9818, PROJ `poly`.
    AmericanPolyconic(PolyParams),
    /// EPSG 1125, 9831 (Guam) and 9832, PROJ `aeqd`.
    AzimuthalEquidistant(AeqdParams),
    /// No EPSG method, PROJ `sinu`.
    Sinusoidal(SinuParams),
    /// EPSG 1078, PROJ `eqearth`.
    EqualEarth(EqEarthParams),
    /// EPSG 9819, 1041, 1042 and 1043, PROJ `krovak` and `mod_krovak`.
    Krovak(KrovakParams),
    /// EPSG 9814 and the 9815 rows PROJ writes as `somerc`, PROJ `somerc`.
    SwissObliqueMercator(SomercParams),
    /// EPSG 9811, PROJ `nzmg`.
    NewZealandMapGrid(NzmgParams),
    /// No EPSG method, PROJ `geos`.
    GeostationarySatellite(GeosParams),
    /// EPSG 9813, PROJ `labrd`.
    LabordeObliqueMercator(LabrdParams),
}

impl ProjectionDef {
    /// Compute the constants of this projection on `ellipsoid`.
    pub fn prepare(&self, ellipsoid: &Ellipsoid) -> Projector {
        let e = ellipsoid;
        match self {
            ProjectionDef::TransverseMercator(p) => {
                Projector::TransverseMercator(TransverseMercator::new(p, e))
            }
            ProjectionDef::HotineObliqueMercator(p) => {
                Projector::HotineObliqueMercator(HotineObliqueMercator::new(p, e))
            }
            ProjectionDef::LambertConicConformal(p) => {
                Projector::LambertConicConformal(LambertConformalConic::new(p, e))
            }
            ProjectionDef::Mercator(p) => Projector::Mercator(Mercator::new(p, e)),
            ProjectionDef::PseudoMercator(p) => Projector::PseudoMercator(WebMercator::new(p, e)),
            ProjectionDef::LambertCylindricalEqualArea(p) => {
                Projector::LambertCylindricalEqualArea(CylindricalEqualArea::new(p, e))
            }
            ProjectionDef::EquidistantCylindrical(p) => {
                Projector::EquidistantCylindrical(EquidistantCylindrical::new(p, e))
            }
            ProjectionDef::Stereographic(p) => Projector::Stereographic(Stereographic::new(p, e)),
            ProjectionDef::ObliqueStereographic(p) => {
                Projector::ObliqueStereographic(ObliqueStereographic::new(p, e))
            }
            ProjectionDef::AlbersEqualArea(p) => Projector::AlbersEqualArea(Albers::new(p, e)),
            ProjectionDef::LambertAzimuthalEqualArea(p) => {
                Projector::LambertAzimuthalEqualArea(LambertAzimuthalEqualArea::new(p, e))
            }
            ProjectionDef::CassiniSoldner(p) => Projector::CassiniSoldner(Cassini::new(p, e)),
            ProjectionDef::AmericanPolyconic(p) => {
                Projector::AmericanPolyconic(Polyconic::new(p, e))
            }
            ProjectionDef::AzimuthalEquidistant(p) => {
                Projector::AzimuthalEquidistant(Box::new(AzimuthalEquidistant::new(p, e)))
            }
            ProjectionDef::Sinusoidal(p) => Projector::Sinusoidal(Sinusoidal::new(p, e)),
            ProjectionDef::EqualEarth(p) => Projector::EqualEarth(EqualEarth::new(p, e)),
            ProjectionDef::Krovak(p) => Projector::Krovak(Krovak::new(p, e)),
            ProjectionDef::SwissObliqueMercator(p) => {
                Projector::SwissObliqueMercator(SwissObliqueMercator::new(p, e))
            }
            ProjectionDef::NewZealandMapGrid(p) => {
                Projector::NewZealandMapGrid(NewZealandMapGrid::new(p, e))
            }
            ProjectionDef::GeostationarySatellite(p) => {
                Projector::GeostationarySatellite(GeostationarySatellite::new(p, e))
            }
            ProjectionDef::LabordeObliqueMercator(p) => {
                Projector::LabordeObliqueMercator(LabordeObliqueMercator::new(p, e))
            }
        }
    }

    /// Append this projection's PROJ-string parameters, each with a leading
    /// space, such as ` +proj=tmerc +lat_0=0`.
    pub fn write_proj(&self, out: &mut String) {
        match self {
            ProjectionDef::TransverseMercator(p) => p.write_proj(out),
            ProjectionDef::HotineObliqueMercator(p) => p.write_proj(out),
            ProjectionDef::LambertConicConformal(p) => p.write_proj(out),
            ProjectionDef::Mercator(p) => p.write_proj(out),
            ProjectionDef::PseudoMercator(p) => p.write_proj(out),
            ProjectionDef::LambertCylindricalEqualArea(p) => p.write_proj(out),
            ProjectionDef::EquidistantCylindrical(p) => p.write_proj(out),
            ProjectionDef::Stereographic(p) => p.write_proj(out),
            ProjectionDef::ObliqueStereographic(p) => p.write_proj(out),
            ProjectionDef::AlbersEqualArea(p) => p.write_proj(out),
            ProjectionDef::LambertAzimuthalEqualArea(p) => p.write_proj(out),
            ProjectionDef::CassiniSoldner(p) => p.write_proj(out),
            ProjectionDef::AmericanPolyconic(p) => p.write_proj(out),
            ProjectionDef::AzimuthalEquidistant(p) => p.write_proj(out),
            ProjectionDef::Sinusoidal(p) => p.write_proj(out),
            ProjectionDef::EqualEarth(p) => p.write_proj(out),
            ProjectionDef::Krovak(p) => p.write_proj(out),
            ProjectionDef::SwissObliqueMercator(p) => p.write_proj(out),
            ProjectionDef::NewZealandMapGrid(p) => p.write_proj(out),
            ProjectionDef::GeostationarySatellite(p) => p.write_proj(out),
            ProjectionDef::LabordeObliqueMercator(p) => p.write_proj(out),
        }
    }

    /// Project λ, φ radians (λ from the CRS prime meridian) to x, y metres.
    /// Prepares the constants per call. A [`Projector`] reuses them.
    pub fn forward(
        &self,
        lam_rad: f64,
        phi_rad: f64,
        ellipsoid: &Ellipsoid,
    ) -> Result<(f64, f64), DomainReason> {
        self.prepare(ellipsoid).forward(lam_rad, phi_rad)
    }

    /// Unproject x, y metres to λ, φ radians.
    pub fn inverse(
        &self,
        x_m: f64,
        y_m: f64,
        ellipsoid: &Ellipsoid,
    ) -> Result<(f64, f64), DomainReason> {
        self.prepare(ellipsoid).inverse(x_m, y_m)
    }
}
