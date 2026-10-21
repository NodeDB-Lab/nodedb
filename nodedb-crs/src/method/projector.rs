// SPDX-License-Identifier: Apache-2.0

//! A projection with its constants computed for one ellipsoid.

use core::f64::consts::FRAC_PI_2;

use super::aea::Albers;
use super::aeqd::AzimuthalEquidistant;
use super::cass::Cassini;
use super::cea::CylindricalEqualArea;
use super::eqc::EquidistantCylindrical;
use super::eqearth::EqualEarth;
use super::geos::GeostationarySatellite;
use super::krovak::Krovak;
use super::labrd::LabordeObliqueMercator;
use super::laea::LambertAzimuthalEqualArea;
use super::lcc::LambertConformalConic;
use super::merc::Mercator;
use super::nzmg::NewZealandMapGrid;
use super::omerc::HotineObliqueMercator;
use super::poly::Polyconic;
use super::sinu::Sinusoidal;
use super::somerc::SwissObliqueMercator;
use super::stere::Stereographic;
use super::sterea::ObliqueStereographic;
use super::tmerc::TransverseMercator;
use super::webmerc::WebMercator;
use crate::error::DomainReason;

/// A prepared projection. Built by
/// [`ProjectionDef::prepare`](super::ProjectionDef::prepare).
#[derive(Debug, Clone, PartialEq)]
pub enum Projector {
    TransverseMercator(TransverseMercator),
    HotineObliqueMercator(HotineObliqueMercator),
    LambertConicConformal(LambertConformalConic),
    Mercator(Mercator),
    PseudoMercator(WebMercator),
    LambertCylindricalEqualArea(CylindricalEqualArea),
    EquidistantCylindrical(EquidistantCylindrical),
    Stereographic(Stereographic),
    ObliqueStereographic(ObliqueStereographic),
    AlbersEqualArea(Albers),
    LambertAzimuthalEqualArea(LambertAzimuthalEqualArea),
    CassiniSoldner(Cassini),
    AmericanPolyconic(Polyconic),
    /// Boxed: the geodesic series coefficients make it the largest by far.
    AzimuthalEquidistant(Box<AzimuthalEquidistant>),
    Sinusoidal(Sinusoidal),
    EqualEarth(EqualEarth),
    Krovak(Krovak),
    SwissObliqueMercator(SwissObliqueMercator),
    NewZealandMapGrid(NewZealandMapGrid),
    GeostationarySatellite(GeostationarySatellite),
    LabordeObliqueMercator(LabordeObliqueMercator),
}

impl Projector {
    /// Project λ, φ radians (λ from the CRS prime meridian) to x, y metres,
    /// false easting and northing included.
    pub fn forward(&self, lam: f64, phi: f64) -> Result<(f64, f64), DomainReason> {
        if !lam.is_finite() || !phi.is_finite() {
            return Err(DomainReason::NonFinite);
        }
        if phi.abs() > FRAC_PI_2 {
            return Err(DomainReason::LatitudeRange);
        }
        let out = match self {
            Projector::TransverseMercator(p) => p.forward(lam, phi),
            Projector::HotineObliqueMercator(p) => p.forward(lam, phi),
            Projector::LambertConicConformal(p) => p.forward(lam, phi),
            Projector::Mercator(p) => p.forward(lam, phi),
            Projector::PseudoMercator(p) => p.forward(lam, phi),
            Projector::LambertCylindricalEqualArea(p) => p.forward(lam, phi),
            Projector::EquidistantCylindrical(p) => p.forward(lam, phi),
            Projector::Stereographic(p) => p.forward(lam, phi),
            Projector::ObliqueStereographic(p) => p.forward(lam, phi),
            Projector::AlbersEqualArea(p) => p.forward(lam, phi),
            Projector::LambertAzimuthalEqualArea(p) => p.forward(lam, phi),
            Projector::CassiniSoldner(p) => p.forward(lam, phi),
            Projector::AmericanPolyconic(p) => p.forward(lam, phi),
            Projector::AzimuthalEquidistant(p) => p.forward(lam, phi),
            Projector::Sinusoidal(p) => p.forward(lam, phi),
            Projector::EqualEarth(p) => p.forward(lam, phi),
            Projector::Krovak(p) => p.forward(lam, phi),
            Projector::SwissObliqueMercator(p) => p.forward(lam, phi),
            Projector::NewZealandMapGrid(p) => p.forward(lam, phi),
            Projector::GeostationarySatellite(p) => p.forward(lam, phi),
            Projector::LabordeObliqueMercator(p) => p.forward(lam, phi),
        }?;
        finite(out)
    }

    /// Unproject x, y metres to λ, φ radians.
    pub fn inverse(&self, x: f64, y: f64) -> Result<(f64, f64), DomainReason> {
        if !x.is_finite() || !y.is_finite() {
            return Err(DomainReason::NonFinite);
        }
        let out = match self {
            Projector::TransverseMercator(p) => p.inverse(x, y),
            Projector::HotineObliqueMercator(p) => p.inverse(x, y),
            Projector::LambertConicConformal(p) => p.inverse(x, y),
            Projector::Mercator(p) => p.inverse(x, y),
            Projector::PseudoMercator(p) => p.inverse(x, y),
            Projector::LambertCylindricalEqualArea(p) => p.inverse(x, y),
            Projector::EquidistantCylindrical(p) => p.inverse(x, y),
            Projector::Stereographic(p) => p.inverse(x, y),
            Projector::ObliqueStereographic(p) => p.inverse(x, y),
            Projector::AlbersEqualArea(p) => p.inverse(x, y),
            Projector::LambertAzimuthalEqualArea(p) => p.inverse(x, y),
            Projector::CassiniSoldner(p) => p.inverse(x, y),
            Projector::AmericanPolyconic(p) => p.inverse(x, y),
            Projector::AzimuthalEquidistant(p) => p.inverse(x, y),
            Projector::Sinusoidal(p) => p.inverse(x, y),
            Projector::EqualEarth(p) => p.inverse(x, y),
            Projector::Krovak(p) => p.inverse(x, y),
            Projector::SwissObliqueMercator(p) => p.inverse(x, y),
            Projector::NewZealandMapGrid(p) => p.inverse(x, y),
            Projector::GeostationarySatellite(p) => p.inverse(x, y),
            Projector::LabordeObliqueMercator(p) => p.inverse(x, y),
        }?;
        finite(out)
    }
}

/// Fail unless both outputs are finite.
fn finite(out: (f64, f64)) -> Result<(f64, f64), DomainReason> {
    if out.0.is_finite() && out.1.is_finite() {
        Ok(out)
    } else {
        Err(DomainReason::NonFinite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::Ellipsoid;
    use crate::method::{ProjectionDef, TmParams};

    fn utm48() -> Projector {
        ProjectionDef::TransverseMercator(TmParams {
            lat0_deg: 0.0,
            lon0_deg: 105.0,
            k0: 0.9996,
            false_easting: 500_000.0,
            false_northing: 0.0,
        })
        .prepare(&Ellipsoid::WGS84)
    }

    #[test]
    fn bad_latitude_and_non_finite_are_out_of_domain() {
        let p = utm48();
        assert_eq!(
            p.forward(105f64.to_radians(), 91f64.to_radians()),
            Err(DomainReason::LatitudeRange)
        );
        assert_eq!(p.forward(f64::NAN, 0.0), Err(DomainReason::NonFinite));
        assert_eq!(p.inverse(f64::INFINITY, 1.0), Err(DomainReason::NonFinite));
    }
}
