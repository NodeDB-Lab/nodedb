// SPDX-License-Identifier: Apache-2.0

//! PROJ-string parser.
//!
//! Follows PROJ's defaults: GRS 1980 when no ellipsoid is given, the first
//! occurrence of a key wins, and explicit `+ellps` / `+towgs84` override
//! what `+datum` supplies.

use super::tokens::{ProjParams, parse_number};
use crate::def::{
    AxisOrder, CrsDef, CrsKind, Datum, DatumShift, Ellipsoid, Helmert, LinearUnit,
    datum_by_proj_name, prime_meridian_by_proj_name,
};
use crate::error::CrsParseError;
use crate::method::{
    AeaParams, AeqdParams, CassParams, CeaParams, EqEarthParams, EqcParams, GeosParams, HomParams,
    HomVariant, KrovakParams, LabrdParams, LaeaParams, LccParams, MercParams, NzmgParams,
    PolyParams, ProjectionDef, SinuParams, SomercParams, StereParams, StereaParams, TmParams,
    WebMercParams, alpha_from_gamma, gamma_from_alpha,
};
use crate::srid::Srid;

/// The `+proj` values this parser implements, for error messages.
pub const IMPLEMENTED_METHODS: &str = "longlat, latlong, utm, tmerc, omerc, lcc, merc, webmerc, \
     cea, eqc, stere, ups, sterea, aea, laea, cass, poly, aeqd, sinu, eqearth, krovak, mod_krovak, \
     somerc, nzmg, geos, labrd";

/// UTM scale factor on the central meridian.
const UTM_K0: f64 = 0.9996;

/// UTM false easting, metres.
const UTM_FALSE_EASTING: f64 = 500_000.0;

/// UTM false northing in the southern hemisphere, metres.
const UTM_FALSE_NORTHING_SOUTH: f64 = 10_000_000.0;

/// Number of UTM zones.
const UTM_ZONES: f64 = 60.0;

/// Width of a UTM zone, degrees.
const UTM_ZONE_WIDTH_DEG: f64 = 6.0;

/// Central meridian of zone 0, so zone z has λ0 = z · 6° − 183°.
const UTM_ZONE0_MERIDIAN_DEG: f64 = -183.0;

/// Largest absolute latitude, degrees.
const MAX_LATITUDE_DEG: f64 = 90.0;

/// Number of values in a 3-parameter `+towgs84`.
const TOWGS84_TRANSLATION_ONLY: usize = 3;

/// Number of values in a 7-parameter `+towgs84`.
const TOWGS84_FULL: usize = 7;

/// Parse a PROJ string into a CRS definition. The area of use stays
/// `None`: PROJ strings do not carry one.
pub fn parse_proj_string(srid: Srid, name: &str, text: &str) -> Result<CrsDef, CrsParseError> {
    let p = ProjParams::tokenize(text)?;
    let axis = match p.text("axis")? {
        None => AxisOrder::ENU,
        Some(v) => AxisOrder::parse(v)?,
    };
    check_type(&p)?;
    let method = p.text("proj")?.ok_or(CrsParseError::Missing {
        key: "proj",
        method: "crs",
    })?;
    let datum = parse_datum(&p)?;
    let projection = match method {
        "longlat" | "latlong" => None,
        other => Some(parse_projection(&p, other, &datum.ellipsoid)?),
    };
    let (kind, unit) = match projection {
        None => (CrsKind::Geographic, LinearUnit::METRE),
        Some(_) => (CrsKind::Projected, parse_unit(&p)?),
    };
    Ok(CrsDef {
        srid,
        name: name.into(),
        kind,
        datum,
        projection,
        unit,
        axis,
        area_of_use: None,
    })
}

/// The projection `+proj=method` names, read from `p`.
fn parse_projection(
    p: &ProjParams<'_>,
    method: &str,
    ellipsoid: &Ellipsoid,
) -> Result<ProjectionDef, CrsParseError> {
    Ok(match method {
        "utm" => parse_utm(p)?,
        "tmerc" => parse_tmerc(p)?,
        "omerc" => parse_omerc(p, ellipsoid)?,
        "lcc" => ProjectionDef::LambertConicConformal(LccParams::from_proj(p)?),
        "merc" => ProjectionDef::Mercator(MercParams::from_proj(p)?),
        "webmerc" => ProjectionDef::PseudoMercator(WebMercParams::from_proj(p)?),
        "cea" => ProjectionDef::LambertCylindricalEqualArea(CeaParams::from_proj(p)?),
        "eqc" => ProjectionDef::EquidistantCylindrical(EqcParams::from_proj(p)?),
        "stere" => ProjectionDef::Stereographic(StereParams::from_proj(p)?),
        // PROJ defines UPS on an ellipsoid only.
        "ups" if ellipsoid.is_sphere() => return Err(CrsParseError::invalid("proj", "ups")),
        "ups" => ProjectionDef::Stereographic(StereParams::from_proj_ups(p)),
        "sterea" => ProjectionDef::ObliqueStereographic(StereaParams::from_proj(p)?),
        "aea" => ProjectionDef::AlbersEqualArea(AeaParams::from_proj(p)?),
        "laea" => ProjectionDef::LambertAzimuthalEqualArea(LaeaParams::from_proj(p)?),
        "cass" => ProjectionDef::CassiniSoldner(CassParams::from_proj(p)?),
        "poly" => ProjectionDef::AmericanPolyconic(PolyParams::from_proj(p)?),
        "aeqd" => ProjectionDef::AzimuthalEquidistant(AeqdParams::from_proj(p)?),
        "sinu" => ProjectionDef::Sinusoidal(SinuParams::from_proj(p)?),
        "eqearth" => ProjectionDef::EqualEarth(EqEarthParams::from_proj(p)?),
        "krovak" | "mod_krovak" => ProjectionDef::Krovak(KrovakParams::from_proj(p)?),
        "somerc" => ProjectionDef::SwissObliqueMercator(SomercParams::from_proj(p)?),
        "nzmg" => ProjectionDef::NewZealandMapGrid(NzmgParams::from_proj(p)?),
        "geos" => ProjectionDef::GeostationarySatellite(GeosParams::from_proj(p)?),
        "labrd" => ProjectionDef::LabordeObliqueMercator(LabrdParams::from_proj(p)?),
        other => {
            return Err(CrsParseError::UnsupportedMethod {
                method: other.into(),
                implemented: IMPLEMENTED_METHODS,
            });
        }
    })
}

/// Accept `+type=crs` only.
fn check_type(p: &ProjParams<'_>) -> Result<(), CrsParseError> {
    match p.text("type")? {
        None | Some("crs") => Ok(()),
        Some(other) => Err(CrsParseError::invalid("type", other)),
    }
}

/// Fail unless `value` is finite and above zero.
fn positive(key: &str, value: f64) -> Result<f64, CrsParseError> {
    if value > 0.0 && value.is_finite() {
        Ok(value)
    } else {
        Err(CrsParseError::invalid(key, &value.to_string()))
    }
}

/// Fail unless `value` is a latitude strictly inside ±90°.
fn open_latitude(key: &str, value: f64) -> Result<f64, CrsParseError> {
    if value.abs() < MAX_LATITUDE_DEG {
        Ok(value)
    } else {
        Err(CrsParseError::invalid(key, &value.to_string()))
    }
}

/// Fail unless `value` is a latitude within ±90°, poles included.
fn closed_latitude(key: &str, value: f64) -> Result<f64, CrsParseError> {
    if value.abs() <= MAX_LATITUDE_DEG {
        Ok(value)
    } else {
        Err(CrsParseError::invalid(key, &value.to_string()))
    }
}

fn parse_datum(p: &ProjParams<'_>) -> Result<Datum, CrsParseError> {
    let named = match p.text("datum")? {
        None => None,
        Some(n) => Some(datum_by_proj_name(n).ok_or_else(|| CrsParseError::invalid("datum", n))?),
    };
    let ellipsoid = parse_ellipsoid(p, named.map(|d| d.ellipsoid))?;
    let to_wgs84 = match p.text("towgs84")? {
        Some(v) => DatumShift::Helmert(parse_towgs84(v)?),
        None => named.map_or(DatumShift::Identity, |d| d.shift),
    };
    let prime_meridian_deg = match p.text("pm")? {
        None => 0.0,
        Some(v) => match prime_meridian_by_proj_name(v) {
            Some(deg) => deg,
            None => parse_number("pm", v)?,
        },
    };
    Ok(Datum {
        ellipsoid,
        prime_meridian_deg,
        to_wgs84,
        // PROJ strings carry no EPSG datum code.
        epsg_code: None,
    })
}

/// Resolve the ellipsoid: `+R` wins, then `+ellps` (or the datum's), then
/// `+a` and a shape key (`+rf`, `+f`, `+b`) override parts of it.
fn parse_ellipsoid(
    p: &ProjParams<'_>,
    datum_ellipsoid: Option<Ellipsoid>,
) -> Result<Ellipsoid, CrsParseError> {
    if let Some(r) = p.number("R")? {
        return Ok(Ellipsoid::sphere(positive("R", r)?));
    }
    let explicit_a = p.number("a")?;
    let named = match p.text("ellps")? {
        None => None,
        Some(n) => {
            Some(Ellipsoid::by_proj_name(n).ok_or_else(|| CrsParseError::invalid("ellps", n))?)
        }
    };
    let base = match (named.or(datum_ellipsoid), explicit_a) {
        (Some(e), _) => e,
        // PROJ reads a bare `+a` with no shape as a sphere.
        (None, Some(a)) => Ellipsoid::sphere(a),
        (None, None) => Ellipsoid::GRS80,
    };
    let a = positive("a", explicit_a.unwrap_or(base.a))?;
    let inv_f = if let Some(rf) = p.number("rf")? {
        positive("rf", rf)?
    } else if let Some(f) = p.number("f")? {
        if f == 0.0 {
            0.0
        } else {
            1.0 / positive("f", f)?
        }
    } else if let Some(b) = p.number("b")? {
        if b == a {
            0.0
        } else if b > 0.0 && b < a {
            a / (a - b)
        } else {
            return Err(CrsParseError::invalid("b", &b.to_string()));
        }
    } else {
        base.inv_f
    };
    Ok(Ellipsoid { a, inv_f })
}

/// Parse 3 or 7 comma-separated `+towgs84` values, position-vector
/// convention.
fn parse_towgs84(text: &str) -> Result<Helmert, CrsParseError> {
    let mut values = [0.0; TOWGS84_FULL];
    let mut count = 0;
    for part in text.split(',') {
        let slot = values
            .get_mut(count)
            .ok_or_else(|| CrsParseError::invalid("towgs84", text))?;
        *slot = parse_number("towgs84", part.trim())?;
        count += 1;
    }
    if count != TOWGS84_TRANSLATION_ONLY && count != TOWGS84_FULL {
        return Err(CrsParseError::invalid("towgs84", text));
    }
    Ok(Helmert::position_vector(values))
}

fn parse_unit(p: &ProjParams<'_>) -> Result<LinearUnit, CrsParseError> {
    if let Some(m) = p.number("to_meter")? {
        return Ok(LinearUnit {
            metres_per_unit: positive("to_meter", m)?,
        });
    }
    match p.text("units")? {
        None => Ok(LinearUnit::METRE),
        Some(n) => LinearUnit::by_proj_name(n).ok_or_else(|| CrsParseError::invalid("units", n)),
    }
}

fn parse_utm(p: &ProjParams<'_>) -> Result<ProjectionDef, CrsParseError> {
    let zone_text = p.text("zone")?.ok_or(CrsParseError::Missing {
        key: "zone",
        method: "utm",
    })?;
    let zone = parse_number("zone", zone_text)?;
    if zone.fract() != 0.0 || !(1.0..=UTM_ZONES).contains(&zone) {
        return Err(CrsParseError::invalid("zone", zone_text));
    }
    let false_northing = if p.has("south") {
        UTM_FALSE_NORTHING_SOUTH
    } else {
        0.0
    };
    Ok(ProjectionDef::TransverseMercator(TmParams {
        lat0_deg: 0.0,
        lon0_deg: zone * UTM_ZONE_WIDTH_DEG + UTM_ZONE0_MERIDIAN_DEG,
        k0: UTM_K0,
        false_easting: UTM_FALSE_EASTING,
        false_northing,
    }))
}

fn parse_tmerc(p: &ProjParams<'_>) -> Result<ProjectionDef, CrsParseError> {
    Ok(ProjectionDef::TransverseMercator(TmParams {
        // PROJ accepts a pole as the TM origin latitude, as EPSG:2081 uses.
        lat0_deg: closed_latitude("lat_0", p.number_or("lat_0", 0.0)?)?,
        lon0_deg: p.number_or("lon_0", 0.0)?,
        k0: positive("k", p.number_any(&["k", "k_0"])?.unwrap_or(1.0))?,
        false_easting: p.number_or("x_0", 0.0)?,
        false_northing: p.number_or("y_0", 0.0)?,
    }))
}

fn parse_omerc(p: &ProjParams<'_>, ellipsoid: &Ellipsoid) -> Result<ProjectionDef, CrsParseError> {
    let lat_c_deg = open_latitude("lat_0", p.number_or("lat_0", 0.0)?)?;
    let (alpha_c_deg, gamma_c_deg) = match (p.number("alpha")?, p.number("gamma")?) {
        (Some(alpha), Some(gamma)) => (alpha, gamma),
        (Some(alpha), None) => (alpha, gamma_from_alpha(lat_c_deg, alpha, ellipsoid)),
        (None, Some(gamma)) => (alpha_from_gamma(lat_c_deg, gamma, ellipsoid), gamma),
        (None, None) => {
            return Err(CrsParseError::Missing {
                key: "alpha",
                method: "omerc",
            });
        }
    };
    if !alpha_c_deg.is_finite() || !gamma_c_deg.is_finite() {
        return Err(CrsParseError::invalid("alpha", &alpha_c_deg.to_string()));
    }
    let variant = if p.has("no_uoff") || p.has("no_off") {
        HomVariant::A
    } else {
        HomVariant::B
    };
    Ok(ProjectionDef::HotineObliqueMercator(HomParams {
        variant,
        lat_c_deg,
        lon_c_deg: p.number_or("lonc", 0.0)?,
        alpha_c_deg,
        gamma_c_deg,
        k_c: positive("k", p.number_any(&["k", "k_0"])?.unwrap_or(1.0))?,
        false_easting: p.number_or("x_0", 0.0)?,
        false_northing: p.number_or("y_0", 0.0)?,
        no_rot: p.has("no_rot"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<CrsDef, CrsParseError> {
        parse_proj_string(Srid::WGS84, "test", text)
    }

    #[test]
    fn wgs84_geographic() {
        let def = parse("+proj=longlat +datum=WGS84 +no_defs").unwrap();
        assert_eq!(def.kind, CrsKind::Geographic);
        assert_eq!(def.datum, Datum::WGS84);
        assert!(def.projection.is_none());
    }

    #[test]
    fn utm_zone_48_north_and_south() {
        let def = parse("+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs").unwrap();
        let Some(ProjectionDef::TransverseMercator(tm)) = def.projection else {
            panic!("expected tmerc");
        };
        assert_eq!(tm.lon0_deg, 105.0);
        assert_eq!(tm.false_northing, 0.0);
        let def = parse("+proj=utm +zone=1 +south +ellps=WGS84").unwrap();
        let Some(ProjectionDef::TransverseMercator(tm)) = def.projection else {
            panic!("expected tmerc");
        };
        assert_eq!(tm.lon0_deg, -177.0);
        assert_eq!(tm.false_northing, 10_000_000.0);
        assert!(parse("+proj=utm +zone=61").is_err());
        assert!(parse("+proj=utm +zone=4.5").is_err());
        assert_eq!(
            parse("+proj=utm"),
            Err(CrsParseError::Missing {
                key: "zone",
                method: "utm"
            })
        );
    }

    #[test]
    fn rso_omerc_matches_epsg_3375() {
        let def = parse(
            "+proj=omerc +no_uoff +lat_0=4 +lonc=102.25 +alpha=323.025796466667 \
             +gamma=323.130102361111 +k=0.99984 +x_0=804671 +y_0=0 +ellps=GRS80 +units=m +no_defs",
        )
        .unwrap();
        let Some(ProjectionDef::HotineObliqueMercator(hom)) = def.projection else {
            panic!("expected omerc");
        };
        assert_eq!(hom.variant, HomVariant::A);
        assert_eq!(hom.k_c, 0.99984);
        assert_eq!(def.datum.ellipsoid, Ellipsoid::GRS80);
    }

    #[test]
    fn omerc_without_gamma_uses_gamma0() {
        let def = parse("+proj=omerc +lat_0=4 +lonc=115 +alpha=53.31580995 +k=0.99984").unwrap();
        let Some(ProjectionDef::HotineObliqueMercator(hom)) = def.projection else {
            panic!("expected omerc");
        };
        assert_eq!(hom.variant, HomVariant::B);
        assert!(
            (hom.gamma_c_deg - 53.13).abs() < 0.01,
            "{}",
            hom.gamma_c_deg
        );
        assert!(parse("+proj=omerc +lat_0=4").is_err());
    }

    #[test]
    fn towgs84_and_ellipsoid_forms() {
        let def = parse("+proj=longlat +ellps=evrst48 +towgs84=-11,851,5 +no_defs").unwrap();
        assert_eq!(def.datum.ellipsoid, Ellipsoid::EVEREST_1830_MODIFIED);
        assert_eq!(
            def.datum.to_wgs84,
            DatumShift::Helmert(Helmert::translation(-11.0, 851.0, 5.0))
        );
        assert!(parse("+proj=longlat +towgs84=1,2").is_err());
        assert!(parse("+proj=longlat +towgs84=1,2,3,4,5,6,7,8").is_err());
        let def = parse("+proj=longlat +a=6378137 +rf=298.257223563").unwrap();
        assert_eq!(def.datum.ellipsoid, Ellipsoid::WGS84);
        let def = parse("+proj=longlat +R=6371007.181").unwrap();
        assert!(def.datum.ellipsoid.is_sphere());
        let def = parse("+proj=longlat +a=6378206.4 +b=6356583.8").unwrap();
        assert_eq!(def.datum.ellipsoid, Ellipsoid::CLARKE_1866);
        let def = parse("+proj=tmerc").unwrap();
        assert_eq!(def.datum.ellipsoid, Ellipsoid::GRS80);
    }

    #[test]
    fn units_pm_axis_type() {
        let def = parse("+proj=tmerc +units=us-ft +pm=paris +axis=enu +type=crs").unwrap();
        assert_eq!(def.unit.metres_per_unit, 1200.0 / 3937.0);
        assert!((def.datum.prime_meridian_deg - 2.337_229_166_667).abs() < 1e-11);
        let def = parse("+proj=tmerc +units=ft +to_meter=0.3047972654").unwrap();
        assert_eq!(def.unit.metres_per_unit, 0.304_797_265_4);
        assert_eq!(def.axis, AxisOrder::ENU);
        let def = parse("+proj=tmerc +axis=wsu").unwrap();
        assert_eq!(def.axis.to_string(), "wsu");
        let def = parse("+proj=longlat +axis=neu").unwrap();
        assert_eq!(def.axis.to_string(), "neu");
        for bad in [
            "+proj=tmerc +axis=ewu",
            "+proj=tmerc +axis=en",
            "+proj=tmerc +axis",
        ] {
            assert!(
                matches!(parse(bad), Err(CrsParseError::InvalidValue { .. })),
                "{bad}"
            );
        }
        assert!(parse("+proj=tmerc +units=furlong").is_err());
    }

    #[test]
    fn unsupported_method_lists_implemented() {
        let err = parse("+proj=bonne +lat_1=10").unwrap_err();
        assert_eq!(
            err,
            CrsParseError::UnsupportedMethod {
                method: "bonne".into(),
                implemented: IMPLEMENTED_METHODS
            }
        );
        assert!(matches!(
            parse("+proj=tmerc +nadgrids=@null"),
            Err(CrsParseError::UnknownParameter { .. })
        ));
    }

    #[test]
    fn every_method_parses() {
        let cases = [
            "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=23 +lon_0=-96",
            "+proj=merc +lat_ts=10",
            "+proj=webmerc +datum=WGS84",
            "+proj=cea +lat_ts=30 +ellps=WGS84",
            "+proj=eqc +lat_ts=0",
            "+proj=stere +lat_0=90 +lat_ts=70",
            "+proj=ups +south +ellps=WGS84",
            "+proj=sterea +lat_0=52.15 +lon_0=5.38 +k=0.9999079",
            "+proj=aea +lat_1=29.5 +lat_2=45.5",
            "+proj=laea +lat_0=52 +lon_0=10",
            "+proj=cass +lat_0=4 +lon_0=101 +hyperbolic",
            "+proj=poly +lon_0=-54",
            "+proj=aeqd +guam +lat_0=13.47 +lon_0=144.75",
            "+proj=sinu +R=6371007.181",
            "+proj=eqearth",
            "+proj=krovak +czech +ellps=bessel",
            "+proj=mod_krovak +ellps=bessel",
            "+proj=somerc +lat_0=46.95 +lon_0=7.44 +k_0=1",
            "+proj=nzmg +ellps=intl",
            "+proj=geos +h=35785831 +sweep=x",
            "+proj=labrd +lat_0=-18.9 +lon_0=46.43 +azi=18.9 +k=0.9995",
        ];
        for text in cases {
            let def = parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(def.kind, CrsKind::Projected, "{text}");
        }
        let Some(ProjectionDef::Krovak(k)) = parse("+proj=mod_krovak").unwrap().projection else {
            panic!("expected krovak");
        };
        assert!(k.modified);
    }

    #[test]
    fn ups_refuses_a_sphere() {
        assert_eq!(
            parse("+proj=ups +R=6371000"),
            Err(CrsParseError::invalid("proj", "ups"))
        );
        let Some(ProjectionDef::Stereographic(s)) = parse("+proj=ups +south").unwrap().projection
        else {
            panic!("expected stere");
        };
        assert_eq!(s.lat0_deg, -90.0);
    }
}
