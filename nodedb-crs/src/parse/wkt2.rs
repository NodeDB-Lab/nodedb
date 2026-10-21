// SPDX-License-Identifier: Apache-2.0

//! WKT2 (ISO 19162:2019) parser.
//!
//! Reads a geographic CRS (`GEOGCRS`, or `GEODCRS` with an ellipsoidal
//! coordinate system), a projected CRS (`PROJCRS`), and a `BOUNDCRS` whose
//! source is one of those and whose transformation to WGS 84 is a static or
//! time-dependent Helmert transformation. A datum may be a `DATUM` or a
//! datum `ENSEMBLE`. WKT1 keywords are refused.

use super::helmert_params::{HelmertValues, helmert_slot};
use super::model::{
    ConversionModel, CrsModel, DEGREE_RAD, DatumModel, ParamModel, build_def, convert,
};
use super::wkt2_tree::{WKT2_FORMAT, WktNode, WktValue, parse_tree};
use crate::def::{AreaOfUse, AxisDirection, AxisOrder, CrsDef, CrsKind, DatumShift, Ellipsoid};
use crate::error::CrsParseError;
use crate::srid::Srid;

const GEOGRAPHIC: &[&str] = &["GEOGCRS", "GEOGRAPHICCRS", "GEODCRS", "GEODETICCRS"];
const BASE_GEOGRAPHIC: &[&str] = &["BASEGEOGCRS", "BASEGEODCRS"];
const DATUMS: &[&str] = &["DATUM", "GEODETICDATUM", "TRF", "ENSEMBLE"];
const ELLIPSOIDS: &[&str] = &["ELLIPSOID", "SPHEROID"];
const PRIME_MERIDIANS: &[&str] = &["PRIMEM", "PRIMEMERIDIAN"];
const IDS: &[&str] = &["ID", "AUTHORITY"];
const ANGLE_UNITS: &[&str] = &["ANGLEUNIT", "UNIT"];
const LENGTH_UNITS: &[&str] = &["LENGTHUNIT", "UNIT"];
const ANY_UNITS: &[&str] = &["ANGLEUNIT", "LENGTHUNIT", "SCALEUNIT", "UNIT"];
/// Units a Helmert parameter carries: the reference epoch is in years.
const HELMERT_UNITS: &[&str] = &["ANGLEUNIT", "LENGTHUNIT", "SCALEUNIT", "TIMEUNIT", "UNIT"];
const WKT1: &[&str] = &["PROJCS", "GEOGCS", "GEOCCS", "COMPD_CS", "VERT_CS"];

/// Parse WKT2 text into a CRS definition named `name` under `srid`.
pub fn parse_wkt2(srid: Srid, name: &str, text: &str) -> Result<CrsDef, CrsParseError> {
    let root = parse_tree(text)?;
    let model = model_of_crs(&root)?;
    build_def(srid, name, &model, WKT2_FORMAT)
}

fn invalid(detail: impl Into<Box<str>>) -> CrsParseError {
    CrsParseError::content(WKT2_FORMAT, detail)
}

fn model_of_crs(node: &WktNode) -> Result<CrsModel, CrsParseError> {
    let keyword = node.keyword.as_str();
    if keyword == "BOUNDCRS" {
        return bound(node);
    }
    if keyword == "PROJCRS" || keyword == "PROJECTEDCRS" {
        return projected(node);
    }
    if GEOGRAPHIC.contains(&keyword) {
        return geographic(node);
    }
    if WKT1.contains(&keyword) {
        return Err(invalid(format!(
            "{keyword} is WKT1; write the CRS as WKT2 (GEOGCRS or PROJCRS)"
        )));
    }
    Err(invalid(format!(
        "{keyword} is not a supported CRS; supported: GEOGCRS, PROJCRS, BOUNDCRS"
    )))
}

/// A `BOUNDCRS`: the source CRS with its Helmert shift to WGS 84.
fn bound(node: &WktNode) -> Result<CrsModel, CrsParseError> {
    let source = node
        .child(&["SOURCECRS"])
        .and_then(WktNode::first_node)
        .ok_or_else(|| invalid("BOUNDCRS has no SOURCECRS"))?;
    let target = node
        .child(&["TARGETCRS"])
        .and_then(WktNode::first_node)
        .ok_or_else(|| invalid("BOUNDCRS has no TARGETCRS"))?;
    let target = model_of_crs(target)?;
    if target.kind != CrsKind::Geographic || target.datum.ellipsoid != Ellipsoid::WGS84 {
        return Err(invalid("BOUNDCRS must transform to WGS 84"));
    }
    let mut model = model_of_crs(source)?;
    if model.datum.to_wgs84 != DatumShift::Identity {
        return Err(invalid("BOUNDCRS source is itself bound"));
    }
    let transform = node
        .child(&["ABRIDGEDTRANSFORMATION"])
        .ok_or_else(|| invalid("BOUNDCRS has no ABRIDGEDTRANSFORMATION"))?;
    model.datum.to_wgs84 = transformation(transform)?;
    Ok(model)
}

/// The Helmert shift of an `ABRIDGEDTRANSFORMATION`. A parameter with no
/// unit is in the parameter's own unit: metre, arc-second, ppm, their rates
/// per year, or year for the reference epoch. A rate unit factor is SI per
/// second, as EPSG and PROJ write it.
fn transformation(node: &WktNode) -> Result<DatumShift, CrsParseError> {
    let method = node
        .child(&["METHOD"])
        .ok_or_else(|| invalid("ABRIDGEDTRANSFORMATION has no METHOD"))?;
    let mut values = HelmertValues::default();
    for param in node.children(&["PARAMETER"]) {
        let name = param.text(0).unwrap_or_default();
        let slot = helmert_slot(epsg_id(param), name)
            .ok_or_else(|| invalid(format!("Helmert parameter '{name}' is not supported")))?;
        let value = param
            .number(1)
            .ok_or_else(|| invalid(format!("parameter '{name}' has no value")))?;
        values.set(slot, value, unit_factor(param, HELMERT_UNITS)?);
    }
    let accuracy = node.child(&["OPERATIONACCURACY"]).and_then(|a| a.number(0));
    values.into_shift(
        epsg_id(method),
        method.text(0).unwrap_or_default(),
        accuracy,
        WKT2_FORMAT,
    )
}

fn geographic(node: &WktNode) -> Result<CrsModel, CrsParseError> {
    let cs_type = node.child(&["CS"]).and_then(|cs| cs.word(0));
    if let Some(cs) = cs_type
        && !cs.eq_ignore_ascii_case("ellipsoidal")
    {
        return Err(invalid(format!(
            "a {cs} geodetic CRS is not supported; supported: ellipsoidal"
        )));
    }
    let angle_rad = crs_unit(node, ANGLE_UNITS)?.unwrap_or(DEGREE_RAD);
    Ok(CrsModel {
        name: node.text(0).unwrap_or_default().to_string(),
        kind: CrsKind::Geographic,
        datum: datum(node, angle_rad)?,
        conversion: None,
        metres_per_unit: 1.0,
        axis: axis_order(node)?,
        area_of_use: area(node),
    })
}

fn projected(node: &WktNode) -> Result<CrsModel, CrsParseError> {
    let base = node
        .child(BASE_GEOGRAPHIC)
        .ok_or_else(|| invalid("PROJCRS has no BASEGEOGCRS"))?;
    let angle_rad = crs_unit(base, ANGLE_UNITS)?.unwrap_or(DEGREE_RAD);
    let metres_per_unit = crs_unit(node, LENGTH_UNITS)?.unwrap_or(1.0);
    let conversion = node
        .child(&["CONVERSION"])
        .ok_or_else(|| invalid("PROJCRS has no CONVERSION"))?;
    let method = conversion
        .child(&["METHOD", "PROJECTION"])
        .ok_or_else(|| invalid("CONVERSION has no METHOD"))?;
    let mut params = Vec::new();
    for param in conversion.children(&["PARAMETER"]) {
        let name = param.text(0).unwrap_or_default();
        params.push(ParamModel {
            name: name.to_string(),
            code: epsg_id(param),
            value: param
                .number(1)
                .ok_or_else(|| invalid(format!("parameter '{name}' has no value")))?,
            factor: unit_factor(param, ANY_UNITS)?,
            kind: None,
        });
    }
    Ok(CrsModel {
        name: node.text(0).unwrap_or_default().to_string(),
        kind: CrsKind::Projected,
        datum: datum(base, angle_rad)?,
        conversion: Some(ConversionModel {
            method_name: method.text(0).unwrap_or_default().to_string(),
            method_code: epsg_id(method),
            params,
            default_angle_rad: angle_rad,
            default_length_m: metres_per_unit,
        }),
        metres_per_unit,
        axis: axis_order(node)?,
        area_of_use: area(node),
    })
}

/// The datum of a geographic CRS node, its prime meridian read in
/// `angle_rad` radians per unit when it names no unit.
fn datum(crs: &WktNode, angle_rad: f64) -> Result<DatumModel, CrsParseError> {
    let datum = crs
        .child(DATUMS)
        .ok_or_else(|| invalid(format!("{} has no DATUM", crs.keyword)))?;
    let ellipsoid = datum
        .child(ELLIPSOIDS)
        .ok_or_else(|| invalid("DATUM has no ELLIPSOID"))?;
    let a = ellipsoid
        .number(1)
        .ok_or_else(|| invalid("ELLIPSOID has no semi-major axis"))?;
    let inv_f = ellipsoid
        .number(2)
        .ok_or_else(|| invalid("ELLIPSOID has no inverse flattening"))?;
    let a = a * unit_factor(ellipsoid, LENGTH_UNITS)?.unwrap_or(1.0);
    if a <= 0.0 || inv_f < 0.0 {
        return Err(invalid(format!("ELLIPSOID [{a}, {inv_f}] is not valid")));
    }
    let prime_meridian_deg = match crs
        .child(PRIME_MERIDIANS)
        .or_else(|| datum.child(PRIME_MERIDIANS))
    {
        None => 0.0,
        Some(pm) => {
            let value = pm
                .number(1)
                .ok_or_else(|| invalid("PRIMEM has no longitude"))?;
            let factor = unit_factor(pm, ANGLE_UNITS)?.unwrap_or(angle_rad);
            convert(value, factor, DEGREE_RAD)
        }
    };
    Ok(DatumModel {
        ellipsoid: Ellipsoid { a, inv_f },
        prime_meridian_deg,
        epsg_code: epsg_id(datum),
        to_wgs84: DatumShift::Identity,
    })
}

/// The factor of the unit node of `node`, among `keywords`.
fn unit_factor(node: &WktNode, keywords: &[&str]) -> Result<Option<f64>, CrsParseError> {
    match node.child(keywords) {
        None => Ok(None),
        Some(unit) => unit
            .number(1)
            .filter(|f| *f > 0.0)
            .map(Some)
            .ok_or_else(|| invalid(format!("{} has no positive factor", unit.keyword))),
    }
}

/// The unit of a CRS's coordinates: from its first axis, else the unit
/// node of the CRS itself.
fn crs_unit(crs: &WktNode, keywords: &[&str]) -> Result<Option<f64>, CrsParseError> {
    if let Some(axis) = crs.child(&["AXIS"])
        && let Some(factor) = unit_factor(axis, keywords)?
    {
        return Ok(Some(factor));
    }
    unit_factor(crs, keywords)
}

/// The EPSG code an `ID` or `AUTHORITY` child names.
fn epsg_id(node: &WktNode) -> Option<u32> {
    node.children(IDS).find_map(|id| {
        if !id.text(0)?.eq_ignore_ascii_case("EPSG") {
            return None;
        }
        match id.args.get(1)? {
            WktValue::Number(n) if n.fract() == 0.0 && *n >= 0.0 && *n <= f64::from(u32::MAX) => {
                Some(*n as u32)
            }
            WktValue::Text(t) => t.trim().parse().ok(),
            WktValue::Number(_) | WktValue::Node(_) | WktValue::Word(_) => None,
        }
    })
}

/// The axis order of a CRS's `AXIS` children. Two axes along east and
/// north, in either order, read as east-north, the order NodeDB keeps
/// every coordinate in.
fn axis_order(crs: &WktNode) -> Result<AxisOrder, CrsParseError> {
    let mut directions = Vec::new();
    for axis in crs.children(&["AXIS"]).take(2) {
        let word = axis
            .word(1)
            .ok_or_else(|| invalid("AXIS has no direction"))?;
        let direction = match word.to_ascii_lowercase().as_str() {
            "east" => AxisDirection::East,
            "west" => AxisDirection::West,
            "north" => AxisDirection::North,
            "south" => AxisDirection::South,
            other => {
                return Err(invalid(format!(
                    "axis direction '{other}' is not supported; supported: east, west, north, south"
                )));
            }
        };
        directions.push(direction);
    }
    order_from_directions(&directions)
}

/// The axis order of two horizontal directions. None or east-north in
/// either order is east, north, up.
pub(crate) fn order_from_directions(
    directions: &[AxisDirection],
) -> Result<AxisOrder, CrsParseError> {
    match directions {
        [] => Ok(AxisOrder::ENU),
        [AxisDirection::East, AxisDirection::North]
        | [AxisDirection::North, AxisDirection::East] => Ok(AxisOrder::ENU),
        [x, y] => {
            let text: String = [x.letter(), y.letter(), 'u'].iter().collect();
            AxisOrder::parse(&text)
        }
        _ => Err(CrsParseError::content(
            WKT2_FORMAT,
            "a CRS needs two horizontal axes",
        )),
    }
}

/// The area of use: the first `BBOX`, directly or inside a `USAGE`.
fn area(crs: &WktNode) -> Option<AreaOfUse> {
    let bbox = crs
        .child(&["BBOX"])
        .or_else(|| crs.children(&["USAGE"]).find_map(|u| u.child(&["BBOX"])))?;
    Some(AreaOfUse {
        south: bbox.number(0)?,
        west: bbox.number(1)?,
        north: bbox.number(2)?,
        east: bbox.number(3)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::method::ProjectionDef;

    fn parse(text: &str) -> Result<CrsDef, CrsParseError> {
        parse_wkt2(Srid::new(910_001).unwrap(), "test", text)
    }

    /// EPSG:32648 as PROJ 9 writes it.
    const UTM_48N: &str = r#"PROJCRS["WGS 84 / UTM zone 48N",
        BASEGEOGCRS["WGS 84",
            ENSEMBLE["World Geodetic System 1984 ensemble",
                MEMBER["World Geodetic System 1984 (Transit)"],
                ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],
                ENSEMBLEACCURACY[2.0]],
            PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],
            ID["EPSG",4326]],
        CONVERSION["UTM zone 48N",
            METHOD["Transverse Mercator",ID["EPSG",9807]],
            PARAMETER["Latitude of natural origin",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8801]],
            PARAMETER["Longitude of natural origin",105,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8802]],
            PARAMETER["Scale factor at natural origin",0.9996,SCALEUNIT["unity",1],ID["EPSG",8805]],
            PARAMETER["False easting",500000,LENGTHUNIT["metre",1],ID["EPSG",8806]],
            PARAMETER["False northing",0,LENGTHUNIT["metre",1],ID["EPSG",8807]]],
        CS[Cartesian,2],
            AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1]],
            AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1]],
        USAGE[SCOPE["Navigation and medium accuracy spatial referencing."],
            AREA["Between 102°E and 108°E, northern hemisphere"],
            BBOX[0,102,84,108]],
        ID["EPSG",32648]]"#;

    #[test]
    fn utm_48n_matches_the_builtin() {
        let def = parse(UTM_48N).unwrap();
        let builtin = crate::CrsContext::builtin_only()
            .resolve(Srid::new(32648).unwrap())
            .unwrap();
        assert_eq!(def.projection, builtin.projection);
        assert_eq!(def.datum.ellipsoid, builtin.datum.ellipsoid);
        assert_eq!(def.unit, builtin.unit);
        assert_eq!(def.area_of_use, builtin.area_of_use);
        assert_eq!(&*def.name, "test");
    }

    #[test]
    fn bound_crs_carries_its_helmert_shift() {
        let text = r#"BOUNDCRS[
            SOURCECRS[GEOGCRS["Kertau",DATUM["Kertau 1968",ELLIPSOID["Everest 1830 Modified",6377304.063,300.8017,LENGTHUNIT["metre",1]],ID["EPSG",6245]],
                PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],
                AXIS["latitude",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],
                AXIS["longitude",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]]]],
            TARGETCRS[GEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],
                CS[ellipsoidal,2],AXIS["latitude",north],AXIS["longitude",east],ANGLEUNIT["degree",0.0174532925199433]]],
            ABRIDGEDTRANSFORMATION["Kertau to WGS 84",
                METHOD["Coordinate Frame rotation (geog2D domain)",ID["EPSG",9607]],
                PARAMETER["X-axis translation",-11,ID["EPSG",8605]],
                PARAMETER["Y-axis translation",851,ID["EPSG",8606]],
                PARAMETER["Z-axis translation",5,ID["EPSG",8607]],
                PARAMETER["X-axis rotation",0.5,ANGLEUNIT["arc-second",4.84813681109536E-06],ID["EPSG",8608]],
                OPERATIONACCURACY[10]]]"#;
        let def = parse(text).unwrap();
        assert!(def.is_geographic());
        assert!(def.axis.is_enu());
        assert_eq!(def.datum.epsg_code, Some(6245));
        let DatumShift::Helmert(h) = def.datum.to_wgs84 else {
            panic!("expected a Helmert shift");
        };
        assert_eq!(
            h.convention,
            crate::def::RotationConvention::CoordinateFrame
        );
        assert_eq!(
            [h.tx_m, h.ty_m, h.tz_m, h.rx_arcsec],
            [-11.0, 851.0, 5.0, 0.5]
        );
        assert_eq!(h.accuracy_m, Some(10.0));
    }

    #[test]
    fn feet_and_south_west_axes() {
        let text = r#"PROJCRS["x",BASEGEOGCRS["y",DATUM["z",ELLIPSOID["GRS 1980",6378137,298.257222101]]],
            CONVERSION["c",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],
                PARAMETER["Latitude of false origin",40.1666666666667,ID["EPSG",8821]],
                PARAMETER["Longitude of false origin",-74,ID["EPSG",8822]],
                PARAMETER["Latitude of 1st standard parallel",41.0333333333333,ID["EPSG",8823]],
                PARAMETER["Latitude of 2nd standard parallel",40.6666666666667,ID["EPSG",8824]],
                PARAMETER["Easting at false origin",984250,ID["EPSG",8826]],
                PARAMETER["Northing at false origin",0,ID["EPSG",8827]]],
            CS[Cartesian,2],AXIS["x",west],AXIS["y",south],LENGTHUNIT["US survey foot",0.304800609601219]]"#;
        let def = parse(text).unwrap();
        assert_eq!(def.unit.metres_per_unit, 0.304800609601219);
        assert_eq!(def.axis.to_string(), "wsu");
        let Some(ProjectionDef::LambertConicConformal(lcc)) = def.projection else {
            panic!("expected lcc");
        };
        // A length parameter with no unit is in the CRS's unit.
        assert!((lcc.false_easting - 984_250.0 * 0.304800609601219).abs() < 1e-6);
    }

    #[test]
    fn refusals_name_the_problem() {
        assert!(matches!(
            parse(r#"PROJCS["x",GEOGCS["y"]]"#),
            Err(CrsParseError::Invalid { .. })
        ));
        let bonne = r#"PROJCRS["x",BASEGEOGCRS["y",DATUM["z",ELLIPSOID["e",6378137,298.257223563]]],
            CONVERSION["c",METHOD["Bonne",ID["EPSG",9827]]],CS[Cartesian,2]]"#;
        let err = parse(bonne).unwrap_err();
        assert!(err.to_string().contains("Bonne"), "{err}");
        assert!(matches!(
            parse("GEOGCRS["),
            Err(CrsParseError::Malformed { .. })
        ));
    }
}
