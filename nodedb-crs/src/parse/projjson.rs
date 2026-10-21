// SPDX-License-Identifier: Apache-2.0

//! PROJJSON parser.
//!
//! Reads a `GeographicCRS` (or a `GeodeticCRS` with an ellipsoidal
//! coordinate system), a `ProjectedCRS`, and a `BoundCRS` whose source is
//! one of those and whose transformation to WGS 84 is a static or
//! time-dependent Helmert transformation. A datum may be a `datum` or a
//! `datum_ensemble`.

use super::helmert_params::{HelmertValues, helmert_slot};
use super::json::{Json, PROJJSON_FORMAT, parse_json};
use super::model::{
    ConversionModel, CrsModel, DEGREE_RAD, DatumModel, ParamModel, build_def, convert,
};
use super::unit_names::{angular_factor, any_factor, linear_factor};
use super::wkt2::order_from_directions;
use crate::def::{AreaOfUse, AxisDirection, CrsDef, CrsKind, DatumShift, Ellipsoid};
use crate::error::CrsParseError;
use crate::srid::Srid;

/// Parse PROJJSON text into a CRS definition named `name` under `srid`.
pub fn parse_projjson(srid: Srid, name: &str, text: &str) -> Result<CrsDef, CrsParseError> {
    let root = parse_json(text)?;
    let model = model_of_crs(&root)?;
    build_def(srid, name, &model, PROJJSON_FORMAT)
}

fn invalid(detail: impl Into<Box<str>>) -> CrsParseError {
    CrsParseError::content(PROJJSON_FORMAT, detail)
}

/// A required member of `object`.
fn member<'a>(object: &'a Json, key: &str, owner: &str) -> Result<&'a Json, CrsParseError> {
    object
        .get(key)
        .ok_or_else(|| invalid(format!("{owner} has no \"{key}\"")))
}

fn type_of(object: &Json) -> &str {
    object
        .get("type")
        .and_then(Json::as_str)
        .unwrap_or_default()
}

fn model_of_crs(crs: &Json) -> Result<CrsModel, CrsParseError> {
    match type_of(crs) {
        "BoundCRS" => bound(crs),
        "ProjectedCRS" => projected(crs),
        "GeographicCRS" | "GeodeticCRS" => geographic(crs),
        other => Err(invalid(format!(
            "type \"{other}\" is not a supported CRS; supported: GeographicCRS, ProjectedCRS, \
             BoundCRS"
        ))),
    }
}

fn bound(crs: &Json) -> Result<CrsModel, CrsParseError> {
    let target = model_of_crs(member(crs, "target_crs", "BoundCRS")?)?;
    if target.kind != CrsKind::Geographic || target.datum.ellipsoid != Ellipsoid::WGS84 {
        return Err(invalid("BoundCRS must transform to WGS 84"));
    }
    let mut model = model_of_crs(member(crs, "source_crs", "BoundCRS")?)?;
    if model.datum.to_wgs84 != DatumShift::Identity {
        return Err(invalid("BoundCRS source is itself bound"));
    }
    let transformation = member(crs, "transformation", "BoundCRS")?;
    let method = member(transformation, "method", "transformation")?;
    // A parameter with no unit is in the parameter's own unit. A rate unit
    // factor is SI per second, as EPSG and PROJ write it.
    let mut values = HelmertValues::default();
    for param in parameters(transformation)? {
        let name = param.get("name").and_then(Json::as_str).unwrap_or_default();
        let slot = helmert_slot(epsg_id(param), name)
            .ok_or_else(|| invalid(format!("Helmert parameter '{name}' is not supported")))?;
        let value = number(member(param, "value", name)?, name)?;
        let factor = match param.get("unit") {
            None => None,
            Some(unit) => Some(unit_factor(unit, any_factor)?),
        };
        values.set(slot, value, factor);
    }
    let accuracy = match transformation.get("accuracy") {
        None => None,
        Some(Json::Number(n)) => Some(*n),
        Some(Json::String(s)) => Some(
            s.trim()
                .parse::<f64>()
                .map_err(|_| invalid(format!("accuracy '{s}' is not a number")))?,
        ),
        Some(Json::Null | Json::Bool(_) | Json::Array(_) | Json::Object(_)) => {
            return Err(invalid("accuracy is not a number"));
        }
    };
    model.datum.to_wgs84 = values.into_shift(
        epsg_id(method),
        method
            .get("name")
            .and_then(Json::as_str)
            .unwrap_or_default(),
        accuracy,
        PROJJSON_FORMAT,
    )?;
    Ok(model)
}

fn geographic(crs: &Json) -> Result<CrsModel, CrsParseError> {
    let cs = crs.get("coordinate_system");
    if let Some(subtype) = cs.and_then(|c| c.get("subtype")).and_then(Json::as_str)
        && subtype != "ellipsoidal"
    {
        return Err(invalid(format!(
            "a {subtype} geodetic CRS is not supported; supported: ellipsoidal"
        )));
    }
    let angle_rad = axis_unit(cs, angular_factor)?.unwrap_or(DEGREE_RAD);
    Ok(CrsModel {
        name: crs
            .get("name")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string(),
        kind: CrsKind::Geographic,
        datum: datum(crs, angle_rad)?,
        conversion: None,
        metres_per_unit: 1.0,
        axis: axis_order(cs)?,
        area_of_use: area(crs)?,
    })
}

fn projected(crs: &Json) -> Result<CrsModel, CrsParseError> {
    let base = member(crs, "base_crs", "ProjectedCRS")?;
    let angle_rad = axis_unit(base.get("coordinate_system"), angular_factor)?.unwrap_or(DEGREE_RAD);
    let cs = crs.get("coordinate_system");
    let metres_per_unit = axis_unit(cs, linear_factor)?.unwrap_or(1.0);
    let conversion = member(crs, "conversion", "ProjectedCRS")?;
    let method = member(conversion, "method", "conversion")?;
    let mut params = Vec::new();
    for param in parameters(conversion)? {
        let name = param.get("name").and_then(Json::as_str).unwrap_or_default();
        params.push(ParamModel {
            name: name.to_string(),
            code: epsg_id(param),
            value: number(member(param, "value", name)?, name)?,
            factor: param
                .get("unit")
                .map(|u| unit_factor(u, any_factor))
                .transpose()?,
            kind: None,
        });
    }
    Ok(CrsModel {
        name: crs
            .get("name")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string(),
        kind: CrsKind::Projected,
        datum: datum(base, angle_rad)?,
        conversion: Some(ConversionModel {
            method_name: method
                .get("name")
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_string(),
            method_code: epsg_id(method),
            params,
            default_angle_rad: angle_rad,
            default_length_m: metres_per_unit,
        }),
        metres_per_unit,
        axis: axis_order(cs)?,
        area_of_use: area(crs)?,
    })
}

/// The `parameters` array of a conversion or transformation. Absent is
/// empty.
fn parameters(object: &Json) -> Result<&[Json], CrsParseError> {
    match object.get("parameters") {
        None => Ok(&[]),
        Some(p) => p
            .as_array()
            .ok_or_else(|| invalid("\"parameters\" is not an array")),
    }
}

/// A plain number.
fn number(value: &Json, what: &str) -> Result<f64, CrsParseError> {
    value
        .as_f64()
        .ok_or_else(|| invalid(format!("'{what}' is not a number")))
}

/// A measure that is a number in its default unit, or a
/// `{"value": n, "unit": u}` object.
fn measure(
    value: &Json,
    what: &str,
    factor_of: fn(&str) -> Option<f64>,
    default_factor: f64,
    canonical: f64,
) -> Result<f64, CrsParseError> {
    match value {
        Json::Number(n) => Ok(convert(*n, default_factor, canonical)),
        Json::Object(_) => {
            let n = number(member(value, "value", what)?, what)?;
            let factor = match value.get("unit") {
                None => default_factor,
                Some(unit) => unit_factor(unit, factor_of)?,
            };
            Ok(convert(n, factor, canonical))
        }
        Json::Null | Json::Bool(_) | Json::String(_) | Json::Array(_) => {
            Err(invalid(format!("'{what}' is not a number")))
        }
    }
}

/// The factor of a unit: a name `factor_of` knows, or an object with a
/// `conversion_factor`.
fn unit_factor(unit: &Json, factor_of: fn(&str) -> Option<f64>) -> Result<f64, CrsParseError> {
    match unit {
        Json::String(name) => {
            factor_of(name).ok_or_else(|| invalid(format!("unit '{name}' is not supported")))
        }
        Json::Object(_) => unit
            .get("conversion_factor")
            .and_then(Json::as_f64)
            .filter(|f| *f > 0.0)
            .ok_or_else(|| invalid("unit has no positive \"conversion_factor\"")),
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::Array(_) => {
            Err(invalid("unit is neither a name nor an object"))
        }
    }
}

/// The unit of a coordinate system's first axis.
fn axis_unit(
    cs: Option<&Json>,
    factor_of: fn(&str) -> Option<f64>,
) -> Result<Option<f64>, CrsParseError> {
    let Some(unit) = cs
        .and_then(|c| c.get("axis"))
        .and_then(Json::as_array)
        .and_then(<[Json]>::first)
        .and_then(|axis| axis.get("unit"))
    else {
        return Ok(None);
    };
    unit_factor(unit, factor_of).map(Some)
}

/// The datum of a geographic CRS object.
fn datum(crs: &Json, angle_rad: f64) -> Result<DatumModel, CrsParseError> {
    let datum = crs
        .get("datum")
        .or_else(|| crs.get("datum_ensemble"))
        .ok_or_else(|| invalid("geographic CRS has no \"datum\" or \"datum_ensemble\""))?;
    let ellipsoid = member(datum, "ellipsoid", "datum")?;
    let length = |key: &str| -> Result<Option<f64>, CrsParseError> {
        ellipsoid
            .get(key)
            .map(|v| measure(v, key, linear_factor, 1.0, 1.0))
            .transpose()
    };
    let ellipsoid = if let Some(r) = length("radius")? {
        Ellipsoid::sphere(r)
    } else {
        let a = length("semi_major_axis")?
            .ok_or_else(|| invalid("ellipsoid has no \"semi_major_axis\" or \"radius\""))?;
        let inv_f = match (
            ellipsoid.get("inverse_flattening"),
            length("semi_minor_axis")?,
        ) {
            (Some(rf), _) => number(rf, "inverse_flattening")?,
            (None, Some(b)) if b == a => 0.0,
            (None, Some(b)) if b > 0.0 && b < a => a / (a - b),
            (None, Some(_) | None) => {
                return Err(invalid(
                    "ellipsoid needs \"inverse_flattening\" or a smaller \"semi_minor_axis\"",
                ));
            }
        };
        Ellipsoid { a, inv_f }
    };
    if ellipsoid.a <= 0.0 || ellipsoid.inv_f < 0.0 {
        return Err(invalid("ellipsoid axes are not valid"));
    }
    let prime_meridian_deg = match datum
        .get("prime_meridian")
        .or_else(|| crs.get("prime_meridian"))
        .and_then(|pm| pm.get("longitude"))
    {
        None => 0.0,
        Some(lon) => measure(lon, "longitude", angular_factor, angle_rad, DEGREE_RAD)?,
    };
    Ok(DatumModel {
        ellipsoid,
        prime_meridian_deg,
        epsg_code: epsg_id(datum),
        to_wgs84: DatumShift::Identity,
    })
}

/// The EPSG code of an object's `id`, or of the first of its `ids`.
fn epsg_id(object: &Json) -> Option<u32> {
    let id = object
        .get("id")
        .or_else(|| object.get("ids").and_then(Json::as_array)?.first())?;
    if !id.get("authority")?.as_str()?.eq_ignore_ascii_case("EPSG") {
        return None;
    }
    match id.get("code")? {
        Json::Number(n) if n.fract() == 0.0 && *n >= 0.0 && *n <= f64::from(u32::MAX) => {
            Some(*n as u32)
        }
        Json::String(s) => s.trim().parse().ok(),
        Json::Number(_) | Json::Null | Json::Bool(_) | Json::Array(_) | Json::Object(_) => None,
    }
}

/// The axis order of a coordinate system.
fn axis_order(cs: Option<&Json>) -> Result<crate::def::AxisOrder, CrsParseError> {
    let axes = cs
        .and_then(|c| c.get("axis"))
        .and_then(Json::as_array)
        .unwrap_or_default();
    let mut directions = Vec::new();
    for axis in axes.iter().take(2) {
        let word = axis
            .get("direction")
            .and_then(Json::as_str)
            .ok_or_else(|| invalid("axis has no \"direction\""))?;
        directions.push(match word {
            "east" => AxisDirection::East,
            "west" => AxisDirection::West,
            "north" => AxisDirection::North,
            "south" => AxisDirection::South,
            other => {
                return Err(invalid(format!(
                    "axis direction '{other}' is not supported; supported: east, west, north, south"
                )));
            }
        });
    }
    order_from_directions(&directions)
}

/// The area of use: `bbox`, or the `bbox` of the first usage that has
/// one.
fn area(crs: &Json) -> Result<Option<AreaOfUse>, CrsParseError> {
    let bbox = crs.get("bbox").or_else(|| {
        crs.get("usages")?
            .as_array()?
            .iter()
            .find_map(|u| u.get("bbox"))
    });
    let Some(bbox) = bbox else {
        return Ok(None);
    };
    let side = |key: &str| number(member(bbox, key, "bbox")?, key);
    Ok(Some(AreaOfUse {
        south: side("south_latitude")?,
        west: side("west_longitude")?,
        north: side("north_latitude")?,
        east: side("east_longitude")?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<CrsDef, CrsParseError> {
        parse_projjson(Srid::new(910_001).unwrap(), "test", text)
    }

    /// EPSG:3375 GDM2000 / Peninsula RSO as PROJ 9 writes it, trimmed.
    const RSO: &str = r#"{
      "type": "ProjectedCRS", "name": "GDM2000 / Peninsula RSO",
      "base_crs": {"name": "GDM2000",
        "datum": {"type": "GeodeticReferenceFrame", "name": "Geodetic Datum of Malaysia 2000",
          "ellipsoid": {"name": "GRS 1980", "semi_major_axis": 6378137, "inverse_flattening": 298.257222101}},
        "coordinate_system": {"subtype": "ellipsoidal", "axis": [
          {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
          {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
        "id": {"authority": "EPSG", "code": 4742}},
      "conversion": {"name": "Peninsular RSO",
        "method": {"name": "Hotine Oblique Mercator (variant A)", "id": {"authority": "EPSG", "code": 9812}},
        "parameters": [
          {"name": "Latitude of projection centre", "value": 4, "unit": "degree", "id": {"authority": "EPSG", "code": 8811}},
          {"name": "Longitude of projection centre", "value": 102.25, "unit": "degree", "id": {"authority": "EPSG", "code": 8812}},
          {"name": "Azimuth at projection centre", "value": 323.025796466667, "unit": "degree", "id": {"authority": "EPSG", "code": 8813}},
          {"name": "Angle from Rectified to Skew Grid", "value": 323.130102361111, "unit": "degree", "id": {"authority": "EPSG", "code": 8814}},
          {"name": "Scale factor at projection centre", "value": 0.99984, "unit": "unity", "id": {"authority": "EPSG", "code": 8815}},
          {"name": "False easting", "value": 804671, "unit": "metre", "id": {"authority": "EPSG", "code": 8806}},
          {"name": "False northing", "value": 0, "unit": "metre", "id": {"authority": "EPSG", "code": 8807}}]},
      "coordinate_system": {"subtype": "Cartesian", "axis": [
        {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
        {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"}]},
      "bbox": {"south_latitude": 1.13, "west_longitude": 98.02, "north_latitude": 7.81, "east_longitude": 105.82},
      "id": {"authority": "EPSG", "code": 3375}}"#;

    #[test]
    fn rso_matches_the_builtin_projection() {
        let def = parse(RSO).unwrap();
        let builtin = crate::CrsContext::builtin_only()
            .resolve(Srid::new(3375).unwrap())
            .unwrap();
        assert_eq!(def.projection, builtin.projection);
        assert_eq!(def.datum.ellipsoid, builtin.datum.ellipsoid);
        assert_eq!(def.area_of_use, builtin.area_of_use);
    }

    #[test]
    fn units_as_objects_and_spheres() {
        let text = r#"{"type": "ProjectedCRS", "name": "x",
          "base_crs": {"name": "s", "datum": {"name": "d", "ellipsoid": {"name": "e", "radius": 6371007.181}}},
          "conversion": {"name": "c", "method": {"name": "Sinusoidal"},
            "parameters": [{"name": "Longitude of natural origin", "value": 0, "unit": "degree"}]},
          "coordinate_system": {"subtype": "Cartesian", "axis": [
            {"name": "E", "direction": "east", "unit": {"type": "LinearUnit", "name": "US survey foot", "conversion_factor": 0.304800609601219}},
            {"name": "N", "direction": "north", "unit": {"type": "LinearUnit", "name": "US survey foot", "conversion_factor": 0.304800609601219}}]}}"#;
        let def = parse(text).unwrap();
        assert!(def.datum.ellipsoid.is_sphere());
        assert_eq!(def.unit.metres_per_unit, 0.304800609601219);
    }

    #[test]
    fn refusals_name_the_problem() {
        assert!(matches!(parse("{"), Err(CrsParseError::Malformed { .. })));
        let err = parse(r#"{"type": "VerticalCRS"}"#).unwrap_err();
        assert!(err.to_string().contains("VerticalCRS"), "{err}");
        let err =
            parse(r#"{"type": "GeographicCRS", "datum": {"ellipsoid": {"semi_major_axis": 1}}}"#)
                .unwrap_err();
        assert!(err.to_string().contains("inverse_flattening"), "{err}");
    }
}
