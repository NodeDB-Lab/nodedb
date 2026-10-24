// SPDX-License-Identifier: Apache-2.0

//! PROJJSON writer (schema v0.7).
//!
//! Numbers print in Rust's shortest round-trip form and every parameter
//! names its unit, so [`parse_projjson`](super::parse_projjson) reads back
//! the same definition. A datum with a Helmert shift is written as a
//! `BoundCRS` to WGS 84. A time-dependent shift adds its rates and its
//! parameter reference epoch, each with a unit object carrying the EPSG
//! per-second factor. A PROJ parameter no EPSG parameter names is
//! written as `{"name": "PROJ <key>", "value": v}`, which this crate reads
//! back.

use core::fmt::Write;

use super::helmert_params::{HelmertUnit, writer_method, writer_params};
use super::json::push_json_string;
use super::methods::ParamKind;
use super::model::{ConversionModel, CrsModel, DatumModel, model_of};
use super::unit_names::{ellipsoid_name, linear_name};
use crate::def::{AxisDirection, CrsDef, CrsKind, DatumShift};
use crate::srid::SridAuthority;

/// The PROJJSON schema this writer follows.
const SCHEMA: &str = "https://proj.org/schemas/v0.7/projjson.schema.json";

/// WGS 84 as a target CRS.
const WGS84_JSON: &str = "{\"type\":\"GeographicCRS\",\"name\":\"WGS 84\",\
     \"datum\":{\"type\":\"GeodeticReferenceFrame\",\"name\":\"World Geodetic System 1984\",\
     \"ellipsoid\":{\"name\":\"WGS 84\",\"semi_major_axis\":6378137,\
     \"inverse_flattening\":298.257223563},\"id\":{\"authority\":\"EPSG\",\"code\":6326}},\
     \"coordinate_system\":{\"subtype\":\"ellipsoidal\",\"axis\":[\
     {\"name\":\"Geodetic longitude\",\"abbreviation\":\"Lon\",\"direction\":\"east\",\"unit\":\"degree\"},\
     {\"name\":\"Geodetic latitude\",\"abbreviation\":\"Lat\",\"direction\":\"north\",\"unit\":\"degree\"}]},\
     \"id\":{\"authority\":\"EPSG\",\"code\":4326}}";

/// Append formatted text. Writing to a `String` cannot fail.
fn push(out: &mut String, args: core::fmt::Arguments<'_>) {
    let _ = out.write_fmt(args);
}

impl CrsDef {
    /// This definition as PROJJSON text.
    pub fn to_projjson(&self) -> String {
        let model = model_of(self);
        let id = match self.srid.authority() {
            SridAuthority::Epsg => Some(self.srid.code()),
            SridAuthority::NodeDb | SridAuthority::User | SridAuthority::Reserved => None,
        };
        match &model.datum.to_wgs84 {
            DatumShift::Identity => {
                let mut out = String::new();
                write_crs(&mut out, &model, Some(SCHEMA), id);
                out
            }
            DatumShift::Helmert(h) => {
                let mut out =
                    format!("{{\"$schema\":\"{SCHEMA}\",\"type\":\"BoundCRS\",\"source_crs\":");
                write_crs(&mut out, &model, None, id);
                out.push_str(",\"target_crs\":");
                out.push_str(WGS84_JSON);
                let (method, code) = writer_method(h);
                push(
                    &mut out,
                    format_args!(
                        ",\"transformation\":{{\"name\":\"Transformation to WGS 84\",\
                         \"method\":{{\"name\":\"{method}\",\"id\":{{\"authority\":\"EPSG\",\"code\":{code}}}}},\
                         \"parameters\":["
                    ),
                );
                for (i, (code, name, unit, value)) in writer_params(h).enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    push(
                        &mut out,
                        format_args!("{{\"name\":\"{name}\",\"value\":{value},\"unit\":"),
                    );
                    write_helmert_unit(&mut out, unit);
                    push(
                        &mut out,
                        format_args!(",\"id\":{{\"authority\":\"EPSG\",\"code\":{code}}}}}"),
                    );
                }
                out.push(']');
                if let Some(accuracy) = h.accuracy_m {
                    push(&mut out, format_args!(",\"accuracy\":\"{accuracy}\""));
                }
                out.push_str("}}");
                out
            }
        }
    }
}

/// A Helmert parameter unit: a name PROJ knows for the static units, a unit
/// object with its SI factor for the rates and the year.
fn write_helmert_unit(out: &mut String, unit: HelmertUnit) {
    match unit {
        HelmertUnit::Length | HelmertUnit::Angle | HelmertUnit::Scale => {
            push(out, format_args!("\"{}\"", unit.name()));
        }
        HelmertUnit::LengthRate
        | HelmertUnit::AngleRate
        | HelmertUnit::ScaleRate
        | HelmertUnit::Time => push(
            out,
            format_args!(
                "{{\"type\":\"{}\",\"name\":\"{}\",\"conversion_factor\":{}}}",
                unit.projjson_type(),
                unit.name(),
                unit.factor()
            ),
        ),
    }
}

fn write_crs(out: &mut String, model: &CrsModel, schema: Option<&str>, id: Option<u32>) {
    out.push('{');
    if let Some(schema) = schema {
        push(out, format_args!("\"$schema\":\"{schema}\","));
    }
    match &model.conversion {
        Some(conversion) => {
            out.push_str("\"type\":\"ProjectedCRS\",\"name\":");
            push_json_string(out, &model.name);
            out.push_str(",\"base_crs\":{\"name\":\"unknown\",");
            write_datum(out, &model.datum);
            out.push_str(",\"coordinate_system\":{\"subtype\":\"ellipsoidal\",\"axis\":[");
            out.push_str("{\"name\":\"Geodetic longitude\",\"abbreviation\":\"Lon\",\"direction\":\"east\",\"unit\":\"degree\"},");
            out.push_str("{\"name\":\"Geodetic latitude\",\"abbreviation\":\"Lat\",\"direction\":\"north\",\"unit\":\"degree\"}]}},");
            write_conversion(out, conversion);
            out.push_str(",\"coordinate_system\":{\"subtype\":\"Cartesian\",\"axis\":[");
            write_axes(out, model, &linear_unit(model.metres_per_unit));
            out.push_str("]}");
        }
        None => {
            out.push_str("\"type\":\"GeographicCRS\",\"name\":");
            push_json_string(out, &model.name);
            out.push(',');
            write_datum(out, &model.datum);
            out.push_str(",\"coordinate_system\":{\"subtype\":\"ellipsoidal\",\"axis\":[");
            write_axes(out, model, "\"degree\"");
            out.push_str("]}");
        }
    }
    if let Some(area) = &model.area_of_use {
        push(
            out,
            format_args!(
                ",\"bbox\":{{\"south_latitude\":{},\"west_longitude\":{},\
                 \"north_latitude\":{},\"east_longitude\":{}}}",
                area.south, area.west, area.north, area.east
            ),
        );
    }
    if let Some(code) = id {
        push(
            out,
            format_args!(",\"id\":{{\"authority\":\"EPSG\",\"code\":{code}}}"),
        );
    }
    out.push('}');
}

/// The `"datum"` member, with its ellipsoid and prime meridian.
fn write_datum(out: &mut String, datum: &DatumModel) {
    out.push_str("\"datum\":{\"type\":\"GeodeticReferenceFrame\",\"name\":\"unknown\",\"ellipsoid\":{\"name\":");
    push_json_string(out, ellipsoid_name(&datum.ellipsoid));
    if datum.ellipsoid.is_sphere() {
        push(out, format_args!(",\"radius\":{}}}", datum.ellipsoid.a));
    } else {
        push(
            out,
            format_args!(
                ",\"semi_major_axis\":{},\"inverse_flattening\":{}}}",
                datum.ellipsoid.a, datum.ellipsoid.inv_f
            ),
        );
    }
    if datum.prime_meridian_deg != 0.0 {
        push(
            out,
            format_args!(
                ",\"prime_meridian\":{{\"name\":\"unknown\",\"longitude\":{}}}",
                datum.prime_meridian_deg
            ),
        );
    }
    if let Some(code) = datum.epsg_code {
        push(
            out,
            format_args!(",\"id\":{{\"authority\":\"EPSG\",\"code\":{code}}}"),
        );
    }
    out.push('}');
}

fn write_conversion(out: &mut String, conversion: &ConversionModel) {
    out.push_str("\"conversion\":{\"name\":\"unknown\",\"method\":{\"name\":");
    push_json_string(out, &conversion.method_name);
    if let Some(code) = conversion.method_code {
        push(
            out,
            format_args!(",\"id\":{{\"authority\":\"EPSG\",\"code\":{code}}}"),
        );
    }
    out.push_str("},\"parameters\":[");
    for (i, param) in conversion.params.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"name\":");
        push_json_string(out, &param.name);
        push(out, format_args!(",\"value\":{}", param.value));
        let unit = match param.kind {
            Some(ParamKind::Angle) => Some("degree"),
            Some(ParamKind::Length) => Some("metre"),
            Some(ParamKind::Scale) => Some("unity"),
            None => None,
        };
        if let Some(unit) = unit {
            push(out, format_args!(",\"unit\":\"{unit}\""));
        }
        if let Some(code) = param.code {
            push(
                out,
                format_args!(",\"id\":{{\"authority\":\"EPSG\",\"code\":{code}}}"),
            );
        }
        out.push('}');
    }
    out.push_str("]}");
}

/// A linear unit: its name when it is the metre, else an object with its
/// conversion factor.
fn linear_unit(metres_per_unit: f64) -> String {
    if metres_per_unit == 1.0 {
        return "\"metre\"".to_string();
    }
    let mut out = String::from("{\"type\":\"LinearUnit\",\"name\":");
    push_json_string(&mut out, linear_name(metres_per_unit).unwrap_or("unknown"));
    push(
        &mut out,
        format_args!(",\"conversion_factor\":{metres_per_unit}}}"),
    );
    out
}

/// Two axis objects in the model's axis order, each with `unit`.
fn write_axes(out: &mut String, model: &CrsModel, unit: &str) {
    let geographic = model.kind == CrsKind::Geographic;
    for (i, direction) in [model.axis.x, model.axis.y].into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let (name, abbreviation, word) = axis_name(direction, geographic);
        push(
            out,
            format_args!(
                "{{\"name\":\"{name}\",\"abbreviation\":\"{abbreviation}\",\
                 \"direction\":\"{word}\",\"unit\":{unit}}}"
            ),
        );
    }
}

/// The name, abbreviation and direction of one axis.
fn axis_name(
    direction: AxisDirection,
    geographic: bool,
) -> (&'static str, &'static str, &'static str) {
    match (direction, geographic) {
        (AxisDirection::East, true) => ("Geodetic longitude", "Lon", "east"),
        (AxisDirection::West, true) => ("Geodetic longitude", "Lon", "west"),
        (AxisDirection::North, true) => ("Geodetic latitude", "Lat", "north"),
        (AxisDirection::South, true) => ("Geodetic latitude", "Lat", "south"),
        (AxisDirection::East, false) => ("Easting", "X", "east"),
        (AxisDirection::West, false) => ("Westing", "X", "west"),
        (AxisDirection::North, false) => ("Northing", "Y", "north"),
        (AxisDirection::South, false) => ("Southing", "Y", "south"),
        (AxisDirection::Up, _) => ("Height", "h", "up"),
        (AxisDirection::Down, _) => ("Depth", "d", "down"),
    }
}

#[cfg(test)]
mod tests {
    use crate::CrsContext;
    use crate::parse::{parse_proj_string, parse_projjson};
    use crate::srid::Srid;

    #[test]
    fn every_method_round_trips() {
        let srid = Srid::new(910_000).unwrap();
        for text in crate::parse::round_trip_cases::CASES {
            let def = parse_proj_string(srid, "rt", text).unwrap();
            let json = def.to_projjson();
            let back =
                parse_projjson(srid, "rt", &json).unwrap_or_else(|e| panic!("{text}\n{json}\n{e}"));
            assert_eq!(back, def, "{text}\n{json}");
        }
    }

    #[test]
    fn builtins_round_trip_with_datum_code_and_area() {
        let ctx = CrsContext::builtin_only();
        for code in [4326, 4742, 3375, 32648, 2263, 3857, 5514, 990_001, 990_002] {
            let srid = Srid::new(code).unwrap();
            let def = ctx.resolve(srid).unwrap();
            let json = def.to_projjson();
            let back =
                parse_projjson(srid, &def.name, &json).unwrap_or_else(|e| panic!("{json}\n{e}"));
            assert_eq!(back, *def, "{json}");
        }
    }

    #[test]
    fn names_are_escaped() {
        let srid = Srid::new(910_000).unwrap();
        let def =
            parse_proj_string(srid, "a \"quoted\" name", "+proj=longlat +datum=WGS84").unwrap();
        let json = def.to_projjson();
        assert!(json.contains(r#""name":"a \"quoted\" name""#), "{json}");
        assert!(parse_projjson(srid, "x", &json).is_ok());
    }
}
