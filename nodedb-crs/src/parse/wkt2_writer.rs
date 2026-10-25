// SPDX-License-Identifier: Apache-2.0

//! WKT2 (ISO 19162:2019) writer.
//!
//! Numbers print in Rust's shortest round-trip form and every parameter
//! names its unit, so [`parse_wkt2`](super::parse_wkt2) reads back the same
//! definition. A datum with a Helmert shift is written as a `BOUNDCRS` to
//! WGS 84. A time-dependent shift adds its rates, each with its EPSG
//! per-second unit factor, and its parameter reference epoch in
//! `TIMEUNIT["year",31556925.445]`. A PROJ parameter no EPSG parameter names is written as
//! `PARAMETER["PROJ <key>", value]`, which this crate reads back.

use core::fmt::Write;

use super::helmert_params::{writer_method, writer_params};
use super::methods::ParamKind;
use super::model::{ConversionModel, CrsModel, DEGREE_RAD, DatumModel, model_of};
use super::unit_names::{ellipsoid_name, linear_name};
use super::wkt2_tree::push_quoted;
use crate::def::{AxisDirection, CrsDef, CrsKind, DatumShift};
use crate::srid::SridAuthority;

/// Append formatted text. Writing to a `String` cannot fail.
fn push(out: &mut String, args: core::fmt::Arguments<'_>) {
    let _ = out.write_fmt(args);
}

impl CrsDef {
    /// This definition as WKT2:2019 text.
    pub fn to_wkt2(&self) -> String {
        let model = model_of(self);
        let mut crs = String::new();
        write_crs(&mut crs, &model);
        if let Some(code) = epsg_crs_code(self) {
            // The `ID` closes the CRS node: drop its `]` and append.
            crs.pop();
            push(&mut crs, format_args!(",ID[\"EPSG\",{code}]]"));
        }
        match &model.datum.to_wgs84 {
            DatumShift::Identity => crs,
            DatumShift::Helmert(h) => {
                let mut out = String::from("BOUNDCRS[SOURCECRS[");
                out.push_str(&crs);
                out.push_str("],TARGETCRS[");
                out.push_str(WGS84_WKT);
                out.push_str("],ABRIDGEDTRANSFORMATION[\"Transformation to WGS 84\",");
                let (method, code) = writer_method(h);
                push(
                    &mut out,
                    format_args!("METHOD[\"{method}\",ID[\"EPSG\",{code}]]"),
                );
                for (code, name, unit, value) in writer_params(h) {
                    push(
                        &mut out,
                        format_args!(
                            ",PARAMETER[\"{name}\",{value},{}[\"{}\",{}],ID[\"EPSG\",{code}]]",
                            unit.wkt_keyword(),
                            unit.name(),
                            unit.factor()
                        ),
                    );
                }
                if let Some(accuracy) = h.accuracy_m {
                    push(&mut out, format_args!(",OPERATIONACCURACY[{accuracy}]"));
                }
                out.push_str("]]");
                out
            }
        }
    }
}

/// The EPSG code of an EPSG-authority CRS.
fn epsg_crs_code(def: &CrsDef) -> Option<u32> {
    match def.srid.authority() {
        SridAuthority::Epsg => Some(def.srid.code()),
        SridAuthority::NodeDb | SridAuthority::User | SridAuthority::Reserved => None,
    }
}

const METRE: &str = "LENGTHUNIT[\"metre\",1]";

/// WGS 84 as a target CRS.
const WGS84_WKT: &str = "GEOGCRS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",\
     ELLIPSOID[\"WGS 84\",6378137,298.257223563,LENGTHUNIT[\"metre\",1]],ID[\"EPSG\",6326]],\
     PRIMEM[\"Greenwich\",0,ANGLEUNIT[\"degree\",0.0174532925199433]],CS[ellipsoidal,2],\
     AXIS[\"longitude\",east,ORDER[1],ANGLEUNIT[\"degree\",0.0174532925199433]],\
     AXIS[\"latitude\",north,ORDER[2],ANGLEUNIT[\"degree\",0.0174532925199433]],\
     ID[\"EPSG\",4326]]";

fn degree() -> String {
    format!("ANGLEUNIT[\"degree\",{DEGREE_RAD}]")
}

fn write_crs(out: &mut String, model: &CrsModel) {
    match &model.conversion {
        Some(conversion) => {
            out.push_str("PROJCRS[");
            push_quoted(out, &model.name);
            out.push_str(",BASEGEOGCRS[\"unknown\",");
            write_datum(out, &model.datum);
            out.push_str(",ANGLEUNIT[\"degree\",");
            push(out, format_args!("{DEGREE_RAD}]],"));
            write_conversion(out, conversion);
            out.push_str(",CS[Cartesian,2]");
            let unit = linear_unit(model.metres_per_unit);
            write_axes(out, model, &unit);
        }
        None => {
            out.push_str("GEOGCRS[");
            push_quoted(out, &model.name);
            out.push(',');
            write_datum(out, &model.datum);
            out.push_str(",CS[ellipsoidal,2]");
            write_axes(out, model, &degree());
        }
    }
    if let Some(area) = &model.area_of_use {
        push(
            out,
            format_args!(
                ",USAGE[SCOPE[\"unknown\"],BBOX[{},{},{},{}]]",
                area.south, area.west, area.north, area.east
            ),
        );
    }
    out.push(']');
}

/// `DATUM[...]` then `PRIMEM[...]`.
fn write_datum(out: &mut String, datum: &DatumModel) {
    out.push_str("DATUM[\"unknown\",ELLIPSOID[");
    push_quoted(out, ellipsoid_name(&datum.ellipsoid));
    push(
        out,
        format_args!(",{},{},{METRE}]", datum.ellipsoid.a, datum.ellipsoid.inv_f),
    );
    if let Some(code) = datum.epsg_code {
        push(out, format_args!(",ID[\"EPSG\",{code}]"));
    }
    out.push(']');
    let pm_name = if datum.prime_meridian_deg == 0.0 {
        "Greenwich"
    } else {
        "unknown"
    };
    push(
        out,
        format_args!(
            ",PRIMEM[\"{pm_name}\",{},{}]",
            datum.prime_meridian_deg,
            degree()
        ),
    );
}

fn write_conversion(out: &mut String, conversion: &ConversionModel) {
    out.push_str("CONVERSION[\"unknown\",METHOD[");
    push_quoted(out, &conversion.method_name);
    if let Some(code) = conversion.method_code {
        push(out, format_args!(",ID[\"EPSG\",{code}]"));
    }
    out.push(']');
    for param in &conversion.params {
        out.push_str(",PARAMETER[");
        push_quoted(out, &param.name);
        push(out, format_args!(",{}", param.value));
        match param.kind {
            Some(ParamKind::Angle) => push(out, format_args!(",{}", degree())),
            Some(ParamKind::Length) => push(out, format_args!(",{METRE}")),
            Some(ParamKind::Scale) => out.push_str(",SCALEUNIT[\"unity\",1]"),
            None => {}
        }
        if let Some(code) = param.code {
            push(out, format_args!(",ID[\"EPSG\",{code}]"));
        }
        out.push(']');
    }
    out.push(']');
}

/// `LENGTHUNIT[...]` of `metres_per_unit`.
fn linear_unit(metres_per_unit: f64) -> String {
    let name = linear_name(metres_per_unit).unwrap_or("unknown");
    let mut out = String::from("LENGTHUNIT[");
    push_quoted(&mut out, name);
    push(&mut out, format_args!(",{metres_per_unit}]"));
    out
}

/// Two `AXIS[...]` nodes in the model's axis order, each with `unit`.
fn write_axes(out: &mut String, model: &CrsModel, unit: &str) {
    let geographic = model.kind == CrsKind::Geographic;
    for (order, direction) in [model.axis.x, model.axis.y].into_iter().enumerate() {
        let (name, word) = axis_name(direction, geographic);
        push(
            out,
            format_args!(",AXIS[\"{name}\",{word},ORDER[{}],{unit}]", order + 1),
        );
    }
}

/// The name and WKT direction word of one axis.
fn axis_name(direction: AxisDirection, geographic: bool) -> (&'static str, &'static str) {
    match (direction, geographic) {
        (AxisDirection::East, true) => ("longitude", "east"),
        (AxisDirection::West, true) => ("longitude", "west"),
        (AxisDirection::North, true) => ("latitude", "north"),
        (AxisDirection::South, true) => ("latitude", "south"),
        (AxisDirection::East, false) => ("easting (X)", "east"),
        (AxisDirection::West, false) => ("westing (X)", "west"),
        (AxisDirection::North, false) => ("northing (Y)", "north"),
        (AxisDirection::South, false) => ("southing (Y)", "south"),
        (AxisDirection::Up, _) => ("height", "up"),
        (AxisDirection::Down, _) => ("depth", "down"),
    }
}

#[cfg(test)]
mod tests {
    use crate::CrsContext;
    use crate::parse::{parse_proj_string, parse_wkt2};
    use crate::srid::Srid;

    fn round_trip(srid: Srid, text: &str) {
        let def = parse_proj_string(srid, "rt", text).unwrap();
        let wkt = def.to_wkt2();
        let back = parse_wkt2(srid, "rt", &wkt).unwrap_or_else(|e| panic!("{text}\n{wkt}\n{e}"));
        assert_eq!(back, def, "{text}\n{wkt}");
    }

    #[test]
    fn every_method_round_trips() {
        let srid = Srid::new(910_000).unwrap();
        for text in crate::parse::round_trip_cases::CASES {
            round_trip(srid, text);
        }
    }

    /// A built-in with an EPSG datum code, a shift and an area keeps all
    /// three, and names its EPSG code.
    #[test]
    fn builtins_round_trip_with_datum_code_and_area() {
        let ctx = CrsContext::builtin_only();
        for code in [4326, 4742, 3375, 32648, 2263, 3857, 5514, 990_001, 990_002] {
            let srid = Srid::new(code).unwrap();
            let def = ctx.resolve(srid).unwrap();
            let wkt = def.to_wkt2();
            let back = parse_wkt2(srid, &def.name, &wkt).unwrap_or_else(|e| panic!("{wkt}\n{e}"));
            assert_eq!(back, *def, "{wkt}");
            if code < 900_000 {
                assert!(wkt.contains(&format!("ID[\"EPSG\",{code}]")), "{wkt}");
            }
        }
    }
}
