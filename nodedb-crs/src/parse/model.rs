// SPDX-License-Identifier: Apache-2.0

//! The CRS model WKT2 and PROJJSON share: a datum, an optional conversion
//! named by its EPSG method and parameters, a linear unit, an axis order
//! and an area of use.
//!
//! Both parsers read text into this model, and [`build_def`] turns it into
//! a [`CrsDef`] through the PROJ-string parser, so every method, datum and
//! unit that parser supports reads the same way from every format. Both
//! writers go the other way through [`model_of`].

use core::fmt::Write;

use super::methods::{Derive, FLAG_KEYS, MethodSpec, ParamKind, Target, method_by, writer_spec};
use super::proj_string::parse_proj_string;
use crate::def::{AreaOfUse, AxisOrder, CrsDef, CrsKind, DatumShift, Ellipsoid};
use crate::error::CrsParseError;
use crate::srid::Srid;

/// Radians per degree.
pub(crate) const DEGREE_RAD: f64 = core::f64::consts::PI / 180.0;

/// Radians per arc-second.
pub(crate) const ARC_SECOND_RAD: f64 = DEGREE_RAD / 3600.0;

/// Parts per million as a scale.
pub(crate) const PPM: f64 = 1e-6;

/// Relative distance within which a unit factor equals a canonical one.
/// Text forms print π-derived factors to 15 digits. An exact compare
/// misses the canonical factor and scales every angle by a rounding error.
const FACTOR_TOLERANCE: f64 = 1e-9;

/// Name prefix of a parameter that carries a PROJ key no EPSG parameter
/// names.
pub(crate) const PROJ_PARAM_PREFIX: &str = "PROJ ";

/// One CRS in the shared model.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CrsModel {
    pub name: String,
    pub kind: CrsKind,
    pub datum: DatumModel,
    pub conversion: Option<ConversionModel>,
    /// Metres per unit of projected coordinates.
    pub metres_per_unit: f64,
    pub axis: AxisOrder,
    pub area_of_use: Option<AreaOfUse>,
}

/// The geodetic datum of a model.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DatumModel {
    pub ellipsoid: Ellipsoid,
    pub prime_meridian_deg: f64,
    pub epsg_code: Option<u32>,
    pub to_wgs84: DatumShift,
}

/// A map projection as an EPSG method and its parameters.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConversionModel {
    pub method_name: String,
    pub method_code: Option<u32>,
    pub params: Vec<ParamModel>,
    /// Radians per unit of an angle parameter written without a unit.
    pub default_angle_rad: f64,
    /// Metres per unit of a length parameter written without a unit.
    pub default_length_m: f64,
}

/// One conversion parameter as written.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParamModel {
    pub name: String,
    pub code: Option<u32>,
    pub value: f64,
    /// The unit's factor to radians, metres or unity. `None` when the text
    /// gives no unit.
    pub factor: Option<f64>,
    /// What the parameter measures, when the method names it. Writers name
    /// the unit from it.
    pub kind: Option<ParamKind>,
}

/// `value` in a unit of `factor`, expressed in the unit of `canonical`.
/// A factor within [`FACTOR_TOLERANCE`] of the canonical one keeps the
/// value as written.
pub(crate) fn convert(value: f64, factor: f64, canonical: f64) -> f64 {
    if ((factor / canonical) - 1.0).abs() <= FACTOR_TOLERANCE {
        value
    } else {
        value * factor / canonical
    }
}

/// Lower-case `text` with every character but letters and digits dropped,
/// so names compare without case, spaces or punctuation.
pub(crate) fn normalize_name(text: &str) -> String {
    text.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Append formatted text. Writing to a `String` cannot fail.
fn push(out: &mut String, args: core::fmt::Arguments<'_>) {
    let _ = out.write_fmt(args);
}

/// Build the definition `model` describes, named `name` under `srid`.
pub(crate) fn build_def(
    srid: Srid,
    name: &str,
    model: &CrsModel,
    format: &'static str,
) -> Result<CrsDef, CrsParseError> {
    let mut proj = String::new();
    match (&model.conversion, model.kind) {
        (None, CrsKind::Geographic) => proj.push_str("+proj=longlat"),
        (Some(conversion), CrsKind::Projected) => write_conversion(&mut proj, conversion, format)?,
        (None, CrsKind::Projected) => {
            return Err(CrsParseError::content(
                format,
                "a projected CRS needs a conversion",
            ));
        }
        (Some(_), CrsKind::Geographic) => {
            return Err(CrsParseError::content(
                format,
                "a geographic CRS has no conversion",
            ));
        }
    }
    let e = &model.datum.ellipsoid;
    if e.is_sphere() {
        push(&mut proj, format_args!(" +R={}", e.a));
    } else {
        push(&mut proj, format_args!(" +a={} +rf={}", e.a, e.inv_f));
    }
    if model.datum.prime_meridian_deg != 0.0 {
        push(
            &mut proj,
            format_args!(" +pm={}", model.datum.prime_meridian_deg),
        );
    }
    if model.kind == CrsKind::Projected {
        push(
            &mut proj,
            format_args!(" +to_meter={}", model.metres_per_unit),
        );
    }
    if !model.axis.is_enu() {
        push(&mut proj, format_args!(" +axis={}", model.axis));
    }
    let mut def = parse_proj_string(srid, name, &proj)?;
    def.datum.to_wgs84 = model.datum.to_wgs84;
    def.datum.epsg_code = model.datum.epsg_code;
    def.area_of_use = model.area_of_use;
    Ok(def)
}

/// Append the PROJ parameters of `conversion`.
fn write_conversion(
    out: &mut String,
    conversion: &ConversionModel,
    format: &'static str,
) -> Result<(), CrsParseError> {
    let spec = method_by(conversion.method_code, &conversion.method_name).ok_or_else(|| {
        CrsParseError::content(
            format,
            format!(
                "projection method '{}' is not implemented",
                conversion.method_name
            ),
        )
    })?;
    push(out, format_args!("+proj={}", spec.proj));
    for flag in spec.flags {
        push(out, format_args!(" +{flag}"));
    }
    let mut written: Vec<(&str, f64)> = Vec::new();
    for param in &conversion.params {
        write_param(out, spec, conversion, param, format, &mut written)?;
    }
    match spec.derive {
        Derive::None => {}
        Derive::PolarLatitudeFromStandardParallel => {
            let has_lat0 = written.iter().any(|(k, _)| *k == "lat_0");
            let lat_ts = written
                .iter()
                .find(|(k, _)| *k == "lat_ts")
                .map(|(_, v)| *v);
            if let (false, Some(lat_ts)) = (has_lat0, lat_ts) {
                let pole = if lat_ts < 0.0 { -90.0 } else { 90.0 };
                push(out, format_args!(" +lat_0={pole}"));
            }
        }
    }
    Ok(())
}

/// Append one parameter, normalized to degrees, metres or unity.
fn write_param<'s>(
    out: &mut String,
    spec: &'s MethodSpec,
    conversion: &ConversionModel,
    param: &ParamModel,
    format: &'static str,
    written: &mut Vec<(&'s str, f64)>,
) -> Result<(), CrsParseError> {
    if let Some(key) = param.name.strip_prefix(PROJ_PARAM_PREFIX) {
        return write_proj_param(out, key, param.value, format);
    }
    let found = spec.param(param.code, &param.name).ok_or_else(|| {
        CrsParseError::content(
            format,
            format!("method '{}' has no parameter '{}'", spec.name, param.name),
        )
    })?;
    let value = match found.kind {
        ParamKind::Angle => convert(
            param.value,
            param.factor.unwrap_or(conversion.default_angle_rad),
            DEGREE_RAD,
        ),
        ParamKind::Length => convert(
            param.value,
            param.factor.unwrap_or(conversion.default_length_m),
            1.0,
        ),
        ParamKind::Scale => convert(param.value, param.factor.unwrap_or(1.0), 1.0),
    };
    match found.target {
        Target::Keys(keys) => {
            for &key in keys {
                push(out, format_args!(" +{key}={value}"));
                written.push((key, value));
            }
        }
        Target::Fixed(expected) => {
            if (value - expected).abs() > FACTOR_TOLERANCE * expected.abs().max(1.0) {
                return Err(CrsParseError::content(
                    format,
                    format!(
                        "parameter '{}' of method '{}' must be {expected}, found {value}",
                        found.name, spec.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Append a parameter that carries a PROJ key directly. A flag key is
/// written when its value is not zero.
fn write_proj_param(
    out: &mut String,
    key: &str,
    value: f64,
    format: &'static str,
) -> Result<(), CrsParseError> {
    if FLAG_KEYS.contains(&key) {
        if value != 0.0 {
            push(out, format_args!(" +{key}"));
        }
        return Ok(());
    }
    if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(CrsParseError::content(
            format,
            format!("parameter '{PROJ_PARAM_PREFIX}{key}' names no PROJ key"),
        ));
    }
    push(out, format_args!(" +{key}={value}"));
    Ok(())
}

/// The model of `def`, for a writer.
pub(crate) fn model_of(def: &CrsDef) -> CrsModel {
    CrsModel {
        name: def.name.to_string(),
        kind: def.kind,
        datum: DatumModel {
            ellipsoid: def.datum.ellipsoid,
            prime_meridian_deg: def.datum.prime_meridian_deg,
            epsg_code: def.datum.epsg_code,
            to_wgs84: def.datum.to_wgs84,
        },
        conversion: def.projection.as_ref().map(conversion_of),
        metres_per_unit: def.unit.metres_per_unit,
        axis: def.axis,
        area_of_use: def.area_of_use,
    }
}

/// The conversion of `projection`: its writer method, each PROJ parameter
/// under its EPSG name, and every fixed parameter of the method.
fn conversion_of(projection: &crate::method::ProjectionDef) -> ConversionModel {
    let spec = writer_spec(projection);
    let mut text = String::new();
    projection.write_proj(&mut text);
    let mut params = Vec::new();
    for token in text.split_whitespace() {
        let token = token.trim_start_matches('+');
        let (key, value) = match token.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (token, None),
        };
        if key == "proj" || spec.flags.contains(&token) {
            continue;
        }
        let number = value.and_then(|v| v.parse::<f64>().ok());
        let found = spec.params.iter().find(|p| match p.target {
            Target::Keys(keys) => keys.first() == Some(&key),
            Target::Fixed(_) => false,
        });
        match (found, number) {
            (Some(p), Some(v)) => params.push(ParamModel {
                name: p.name.to_string(),
                code: p.code,
                value: v,
                factor: Some(p.kind.canonical_factor()),
                kind: Some(p.kind),
            }),
            _ => params.push(ParamModel {
                name: format!("{PROJ_PARAM_PREFIX}{key}"),
                code: None,
                value: number.unwrap_or(1.0),
                factor: None,
                kind: None,
            }),
        }
    }
    for p in spec.params {
        if let Target::Fixed(v) = p.target {
            params.push(ParamModel {
                name: p.name.to_string(),
                code: p.code,
                value: v,
                factor: Some(p.kind.canonical_factor()),
                kind: Some(p.kind),
            });
        }
    }
    ConversionModel {
        method_name: spec.name.to_string(),
        method_code: spec.code,
        params,
        default_angle_rad: DEGREE_RAD,
        default_length_m: 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn near_canonical_factors_keep_the_value() {
        assert_eq!(convert(102.25, 0.0174532925199433, DEGREE_RAD), 102.25);
        assert_eq!(convert(1.5, 4.84813681109536e-6, ARC_SECOND_RAD), 1.5);
        let grads = convert(100.0, core::f64::consts::PI / 200.0, DEGREE_RAD);
        assert!((grads - 90.0).abs() < 1e-12);
        assert_eq!(convert(1000.0, 0.3048, 1.0), 304.8);
    }

    #[test]
    fn names_normalize() {
        assert_eq!(normalize_name("Transverse Mercator"), "transversemercator");
        assert_eq!(normalize_name("Transverse_Mercator"), "transversemercator");
    }
}
