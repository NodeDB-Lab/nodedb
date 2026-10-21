// SPDX-License-Identifier: Apache-2.0

//! Argument coercion shared by the geo function evaluators.

use crate::expr::EvalError;
use crate::value_ops::value_to_f64;
use nodedb_crs::CrsContext;
use nodedb_spatial::{Metric, SpatialError, SridCheck};
use nodedb_types::geometry::{GeoJsonError, Srid, SridGeometry, geojson_object};
use nodedb_types::{BboxSpace, Value};

/// What a geo family evaluator returns: `None` when the canonical name is
/// not its own, else the call's value or the fault that fails the statement.
pub(super) type GeoEval = Option<Result<Value, EvalError>>;

pub(super) fn str_arg(args: &[Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

pub(super) fn num_arg(args: &[Value], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| value_to_f64(v, true))
}

/// A numeric argument that must be present and finite. NULL gives
/// `Ok(None)`. A non-number fails with [`EvalError::ArgumentType`].
pub(super) fn finite_arg(
    args: &[Value],
    idx: usize,
    function: &'static str,
) -> Result<Option<f64>, EvalError> {
    match args.get(idx) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match value_to_f64(v, true) {
            Some(n) if n.is_finite() => Ok(Some(n)),
            Some(_) => Err(EvalError::ArgumentValue {
                function,
                position: idx + 1,
                reason: "must be a finite number".into(),
            }),
            None => Err(EvalError::ArgumentType {
                function,
                position: idx + 1,
                expected: "a number",
                got: v.type_name(),
            }),
        },
    }
}

/// The fault a geometry parse or operation in `function` raises.
pub(super) fn geometry_fault(function: &'static str, source: SpatialError) -> EvalError {
    match source {
        SpatialError::Srid(source) => EvalError::Crs { function, source },
        SpatialError::GeoJson(GeoJsonError::Srid(source)) => EvalError::Crs { function, source },
        SpatialError::Crs(e) => EvalError::from_crs(function, e),
        source => EvalError::Geometry { function, source },
    }
}

/// Extract a geometry argument.
///
/// Accepts every representation geometry takes on its way through the
/// system: a native `Value::Geometry`, a GeoJSON object decoded from a
/// document, text (GeoJSON, EWKT, WKT or hex EWKB, matching PostGIS's
/// treatment of an unknown-typed literal in geometry position), and WKB /
/// EWKB bytes. GeoJSON and EWKT keep the SRID they name. WKT and ISO WKB
/// are WGS 84.
///
/// SQL NULL and an object that is not GeoJSON give `Ok(None)`. Text or
/// bytes that do not parse fail with [`EvalError::Geometry`]. An SRID `ctx`
/// does not define fails with [`EvalError::Crs`].
pub(super) fn geom_arg(
    args: &[Value],
    idx: usize,
    function: &'static str,
    ctx: &CrsContext<'_>,
) -> Result<Option<SridGeometry>, EvalError> {
    match args.get(idx) {
        Some(Value::Geometry(g)) => Ok(Some(g.clone())),
        Some(Value::String(s)) => text_geometry(function, s, ctx).map(Some),
        Some(Value::Bytes(b)) => nodedb_spatial::geometry_from_bytes(b, SridCheck::Resolve(ctx))
            .map(Some)
            .map_err(|e| geometry_fault(function, e)),
        Some(obj @ Value::Object(_)) => match geojson_object(obj, ctx) {
            Ok(g) => Ok(Some(g)),
            Err(GeoJsonError::NotGeometry) => Ok(None),
            Err(GeoJsonError::Srid(source)) => Err(EvalError::Crs { function, source }),
            Err(e) => Err(EvalError::Geometry {
                function,
                source: SpatialError::GeoJson(e),
            }),
        },
        _ => Ok(None),
    }
}

/// Two geometry arguments at positions 0 and 1. `None` when either is not
/// a geometry.
pub(super) fn geom_pair(
    args: &[Value],
    function: &'static str,
    ctx: &CrsContext<'_>,
) -> Result<Option<(SridGeometry, SridGeometry)>, EvalError> {
    let a = geom_arg(args, 0, function, ctx)?;
    let b = geom_arg(args, 1, function, ctx)?;
    Ok(a.zip(b))
}

/// The value `f` gives for the geometry at position 0. A missing geometry
/// is NULL.
pub(super) fn unary(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: impl FnOnce(&SridGeometry) -> Result<Value, EvalError>,
) -> Result<Value, EvalError> {
    match geom_arg(args, 0, function, ctx)? {
        Some(g) => f(&g),
        None => Ok(Value::Null),
    }
}

/// The value `f` gives for the geometries at positions 0 and 1. A missing
/// geometry is NULL. Two SRIDs fail with `EvalError::MixedSrid`.
pub(super) fn pair(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: impl FnOnce(&SridGeometry, &SridGeometry) -> Result<Value, EvalError>,
) -> Result<Value, EvalError> {
    let Some((a, b)) = geom_pair(args, function, ctx)? else {
        return Ok(Value::Null);
    };
    same_srid(function, &a, &b)?;
    f(&a, &b)
}

/// Parse a textual geometry: GeoJSON, EWKT, WKT or hex EWKB. `None` for
/// text that is not a geometry or names an SRID `ctx` does not define.
pub fn geometry_from_text(text: &str, ctx: &CrsContext<'_>) -> Option<SridGeometry> {
    nodedb_spatial::geometry_from_text(text, SridCheck::Resolve(ctx)).ok()
}

/// A textual geometry argument. Text that does not parse fails the
/// statement.
pub(super) fn text_geometry(
    function: &'static str,
    text: &str,
    ctx: &CrsContext<'_>,
) -> Result<SridGeometry, EvalError> {
    nodedb_spatial::geometry_from_text(text, SridCheck::Resolve(ctx))
        .map_err(|e| geometry_fault(function, e))
}

/// An optional SRID argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SridArg {
    /// The call has no argument at this position.
    Absent,
    /// The argument is SQL NULL.
    Null,
    Srid(Srid),
}

/// Read the SRID argument at `idx`.
///
/// An integer, an integral float, or integer text resolves through
/// [`CrsContext::resolve_code`]. A code no CRS defines fails with
/// [`EvalError::Crs`]. A value of any other kind fails with
/// [`EvalError::ArgumentType`].
pub(super) fn srid_arg(
    args: &[Value],
    idx: usize,
    function: &'static str,
    ctx: &CrsContext<'_>,
) -> Result<SridArg, EvalError> {
    let Some(value) = args.get(idx) else {
        return Ok(SridArg::Absent);
    };
    let code = match value {
        Value::Null => return Ok(SridArg::Null),
        Value::Integer(i) => Some(*i),
        Value::Float(f) if f.is_finite() && f.fract() == 0.0 => Some(*f as i64),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    };
    let Some(code) = code else {
        return Err(EvalError::ArgumentType {
            function,
            position: idx + 1,
            expected: "an integer SRID",
            got: value.type_name(),
        });
    };
    ctx.resolve_code(code)
        .map(SridArg::Srid)
        .map_err(|source| EvalError::Crs { function, source })
}

/// The shared SRID of two operands, or [`EvalError::MixedSrid`].
pub(super) fn same_srid(
    function: &'static str,
    a: &SridGeometry,
    b: &SridGeometry,
) -> Result<Srid, EvalError> {
    if a.srid == b.srid {
        Ok(a.srid)
    } else {
        Err(EvalError::MixedSrid {
            function,
            left: a.srid.code(),
            right: b.srid.code(),
        })
    }
}

/// The coordinate space of `srid`: geographic degrees or planar units.
/// An SRID `ctx` does not define fails with [`EvalError::Crs`].
pub(super) fn space_of(
    function: &'static str,
    srid: Srid,
    ctx: &CrsContext<'_>,
) -> Result<BboxSpace, EvalError> {
    BboxSpace::of(ctx, srid).map_err(|source| EvalError::Crs { function, source })
}

/// The measure basis of `srid`: geodesic on its ellipsoid or planar in its
/// unit. An SRID `ctx` does not define fails with [`EvalError::Crs`].
pub(super) fn metric_of(
    function: &'static str,
    srid: Srid,
    ctx: &CrsContext<'_>,
) -> Result<Metric, EvalError> {
    Metric::resolve(ctx, srid).map_err(|source| EvalError::Crs { function, source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::UnknownSrid;
    use nodedb_types::geometry::Geometry;

    fn ctx() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    #[test]
    fn geojson_and_wkt_strings_both_parse() {
        let geojson = geometry_from_text(r#"{"type":"Point","coordinates":[1.0,2.0]}"#, &ctx());
        let wkt = geometry_from_text("POINT(1 2)", &ctx());
        assert_eq!(geojson, wkt);
        assert_eq!(
            geojson,
            Some(SridGeometry::wgs84(Geometry::point(1.0, 2.0)))
        );
    }

    #[test]
    fn geojson_crs_member_sets_srid() {
        let g = geometry_from_text(
            r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"name","properties":{"name":"EPSG:3375"}}}"#,
            &ctx(),
        );
        assert_eq!(g.map(|g| g.srid), Some(srid(3375)));
    }

    #[test]
    fn geojson_crs_member_with_an_undefined_srid_fails() {
        let text = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"name","properties":{"name":"EPSG:999"}}}"#;
        assert_eq!(
            geom_arg(&[Value::String(text.into())], 0, "st_x", &ctx()),
            Err(EvalError::Crs {
                function: "st_x",
                source: UnknownSrid::Undefined { code: 999 }
            })
        );
    }

    #[test]
    fn surrounding_whitespace_does_not_defeat_parsing() {
        assert!(geometry_from_text("  POINT(1 2)  ", &ctx()).is_some());
        assert!(
            geometry_from_text("  {\"type\":\"Point\",\"coordinates\":[1,2]}  ", &ctx()).is_some()
        );
    }

    /// Text in geometry position that does not parse fails the statement.
    #[test]
    fn unparseable_text_is_a_statement_error() {
        assert!(matches!(
            geom_arg(&[Value::String("not a geometry".into())], 0, "st_x", &ctx()),
            Err(EvalError::Geometry {
                function: "st_x",
                ..
            })
        ));
        assert!(matches!(
            geom_arg(
                &[Value::String("POINT(1 2 3 4 5)".into())],
                0,
                "st_x",
                &ctx()
            ),
            Err(EvalError::Geometry { .. })
        ));
        assert_eq!(geom_arg(&[Value::Null], 0, "st_x", &ctx()), Ok(None));
    }

    #[test]
    fn wkb_bytes_are_a_geometry() {
        let wkb = nodedb_spatial::geometry_to_wkb(&SridGeometry::wgs84(Geometry::point(1.0, 2.0)))
            .unwrap();
        assert_eq!(
            geom_arg(&[Value::Bytes(wkb)], 0, "st_x", &ctx()),
            Ok(Some(SridGeometry::wgs84(Geometry::point(1.0, 2.0))))
        );
    }

    #[test]
    fn srid_arg_forms() {
        let f = "st_setsrid";
        let ctx = ctx();
        let args = [
            Value::Integer(32648),
            Value::Float(3375.0),
            Value::String("4742".into()),
            Value::Null,
        ];
        assert_eq!(srid_arg(&args, 0, f, &ctx), Ok(SridArg::Srid(srid(32648))));
        assert_eq!(srid_arg(&args, 1, f, &ctx), Ok(SridArg::Srid(srid(3375))));
        assert_eq!(srid_arg(&args, 2, f, &ctx), Ok(SridArg::Srid(srid(4742))));
        assert_eq!(srid_arg(&args, 3, f, &ctx), Ok(SridArg::Null));
        assert_eq!(srid_arg(&args, 4, f, &ctx), Ok(SridArg::Absent));
        assert_eq!(
            srid_arg(&[Value::Integer(999)], 0, f, &ctx),
            Err(EvalError::Crs {
                function: f,
                source: UnknownSrid::Undefined { code: 999 }
            })
        );
        assert!(matches!(
            srid_arg(&[Value::Bool(true)], 0, f, &ctx),
            Err(EvalError::ArgumentType { .. })
        ));
    }

    #[test]
    fn space_follows_the_crs_kind() {
        let ctx = ctx();
        assert_eq!(space_of("f", srid(4742), &ctx), Ok(BboxSpace::Geographic));
        assert_eq!(space_of("f", srid(32650), &ctx), Ok(BboxSpace::Planar));
    }
}
