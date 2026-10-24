// SPDX-License-Identifier: Apache-2.0

//! Geometry constructors.
//!
//! A constructor from bare numbers builds a WGS 84 geometry. A constructor
//! from geometries keeps their SRID and needs them in one SRID. Text or
//! bytes that do not parse fail the statement.

use nodedb_crs::CrsContext;
use nodedb_spatial::SridCheck;
use nodedb_types::Value;
use nodedb_types::geometry::{Dimension, Geometry, Ordinates, Srid, SridGeometry};

use super::helpers::{
    GeoEval, SridArg, finite_arg, geom_arg, geometry_fault, num_arg, same_srid, srid_arg, str_arg,
    text_geometry,
};
use crate::expr::EvalError;

/// Default segment count for the circular approximations `ST_Buffer` and
/// `geo_circle` produce when the caller does not specify one.
pub(super) const DEFAULT_BUFFER_SEGMENTS: usize = 32;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_point" => point(canonical, args, Dimension::Xy),
        "st_makepoint" => {
            let dimension = match args.len() {
                3 => Dimension::Xyz,
                4 => Dimension::Xyzm,
                _ => Dimension::Xy,
            };
            point(canonical, args, dimension)
        }
        "st_makepointm" => point(canonical, args, Dimension::Xym),
        "st_geomfromtext" => from_text(canonical, args, ctx, true),
        "st_geomfromgeojson" => from_text(canonical, args, ctx, false),
        "st_geomfromwkb" => from_wkb(canonical, args, ctx),
        "st_makeline" => make_line(canonical, args, ctx),
        "st_makepolygon" => make_polygon(canonical, args),
        "st_makeenvelope" => make_envelope(canonical, args),
        "st_tileenvelope" => tile_envelope(canonical, args, ctx),
        "geo_circle" => circle(canonical, args, ctx),
        _ => return None,
    };
    Some(result)
}

/// `ST_Point(x, y)`, `ST_MakePoint(x, y [, z [, m]])`, `ST_MakePointM(x,
/// y, m)`. A NULL ordinate makes the point NULL.
fn point(function: &'static str, args: &[Value], dimension: Dimension) -> Result<Value, EvalError> {
    let n = usize::from(dimension.coord_dim());
    let mut v = [0.0f64; 4];
    for (i, slot) in v.iter_mut().enumerate().take(n) {
        match finite_arg(args, i, function)? {
            Some(x) => *slot = x,
            None => return Ok(Value::Null),
        }
    }
    let z = dimension.has_z().then(|| vec![v[2]]);
    let m = dimension
        .has_m()
        .then(|| vec![v[if dimension.has_z() { 3 } else { 2 }]]);
    let g = SridGeometry::with_ordinates(
        Srid::WGS84,
        Geometry::point(v[0], v[1]),
        Ordinates::new(z, m),
    )
    .map_err(|e| geometry_fault(function, e.into()))?;
    Ok(Value::Geometry(g))
}

/// `ST_GeomFromText(text [, srid])` and `ST_GeomFromGeoJSON(text)`. GeoJSON
/// and EWKT keep the SRID they name. An explicit SRID argument relabels the
/// result.
fn from_text(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    takes_srid: bool,
) -> Result<Value, EvalError> {
    let explicit = if takes_srid {
        srid_arg(args, 1, function, ctx)?
    } else {
        SridArg::Absent
    };
    let Some(text) = str_arg(args, 0) else {
        return Ok(Value::Null);
    };
    let geom = text_geometry(function, &text, ctx)?;
    Ok(match explicit {
        SridArg::Absent => Value::Geometry(geom),
        SridArg::Null => Value::Null,
        SridArg::Srid(srid) => Value::Geometry(geom.with_srid(srid)),
    })
}

/// `ST_GeomFromWKB(wkb [, srid])`. An explicit SRID argument wins over the
/// SRID an EWKB header carries. ISO WKB with no SRID argument is WGS 84.
fn from_wkb(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let explicit = srid_arg(args, 1, function, ctx)?;
    let bytes = match args.first() {
        Some(Value::Bytes(bytes)) => bytes.clone(),
        Some(Value::String(hex)) => decode_hex(hex.trim()).ok_or(EvalError::ArgumentValue {
            function,
            position: 1,
            reason: "is not hex-encoded WKB".into(),
        })?,
        _ => return Ok(Value::Null),
    };
    let geom = nodedb_spatial::geometry_from_bytes(&bytes, SridCheck::Resolve(ctx))
        .map_err(|e| geometry_fault(function, e))?;
    Ok(match explicit {
        SridArg::Absent => Value::Geometry(geom),
        SridArg::Null => Value::Null,
        SridArg::Srid(srid) => Value::Geometry(geom.with_srid(srid)),
    })
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// `geo_circle(lng, lat, radius_m [, segments])`: a WGS 84 geodesic circle.
fn circle(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let (Some(lng), Some(lat), Some(radius)) = (
        finite_arg(args, 0, function)?,
        finite_arg(args, 1, function)?,
        finite_arg(args, 2, function)?,
    ) else {
        return Ok(Value::Null);
    };
    let segments = segments_arg(args, 3);
    let def = ctx
        .resolve(Srid::WGS84)
        .map_err(|source| EvalError::Crs { function, source })?;
    let shape = nodedb_spatial::ops::buffer(&Geometry::point(lng, lat), radius, segments, &def)
        .map_err(|e| geometry_fault(function, e))?;
    Ok(Value::Geometry(SridGeometry::wgs84(shape)))
}

/// Segment count for a circular approximation, floored at 3 — fewer than three
/// segments cannot enclose an area, and a caller asking for 0 receives the
/// default rather than a degenerate geometry.
pub(super) fn segments_arg(args: &[Value], idx: usize) -> usize {
    num_arg(args, idx)
        .filter(|n| n.is_finite() && *n >= 3.0)
        .map_or(DEFAULT_BUFFER_SEGMENTS, |n| n as usize)
}

/// `ST_MakeLine(point, point, ...)`. Every argument must be a point in one
/// SRID and one dimension, which the line keeps. A NULL argument makes the
/// line NULL.
fn make_line(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let mut points: Vec<SridGeometry> = Vec::with_capacity(args.len());
    for idx in 0..args.len() {
        let Some(g) = geom_arg(args, idx, function, ctx)? else {
            return Ok(Value::Null);
        };
        if !matches!(g.geometry(), Geometry::Point { .. }) || g.is_empty() {
            return Err(EvalError::ArgumentValue {
                function,
                position: idx + 1,
                reason: format!(
                    "must be a non-empty point, got {}",
                    g.geometry().geometry_type()
                ),
            });
        }
        if let Some(head) = points.first() {
            same_srid(function, head, &g)?;
        }
        points.push(g);
    }
    let Some(first) = points.first() else {
        return Ok(Value::Null);
    };
    if points.len() < 2 {
        return Err(EvalError::ArgumentValue {
            function,
            position: 1,
            reason: "a line needs at least two points".into(),
        });
    }
    let dimension = first.dimension();
    let mut builder = nodedb_types::geometry::OrdinatesBuilder::with_dimension(dimension);
    let mut coords = Vec::with_capacity(points.len());
    for p in points.iter().flat_map(|g| g.positions()) {
        builder
            .push(p.z, p.m)
            .map_err(|e| geometry_fault(function, e.into()))?;
        coords.push([p.x, p.y]);
    }
    SridGeometry::with_ordinates(first.srid, Geometry::line_string(coords), builder.finish())
        .map(Value::Geometry)
        .map_err(|e| geometry_fault(function, e.into()))
}

/// `ST_MakePolygon(ring, ...)` — each argument is an array of coordinate
/// pairs. A malformed vertex fails the whole call.
fn make_polygon(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let mut rings = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        if matches!(arg, Value::Null) {
            return Ok(Value::Null);
        }
        let bad = |reason: &str| EvalError::ArgumentValue {
            function,
            position: idx + 1,
            reason: reason.into(),
        };
        let points = arg
            .as_array()
            .ok_or_else(|| bad("must be an array of [x, y] pairs"))?;
        let mut ring = Vec::with_capacity(points.len());
        for point in points {
            let pair = point
                .as_array()
                .ok_or_else(|| bad("holds a vertex that is not an array"))?;
            let (Some(x), Some(y)) = (
                pair.first().and_then(Value::as_f64),
                pair.get(1).and_then(Value::as_f64),
            ) else {
                return Err(bad("holds a vertex without two numbers"));
            };
            ring.push([x, y]);
        }
        if ring.len() < 4 {
            return Err(bad("a ring needs at least 4 points"));
        }
        rings.push(ring);
    }
    Ok(Value::Geometry(SridGeometry::wgs84(Geometry::polygon(
        rings,
    ))))
}

fn make_envelope(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let mut v = [0.0f64; 4];
    for (i, slot) in v.iter_mut().enumerate() {
        match finite_arg(args, i, function)? {
            Some(x) => *slot = x,
            None => return Ok(Value::Null),
        }
    }
    let [min_x, min_y, max_x, max_y] = v;
    Ok(Value::Geometry(SridGeometry::wgs84(Geometry::polygon(
        vec![vec![
            [min_x, min_y],
            [max_x, min_y],
            [max_x, max_y],
            [min_x, max_y],
            [min_x, min_y],
        ]],
    ))))
}

/// `ST_TileEnvelope(z, x, y [, bounds])`: an XYZ tile as an EPSG:3857
/// polygon, or over the box and SRID of `bounds`.
fn tile_envelope(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let mut zxy = [0i64; 3];
    for (i, slot) in zxy.iter_mut().enumerate() {
        match args.get(i) {
            None | Some(Value::Null) => return Ok(Value::Null),
            Some(Value::Integer(n)) => *slot = *n,
            Some(Value::Float(f)) if f.fract() == 0.0 && f.is_finite() => *slot = *f as i64,
            Some(other) => {
                return Err(EvalError::ArgumentType {
                    function,
                    position: i + 1,
                    expected: "an integer",
                    got: other.type_name(),
                });
            }
        }
    }
    let (bounds, srid) = match geom_arg(args, 3, function, ctx)? {
        Some(b) => {
            let bb = b
                .bbox(ctx)
                .map_err(|source| EvalError::Crs { function, source })?;
            (bb, b.srid)
        }
        None => (
            nodedb_spatial::ops::web_mercator_bounds(),
            Srid::WEB_MERCATOR,
        ),
    };
    let shape = nodedb_spatial::ops::tile_envelope(zxy[0], zxy[1], zxy[2], &bounds)
        .map_err(|e| geometry_fault(function, e.into()))?;
    Ok(Value::Geometry(SridGeometry::new(srid, shape)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::UnknownSrid;

    fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
        super::eval(canonical, args, &CrsContext::builtin_only())
    }

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    /// The evaluator, with a fault turned into a test failure.
    fn run(canonical: &'static str, args: &[Value]) -> Option<Value> {
        eval(canonical, args).map(|r| r.expect("no fault expected"))
    }

    fn wgs84(g: Geometry) -> Value {
        Value::Geometry(SridGeometry::wgs84(g))
    }

    fn projected(srid: Srid, g: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(srid, g))
    }

    #[test]
    fn point_constructors_agree() {
        let args = vec![Value::Float(1.0), Value::Float(2.0)];
        let expected = Some(wgs84(Geometry::point(1.0, 2.0)));
        assert_eq!(run("st_point", &args), expected);
        assert_eq!(run("st_makepoint", &args), expected);
    }

    #[test]
    fn makepoint_takes_z_and_m() {
        let Some(Value::Geometry(z)) = run(
            "st_makepoint",
            &[Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
        ) else {
            panic!();
        };
        assert_eq!(z.ordinates().z(), Some(&[3.0][..]));
        let Some(Value::Geometry(zm)) = run(
            "st_makepoint",
            &[
                Value::Float(1.0),
                Value::Float(2.0),
                Value::Float(3.0),
                Value::Float(4.0),
            ],
        ) else {
            panic!();
        };
        assert_eq!(zm.dimension(), Dimension::Xyzm);
        let Some(Value::Geometry(m)) = run(
            "st_makepointm",
            &[Value::Float(1.0), Value::Float(2.0), Value::Float(9.0)],
        ) else {
            panic!();
        };
        assert_eq!(m.ordinates().m(), Some(&[9.0][..]));
        assert_eq!(m.ordinates().z(), None);
    }

    #[test]
    fn geomfromtext_accepts_wkt_ewkt_and_geojson() {
        let expected = Some(wgs84(Geometry::point(1.0, 2.0)));
        assert_eq!(
            run("st_geomfromtext", &[Value::String("POINT(1 2)".into())]),
            expected
        );
        assert_eq!(
            run(
                "st_geomfromgeojson",
                &[Value::String(
                    r#"{"type":"Point","coordinates":[1.0,2.0]}"#.into()
                )]
            ),
            expected
        );
        assert_eq!(
            run(
                "st_geomfromtext",
                &[Value::String("SRID=32648;POINT(1 2)".into())]
            ),
            Some(projected(srid(32648), Geometry::point(1.0, 2.0)))
        );
    }

    #[test]
    fn geomfromtext_srid_argument_sets_srid() {
        let args = vec![Value::String("POINT(1 2)".into()), Value::Integer(3375)];
        assert_eq!(
            run("st_geomfromtext", &args),
            Some(projected(srid(3375), Geometry::point(1.0, 2.0)))
        );
    }

    #[test]
    fn geomfromtext_undefined_srid_fails() {
        let args = vec![Value::String("POINT(1 2)".into()), Value::Integer(999)];
        assert_eq!(
            eval("st_geomfromtext", &args),
            Some(Err(EvalError::Crs {
                function: "st_geomfromtext",
                source: UnknownSrid::Undefined { code: 999 }
            }))
        );
    }

    /// Malformed text is a statement error, never NULL.
    #[test]
    fn malformed_text_fails() {
        for text in ["POINT(", "POINT(1 2 3 4 5)", "POINT Z (1 2)"] {
            assert!(
                matches!(
                    eval("st_geomfromtext", &[Value::String(text.into())]),
                    Some(Err(EvalError::Geometry { .. }))
                ),
                "{text}"
            );
        }
    }

    #[test]
    fn geomfromwkb_reads_ewkb_srid_and_explicit_srid_wins() {
        let g = SridGeometry::new(srid(32648), Geometry::point(1.0, 2.0));
        let ewkb = nodedb_spatial::geometry_to_ewkb(&g).expect("encode");
        assert_eq!(
            run("st_geomfromwkb", &[Value::Bytes(ewkb.clone())]),
            Some(Value::Geometry(g))
        );
        assert_eq!(
            run(
                "st_geomfromwkb",
                &[Value::Bytes(ewkb), Value::Integer(3376)]
            ),
            Some(projected(srid(3376), Geometry::point(1.0, 2.0)))
        );
    }

    /// A byte literal and its hex-string spelling must decode identically.
    #[test]
    fn geomfromwkb_accepts_bytes_and_hex_string() {
        let hex = "0101000000000000000000F03F0000000000000040";
        let bytes = decode_hex(hex).expect("test vector must decode");
        let from_bytes = run("st_geomfromwkb", &[Value::Bytes(bytes)]);
        let from_hex = run("st_geomfromwkb", &[Value::String(hex.into())]);
        assert_eq!(from_bytes, from_hex);
        assert_eq!(from_bytes, Some(wgs84(Geometry::point(1.0, 2.0))));
    }

    #[test]
    fn malformed_wkb_fails() {
        assert!(matches!(
            eval("st_geomfromwkb", &[Value::String("zz".into())]),
            Some(Err(EvalError::ArgumentValue { .. }))
        ));
        assert!(matches!(
            eval("st_geomfromwkb", &[Value::String("0101".into())]),
            Some(Err(EvalError::Geometry { .. }))
        ));
    }

    #[test]
    fn makeline_keeps_z_and_rejects_mixed_input() {
        let pz = |x: f64, z: f64| {
            Value::Geometry(
                SridGeometry::with_ordinates(
                    Srid::WGS84,
                    Geometry::point(x, 0.0),
                    Ordinates::new(Some(vec![z]), None),
                )
                .unwrap(),
            )
        };
        let Some(Value::Geometry(line)) = run("st_makeline", &[pz(0.0, 1.0), pz(1.0, 2.0)]) else {
            panic!();
        };
        assert_eq!(line.ordinates().z(), Some(&[1.0, 2.0][..]));
        assert!(matches!(
            eval(
                "st_makeline",
                &[pz(0.0, 1.0), wgs84(Geometry::point(1.0, 1.0))]
            ),
            Some(Err(EvalError::Geometry { .. }))
        ));
        assert!(matches!(
            eval(
                "st_makeline",
                &[
                    wgs84(Geometry::point(0.0, 0.0)),
                    projected(srid(32648), Geometry::point(1.0, 1.0))
                ]
            ),
            Some(Err(EvalError::MixedSrid { .. }))
        ));
        assert!(matches!(
            eval("st_makeline", &[wgs84(Geometry::point(0.0, 0.0))]),
            Some(Err(EvalError::ArgumentValue { .. }))
        ));
    }

    #[test]
    fn makeenvelope_closes_its_ring() {
        let args = vec![
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(1.0),
            Value::Float(1.0),
        ];
        let Some(Value::Geometry(g)) = run("st_makeenvelope", &args) else {
            panic!("expected a polygon");
        };
        let Geometry::Polygon { coordinates } = g.geometry() else {
            panic!("expected a polygon");
        };
        let ring = &coordinates[0];
        assert_eq!(ring.first(), ring.last(), "envelope ring must be closed");
    }

    #[test]
    fn tile_envelope_is_web_mercator() {
        let Some(Value::Geometry(g)) = run(
            "st_tileenvelope",
            &[Value::Integer(0), Value::Integer(0), Value::Integer(0)],
        ) else {
            panic!();
        };
        assert_eq!(g.srid, srid(3857));
        assert!(matches!(
            eval(
                "st_tileenvelope",
                &[Value::Integer(1), Value::Integer(5), Value::Integer(0)]
            ),
            Some(Err(EvalError::Geometry { .. }))
        ));
    }

    #[test]
    fn circle_is_geodesic() {
        let Some(Value::Geometry(g)) = run(
            "geo_circle",
            &[Value::Float(0.0), Value::Float(0.0), Value::Float(1000.0)],
        ) else {
            panic!();
        };
        let m = nodedb_spatial::Metric::wgs84();
        for c in g.geometry().coords() {
            let d = m.segment_length([0.0, 0.0], c);
            assert!((d - 1000.0).abs() < 10.0, "{d}");
        }
    }

    #[test]
    fn unknown_name_falls_through() {
        assert_eq!(eval("st_x", &[]), None);
    }
}
