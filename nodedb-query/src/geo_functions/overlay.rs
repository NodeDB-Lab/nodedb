// SPDX-License-Identifier: Apache-2.0

//! Geometry-returning operations: buffer, envelope, centroid, boolean
//! overlay, hulls, simplification, repair, subdivision, splitting, closest
//! point and dimension forcing.
//!
//! An operation keeps its input SRID. A binary operation needs both inputs
//! in one SRID. A derived shape has no Z or M of its own.

use nodedb_crs::CrsContext;
use nodedb_spatial::ops::{Hull, Simplifier};
use nodedb_types::Value;
use nodedb_types::geometry::{Dimension, Geometry, GeometryKind, SridGeometry};

use super::constructors::segments_arg;
use super::helpers::{
    GeoEval, finite_arg, geom_arg, geometry_fault, metric_of, pair, space_of, unary,
};
use crate::expr::EvalError;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_buffer" => buffer(canonical, args, ctx),
        "st_envelope" => in_space(canonical, args, ctx, |g, space| {
            Ok(nodedb_spatial::st_envelope_in(g, space))
        }),
        "st_centroid" => centroid(canonical, args, ctx),
        "st_union" => binary(canonical, args, ctx, nodedb_spatial::ops::union),
        "st_intersection" => binary(canonical, args, ctx, nodedb_spatial::ops::intersection),
        "st_difference" => binary(canonical, args, ctx, nodedb_spatial::ops::difference),
        "st_symdifference" => binary(canonical, args, ctx, nodedb_spatial::ops::sym_difference),
        "st_convexhull" => in_space(canonical, args, ctx, |g, _| {
            Ok(nodedb_spatial::ops::hull(g, Hull::Convex))
        }),
        "st_concavehull" => with_number(canonical, args, ctx, |g, pct| {
            if !(0.0..=1.0).contains(&pct) {
                return Err(out_of_range(canonical, 2, "must be in [0, 1]"));
            }
            Ok(nodedb_spatial::ops::hull(
                g,
                Hull::Concave { pct_convex: pct },
            ))
        }),
        "st_simplify" => simplify(canonical, args, ctx, Simplifier::DouglasPeucker),
        "st_simplifypreservetopology" => {
            simplify(canonical, args, ctx, Simplifier::PreserveTopology)
        }
        "st_makevalid" => in_space(canonical, args, ctx, |g, _| {
            nodedb_spatial::ops::make_valid(g).map_err(|e| geometry_fault(canonical, e))
        }),
        "st_subdivide" => subdivide(canonical, args, ctx),
        "st_split" => binary_fallible(canonical, args, ctx, nodedb_spatial::ops::split),
        "st_closestpoint" => closest_point(canonical, args, ctx),
        "st_segmentize" => segmentize(canonical, args, ctx),
        "st_force2d" => force(canonical, args, ctx, Dimension::Xy),
        "st_force3d" => force(canonical, args, ctx, Dimension::Xyz),
        "st_force3dm" => force(canonical, args, ctx, Dimension::Xym),
        "st_force4d" => force(canonical, args, ctx, Dimension::Xyzm),
        _ => return None,
    };
    Some(result)
}

fn out_of_range(function: &'static str, position: usize, reason: &str) -> EvalError {
    EvalError::ArgumentValue {
        function,
        position,
        reason: reason.into(),
    }
}

/// A unary operation in the input's coordinate space. The result keeps the
/// input SRID.
fn in_space(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: impl FnOnce(&Geometry, nodedb_types::BboxSpace) -> Result<Geometry, EvalError>,
) -> Result<Value, EvalError> {
    unary(function, args, ctx, |g| {
        let space = space_of(function, g.srid, ctx)?;
        Ok(Value::Geometry(g.with_geometry(f(g.geometry(), space)?)))
    })
}

/// A unary operation with a required numeric second argument.
fn with_number(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: impl Fn(&Geometry, f64) -> Result<Geometry, EvalError>,
) -> Result<Value, EvalError> {
    let (Some(g), Some(n)) = (
        geom_arg(args, 0, function, ctx)?,
        finite_arg(args, 1, function)?,
    ) else {
        return Ok(Value::Null);
    };
    Ok(Value::Geometry(g.with_geometry(f(g.geometry(), n)?)))
}

/// A binary operation over two inputs in one SRID.
fn binary(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: fn(&Geometry, &Geometry) -> Geometry,
) -> Result<Value, EvalError> {
    binary_fallible(function, args, ctx, |a, b| Ok(f(a, b)))
}

fn binary_fallible(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: impl FnOnce(&Geometry, &Geometry) -> Result<Geometry, nodedb_spatial::SpatialError>,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        let out = f(a.geometry(), b.geometry()).map_err(|e| geometry_fault(function, e))?;
        Ok(Value::Geometry(SridGeometry::new(a.srid, out)))
    })
}

/// `ST_Buffer(geom, distance_m [, segments])`. Distance is metres in either
/// space: geodesic in a local projection on a geographic CRS's ellipsoid,
/// planar in a projected CRS's unit.
fn buffer(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let (Some(g), Some(distance)) = (
        geom_arg(args, 0, function, ctx)?,
        finite_arg(args, 1, function)?,
    ) else {
        return Ok(Value::Null);
    };
    let def = ctx
        .resolve(g.srid)
        .map_err(|source| EvalError::Crs { function, source })?;
    let out = nodedb_spatial::ops::buffer(g.geometry(), distance, segments_arg(args, 2), &def)
        .map_err(|e| geometry_fault(function, e))?;
    Ok(Value::Geometry(g.with_geometry(out)))
}

/// `ST_Centroid(geom)`: weighted by geodesic area or length on a
/// geographic CRS, planar on a projected one. EMPTY gives `POINT EMPTY`.
fn centroid(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    unary(function, args, ctx, |g| {
        let metric = metric_of(function, g.srid, ctx)?;
        let point = nodedb_spatial::st_centroid(g.geometry(), &metric)
            .unwrap_or_else(|| Geometry::empty(GeometryKind::Point));
        Ok(Value::Geometry(g.with_geometry(point)))
    })
}

/// `ST_Simplify` / `ST_SimplifyPreserveTopology(geom, tolerance)`, the
/// tolerance in coordinate units. A negative tolerance fails.
fn simplify(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    simplifier: Simplifier,
) -> Result<Value, EvalError> {
    with_number(function, args, ctx, |g, tolerance| {
        if tolerance < 0.0 {
            return Err(out_of_range(function, 2, "must not be negative"));
        }
        Ok(nodedb_spatial::ops::simplify(g, tolerance, simplifier))
    })
}

/// `ST_Subdivide(geom [, max_vertices])`, 256 vertices when absent. The
/// pieces come back as one GEOMETRYCOLLECTION.
fn subdivide(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    const DEFAULT_MAX_VERTICES: f64 = 256.0;
    let Some(g) = geom_arg(args, 0, function, ctx)? else {
        return Ok(Value::Null);
    };
    let max = match args.get(1) {
        None => DEFAULT_MAX_VERTICES,
        Some(_) => match finite_arg(args, 1, function)? {
            Some(n) => n,
            None => return Ok(Value::Null),
        },
    };
    if max < nodedb_spatial::ops::MIN_SUBDIVIDE_VERTICES as f64 {
        return Err(out_of_range(function, 2, "must be at least 5"));
    }
    Ok(Value::Geometry(g.with_geometry(
        nodedb_spatial::ops::subdivide(g.geometry(), max as usize),
    )))
}

/// `ST_ClosestPoint(a, b)`: the point of `a` nearest `b`. EMPTY gives NULL.
fn closest_point(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        Ok(
            match nodedb_spatial::ops::closest_point(a.geometry(), b.geometry()) {
                Some([x, y]) => Value::Geometry(SridGeometry::new(a.srid, Geometry::point(x, y))),
                None => Value::Null,
            },
        )
    })
}

/// `ST_Segmentize(geom, max_length_m)`: geodesic on a geographic CRS, in
/// metres of the grid on a projected one. Z and M interpolate.
fn segmentize(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let (Some(g), Some(max)) = (
        geom_arg(args, 0, function, ctx)?,
        finite_arg(args, 1, function)?,
    ) else {
        return Ok(Value::Null);
    };
    if max <= 0.0 {
        return Err(out_of_range(function, 2, "must be positive"));
    }
    let metric = metric_of(function, g.srid, ctx)?;
    nodedb_spatial::ops::segmentize(&g, max, &metric)
        .map(Value::Geometry)
        .map_err(|e| geometry_fault(function, e))
}

/// `ST_Force2D/3D/3DM/4D(geom [, z [, m]])`: keep the shape, set the
/// dimension. A missing ordinate fills with the given value, 0 by default.
fn force(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    dimension: Dimension,
) -> Result<Value, EvalError> {
    let Some(g) = geom_arg(args, 0, function, ctx)? else {
        return Ok(Value::Null);
    };
    let fill = |idx: usize| finite_arg(args, idx, function).map(|v| v.unwrap_or(0.0));
    let (fill_z, fill_m) = match dimension {
        Dimension::Xy => (0.0, 0.0),
        Dimension::Xyz => (fill(1)?, 0.0),
        Dimension::Xym => (0.0, fill(1)?),
        Dimension::Xyzm => (fill(1)?, fill(2)?),
    };
    g.force_dimension(dimension, fill_z, fill_m)
        .map(Value::Geometry)
        .map_err(|e| geometry_fault(function, e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Ordinates, Srid};

    fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
        super::eval(canonical, args, &CrsContext::builtin_only())
    }

    fn run(canonical: &'static str, args: &[Value]) -> SridGeometry {
        match eval(canonical, args) {
            Some(Ok(Value::Geometry(g))) => g,
            other => panic!("{canonical}: {other:?}"),
        }
    }

    fn utm(g: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(Srid::new(32648).unwrap(), g))
    }

    fn square(x0: f64, y0: f64, side: f64) -> Geometry {
        Geometry::polygon(vec![vec![
            [x0, y0],
            [x0 + side, y0],
            [x0 + side, y0 + side],
            [x0, y0 + side],
            [x0, y0],
        ]])
    }

    #[test]
    fn overlay_ops_keep_srid() {
        let (a, b) = (utm(square(0.0, 0.0, 2.0)), utm(square(1.0, 1.0, 2.0)));
        for f in [
            "st_union",
            "st_intersection",
            "st_difference",
            "st_symdifference",
        ] {
            let g = run(f, &[a.clone(), b.clone()]);
            assert_eq!(g.srid.code(), 32648, "{f}");
        }
        assert!(matches!(
            eval(
                "st_union",
                &[
                    a,
                    Value::Geometry(SridGeometry::wgs84(Geometry::point(0.0, 0.0)))
                ]
            ),
            Some(Err(EvalError::MixedSrid { .. }))
        ));
    }

    #[test]
    fn projected_buffer_is_planar_and_keeps_srid() {
        let g = run(
            "st_buffer",
            &[
                utm(Geometry::point(500_000.0, 100_000.0)),
                Value::Float(100.0),
            ],
        );
        assert_eq!(g.srid.code(), 32648);
        let Geometry::Polygon { coordinates } = g.geometry() else {
            panic!("expected a polygon");
        };
        let [x, y] = coordinates[0][0];
        // A 32-gon vertex sits at most 0.5 % off the circle.
        assert!(((x - 500_000.0).hypot(y - 100_000.0) - 100.0).abs() < 0.5);
    }

    #[test]
    fn multi_buffer_is_not_a_bbox() {
        let multi = Value::Geometry(SridGeometry::wgs84(Geometry::MultiPoint {
            coordinates: vec![[0.0, 0.0], [0.1, 0.0]],
        }));
        let g = run("st_buffer", &[multi, Value::Float(100.0)]);
        assert!(
            matches!(g.geometry(), Geometry::MultiPolygon { coordinates } if coordinates.len() == 2),
            "{g:?}"
        );
    }

    #[test]
    fn force_sets_dimension() {
        let xy = utm(Geometry::point(1.0, 2.0));
        let z = run("st_force3d", &[xy.clone(), Value::Float(9.0)]);
        assert_eq!(z.ordinates().z(), Some(&[9.0][..]));
        let zm = run("st_force4d", std::slice::from_ref(&xy));
        assert_eq!(zm.dimension(), Dimension::Xyzm);
        let m = run("st_force3dm", &[Value::Geometry(zm.clone())]);
        assert_eq!(m.dimension(), Dimension::Xym);
        assert_eq!(
            run("st_force2d", &[Value::Geometry(zm)]).dimension(),
            Dimension::Xy
        );
    }

    #[test]
    fn hulls_simplify_and_repair() {
        let pts = utm(Geometry::MultiPoint {
            coordinates: vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [2.0, 2.0]],
        });
        assert!(matches!(
            run("st_convexhull", std::slice::from_ref(&pts)).geometry(),
            Geometry::Polygon { .. }
        ));
        assert!(matches!(
            eval("st_concavehull", &[pts, Value::Float(2.0)]),
            Some(Err(EvalError::ArgumentValue { .. }))
        ));
        let line = utm(Geometry::line_string(vec![
            [0.0, 0.0],
            [5.0, 0.1],
            [10.0, 0.0],
        ]));
        let simple = run("st_simplify", &[line, Value::Float(1.0)]);
        assert_eq!(simple.geometry().coord_count(), 2);
        let bowtie = utm(Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [0.0, 10.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 0.0],
        ]]));
        assert!(nodedb_spatial::is_valid(
            run("st_makevalid", &[bowtie]).geometry()
        ));
    }

    #[test]
    fn centroid_of_empty_is_empty_point() {
        let g = run(
            "st_centroid",
            &[Value::Geometry(
                SridGeometry::wgs84(Geometry::empty_point()),
            )],
        );
        assert!(g.is_empty());
    }

    #[test]
    fn segmentize_keeps_z() {
        let line = Value::Geometry(
            SridGeometry::with_ordinates(
                Srid::new(32648).unwrap(),
                Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]),
                Ordinates::new(Some(vec![0.0, 10.0]), None),
            )
            .unwrap(),
        );
        let g = run("st_segmentize", &[line, Value::Float(5.0)]);
        assert_eq!(g.ordinates().z(), Some(&[0.0, 5.0, 10.0][..]));
    }

    #[test]
    fn subdivide_and_split_return_collections() {
        let sq = utm(square(0.0, 0.0, 4.0));
        let blade = utm(Geometry::line_string(vec![[2.0, -1.0], [2.0, 5.0]]));
        let parts = run("st_split", &[sq.clone(), blade]);
        assert!(
            matches!(parts.geometry(), Geometry::GeometryCollection { geometries } if geometries.len() == 2)
        );
        assert!(matches!(
            run("st_subdivide", std::slice::from_ref(&sq)).geometry(),
            Geometry::GeometryCollection { .. }
        ));
        assert!(matches!(
            eval("st_subdivide", &[sq, Value::Integer(2)]),
            Some(Err(EvalError::ArgumentValue { .. }))
        ));
    }
}
