// SPDX-License-Identifier: Apache-2.0

//! Readers over a geometry's own contents: ordinates, dimension, type name
//! and vertex count. The encodings live in `encode`, the SRID reader in
//! `crs`.

use nodedb_crs::CrsContext;
use nodedb_types::Value;
use nodedb_types::geometry::{Geometry, SridGeometry};

use super::helpers::{GeoEval, geom_arg, unary};
use crate::expr::EvalError;
use crate::value_ops::to_value_number;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_x" => ordinate(canonical, args, ctx, Ordinate::X),
        "st_y" => ordinate(canonical, args, ctx, Ordinate::Y),
        "st_z" => ordinate(canonical, args, ctx, Ordinate::Z),
        "st_m" => ordinate(canonical, args, ctx, Ordinate::M),
        "st_zmin" => unary(canonical, args, ctx, |g| Ok(z_extreme(g, f64::min))),
        "st_zmax" => unary(canonical, args, ctx, |g| Ok(z_extreme(g, f64::max))),
        "st_coorddim" | "st_ndims" => unary(canonical, args, ctx, |g| {
            Ok(Value::Integer(i64::from(g.dimension().coord_dim())))
        }),
        "st_zmflag" => unary(canonical, args, ctx, |g| {
            Ok(Value::Integer(i64::from(g.dimension().zm_flag())))
        }),
        "st_hasz" => unary(canonical, args, ctx, |g| Ok(Value::Bool(g.has_z()))),
        "st_hasm" => unary(canonical, args, ctx, |g| Ok(Value::Bool(g.has_m()))),
        "st_isempty" => unary(canonical, args, ctx, |g| Ok(Value::Bool(g.is_empty()))),
        "st_geometrytype" => unary(canonical, args, ctx, |g| {
            Ok(Value::String(g.geometry().geometry_type().to_string()))
        }),
        "st_npoints" => unary(canonical, args, ctx, |g| {
            Ok(Value::Integer(g.geometry().coord_count() as i64))
        }),
        _ => return None,
    };
    Some(result)
}

#[derive(Clone, Copy)]
enum Ordinate {
    X,
    Y,
    Z,
    M,
}

/// One ordinate of a point. PostGIS defines these on points only: any other
/// geometry, an EMPTY point, and a missing Z or M yield NULL.
fn ordinate(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    which: Ordinate,
) -> Result<Value, EvalError> {
    let Some(g) = geom_arg(args, 0, function, ctx)? else {
        return Ok(Value::Null);
    };
    if !matches!(g.geometry(), Geometry::Point { .. }) || g.is_empty() {
        return Ok(Value::Null);
    }
    let Some(p) = g.positions().next() else {
        return Ok(Value::Null);
    };
    let value = match which {
        Ordinate::X => Some(p.x),
        Ordinate::Y => Some(p.y),
        Ordinate::Z => p.z,
        Ordinate::M => p.m,
    };
    Ok(value.map_or(Value::Null, to_value_number))
}

/// The least or greatest Z. A geometry without Z has a Z of 0 everywhere,
/// as PostGIS reports it. EMPTY is NULL.
fn z_extreme(g: &SridGeometry, pick: fn(f64, f64) -> f64) -> Value {
    if g.is_empty() {
        return Value::Null;
    }
    match g.ordinates().z() {
        Some(z) => z
            .iter()
            .copied()
            .reduce(pick)
            .map_or(Value::Null, to_value_number),
        None => to_value_number(0.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Ordinates, Srid};

    fn ctx() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    /// The evaluator with a fault turned into a test failure.
    fn eval(canonical: &'static str, args: &[Value]) -> Option<Value> {
        super::eval(canonical, args, &ctx()).map(|r| r.expect("no fault expected"))
    }

    fn geom(v: Geometry) -> Vec<Value> {
        vec![Value::Geometry(SridGeometry::wgs84(v))]
    }

    fn zm_point() -> Vec<Value> {
        vec![Value::Geometry(
            SridGeometry::with_ordinates(
                Srid::WGS84,
                Geometry::point(1.0, 2.0),
                Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
            )
            .unwrap(),
        )]
    }

    #[test]
    fn ordinates_read_a_point() {
        let point = zm_point();
        assert_eq!(eval("st_x", &point), Some(to_value_number(1.0)));
        assert_eq!(eval("st_y", &point), Some(to_value_number(2.0)));
        assert_eq!(eval("st_z", &point), Some(to_value_number(3.0)));
        assert_eq!(eval("st_m", &point), Some(to_value_number(4.0)));
        let xy = geom(Geometry::point(1.0, 2.0));
        assert_eq!(eval("st_z", &xy), Some(Value::Null));
        assert_eq!(eval("st_m", &xy), Some(Value::Null));
    }

    /// A non-point has no single X; returning a vertex would be a guess.
    #[test]
    fn ordinates_of_a_non_point_or_empty_are_null() {
        let line = geom(Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]));
        assert_eq!(eval("st_x", &line), Some(Value::Null));
        assert_eq!(
            eval("st_x", &geom(Geometry::empty_point())),
            Some(Value::Null)
        );
    }

    #[test]
    fn dimension_readers() {
        let zm = zm_point();
        assert_eq!(eval("st_coorddim", &zm), Some(Value::Integer(4)));
        assert_eq!(eval("st_ndims", &zm), Some(Value::Integer(4)));
        assert_eq!(eval("st_zmflag", &zm), Some(Value::Integer(3)));
        assert_eq!(eval("st_hasz", &zm), Some(Value::Bool(true)));
        assert_eq!(eval("st_hasm", &zm), Some(Value::Bool(true)));
        let xy = geom(Geometry::point(0.0, 0.0));
        assert_eq!(eval("st_zmflag", &xy), Some(Value::Integer(0)));
        assert_eq!(eval("st_coorddim", &xy), Some(Value::Integer(2)));
    }

    #[test]
    fn z_extremes() {
        let line = vec![Value::Geometry(
            SridGeometry::with_ordinates(
                Srid::WGS84,
                Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]]),
                Ordinates::new(Some(vec![5.0, -1.0, 3.0]), None),
            )
            .unwrap(),
        )];
        assert_eq!(eval("st_zmin", &line), Some(to_value_number(-1.0)));
        assert_eq!(eval("st_zmax", &line), Some(to_value_number(5.0)));
        assert_eq!(
            eval("st_zmax", &geom(Geometry::point(0.0, 0.0))),
            Some(to_value_number(0.0))
        );
    }

    #[test]
    fn type_count_and_empty() {
        assert_eq!(
            eval("st_geometrytype", &geom(Geometry::point(0.0, 0.0))),
            Some(Value::String("Point".into()))
        );
        let line = geom(Geometry::line_string(vec![
            [0.0, 0.0],
            [1.0, 1.0],
            [2.0, 2.0],
        ]));
        assert_eq!(eval("st_npoints", &line), Some(Value::Integer(3)));
        assert_eq!(
            eval("st_npoints", &geom(Geometry::empty_point())),
            Some(Value::Integer(0))
        );
        assert_eq!(
            eval("st_isempty", &geom(Geometry::empty_point())),
            Some(Value::Bool(true))
        );
    }

    /// A WKT string argument is a geometry, so accessors read it directly.
    #[test]
    fn wkt_string_argument_is_accepted() {
        let wkt = vec![Value::String("POINT Z (3 4 5)".into())];
        assert_eq!(eval("st_x", &wkt), Some(to_value_number(3.0)));
        assert_eq!(eval("st_z", &wkt), Some(to_value_number(5.0)));
    }

    #[test]
    fn unreadable_text_fails_the_statement() {
        let bad = vec![Value::String("not a geometry".into())];
        assert!(matches!(
            super::eval("st_npoints", &bad, &ctx()),
            Some(Err(EvalError::Geometry { .. }))
        ));
    }
}
