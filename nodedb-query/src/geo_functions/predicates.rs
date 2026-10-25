// SPDX-License-Identifier: Apache-2.0

//! Topological predicate evaluation. Every predicate is NULL-propagating: an
//! argument that is SQL NULL yields NULL rather than `false`, so "no
//! geometry" stays distinguishable from "does not match". Two operands in
//! different SRIDs fail with `EvalError::MixedSrid`. An EMPTY operand
//! intersects, contains and touches nothing, as in PostGIS.

use nodedb_crs::CrsContext;
use nodedb_spatial::ops::{Relation, relates};
use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::{GeoEval, finite_arg, geometry_fault, metric_of, pair, str_arg, unary};
use crate::expr::EvalError;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_contains" => predicate(canonical, args, ctx, false, nodedb_spatial::st_contains),
        "st_intersects" => predicate(canonical, args, ctx, false, nodedb_spatial::st_intersects),
        "st_within" => predicate(canonical, args, ctx, false, nodedb_spatial::st_within),
        "st_disjoint" => predicate(canonical, args, ctx, true, nodedb_spatial::st_disjoint),
        "st_touches" => named(canonical, args, ctx, Relation::Touches),
        "st_crosses" => named(canonical, args, ctx, Relation::Crosses),
        "st_overlaps" => named(canonical, args, ctx, Relation::Overlaps),
        "st_covers" => named(canonical, args, ctx, Relation::Covers),
        "st_coveredby" => named(canonical, args, ctx, Relation::CoveredBy),
        "st_equals" => equals(canonical, args, ctx),
        "st_relate" => relate(canonical, args, ctx),
        "st_dwithin" => dwithin(canonical, args, ctx, false),
        "st_3ddwithin" => dwithin(canonical, args, ctx, true),
        "st_3dintersects" => intersects_3d(canonical, args, ctx),
        "st_isvalid" => unary(canonical, args, ctx, |g| {
            Ok(Value::Bool(nodedb_spatial::is_valid(g.geometry())))
        }),
        "st_isvalidreason" => unary(canonical, args, ctx, |g| {
            Ok(Value::String(nodedb_spatial::validity_reason(g.geometry())))
        }),
        _ => return None,
    };
    Some(result)
}

/// A planar predicate. An EMPTY operand gives `on_empty`.
fn predicate(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    on_empty: bool,
    f: fn(&Geometry, &Geometry) -> bool,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        if a.is_empty() || b.is_empty() {
            return Ok(Value::Bool(on_empty));
        }
        Ok(Value::Bool(f(a.geometry(), b.geometry())))
    })
}

/// A named DE-9IM predicate. An EMPTY operand gives `false`.
fn named(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    relation: Relation,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        if a.is_empty() || b.is_empty() {
            return Ok(Value::Bool(false));
        }
        Ok(Value::Bool(relates(a.geometry(), b.geometry(), relation)))
    })
}

/// `ST_Equals`: the same point set. Two EMPTY operands are equal.
fn equals(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        Ok(Value::Bool(match (a.is_empty(), b.is_empty()) {
            (true, true) => true,
            (false, false) => relates(a.geometry(), b.geometry(), Relation::Equals),
            _ => false,
        }))
    })
}

/// `ST_Relate(a, b)`: the DE-9IM matrix text. `ST_Relate(a, b, pattern)`:
/// whether the matrix matches `pattern`.
fn relate(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| match args.get(2) {
        None => Ok(Value::String(nodedb_spatial::ops::relate(
            a.geometry(),
            b.geometry(),
        ))),
        Some(Value::Null) => Ok(Value::Null),
        Some(_) => {
            let pattern = str_arg(args, 2).ok_or(EvalError::ArgumentType {
                function,
                position: 3,
                expected: "a DE-9IM pattern text",
                got: args.get(2).map_or("null", Value::type_name),
            })?;
            nodedb_spatial::ops::relate_pattern(a.geometry(), b.geometry(), &pattern)
                .map(Value::Bool)
                .map_err(|e| geometry_fault(function, e))
        }
    })
}

/// `ST_DWithin(a, b, metres)` and `ST_3DDWithin(a, b, metres)`. Metres
/// are geodesic on a geographic CRS's ellipsoid, planar in a projected
/// CRS's unit. The 3D form adds Z: Euclidean in a projected CRS, the ECEF
/// chord in a geographic one. A negative distance fails.
fn dwithin(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    three_d: bool,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        let Some(distance) = finite_arg(args, 2, function)? else {
            return Ok(Value::Null);
        };
        if distance < 0.0 {
            return Err(EvalError::ArgumentValue {
                function,
                position: 3,
                reason: "must not be negative".into(),
            });
        }
        let metric = metric_of(function, a.srid, ctx)?;
        Ok(Value::Bool(if three_d {
            metric.dwithin_3d(a, b, distance)
        } else {
            metric.dwithin(a.geometry(), b.geometry(), distance)
        }))
    })
}

/// `ST_3DIntersects(a, b)`: the 3D distance is zero.
fn intersects_3d(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        let metric = metric_of(function, a.srid, ctx)?;
        if a.is_empty() || b.is_empty() {
            return Ok(Value::Bool(false));
        }
        Ok(Value::Bool(
            metric.distance_3d(a, b) <= INTERSECT_TOLERANCE_M,
        ))
    })
}

/// 3D distance, metres, at or under which two geometries intersect: the
/// float noise of an ECEF chord.
const INTERSECT_TOLERANCE_M: f64 = 1e-6;

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Ordinates, Srid, SridGeometry};

    fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
        super::eval(canonical, args, &CrsContext::builtin_only())
    }

    fn utm48n() -> Srid {
        Srid::new(32648).unwrap()
    }

    fn geom(srid: Srid, g: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(srid, g))
    }

    #[test]
    fn mixed_srid_intersects_fails() {
        let args = vec![
            geom(Srid::WGS84, Geometry::point(1.0, 2.0)),
            geom(utm48n(), Geometry::point(1.0, 2.0)),
        ];
        assert_eq!(
            eval("st_intersects", &args),
            Some(Err(EvalError::MixedSrid {
                function: "st_intersects",
                left: 4326,
                right: 32648
            }))
        );
    }

    #[test]
    fn projected_dwithin_is_planar_metres() {
        let args = |d: f64| {
            vec![
                geom(utm48n(), Geometry::point(500_000.0, 0.0)),
                geom(utm48n(), Geometry::point(500_300.0, 400.0)),
                Value::Float(d),
            ]
        };
        assert_eq!(
            eval("st_dwithin", &args(500.0)),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_dwithin", &args(499.0)),
            Some(Ok(Value::Bool(false)))
        );
    }

    #[test]
    fn same_srid_predicate_evaluates() {
        let square = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        let args = vec![
            geom(Srid::new(3375).unwrap(), square),
            geom(Srid::new(3375).unwrap(), Geometry::point(5.0, 5.0)),
        ];
        assert_eq!(eval("st_contains", &args), Some(Ok(Value::Bool(true))));
    }

    /// EPSG:2263 is in US survey feet: 300 ft apart is about 91.44 m.
    #[test]
    fn feet_crs_dwithin_compares_metres() {
        let ft = Srid::new(2263).unwrap();
        let args = |d: f64| {
            vec![
                geom(ft, Geometry::point(1_000_000.0, 200_000.0)),
                geom(ft, Geometry::point(1_000_300.0, 200_000.0)),
                Value::Float(d),
            ]
        };
        assert_eq!(eval("st_dwithin", &args(92.0)), Some(Ok(Value::Bool(true))));
        assert_eq!(
            eval("st_dwithin", &args(91.0)),
            Some(Ok(Value::Bool(false)))
        );
    }

    /// A distance predicate needs the CRS kind, so an undefined SRID fails.
    #[test]
    fn dwithin_in_an_undefined_srid_fails() {
        let undefined = Srid::new(999).unwrap();
        let args = vec![
            geom(undefined, Geometry::point(0.0, 0.0)),
            geom(undefined, Geometry::point(1.0, 1.0)),
            Value::Float(10.0),
        ];
        assert!(matches!(
            eval("st_dwithin", &args),
            Some(Err(EvalError::Crs { .. }))
        ));
    }

    fn square(srid: Srid, side: f64) -> Value {
        geom(
            srid,
            Geometry::polygon(vec![vec![
                [0.0, 0.0],
                [side, 0.0],
                [side, side],
                [0.0, side],
                [0.0, 0.0],
            ]]),
        )
    }

    #[test]
    fn named_relations_and_relate() {
        let s = Srid::new(3375).unwrap();
        let shifted = geom(
            s,
            Geometry::polygon(vec![vec![
                [10.0, 0.0],
                [20.0, 0.0],
                [20.0, 10.0],
                [10.0, 10.0],
                [10.0, 0.0],
            ]]),
        );
        assert_eq!(
            eval("st_touches", &[square(s, 10.0), shifted.clone()]),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_covers", &[square(s, 20.0), square(s, 10.0)]),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_relate", &[square(s, 10.0), shifted.clone()]),
            Some(Ok(Value::String("FF2F11212".into())))
        );
        assert_eq!(
            eval(
                "st_relate",
                &[square(s, 10.0), shifted, Value::String("FF*F1****".into())]
            ),
            Some(Ok(Value::Bool(true)))
        );
    }

    #[test]
    fn empty_operands_follow_postgis() {
        let empty = geom(Srid::WGS84, Geometry::empty_point());
        let pt = geom(Srid::WGS84, Geometry::point(0.0, 0.0));
        assert_eq!(
            eval("st_intersects", &[empty.clone(), pt.clone()]),
            Some(Ok(Value::Bool(false)))
        );
        assert_eq!(
            eval("st_disjoint", &[empty.clone(), pt.clone()]),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_equals", &[empty.clone(), empty.clone()]),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_dwithin", &[empty, pt, Value::Float(1e9)]),
            Some(Ok(Value::Bool(false)))
        );
    }

    #[test]
    fn three_d_within_and_intersects() {
        let z = |x: f64, h: f64| {
            Value::Geometry(
                SridGeometry::with_ordinates(
                    utm48n(),
                    Geometry::point(x, 0.0),
                    Ordinates::new(Some(vec![h]), None),
                )
                .unwrap(),
            )
        };
        let args = |d: f64| vec![z(500_000.0, 0.0), z(500_003.0, 4.0), Value::Float(d)];
        assert_eq!(
            eval("st_3ddwithin", &args(5.0)),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_3ddwithin", &args(4.9)),
            Some(Ok(Value::Bool(false)))
        );
        assert_eq!(eval("st_dwithin", &args(4.9)), Some(Ok(Value::Bool(true))));
        assert_eq!(
            eval("st_3dintersects", &[z(500_000.0, 1.0), z(500_000.0, 1.0)]),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_3dintersects", &[z(500_000.0, 1.0), z(500_000.0, 2.0)]),
            Some(Ok(Value::Bool(false)))
        );
    }

    #[test]
    fn negative_dwithin_distance_fails() {
        let args = vec![
            geom(utm48n(), Geometry::point(0.0, 0.0)),
            geom(utm48n(), Geometry::point(0.0, 0.0)),
            Value::Float(-1.0),
        ];
        assert!(matches!(
            eval("st_dwithin", &args),
            Some(Err(EvalError::ArgumentValue { .. }))
        ));
    }

    #[test]
    fn validity_reason_text() {
        let bowtie = geom(
            Srid::WGS84,
            Geometry::polygon(vec![vec![
                [0.0, 0.0],
                [1.0, 1.0],
                [1.0, 0.0],
                [0.0, 1.0],
                [0.0, 0.0],
            ]]),
        );
        let Some(Ok(Value::String(reason))) = eval("st_isvalidreason", &[bowtie]) else {
            panic!();
        };
        assert!(reason.contains("crosses itself"), "{reason}");
    }
}
