// SPDX-License-Identifier: Apache-2.0

//! Linear referencing: `ST_LineInterpolatePoint`, `ST_LocateAlong`,
//! `ST_LocateBetween`, `ST_InterpolatePoint`.

use nodedb_crs::CrsContext;
use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::{
    GeoEval, finite_arg, geom_arg, geom_pair, geometry_fault, metric_of, same_srid,
};
use crate::expr::EvalError;
use crate::value_ops::to_value_number;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_lineinterpolatepoint" => line_interpolate_point(canonical, args, ctx),
        "st_locatealong" => locate_along(canonical, args, ctx),
        "st_locatebetween" => locate_between(canonical, args, ctx),
        "st_interpolatepoint" => interpolate_point(canonical, args, ctx),
        _ => return None,
    };
    Some(result)
}

/// `ST_LineInterpolatePoint(line, fraction)`: geodesic along a line in a
/// geographic CRS, straight in a projected one. Z and M interpolate.
fn line_interpolate_point(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let (Some(g), Some(fraction)) = (
        geom_arg(args, 0, function, ctx)?,
        finite_arg(args, 1, function)?,
    ) else {
        return Ok(Value::Null);
    };
    let metric = metric_of(function, g.srid, ctx)?;
    nodedb_spatial::ops::line_interpolate_point(&g, fraction, &metric)
        .map(Value::Geometry)
        .map_err(|e| geometry_fault(function, e))
}

/// `ST_LocateAlong(geom, m [, offset])`.
fn locate_along(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let (Some(g), Some(m)) = (
        geom_arg(args, 0, function, ctx)?,
        finite_arg(args, 1, function)?,
    ) else {
        return Ok(Value::Null);
    };
    let offset = finite_arg(args, 2, function)?.unwrap_or(0.0);
    nodedb_spatial::ops::locate_along(&g, m, offset)
        .map(Value::Geometry)
        .map_err(|e| geometry_fault(function, e))
}

/// `ST_LocateBetween(geom, m_start, m_end [, offset])`.
fn locate_between(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let (Some(g), Some(start), Some(end)) = (
        geom_arg(args, 0, function, ctx)?,
        finite_arg(args, 1, function)?,
        finite_arg(args, 2, function)?,
    ) else {
        return Ok(Value::Null);
    };
    let offset = finite_arg(args, 3, function)?.unwrap_or(0.0);
    nodedb_spatial::ops::locate_between(&g, start, end, offset)
        .map(Value::Geometry)
        .map_err(|e| geometry_fault(function, e))
}

/// `ST_InterpolatePoint(line, point)`: the M of `line` nearest `point`.
fn interpolate_point(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let Some((line, point)) = geom_pair(args, function, ctx)? else {
        return Ok(Value::Null);
    };
    same_srid(function, &line, &point)?;
    let Geometry::Point { coordinates } = point.geometry() else {
        return Err(EvalError::ArgumentValue {
            function,
            position: 2,
            reason: format!("must be a point, got {}", point.geometry().geometry_type()),
        });
    };
    if point.is_empty() {
        return Ok(Value::Null);
    }
    Ok(
        match nodedb_spatial::ops::interpolate_point(&line, *coordinates)
            .map_err(|e| geometry_fault(function, e))?
        {
            Some(m) => to_value_number(m),
            None => Value::Null,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Ordinates, Srid, SridGeometry};

    fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
        super::eval(canonical, args, &CrsContext::builtin_only())
    }

    fn line_m() -> Value {
        Value::Geometry(
            SridGeometry::with_ordinates(
                Srid::new(32648).unwrap(),
                Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]),
                Ordinates::new(None, Some(vec![0.0, 100.0])),
            )
            .unwrap(),
        )
    }

    #[test]
    fn interpolate_and_locate() {
        let Some(Ok(Value::Geometry(p))) =
            eval("st_lineinterpolatepoint", &[line_m(), Value::Float(0.5)])
        else {
            panic!();
        };
        assert_eq!(p.geometry(), &Geometry::point(5.0, 0.0));
        assert_eq!(p.ordinates().m(), Some(&[50.0][..]));
        let Some(Ok(Value::Geometry(at))) = eval("st_locatealong", &[line_m(), Value::Float(30.0)])
        else {
            panic!();
        };
        assert_eq!(
            at.geometry(),
            &Geometry::MultiPoint {
                coordinates: vec![[3.0, 0.0]]
            }
        );
        let Some(Ok(Value::Geometry(between))) = eval(
            "st_locatebetween",
            &[line_m(), Value::Float(10.0), Value::Float(20.0)],
        ) else {
            panic!();
        };
        assert_eq!(between.geometry().coord_count(), 2);
        let point = Value::Geometry(SridGeometry::new(
            Srid::new(32648).unwrap(),
            Geometry::point(7.0, 3.0),
        ));
        assert_eq!(
            eval("st_interpolatepoint", &[line_m(), point]),
            Some(Ok(to_value_number(70.0)))
        );
    }

    #[test]
    fn measures_without_m_fail() {
        let xy = Value::Geometry(SridGeometry::new(
            Srid::new(32648).unwrap(),
            Geometry::line_string(vec![[0.0, 0.0], [1.0, 0.0]]),
        ));
        assert!(matches!(
            eval("st_locatealong", &[xy, Value::Float(1.0)]),
            Some(Err(EvalError::Geometry { .. }))
        ));
    }
}
