// SPDX-License-Identifier: Apache-2.0

//! Spatial reference system functions: read, set, and transform a
//! geometry's SRID, and shift its longitudes across the antimeridian.

use nodedb_crs::{CoordinateEpoch, CrsContext};
use nodedb_types::Value;

use super::helpers::{GeoEval, SridArg, finite_arg, geom_arg, srid_arg};
use crate::expr::EvalError;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_srid" => geom_arg(args, 0, canonical, ctx).map(|g| match g {
            Some(g) => Value::Integer(i64::from(g.srid.code())),
            None => Value::Null,
        }),
        "st_setsrid" => set_srid(canonical, args, ctx),
        "st_transform" => transform(canonical, args, ctx),
        "st_shiftlongitude" => geom_arg(args, 0, canonical, ctx).map(|g| match g {
            Some(g) => Value::Geometry(nodedb_spatial::ops::shift_longitude(&g)),
            None => Value::Null,
        }),
        _ => return None,
    };
    Some(result)
}

/// `ST_SetSRID(geom, srid)`: relabel the geometry's SRID. Coordinates are
/// kept as they are. The SRID must be defined.
fn set_srid(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let srid = match srid_arg(args, 1, function, ctx)? {
        SridArg::Srid(srid) => srid,
        SridArg::Absent | SridArg::Null => return Ok(Value::Null),
    };
    Ok(match geom_arg(args, 0, function, ctx)? {
        Some(mut g) => {
            g.srid = srid;
            Value::Geometry(g)
        }
        None => Value::Null,
    })
}

/// Position of the optional coordinate-epoch argument of `ST_Transform`.
const EPOCH_ARG: usize = 2;

/// `ST_Transform(geom, srid [, epoch])`: reproject every coordinate into
/// `srid`. Z is ellipsoidal height and goes through the datum shift. M
/// passes through.
///
/// `epoch` is the coordinate epoch as a decimal year in 1900..=2200. A
/// time-dependent datum shift takes its parameters at that epoch. Without
/// it, each shift runs at its own parameter reference epoch. A NULL epoch
/// gives NULL.
fn transform(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
) -> Result<Value, EvalError> {
    let target = match srid_arg(args, 1, function, ctx)? {
        SridArg::Srid(srid) => srid,
        SridArg::Absent | SridArg::Null => return Ok(Value::Null),
    };
    let epoch = match epoch_arg(args, function)? {
        EpochArg::Absent => None,
        EpochArg::Null => return Ok(Value::Null),
        EpochArg::Epoch(epoch) => Some(epoch),
    };
    let Some(g) = geom_arg(args, 0, function, ctx)? else {
        return Ok(Value::Null);
    };
    let out = match epoch {
        None => nodedb_spatial::transform_geometry(&g, target, ctx),
        Some(epoch) => nodedb_spatial::transform_geometry_at(&g, target, epoch, ctx),
    };
    out.map(Value::Geometry)
        .map_err(|e| EvalError::from_crs(function, e))
}

/// The optional coordinate-epoch argument of `ST_Transform`.
enum EpochArg {
    Absent,
    Null,
    Epoch(CoordinateEpoch),
}

/// Read the epoch argument. A non-number fails with
/// [`EvalError::ArgumentType`]. A non-finite or out-of-range year fails
/// with [`EvalError::ArgumentValue`].
fn epoch_arg(args: &[Value], function: &'static str) -> Result<EpochArg, EvalError> {
    if args.get(EPOCH_ARG).is_none() {
        return Ok(EpochArg::Absent);
    }
    let Some(year) = finite_arg(args, EPOCH_ARG, function)? else {
        return Ok(EpochArg::Null);
    };
    CoordinateEpoch::new(year)
        .map(EpochArg::Epoch)
        .map_err(|e| EvalError::ArgumentValue {
            function,
            position: EPOCH_ARG + 1,
            reason: format!("is out of range: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::{CrsError, UnknownSrid};
    use nodedb_types::geometry::{Geometry, Srid, SridGeometry};

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    fn point(srid: Srid, x: f64, y: f64) -> Value {
        Value::Geometry(SridGeometry::new(srid, Geometry::point(x, y)))
    }

    fn run(canonical: &'static str, args: &[Value]) -> GeoEval {
        eval(canonical, args, &CrsContext::builtin_only())
    }

    #[test]
    fn srid_reads_the_geometry_srid() {
        assert_eq!(
            run("st_srid", &[point(Srid::WGS84, 0.0, 0.0)]),
            Some(Ok(Value::Integer(4326)))
        );
        assert_eq!(
            run("st_srid", &[point(srid(3376), 0.0, 0.0)]),
            Some(Ok(Value::Integer(3376)))
        );
        assert!(matches!(
            run("st_srid", &[Value::String("not a geometry".into())]),
            Some(Err(EvalError::Geometry { .. }))
        ));
    }

    #[test]
    fn setsrid_relabels_without_moving() {
        let got = run(
            "st_setsrid",
            &[point(Srid::WGS84, 1.0, 2.0), Value::Integer(4742)],
        );
        assert_eq!(got, Some(Ok(point(srid(4742), 1.0, 2.0))));
    }

    /// PROJ 9.8.1 reference: EPSG:4326 (99.828, 3.975) to EPSG:32647.
    const UTM47_X: f64 = 591_918.695_587_182_4;
    const UTM47_Y: f64 = 439_410.048_656_111_64;

    #[test]
    fn transform_matches_proj_for_utm47n() {
        let Some(Ok(Value::Geometry(g))) = run(
            "st_transform",
            &[point(Srid::WGS84, 99.828, 3.975), Value::Integer(32647)],
        ) else {
            panic!("expected a geometry");
        };
        assert_eq!(g.srid, srid(32647));
        let Geometry::Point { coordinates } = *g.geometry() else {
            panic!("expected a point");
        };
        assert!((coordinates[0] - UTM47_X).abs() < 1e-3, "{coordinates:?}");
        assert!((coordinates[1] - UTM47_Y).abs() < 1e-3, "{coordinates:?}");
    }

    fn coords(v: Value) -> [f64; 2] {
        let Value::Geometry(g) = v else {
            panic!("expected a geometry");
        };
        let Geometry::Point { coordinates } = *g.geometry() else {
            panic!("expected a point");
        };
        coordinates
    }

    /// An epoch changes nothing on a static datum shift: EPSG:4742 to
    /// EPSG:32647 gives the same point with and without it.
    #[test]
    fn transform_with_epoch_matches_without_for_static_shifts() {
        let input = point(srid(4742), 99.828, 3.975);
        let plain = run("st_transform", &[input.clone(), Value::Integer(32647)]);
        let at = run(
            "st_transform",
            &[input, Value::Integer(32647), Value::Float(2025.5)],
        );
        let (Some(Ok(plain)), Some(Ok(at))) = (plain, at) else {
            panic!("expected two geometries");
        };
        assert_eq!(coords(plain), coords(at));
    }

    #[test]
    fn transform_epoch_out_of_range_is_an_argument_fault() {
        for epoch in [
            Value::Float(1899.5),
            Value::Integer(2201),
            Value::Float(f64::NAN),
        ] {
            let got = run(
                "st_transform",
                &[
                    point(Srid::WGS84, 99.828, 3.975),
                    Value::Integer(32647),
                    epoch,
                ],
            );
            assert!(
                matches!(
                    got,
                    Some(Err(EvalError::ArgumentValue {
                        function: "st_transform",
                        position: 3,
                        ..
                    }))
                ),
                "{got:?}"
            );
        }
        let got = run(
            "st_transform",
            &[
                point(Srid::WGS84, 99.828, 3.975),
                Value::Integer(32647),
                Value::String("soon".into()),
            ],
        );
        assert!(matches!(
            got,
            Some(Err(EvalError::ArgumentType { position: 3, .. }))
        ));
    }

    #[test]
    fn transform_null_epoch_yields_null() {
        let got = run(
            "st_transform",
            &[
                point(Srid::WGS84, 99.828, 3.975),
                Value::Integer(32647),
                Value::Null,
            ],
        );
        assert_eq!(got, Some(Ok(Value::Null)));
    }

    #[test]
    fn transform_keeps_z_and_m() {
        use nodedb_types::geometry::Ordinates;
        let g = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::point(99.828, 3.975),
            Ordinates::new(Some(vec![50.0]), Some(vec![7.0])),
        )
        .unwrap();
        let Some(Ok(Value::Geometry(out))) =
            run("st_transform", &[Value::Geometry(g), Value::Integer(32647)])
        else {
            panic!("expected a geometry");
        };
        assert_eq!(out.ordinates().z(), Some(&[50.0][..]));
        assert_eq!(out.ordinates().m(), Some(&[7.0][..]));
    }

    #[test]
    fn shift_longitude_moves_negative_longitudes() {
        let Some(Ok(Value::Geometry(out))) =
            run("st_shiftlongitude", &[point(Srid::WGS84, -170.0, 1.0)])
        else {
            panic!();
        };
        assert_eq!(out.geometry(), &Geometry::point(190.0, 1.0));
    }

    #[test]
    fn transform_to_undefined_srid_fails() {
        let got = run(
            "st_transform",
            &[point(Srid::WGS84, 101.7, 3.1), Value::Integer(999)],
        );
        let Some(Err(err)) = got else {
            panic!("expected an error");
        };
        assert_eq!(
            err,
            EvalError::Crs {
                function: "st_transform",
                source: UnknownSrid::Undefined { code: 999 }
            }
        );
        assert!(err.to_string().contains("999"), "{err}");
    }

    #[test]
    fn transform_out_of_domain_is_a_transform_fault() {
        let got = run(
            "st_transform",
            &[point(Srid::WGS84, 179.0, 3.0), Value::Integer(32648)],
        );
        assert!(matches!(
            got,
            Some(Err(EvalError::Transform {
                function: "st_transform",
                source: CrsError::OutOfDomain { .. }
            }))
        ));
    }

    #[test]
    fn null_srid_yields_null() {
        assert_eq!(
            run("st_setsrid", &[point(Srid::WGS84, 1.0, 2.0), Value::Null]),
            Some(Ok(Value::Null))
        );
    }
}
