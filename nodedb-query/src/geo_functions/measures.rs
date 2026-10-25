// SPDX-License-Identifier: Apache-2.0

//! Distance, length, perimeter, area, and bearing.
//!
//! Every measure is reported in metres (or square metres). A geometry in a
//! geographic SRID is measured geodesically on that CRS's own ellipsoid. A
//! projected geometry is measured in its planar grid, then scaled from the
//! CRS's linear unit to metres. `geo_distance` and `geo_bearing` are the
//! spherical haversine formulas on bare numbers.

use nodedb_crs::CrsContext;
use nodedb_types::Value;

use super::helpers::{GeoEval, metric_of, num_arg, pair, unary};
use crate::expr::EvalError;
use crate::value_ops::to_value_number;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result = match canonical {
        "st_distance" => measure_pair(canonical, args, ctx, |m, a, b| {
            m.distance(a.geometry(), b.geometry())
        }),
        "st_3ddistance" => measure_pair(canonical, args, ctx, |m, a, b| m.distance_3d(a, b)),
        "st_length" => measure(canonical, args, ctx, |m, g| m.length(g.geometry())),
        "st_3dlength" => measure(canonical, args, ctx, |m, g| m.length_3d(g)),
        "st_perimeter" => measure(canonical, args, ctx, |m, g| m.perimeter(g.geometry())),
        "st_area" => measure(canonical, args, ctx, |m, g| m.area(g.geometry())),
        "geo_distance" => Ok(haversine(args, nodedb_types::geometry::haversine_distance)),
        "geo_bearing" => Ok(haversine(args, nodedb_types::geometry::haversine_bearing)),
        _ => return None,
    };
    Some(result)
}

type Geom = nodedb_types::geometry::SridGeometry;

/// A measure between two operands in one SRID. EMPTY gives NULL.
fn measure_pair(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: fn(&nodedb_spatial::Metric, &Geom, &Geom) -> f64,
) -> Result<Value, EvalError> {
    pair(function, args, ctx, |a, b| {
        let metric = metric_of(function, a.srid, ctx)?;
        if a.is_empty() || b.is_empty() {
            return Ok(Value::Null);
        }
        Ok(to_value_number(f(&metric, a, b)))
    })
}

/// A measure of one operand in its own SRID.
fn measure(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: fn(&nodedb_spatial::Metric, &Geom) -> f64,
) -> Result<Value, EvalError> {
    unary(function, args, ctx, |g| {
        let metric = metric_of(function, g.srid, ctx)?;
        Ok(to_value_number(f(&metric, g)))
    })
}

fn haversine(args: &[Value], f: fn(f64, f64, f64, f64) -> f64) -> Value {
    let (Some(lng1), Some(lat1), Some(lng2), Some(lat2)) = (
        num_arg(args, 0),
        num_arg(args, 1),
        num_arg(args, 2),
        num_arg(args, 3),
    ) else {
        return Value::Null;
    };
    to_value_number(f(lng1, lat1, lng2, lat2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value_ops::value_to_f64;
    use nodedb_types::geometry::{Geometry, Ordinates, Srid, SridGeometry};

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

    fn utm48(v: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(Srid::new(32648).unwrap(), v))
    }

    fn ft2263(v: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(Srid::new(2263).unwrap(), v))
    }

    /// Metres per US survey foot, the unit of EPSG:2263.
    const US_FOOT_M: f64 = 1200.0 / 3937.0;

    /// EPSG:2263 coordinates are US survey feet: distance, length and area
    /// come back in metres and square metres.
    #[test]
    fn feet_crs_measures_report_metres() {
        let a = ft2263(Geometry::point(1_000_000.0, 200_000.0));
        let b = ft2263(Geometry::point(1_001_000.0, 200_000.0));
        let d = eval("st_distance", &[a, b]).and_then(|v| value_to_f64(&v, false));
        let d = d.expect("a distance");
        assert!((d - 1000.0 * US_FOOT_M).abs() < 1e-6, "{d}");

        let line = ft2263(Geometry::line_string(vec![
            [1_000_000.0, 200_000.0],
            [1_000_000.0, 200_500.0],
        ]));
        let len = eval("st_length", &[line]).and_then(|v| value_to_f64(&v, false));
        let len = len.expect("a length");
        assert!((len - 500.0 * US_FOOT_M).abs() < 1e-6, "{len}");

        let square = ft2263(Geometry::polygon(vec![vec![
            [1_000_000.0, 200_000.0],
            [1_000_100.0, 200_000.0],
            [1_000_100.0, 200_100.0],
            [1_000_000.0, 200_100.0],
            [1_000_000.0, 200_000.0],
        ]]));
        let area = eval("st_area", &[square]).and_then(|v| value_to_f64(&v, false));
        let area = area.expect("an area");
        let expected = 10_000.0 * US_FOOT_M * US_FOOT_M;
        assert!((area - expected).abs() < 1e-6, "{area}");
    }

    #[test]
    fn projected_area_of_hundred_metre_square() {
        let square = utm48(Geometry::polygon(vec![vec![
            [500_000.0, 100_000.0],
            [500_100.0, 100_000.0],
            [500_100.0, 100_100.0],
            [500_000.0, 100_100.0],
            [500_000.0, 100_000.0],
        ]]));
        let area = eval("st_area", &[square]).and_then(|v| value_to_f64(&v, true));
        assert_eq!(area, Some(10_000.0));
    }

    #[test]
    fn projected_distance_and_length_are_planar() {
        let a = utm48(Geometry::point(500_000.0, 0.0));
        let b = utm48(Geometry::point(500_300.0, 400.0));
        let d = eval("st_distance", &[a, b]).and_then(|v| value_to_f64(&v, true));
        assert_eq!(d, Some(500.0));
        let line = utm48(Geometry::line_string(vec![[0.0, 0.0], [300.0, 400.0]]));
        let l = eval("st_length", &[line]).and_then(|v| value_to_f64(&v, true));
        assert_eq!(l, Some(500.0));
    }

    #[test]
    fn mixed_srid_distance_fails() {
        let args = [
            Value::Geometry(SridGeometry::wgs84(Geometry::point(0.0, 0.0))),
            utm48(Geometry::point(0.0, 0.0)),
        ];
        assert!(matches!(
            super::eval("st_distance", &args, &ctx()),
            Some(Err(crate::expr::EvalError::MixedSrid { .. }))
        ));
    }

    /// pyproj 3.8.0 `Geod(ellps="WGS84").inv(0, 0, 0, 1)`: 110574.38855779878 m.
    #[test]
    fn length_measures_linestrings_only() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [0.0, 1.0]]);
        let measured = eval("st_length", &geom(line)).and_then(|v| value_to_f64(&v, true));
        let Some(m) = measured else {
            panic!("expected a numeric length");
        };
        assert!((m - 110_574.388_557_798_78).abs() < 1e-6, "got {m}");

        let point = geom(Geometry::point(1.0, 2.0));
        assert_eq!(eval("st_length", &point), Some(to_value_number(0.0)));
    }

    #[test]
    fn perimeter_closes_an_unclosed_ring() {
        let closed = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
            [1.0, 0.0],
            [0.0, 0.0],
        ]]);
        let unclosed =
            Geometry::polygon(vec![vec![[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0]]]);
        assert_eq!(
            eval("st_perimeter", &geom(closed)),
            eval("st_perimeter", &geom(unclosed)),
            "an unclosed ring must measure the same boundary as its closed form"
        );
    }

    #[test]
    fn area_of_a_line_is_zero() {
        let line = geom(Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]));
        assert_eq!(eval("st_area", &line), Some(to_value_number(0.0)));
    }

    #[test]
    fn unreadable_geometry_fails_the_statement() {
        let bad = vec![Value::String("not a geometry".into())];
        for f in ["st_area", "st_length", "st_distance"] {
            assert!(matches!(
                super::eval(f, &bad, &ctx()),
                Some(Err(crate::expr::EvalError::Geometry { .. }))
            ));
        }
        assert_eq!(eval("st_area", &[Value::Null]), Some(Value::Null));
    }

    /// pyproj 3.8.0 `Geod(ellps="WGS84").polygon_area_perimeter` on the
    /// 1° square at the equator: 12308778361.469452 m², 443770.917248302 m.
    #[test]
    fn geographic_area_and_perimeter_are_geodesic() {
        let square = geom(Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]));
        let area = eval("st_area", &square).and_then(|v| value_to_f64(&v, true));
        assert!((area.unwrap() - 12_308_778_361.469_452).abs() < 1e-3);
        let per = eval("st_perimeter", &square).and_then(|v| value_to_f64(&v, true));
        assert!((per.unwrap() - 443_770.917_248_302).abs() < 1e-6);
    }

    #[test]
    fn distance_to_empty_is_null() {
        let args = [
            Value::Geometry(SridGeometry::wgs84(Geometry::empty_point())),
            Value::Geometry(SridGeometry::wgs84(Geometry::point(0.0, 0.0))),
        ];
        assert_eq!(eval("st_distance", &args), Some(Value::Null));
    }

    #[test]
    fn three_d_measures() {
        let z = |x: f64, y: f64, h: f64| {
            Value::Geometry(
                SridGeometry::with_ordinates(
                    Srid::new(32648).unwrap(),
                    Geometry::point(x, y),
                    Ordinates::new(Some(vec![h]), None),
                )
                .unwrap(),
            )
        };
        let d = eval("st_3ddistance", &[z(0.0, 0.0, 0.0), z(3.0, 0.0, 4.0)])
            .and_then(|v| value_to_f64(&v, true));
        assert_eq!(d, Some(5.0));
        let line = Value::Geometry(
            SridGeometry::with_ordinates(
                Srid::new(32648).unwrap(),
                Geometry::line_string(vec![[0.0, 0.0], [3.0, 0.0]]),
                Ordinates::new(Some(vec![0.0, 4.0]), None),
            )
            .unwrap(),
        );
        let l = eval("st_3dlength", &[line]).and_then(|v| value_to_f64(&v, true));
        assert_eq!(l, Some(5.0));
    }

    #[test]
    fn unknown_name_falls_through() {
        assert_eq!(eval("st_contains", &[]), None);
    }

    /// A geographic SRID other than WGS 84 is measured geodesically.
    #[test]
    fn geographic_gdm2000_length_is_geodesic() {
        let line = Value::Geometry(SridGeometry::new(
            Srid::new(4742).unwrap(),
            Geometry::line_string(vec![[0.0, 0.0], [0.0, 1.0]]),
        ));
        let Some(m) = eval("st_length", &[line]).and_then(|v| value_to_f64(&v, true)) else {
            panic!("expected a numeric length");
        };
        // GRS80: pyproj Geod(ellps="GRS80").inv(0, 0, 0, 1) = 110574.38855415255.
        assert!((m - 110_574.388_554_152_55).abs() < 1e-6, "got {m}");
    }
}
