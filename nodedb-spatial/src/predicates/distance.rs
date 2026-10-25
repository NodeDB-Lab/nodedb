// SPDX-License-Identifier: Apache-2.0

//! `ST_Distance` and `ST_DWithin` on WGS 84 longitude / latitude, geodesic
//! on the WGS 84 ellipsoid. Callers that know the CRS use
//! [`crate::metric::Metric`] for its own ellipsoid or unit.

use nodedb_types::geometry::Geometry;

use crate::metric::Metric;

/// Minimum geodesic distance in metres between two WGS 84 geometries.
/// Infinite when either is EMPTY.
pub fn st_distance(a: &Geometry, b: &Geometry) -> f64 {
    Metric::wgs84().distance(a, b)
}

/// Whether two WGS 84 geometries lie within `distance_meters` of each
/// other, geodesic.
pub fn st_dwithin(a: &Geometry, b: &Geometry, distance_meters: f64) -> bool {
    Metric::wgs84().dwithin(a, b, distance_meters)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]])
    }

    #[test]
    fn point_to_point_is_geodesic() {
        let d = st_distance(&Geometry::point(0.0, 0.0), &Geometry::point(0.0, 1.0));
        // pyproj Geod(ellps="WGS84").inv(0, 0, 0, 1).
        assert!((d - 110_574.388_557_798_78).abs() < 1e-6, "got {d}");
    }

    #[test]
    fn point_to_point_same() {
        let d = st_distance(&Geometry::point(5.0, 5.0), &Geometry::point(5.0, 5.0));
        assert!(d < 0.01);
    }

    #[test]
    fn point_inside_polygon_zero_distance() {
        let d = st_distance(&Geometry::point(0.5, 0.5), &square());
        assert!(d < 0.01);
    }

    #[test]
    fn point_on_edge_zero_distance() {
        let d = st_distance(&Geometry::point(0.5, 0.0), &square());
        assert!(d < 0.01);
    }

    #[test]
    fn point_to_polygon_outside() {
        let d = st_distance(&Geometry::point(2.0, 0.5), &square());
        // 1 degree longitude at equator ≈ 111 km.
        assert!(d > 100_000.0, "got {d}");
    }

    #[test]
    fn point_to_linestring() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]);
        // Point 1 degree north of an equatorial line: one meridian degree.
        let d = st_distance(&Geometry::point(5.0, 1.0), &line);
        assert!((d - 110_574.388_557_798_78).abs() < 1e-3, "got {d}");
    }

    #[test]
    fn linestring_to_linestring_crossing() {
        let a = Geometry::line_string(vec![[0.0, 0.0], [10.0, 10.0]]);
        let b = Geometry::line_string(vec![[0.0, 10.0], [10.0, 0.0]]);
        let d = st_distance(&a, &b);
        assert!(d < 0.01);
    }

    #[test]
    fn dwithin_points_close() {
        let a = Geometry::point(0.0, 0.0);
        let b = Geometry::point(0.001, 0.0);
        // ~111 meters apart.
        assert!(st_dwithin(&a, &b, 200.0));
        assert!(!st_dwithin(&a, &b, 50.0));
    }

    #[test]
    fn dwithin_point_polygon() {
        let pt = Geometry::point(2.0, 0.5);
        // Point is ~1 degree east of polygon.
        assert!(st_dwithin(&pt, &square(), 200_000.0)); // 200 km
        assert!(!st_dwithin(&pt, &square(), 50_000.0)); // 50 km
    }

    #[test]
    fn dwithin_bbox_prefilter_rejects() {
        // Points very far apart — bbox pre-filter should reject cheaply.
        let a = Geometry::point(0.0, 0.0);
        let b = Geometry::point(90.0, 45.0);
        assert!(!st_dwithin(&a, &b, 1000.0));
    }

    #[test]
    fn polygon_to_polygon_overlapping() {
        let other = Geometry::polygon(vec![vec![
            [0.5, 0.5],
            [1.5, 0.5],
            [1.5, 1.5],
            [0.5, 1.5],
            [0.5, 0.5],
        ]]);
        let d = st_distance(&square(), &other);
        assert!(d < 0.01);
    }

    #[test]
    fn polygon_to_polygon_separated() {
        let far = Geometry::polygon(vec![vec![
            [5.0, 5.0],
            [6.0, 5.0],
            [6.0, 6.0],
            [5.0, 6.0],
            [5.0, 5.0],
        ]]);
        let d = st_distance(&square(), &far);
        assert!(d > 100_000.0, "got {d}");
    }
}
