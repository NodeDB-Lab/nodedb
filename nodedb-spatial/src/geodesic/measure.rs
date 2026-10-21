// SPDX-License-Identifier: Apache-2.0

//! Geodesic length, perimeter, area and distance on a CRS's own ellipsoid.
//!
//! Coordinates are x = longitude, y = latitude, in degrees. Every edge is
//! a geodesic. Results are in metres (the units of the semi-major axis)
//! and square metres.

use nodedb_crs::Ellipsoid;
use nodedb_crs::method::geodesic::Geodesic;
use nodedb_types::geometry::Geometry;

use super::segment::{point_distance, point_segment_distance};
use crate::parts::Parts;
use crate::predicates::edge::{ring_edges, segment_to_segment_dist_sq};

/// Squared degree distance under which two edges count as touching.
const TOUCH_DEG_SQ: f64 = 1e-20;

/// Edge pairs whose approximate distance is within this factor of the
/// smallest approximate distance go through the exact geodesic refine.
const REFINE_FACTOR: f64 = 2.0;

/// Edge pairs closer than this many metres (approximate) always refine.
const REFINE_FLOOR_M: f64 = 1_000.0;

/// Upper bound on metres per degree, used to scale the planar degree
/// distance that ranks edge pairs.
const METRES_PER_DEGREE_MAX: f64 = 111_700.0;

/// Floor on cos(latitude) so ranking near a pole stays finite.
const MIN_COS_LAT: f64 = 0.01;

/// Geodesic measures on one ellipsoid.
#[derive(Debug, Clone, PartialEq)]
pub struct GeodesicMeasure {
    geodesic: Geodesic,
    ellipsoid: Ellipsoid,
}

impl GeodesicMeasure {
    pub fn new(ellipsoid: Ellipsoid) -> Self {
        Self {
            geodesic: Geodesic::new(ellipsoid.a, ellipsoid.f()),
            ellipsoid,
        }
    }

    pub fn ellipsoid(&self) -> &Ellipsoid {
        &self.ellipsoid
    }

    pub fn geodesic(&self) -> &Geodesic {
        &self.geodesic
    }

    /// Distance between two points.
    pub fn point_distance(&self, a: [f64; 2], b: [f64; 2]) -> f64 {
        point_distance(&self.geodesic, a, b)
    }

    /// Total length of every linear component. Areal geometries and points
    /// have no length, matching PostGIS.
    pub fn length(&self, geom: &Geometry) -> f64 {
        match geom {
            Geometry::LineString { coordinates } => self.path_length(coordinates),
            Geometry::MultiLineString { coordinates } => {
                coordinates.iter().map(|l| self.path_length(l)).sum()
            }
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().map(|g| self.length(g)).sum()
            }
            _ => 0.0,
        }
    }

    /// Total length of every areal component's rings, holes included. An
    /// unclosed ring is closed implicitly.
    pub fn perimeter(&self, geom: &Geometry) -> f64 {
        let ring = |r: &Vec<[f64; 2]>| -> f64 {
            ring_edges(r)
                .iter()
                .map(|(a, b)| self.point_distance(*a, *b))
                .sum()
        };
        match geom {
            Geometry::Polygon { coordinates } => coordinates.iter().map(ring).sum(),
            Geometry::MultiPolygon { coordinates } => coordinates.iter().flatten().map(ring).sum(),
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().map(|g| self.perimeter(g)).sum()
            }
            _ => 0.0,
        }
    }

    /// Area of every areal component: exterior ring less its holes, never
    /// negative. Points and lines have zero area.
    pub fn area(&self, geom: &Geometry) -> f64 {
        match geom {
            Geometry::Polygon { coordinates } => self.polygon_area(coordinates),
            Geometry::MultiPolygon { coordinates } => {
                coordinates.iter().map(|p| self.polygon_area(p)).sum()
            }
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().map(|g| self.area(g)).sum()
            }
            _ => 0.0,
        }
    }

    /// Unsigned area enclosed by one ring.
    pub fn ring_area(&self, ring: &[[f64; 2]]) -> f64 {
        if ring.len() < 3 {
            return 0.0;
        }
        let vertices: Vec<(f64, f64)> = ring.iter().map(|c| (c[1], c[0])).collect();
        self.geodesic.polygon_measure(&vertices).area.abs()
    }

    fn polygon_area(&self, rings: &[Vec<[f64; 2]>]) -> f64 {
        let mut iter = rings.iter();
        let Some(exterior) = iter.next() else {
            return 0.0;
        };
        let holes: f64 = iter.map(|r| self.ring_area(r)).sum();
        (self.ring_area(exterior) - holes).max(0.0)
    }

    fn path_length(&self, path: &[[f64; 2]]) -> f64 {
        path.windows(2)
            .map(|w| self.point_distance(w[0], w[1]))
            .sum()
    }

    /// Minimum geodesic distance between two geometries. Zero when one
    /// covers a vertex of the other or their edges cross. Infinite when
    /// either is EMPTY.
    ///
    /// Edge pairs are ranked by a planar degree distance. The pairs near
    /// the best rank go through the exact search: the distance from each
    /// endpoint to the other geodesic edge.
    pub fn distance(&self, a: &Geometry, b: &Geometry) -> f64 {
        let pa = Parts::of(a);
        let pb = Parts::of(b);
        if pa.segments.is_empty() || pb.segments.is_empty() {
            return f64::INFINITY;
        }
        if pa.covers_any_vertex_of(&pb) || pb.covers_any_vertex_of(&pa) {
            return 0.0;
        }
        let mut ranked = Vec::with_capacity(pa.segments.len() * pb.segments.len());
        for &(a1, a2) in &pa.segments {
            for &(b1, b2) in &pb.segments {
                if segment_to_segment_dist_sq(a1, a2, b1, b2) < TOUCH_DEG_SQ {
                    return 0.0;
                }
                ranked.push((local_distance(a1, a2, b1, b2), (a1, a2), (b1, b2)));
            }
        }
        let best = ranked
            .iter()
            .map(|(approx, _, _)| *approx)
            .fold(f64::INFINITY, f64::min);
        let limit = best * REFINE_FACTOR + REFINE_FLOOR_M;
        ranked
            .iter()
            .filter(|(approx, _, _)| *approx <= limit)
            .map(|&(_, (a1, a2), (b1, b2))| self.segment_segment(a1, a2, b1, b2))
            .fold(f64::INFINITY, f64::min)
    }

    /// Distance between two non-crossing geodesic edges: the least of each
    /// endpoint's distance to the other edge.
    fn segment_segment(&self, a1: [f64; 2], a2: [f64; 2], b1: [f64; 2], b2: [f64; 2]) -> f64 {
        let g = &self.geodesic;
        point_segment_distance(g, a1, b1, b2)
            .min(point_segment_distance(g, a2, b1, b2))
            .min(point_segment_distance(g, b1, a1, a2))
            .min(point_segment_distance(g, b2, a1, a2))
    }
}

/// Approximate metres between two edges: the planar distance after
/// longitudes shrink by cos of the edges' mean latitude.
fn local_distance(a1: [f64; 2], a2: [f64; 2], b1: [f64; 2], b2: [f64; 2]) -> f64 {
    let mean_lat = (a1[1] + a2[1] + b1[1] + b2[1]) / 4.0;
    let k = mean_lat.to_radians().cos().max(MIN_COS_LAT);
    let scale = |c: [f64; 2]| [c[0] * k, c[1]];
    segment_to_segment_dist_sq(scale(a1), scale(a2), scale(b1), scale(b2)).sqrt()
        * METRES_PER_DEGREE_MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wgs84() -> GeodesicMeasure {
        GeodesicMeasure::new(Ellipsoid::WGS84)
    }

    fn square() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]])
    }

    /// pyproj 3.8.0 (PROJ 9.8.1) `Geod(ellps="WGS84")` reference values.
    #[test]
    fn values_match_pyproj() {
        let m = wgs84();
        assert!((m.area(&square()) - 12_308_778_361.469_452).abs() < 1e-3);
        assert!((m.perimeter(&square()) - 443_770.917_248_302).abs() < 1e-6);
        let meridian = Geometry::line_string(vec![[0.0, 0.0], [0.0, 1.0]]);
        assert!((m.length(&meridian) - 110_574.388_557_798_78).abs() < 1e-6);
        let d = m.distance(
            &Geometry::point(-74.006, 40.7128),
            &Geometry::point(-0.1278, 51.5074),
        );
        assert!((d - 5_585_233.578_931_3).abs() < 1e-3, "{d}");
    }

    #[test]
    fn hole_is_subtracted() {
        let m = wgs84();
        let with_hole = Geometry::polygon(vec![
            vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]],
            vec![
                [0.25, 0.25],
                [0.75, 0.25],
                [0.75, 0.75],
                [0.25, 0.75],
                [0.25, 0.25],
            ],
        ]);
        assert!(m.area(&with_hole) < m.area(&square()));
    }

    #[test]
    fn point_inside_and_crossing_lines_are_zero() {
        let m = wgs84();
        assert_eq!(m.distance(&square(), &Geometry::point(0.5, 0.5)), 0.0);
        let line = Geometry::line_string(vec![[-1.0, 0.5], [2.0, 0.5]]);
        assert_eq!(m.distance(&square(), &line), 0.0);
    }

    #[test]
    fn point_to_polygon_edge_is_geodesic() {
        let m = wgs84();
        // Due east of the square's east edge, on the equator band.
        let d = m.distance(&square(), &Geometry::point(1.1, 0.5));
        let direct = m.point_distance([1.1, 0.5], [1.0, 0.5]);
        assert!((d - direct).abs() < 0.5, "{d} {direct}");
    }

    #[test]
    fn empty_is_infinitely_far() {
        let m = wgs84();
        assert!(
            m.distance(&Geometry::empty_point(), &square())
                .is_infinite()
        );
        assert_eq!(m.length(&Geometry::empty_point()), 0.0);
    }
}
