//! GeoJSON-compatible geometry types.
//!
//! Supports Point, LineString, Polygon, MultiPoint, MultiLineString,
//! MultiPolygon, and GeometryCollection. Stored as GeoJSON for JSON
//! compatibility. Includes distance (Haversine), area, bearing, and
//! centroid calculations.

use serde::{Deserialize, Serialize};

/// A 2D coordinate (longitude, latitude) following GeoJSON convention.
/// Note: GeoJSON uses [lng, lat] order, NOT [lat, lng].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Coord {
    pub lng: f64,
    pub lat: f64,
}

impl Coord {
    pub fn new(lng: f64, lat: f64) -> Self {
        Self { lng, lat }
    }
}

/// GeoJSON-compatible geometry types.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "type")]
pub enum Geometry {
    Point {
        coordinates: [f64; 2],
    },
    LineString {
        coordinates: Vec<[f64; 2]>,
    },
    Polygon {
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    MultiPoint {
        coordinates: Vec<[f64; 2]>,
    },
    MultiLineString {
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    MultiPolygon {
        coordinates: Vec<Vec<Vec<[f64; 2]>>>,
    },
    GeometryCollection {
        geometries: Vec<Geometry>,
    },
}

impl Geometry {
    /// Create a Point from (longitude, latitude).
    pub fn point(lng: f64, lat: f64) -> Self {
        Geometry::Point {
            coordinates: [lng, lat],
        }
    }

    /// Create a LineString from a series of [lng, lat] pairs.
    pub fn line_string(coords: Vec<[f64; 2]>) -> Self {
        Geometry::LineString {
            coordinates: coords,
        }
    }

    /// Create a Polygon from exterior ring (and optional holes).
    ///
    /// The first ring is the exterior, subsequent rings are holes.
    /// Each ring must be a closed loop (first point == last point).
    pub fn polygon(rings: Vec<Vec<[f64; 2]>>) -> Self {
        Geometry::Polygon { coordinates: rings }
    }

    /// Get the type name of this geometry.
    pub fn geometry_type(&self) -> &'static str {
        match self {
            Geometry::Point { .. } => "Point",
            Geometry::LineString { .. } => "LineString",
            Geometry::Polygon { .. } => "Polygon",
            Geometry::MultiPoint { .. } => "MultiPoint",
            Geometry::MultiLineString { .. } => "MultiLineString",
            Geometry::MultiPolygon { .. } => "MultiPolygon",
            Geometry::GeometryCollection { .. } => "GeometryCollection",
        }
    }

    /// Compute the centroid of the geometry.
    pub fn centroid(&self) -> Option<[f64; 2]> {
        match self {
            Geometry::Point { coordinates } => Some(*coordinates),
            Geometry::LineString { coordinates } => {
                if coordinates.is_empty() {
                    return None;
                }
                let n = coordinates.len() as f64;
                let sum_lng: f64 = coordinates.iter().map(|c| c[0]).sum();
                let sum_lat: f64 = coordinates.iter().map(|c| c[1]).sum();
                Some([sum_lng / n, sum_lat / n])
            }
            Geometry::Polygon { coordinates } => {
                // Centroid of exterior ring.
                coordinates.first().and_then(|ring| {
                    if ring.is_empty() {
                        return None;
                    }
                    let n = ring.len() as f64;
                    let sum_lng: f64 = ring.iter().map(|c| c[0]).sum();
                    let sum_lat: f64 = ring.iter().map(|c| c[1]).sum();
                    Some([sum_lng / n, sum_lat / n])
                })
            }
            Geometry::MultiPoint { coordinates } => {
                if coordinates.is_empty() {
                    return None;
                }
                let n = coordinates.len() as f64;
                let sum_lng: f64 = coordinates.iter().map(|c| c[0]).sum();
                let sum_lat: f64 = coordinates.iter().map(|c| c[1]).sum();
                Some([sum_lng / n, sum_lat / n])
            }
            _ => None,
        }
    }
}

// ── Geo math functions ──

const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// Haversine distance between two points in meters.
///
/// Input: (lng1, lat1) and (lng2, lat2) in degrees.
pub fn haversine_distance(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

/// Haversine bearing from point A to point B in degrees (0-360).
pub fn haversine_bearing(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let y = dlng.sin() * lat2_r.cos();
    let x = lat1_r.cos() * lat2_r.sin() - lat1_r.sin() * lat2_r.cos() * dlng.cos();
    let bearing = y.atan2(x).to_degrees();
    (bearing + 360.0) % 360.0
}

/// Approximate area of a polygon on Earth's surface in square meters.
///
/// Uses the Shoelace formula on projected coordinates (simple equirectangular).
/// Accurate for small polygons; for large polygons use spherical excess.
pub fn polygon_area(ring: &[[f64; 2]]) -> f64 {
    if ring.len() < 3 {
        return 0.0;
    }
    let mut sum = 0.0;
    let n = ring.len();
    for i in 0..n {
        let j = (i + 1) % n;
        // Convert to approximate meters from center.
        let lat_avg = ((ring[i][1] + ring[j][1]) / 2.0).to_radians();
        let x1 = ring[i][0].to_radians() * EARTH_RADIUS_M * lat_avg.cos();
        let y1 = ring[i][1].to_radians() * EARTH_RADIUS_M;
        let x2 = ring[j][0].to_radians() * EARTH_RADIUS_M * lat_avg.cos();
        let y2 = ring[j][1].to_radians() * EARTH_RADIUS_M;
        sum += x1 * y2 - x2 * y1;
    }
    (sum / 2.0).abs()
}

/// Check if a point is inside a polygon (ray casting algorithm).
pub fn point_in_polygon(lng: f64, lat: f64, ring: &[[f64; 2]]) -> bool {
    let mut inside = false;
    let n = ring.len();
    let mut j = n.wrapping_sub(1);
    for i in 0..n {
        let yi = ring[i][1];
        let yj = ring[j][1];
        if ((yi > lat) != (yj > lat))
            && (lng < (ring[j][0] - ring[i][0]) * (lat - yi) / (yj - yi) + ring[i][0])
        {
            inside = !inside;
        }
        j = i;
    }
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_creation() {
        let p = Geometry::point(-73.9857, 40.7484);
        assert_eq!(p.geometry_type(), "Point");
        if let Geometry::Point { coordinates } = &p {
            assert!((coordinates[0] - (-73.9857)).abs() < 1e-6);
            assert!((coordinates[1] - 40.7484).abs() < 1e-6);
        }
    }

    #[test]
    fn haversine_nyc_to_london() {
        // NYC: -74.006, 40.7128 → London: -0.1278, 51.5074
        let d = haversine_distance(-74.006, 40.7128, -0.1278, 51.5074);
        // ~5,570 km
        assert!((d - 5_570_000.0).abs() < 50_000.0, "got {d}m");
    }

    #[test]
    fn haversine_same_point() {
        let d = haversine_distance(0.0, 0.0, 0.0, 0.0);
        assert!(d.abs() < 1e-6);
    }

    #[test]
    fn bearing_north() {
        let b = haversine_bearing(0.0, 0.0, 0.0, 1.0);
        assert!((b - 0.0).abs() < 1.0, "expected ~0, got {b}");
    }

    #[test]
    fn bearing_east() {
        let b = haversine_bearing(0.0, 0.0, 1.0, 0.0);
        assert!((b - 90.0).abs() < 1.0, "expected ~90, got {b}");
    }

    #[test]
    fn polygon_area_simple() {
        // ~1 degree square near equator ≈ 111km × 111km ≈ 12,321 km²
        let ring = vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]];
        let area = polygon_area(&ring);
        let area_km2 = area / 1e6;
        assert!(
            (area_km2 - 12321.0).abs() < 500.0,
            "expected ~12321 km², got {area_km2}"
        );
    }

    #[test]
    fn point_in_polygon_inside() {
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        assert!(point_in_polygon(5.0, 5.0, &ring));
        assert!(!point_in_polygon(15.0, 5.0, &ring));
    }

    #[test]
    fn centroid_point() {
        let p = Geometry::point(10.0, 20.0);
        assert_eq!(p.centroid(), Some([10.0, 20.0]));
    }

    #[test]
    fn centroid_linestring() {
        let ls = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0], [10.0, 10.0]]);
        let c = ls.centroid().unwrap();
        assert!((c[0] - 6.6667).abs() < 0.01);
        assert!((c[1] - 3.3333).abs() < 0.01);
    }

    #[test]
    fn geojson_serialize() {
        let p = Geometry::point(1.0, 2.0);
        let json = sonic_rs::to_string(&p).unwrap();
        assert!(json.contains("\"type\":\"Point\""));
        assert!(json.contains("\"coordinates\":[1.0,2.0]"));
    }

    #[test]
    fn geojson_roundtrip() {
        let original = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]);
        let json = sonic_rs::to_string(&original).unwrap();
        let parsed: Geometry = sonic_rs::from_str(&json).unwrap();
        assert_eq!(original, parsed);
    }
}
