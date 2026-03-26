//! Geometry construction and transformation operations.
//!
//! ST_Buffer, ST_Envelope, ST_Union — produce new geometries from existing ones.

use nodedb_types::geometry::Geometry;
use nodedb_types::{BoundingBox, geometry_bbox};

/// ST_Buffer(geom, distance_meters) → Polygon.
///
/// Expands geometry by distance using equirectangular approximation.
/// - Point → 32-sided circle polygon
/// - LineString → buffered corridor (parallel offset + end caps)
/// - Polygon → expanded polygon (vertex offset outward)
///
/// Meter-to-degree conversion: `Δlng = meters / (111320 * cos(lat))`,
/// `Δlat = meters / 110540`. Accurate to <0.5% for ±70° latitude.
pub fn st_buffer(geom: &Geometry, distance_meters: f64, segments: usize) -> Geometry {
    let segments = segments.max(4);

    match geom {
        Geometry::Point { coordinates } => {
            buffer_point(coordinates[0], coordinates[1], distance_meters, segments)
        }
        Geometry::LineString { coordinates } => {
            buffer_linestring(coordinates, distance_meters, segments)
        }
        Geometry::Polygon { coordinates } => buffer_polygon(coordinates, distance_meters, segments),
        // Multi* types: buffer each component and collect.
        Geometry::MultiPoint { coordinates } => {
            let polys: Vec<Vec<Vec<[f64; 2]>>> = coordinates
                .iter()
                .map(|pt| {
                    if let Geometry::Polygon { coordinates: rings } =
                        buffer_point(pt[0], pt[1], distance_meters, segments)
                    {
                        rings
                    } else {
                        vec![]
                    }
                })
                .collect();
            Geometry::MultiPolygon { coordinates: polys }
        }
        _ => {
            // For unsupported types, fall back to buffering the bbox.
            let bb = geometry_bbox(geom).expand_meters(distance_meters);
            bbox_to_polygon(&bb)
        }
    }
}

/// ST_Envelope(geom) → Polygon — bounding box as a polygon.
pub fn st_envelope(geom: &Geometry) -> Geometry {
    let bb = geometry_bbox(geom);
    bbox_to_polygon(&bb)
}

/// ST_Union(a, b) → Geometry — merge two geometries.
///
/// For this version, collects into appropriate Multi* type or
/// GeometryCollection. True polygon clipping (Weiler-Atherton) is
/// deferred — this handles the common cases correctly.
pub fn st_union(a: &Geometry, b: &Geometry) -> Geometry {
    match (a, b) {
        // Point + Point → MultiPoint
        (Geometry::Point { coordinates: ca }, Geometry::Point { coordinates: cb }) => {
            Geometry::MultiPoint {
                coordinates: vec![*ca, *cb],
            }
        }
        // LineString + LineString → MultiLineString
        (Geometry::LineString { coordinates: la }, Geometry::LineString { coordinates: lb }) => {
            Geometry::MultiLineString {
                coordinates: vec![la.clone(), lb.clone()],
            }
        }
        // Polygon + Polygon → MultiPolygon
        (Geometry::Polygon { coordinates: ra }, Geometry::Polygon { coordinates: rb }) => {
            Geometry::MultiPolygon {
                coordinates: vec![ra.clone(), rb.clone()],
            }
        }
        // Same-type Multi* + element → extend
        (Geometry::MultiPoint { coordinates: pts }, Geometry::Point { coordinates: pt }) => {
            let mut coords = pts.clone();
            coords.push(*pt);
            Geometry::MultiPoint {
                coordinates: coords,
            }
        }
        (Geometry::Point { coordinates: pt }, Geometry::MultiPoint { coordinates: pts }) => {
            let mut coords = vec![*pt];
            coords.extend_from_slice(pts);
            Geometry::MultiPoint {
                coordinates: coords,
            }
        }
        // Everything else → GeometryCollection
        _ => Geometry::GeometryCollection {
            geometries: vec![a.clone(), b.clone()],
        },
    }
}

// ── Buffer helpers ──

/// Buffer a point into an approximate circle polygon.
fn buffer_point(lng: f64, lat: f64, meters: f64, segments: usize) -> Geometry {
    let dlat = meters / 110_540.0;
    let dlng = meters / (111_320.0 * lat.to_radians().cos().max(0.001));

    let mut ring = Vec::with_capacity(segments + 1);
    for i in 0..segments {
        let angle = 2.0 * std::f64::consts::PI * (i as f64) / (segments as f64);
        ring.push([lng + dlng * angle.cos(), lat + dlat * angle.sin()]);
    }
    // Close the ring.
    ring.push(ring[0]);

    Geometry::Polygon {
        coordinates: vec![ring],
    }
}

/// Buffer a linestring by creating a corridor (parallel offset on both sides + end caps).
fn buffer_linestring(line: &[[f64; 2]], meters: f64, segments: usize) -> Geometry {
    if line.is_empty() {
        return Geometry::Polygon {
            coordinates: vec![vec![]],
        };
    }
    if line.len() == 1 {
        return buffer_point(line[0][0], line[0][1], meters, segments);
    }

    // Build the offset polygon: left side forward, right side backward, with
    // semicircular end caps.
    let mut left_side = Vec::new();
    let mut right_side = Vec::new();

    for i in 0..line.len() - 1 {
        let (nl, nr) = offset_segment(line[i], line[i + 1], meters);
        left_side.push(nl.0);
        left_side.push(nl.1);
        right_side.push(nr.0);
        right_side.push(nr.1);
    }

    // Build polygon: left side forward → end cap → right side backward → start cap.
    let mut ring = Vec::new();
    ring.extend_from_slice(&left_side);

    // End cap (semicircle around last point).
    let last = line[line.len() - 1];
    let cap_segments = segments / 2;
    let end_dir = direction(line[line.len() - 2], last);
    for i in 0..=cap_segments {
        let angle = end_dir - std::f64::consts::FRAC_PI_2
            + std::f64::consts::PI * (i as f64) / (cap_segments as f64);
        let dlat = meters / 110_540.0;
        let dlng = meters / (111_320.0 * last[1].to_radians().cos().max(0.001));
        ring.push([last[0] + dlng * angle.cos(), last[1] + dlat * angle.sin()]);
    }

    // Right side backward.
    for pt in right_side.iter().rev() {
        ring.push(*pt);
    }

    // Start cap (semicircle around first point).
    let first = line[0];
    let start_dir = direction(first, line[1]);
    for i in 0..=cap_segments {
        let angle = start_dir
            + std::f64::consts::FRAC_PI_2
            + std::f64::consts::PI * (i as f64) / (cap_segments as f64);
        let dlat = meters / 110_540.0;
        let dlng = meters / (111_320.0 * first[1].to_radians().cos().max(0.001));
        ring.push([first[0] + dlng * angle.cos(), first[1] + dlat * angle.sin()]);
    }

    // Close.
    if let Some(&first_pt) = ring.first() {
        ring.push(first_pt);
    }

    Geometry::Polygon {
        coordinates: vec![ring],
    }
}

/// Buffer a polygon by offsetting each vertex outward.
fn buffer_polygon(rings: &[Vec<[f64; 2]>], meters: f64, _segments: usize) -> Geometry {
    let mut new_rings = Vec::with_capacity(rings.len());

    for (ring_idx, ring) in rings.iter().enumerate() {
        if ring.len() < 3 {
            new_rings.push(ring.clone());
            continue;
        }
        let is_exterior = ring_idx == 0;
        // For exterior: expand outward. For holes: shrink inward.
        let sign = if is_exterior { 1.0 } else { -1.0 };
        let offset_m = meters * sign;

        let mut new_ring = Vec::with_capacity(ring.len());
        let n = if ring.first() == ring.last() {
            ring.len() - 1
        } else {
            ring.len()
        };

        for i in 0..n {
            let prev = ring[(i + n - 1) % n];
            let curr = ring[i];
            let next = ring[(i + 1) % n];

            // Bisector direction (average of the two edge normals).
            let n1 = edge_outward_normal(prev, curr);
            let n2 = edge_outward_normal(curr, next);
            let bisect = [(n1[0] + n2[0]) / 2.0, (n1[1] + n2[1]) / 2.0];
            let len = (bisect[0] * bisect[0] + bisect[1] * bisect[1])
                .sqrt()
                .max(1e-12);
            let unit = [bisect[0] / len, bisect[1] / len];

            let dlat = offset_m / 110_540.0;
            let dlng = offset_m / (111_320.0 * curr[1].to_radians().cos().max(0.001));

            new_ring.push([curr[0] + unit[0] * dlng, curr[1] + unit[1] * dlat]);
        }

        // Close.
        if let Some(&first_pt) = new_ring.first() {
            new_ring.push(first_pt);
        }
        new_rings.push(new_ring);
    }

    Geometry::Polygon {
        coordinates: new_rings,
    }
}

/// Compute offset segments (left and right parallel lines at `meters` distance).
/// A pair of offset segments: (left_segment, right_segment).
type OffsetPair = (([f64; 2], [f64; 2]), ([f64; 2], [f64; 2]));

fn offset_segment(a: [f64; 2], b: [f64; 2], meters: f64) -> OffsetPair {
    let normal = edge_outward_normal(a, b);
    let dlat = meters / 110_540.0;
    let mid_lat = ((a[1] + b[1]) / 2.0).to_radians().cos().max(0.001);
    let dlng = meters / (111_320.0 * mid_lat);

    let left = (
        [a[0] + normal[0] * dlng, a[1] + normal[1] * dlat],
        [b[0] + normal[0] * dlng, b[1] + normal[1] * dlat],
    );
    let right = (
        [a[0] - normal[0] * dlng, a[1] - normal[1] * dlat],
        [b[0] - normal[0] * dlng, b[1] - normal[1] * dlat],
    );
    (left, right)
}

/// Outward-pointing unit normal for an edge (a→b).
///
/// For CCW rings (standard exterior), the outward direction is the right
/// normal (dy, -dx). This is correct because CCW traversal has the
/// interior on the left.
fn edge_outward_normal(a: [f64; 2], b: [f64; 2]) -> [f64; 2] {
    let dx = b[0] - a[0];
    let dy = b[1] - a[1];
    let len = (dx * dx + dy * dy).sqrt().max(1e-12);
    // Right normal: (dy, -dx) normalized — outward for CCW rings.
    [dy / len, -dx / len]
}

/// Direction angle (radians) from a to b.
fn direction(a: [f64; 2], b: [f64; 2]) -> f64 {
    (b[1] - a[1]).atan2(b[0] - a[0])
}

/// Convert a BoundingBox to a Polygon.
fn bbox_to_polygon(bb: &BoundingBox) -> Geometry {
    Geometry::Polygon {
        coordinates: vec![vec![
            [bb.min_lng, bb.min_lat],
            [bb.max_lng, bb.min_lat],
            [bb.max_lng, bb.max_lat],
            [bb.min_lng, bb.max_lat],
            [bb.min_lng, bb.min_lat],
        ]],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_point_circle() {
        let circle = st_buffer(&Geometry::point(0.0, 0.0), 1000.0, 32);
        if let Geometry::Polygon { coordinates } = &circle {
            assert_eq!(coordinates.len(), 1);
            // 32 segments + 1 closing point.
            assert_eq!(coordinates[0].len(), 33);
            // All points should be roughly 1000m from center.
            for pt in &coordinates[0][..32] {
                let d = nodedb_types::geometry::haversine_distance(0.0, 0.0, pt[0], pt[1]);
                assert!((d - 1000.0).abs() < 50.0, "d={d}");
            }
        } else {
            panic!("expected Polygon");
        }
    }

    #[test]
    fn buffer_point_4_segments() {
        let circle = st_buffer(&Geometry::point(10.0, 50.0), 500.0, 4);
        if let Geometry::Polygon { coordinates } = &circle {
            assert_eq!(coordinates[0].len(), 5); // 4 + close
        } else {
            panic!("expected Polygon");
        }
    }

    #[test]
    fn envelope_polygon() {
        let poly = Geometry::polygon(vec![vec![[1.0, 2.0], [5.0, 2.0], [3.0, 8.0], [1.0, 2.0]]]);
        let env = st_envelope(&poly);
        if let Geometry::Polygon { coordinates } = &env {
            let ring = &coordinates[0];
            assert_eq!(ring[0], [1.0, 2.0]); // min_lng, min_lat
            assert_eq!(ring[2], [5.0, 8.0]); // max_lng, max_lat
        } else {
            panic!("expected Polygon");
        }
    }

    #[test]
    fn envelope_point() {
        let env = st_envelope(&Geometry::point(5.0, 10.0));
        if let Geometry::Polygon { coordinates } = &env {
            // Zero-area bbox → degenerate polygon.
            assert_eq!(coordinates[0][0], [5.0, 10.0]);
            assert_eq!(coordinates[0][2], [5.0, 10.0]);
        } else {
            panic!("expected Polygon");
        }
    }

    #[test]
    fn union_points() {
        let a = Geometry::point(1.0, 2.0);
        let b = Geometry::point(3.0, 4.0);
        let u = st_union(&a, &b);
        if let Geometry::MultiPoint { coordinates } = &u {
            assert_eq!(coordinates.len(), 2);
        } else {
            panic!("expected MultiPoint, got {:?}", u.geometry_type());
        }
    }

    #[test]
    fn union_polygons() {
        let a = Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]);
        let b = Geometry::polygon(vec![vec![[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 2.0]]]);
        let u = st_union(&a, &b);
        if let Geometry::MultiPolygon { coordinates } = &u {
            assert_eq!(coordinates.len(), 2);
        } else {
            panic!("expected MultiPolygon");
        }
    }

    #[test]
    fn union_mixed_types() {
        let a = Geometry::point(1.0, 2.0);
        let b = Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]);
        let u = st_union(&a, &b);
        if let Geometry::GeometryCollection { geometries } = &u {
            assert_eq!(geometries.len(), 2);
        } else {
            panic!("expected GeometryCollection");
        }
    }

    #[test]
    fn buffer_linestring_corridor() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [0.1, 0.0]]);
        let buf = st_buffer(&line, 1000.0, 16);
        if let Geometry::Polygon { coordinates } = &buf {
            assert!(!coordinates[0].is_empty());
            // Should have more points than a simple rectangle due to end caps.
            assert!(coordinates[0].len() > 10);
        } else {
            panic!("expected Polygon");
        }
    }

    #[test]
    fn buffer_polygon_expands() {
        let poly = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]);
        let buf = st_buffer(&poly, 1000.0, 32);
        if let Geometry::Polygon { .. } = &buf {
            let orig_bb = geometry_bbox(&poly);
            let buf_bb = geometry_bbox(&buf);
            // Buffered should be larger.
            assert!(buf_bb.min_lng < orig_bb.min_lng);
            assert!(buf_bb.max_lng > orig_bb.max_lng);
            assert!(buf_bb.min_lat < orig_bb.min_lat);
            assert!(buf_bb.max_lat > orig_bb.max_lat);
        } else {
            panic!("expected Polygon");
        }
    }
}
