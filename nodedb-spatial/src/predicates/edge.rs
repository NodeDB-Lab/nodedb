// SPDX-License-Identifier: Apache-2.0

//! Low-level segment and edge geometry primitives.
//!
//! Used by ST_Contains, ST_Intersects and the distance algorithms. Every
//! operation is planar in the coordinates. Geodesic measures live in
//! `crate::geodesic`.

/// Orientation of three points (collinear, clockwise, counter-clockwise).
/// Uses the cross product of vectors (p→q) and (p→r).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Orientation {
    Collinear,
    Clockwise,
    CounterClockwise,
}

/// Whether `geom` is a GeometryCollection.
pub fn is_collection(geom: &nodedb_types::geometry::Geometry) -> bool {
    matches!(
        geom,
        nodedb_types::geometry::Geometry::GeometryCollection { .. }
    )
}

/// Compute orientation of ordered triplet (p, q, r).
pub fn orientation(p: [f64; 2], q: [f64; 2], r: [f64; 2]) -> Orientation {
    let val = (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1]);
    // Use epsilon for floating-point tolerance.
    if val.abs() < 1e-12 {
        Orientation::Collinear
    } else if val > 0.0 {
        Orientation::Clockwise
    } else {
        Orientation::CounterClockwise
    }
}

/// Whether point q lies on segment p-r (given that p, q, r are collinear).
pub fn on_segment(p: [f64; 2], q: [f64; 2], r: [f64; 2]) -> bool {
    q[0] <= p[0].max(r[0])
        && q[0] >= p[0].min(r[0])
        && q[1] <= p[1].max(r[1])
        && q[1] >= p[1].min(r[1])
}

/// Whether two segments (p1-q1) and (p2-q2) intersect.
///
/// Uses the standard orientation-based algorithm. Handles collinear
/// overlapping segments correctly.
pub fn segments_intersect(p1: [f64; 2], q1: [f64; 2], p2: [f64; 2], q2: [f64; 2]) -> bool {
    let o1 = orientation(p1, q1, p2);
    let o2 = orientation(p1, q1, q2);
    let o3 = orientation(p2, q2, p1);
    let o4 = orientation(p2, q2, q1);

    // General case: different orientations mean crossing.
    if o1 != o2 && o3 != o4 {
        return true;
    }

    // Collinear special cases: check if endpoints lie on the other segment.
    if o1 == Orientation::Collinear && on_segment(p1, p2, q1) {
        return true;
    }
    if o2 == Orientation::Collinear && on_segment(p1, q2, q1) {
        return true;
    }
    if o3 == Orientation::Collinear && on_segment(p2, p1, q2) {
        return true;
    }
    if o4 == Orientation::Collinear && on_segment(p2, q1, q2) {
        return true;
    }

    false
}

/// Whether a point lies exactly on a line segment (within epsilon tolerance).
pub fn point_on_segment(pt: [f64; 2], seg_a: [f64; 2], seg_b: [f64; 2]) -> bool {
    // Check collinearity via cross product.
    let cross =
        (pt[0] - seg_a[0]) * (seg_b[1] - seg_a[1]) - (pt[1] - seg_a[1]) * (seg_b[0] - seg_a[0]);
    if cross.abs() > 1e-10 {
        return false;
    }
    // Check that pt is within the segment's bounding box.
    pt[0] >= seg_a[0].min(seg_b[0]) - 1e-10
        && pt[0] <= seg_a[0].max(seg_b[0]) + 1e-10
        && pt[1] >= seg_a[1].min(seg_b[1]) - 1e-10
        && pt[1] <= seg_a[1].max(seg_b[1]) + 1e-10
}

/// Whether a point lies on any edge of a polygon ring.
pub fn point_on_ring_boundary(pt: [f64; 2], ring: &[[f64; 2]]) -> bool {
    if ring.len() < 2 {
        return false;
    }
    for i in 0..ring.len() - 1 {
        if point_on_segment(pt, ring[i], ring[i + 1]) {
            return true;
        }
    }
    // Check closing segment if ring isn't explicitly closed.
    if ring.first() != ring.last()
        && let (Some(&first), Some(&last)) = (ring.first(), ring.last())
        && point_on_segment(pt, last, first)
    {
        return true;
    }
    false
}

/// Minimum squared distance from a point to a line segment.
///
/// Returns the squared distance (avoid sqrt for comparison purposes).
pub fn point_to_segment_dist_sq(pt: [f64; 2], seg_a: [f64; 2], seg_b: [f64; 2]) -> f64 {
    let dx = seg_b[0] - seg_a[0];
    let dy = seg_b[1] - seg_a[1];
    let len_sq = dx * dx + dy * dy;

    if len_sq < 1e-20 {
        // Degenerate segment (zero length) — distance to point.
        let ex = pt[0] - seg_a[0];
        let ey = pt[1] - seg_a[1];
        return ex * ex + ey * ey;
    }

    // Project pt onto the line, clamped to [0, 1].
    let t = ((pt[0] - seg_a[0]) * dx + (pt[1] - seg_a[1]) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);

    let proj_x = seg_a[0] + t * dx;
    let proj_y = seg_a[1] + t * dy;

    let ex = pt[0] - proj_x;
    let ey = pt[1] - proj_y;
    ex * ex + ey * ey
}

/// Minimum squared distance between two line segments.
pub fn segment_to_segment_dist_sq(a1: [f64; 2], a2: [f64; 2], b1: [f64; 2], b2: [f64; 2]) -> f64 {
    if segments_intersect(a1, a2, b1, b2) {
        return 0.0;
    }
    // Min of all endpoint-to-segment distances.
    let d1 = point_to_segment_dist_sq(a1, b1, b2);
    let d2 = point_to_segment_dist_sq(a2, b1, b2);
    let d3 = point_to_segment_dist_sq(b1, a1, a2);
    let d4 = point_to_segment_dist_sq(b2, a1, a2);
    d1.min(d2).min(d3).min(d4)
}

/// Extract all edges from a polygon ring as segment pairs.
pub fn ring_edges(ring: &[[f64; 2]]) -> Vec<([f64; 2], [f64; 2])> {
    if ring.len() < 2 {
        return Vec::new();
    }
    let mut edges = Vec::with_capacity(ring.len());
    for i in 0..ring.len() - 1 {
        edges.push((ring[i], ring[i + 1]));
    }
    // Close the ring if not explicitly closed.
    if ring.first() != ring.last()
        && let (Some(&first), Some(&last)) = (ring.first(), ring.last())
    {
        edges.push((last, first));
    }
    edges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_cross() {
        assert!(segments_intersect(
            [0.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [10.0, 0.0]
        ));
    }

    #[test]
    fn segments_parallel_no_cross() {
        assert!(!segments_intersect(
            [0.0, 0.0],
            [10.0, 0.0],
            [0.0, 1.0],
            [10.0, 1.0]
        ));
    }

    #[test]
    fn segments_collinear_overlap() {
        assert!(segments_intersect(
            [0.0, 0.0],
            [5.0, 0.0],
            [3.0, 0.0],
            [8.0, 0.0]
        ));
    }

    #[test]
    fn segments_collinear_no_overlap() {
        assert!(!segments_intersect(
            [0.0, 0.0],
            [2.0, 0.0],
            [3.0, 0.0],
            [5.0, 0.0]
        ));
    }

    #[test]
    fn segments_endpoint_touch() {
        assert!(segments_intersect(
            [0.0, 0.0],
            [5.0, 5.0],
            [5.0, 5.0],
            [10.0, 0.0]
        ));
    }

    #[test]
    fn point_on_segment_middle() {
        assert!(point_on_segment([5.0, 5.0], [0.0, 0.0], [10.0, 10.0]));
    }

    #[test]
    fn point_on_segment_endpoint() {
        assert!(point_on_segment([0.0, 0.0], [0.0, 0.0], [10.0, 10.0]));
    }

    #[test]
    fn point_off_segment() {
        assert!(!point_on_segment([5.0, 6.0], [0.0, 0.0], [10.0, 10.0]));
    }

    #[test]
    fn point_on_ring_edge() {
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        assert!(point_on_ring_boundary([5.0, 0.0], &ring)); // bottom edge
        assert!(point_on_ring_boundary([0.0, 5.0], &ring)); // left edge
        assert!(!point_on_ring_boundary([5.0, 5.0], &ring)); // interior
    }

    #[test]
    fn point_on_ring_vertex() {
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        assert!(point_on_ring_boundary([0.0, 0.0], &ring));
        assert!(point_on_ring_boundary([10.0, 10.0], &ring));
    }

    #[test]
    fn point_to_segment_perpendicular() {
        let d = point_to_segment_dist_sq([5.0, 1.0], [0.0, 0.0], [10.0, 0.0]);
        assert!((d - 1.0).abs() < 1e-10); // 1 unit away
    }

    #[test]
    fn point_to_segment_endpoint() {
        let d = point_to_segment_dist_sq([12.0, 0.0], [0.0, 0.0], [10.0, 0.0]);
        assert!((d - 4.0).abs() < 1e-10); // 2 units past endpoint
    }

    #[test]
    fn segment_to_segment_parallel() {
        let d = segment_to_segment_dist_sq([0.0, 0.0], [10.0, 0.0], [0.0, 3.0], [10.0, 3.0]);
        assert!((d - 9.0).abs() < 1e-10);
    }

    #[test]
    fn segment_to_segment_crossing() {
        let d = segment_to_segment_dist_sq([0.0, 0.0], [10.0, 10.0], [0.0, 10.0], [10.0, 0.0]);
        assert!(d < 1e-10);
    }

    #[test]
    fn ring_edges_closed() {
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        let edges = ring_edges(&ring);
        assert_eq!(edges.len(), 4);
    }
}
