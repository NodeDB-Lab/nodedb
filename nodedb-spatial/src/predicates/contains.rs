// SPDX-License-Identifier: Apache-2.0

//! ST_Contains — geometry A fully contains geometry B.
//!
//! DE-9IM pattern: T*****FF* — B's interior intersects A's interior, and
//! B does not intersect A's exterior. A point on A's boundary is NOT
//! contained (strict OGC semantics). Use ST_Covers for boundary-inclusive.
//!
//! Implementation: bbox pre-filter → exact geometry test.

use nodedb_types::geometry::{Geometry, point_in_polygon};
use nodedb_types::geometry_bbox;

use super::edge::{is_collection, point_on_ring_boundary, ring_edges};

/// ST_Contains(a, b) — does geometry A fully contain geometry B?
///
/// Returns false if any part of B lies outside A, or if B only touches
/// A's boundary without entering its interior. EMPTY contains nothing and
/// is contained by nothing.
pub fn st_contains(a: &Geometry, b: &Geometry) -> bool {
    if a.is_empty() || b.is_empty() {
        return false;
    }
    // Fast path: bbox pre-filter. A GeometryCollection skips it: its bbox is
    // a union whose wrap rule depends on the SRID, and the arms below test
    // each member on its own.
    if !is_collection(a) && !is_collection(b) {
        let a_bb = geometry_bbox(a);
        let b_bb = geometry_bbox(b);
        if !a_bb.contains_bbox(&b_bb) {
            return false;
        }
    }

    match (a, b) {
        // Point contains Point — only if identical.
        (Geometry::Point { coordinates: ca }, Geometry::Point { coordinates: cb }) => {
            (ca[0] - cb[0]).abs() < 1e-12 && (ca[1] - cb[1]).abs() < 1e-12
        }

        // Polygon contains Point — ray casting, point on edge = false (DE-9IM).
        (Geometry::Polygon { coordinates: rings }, Geometry::Point { coordinates: pt }) => {
            polygon_contains_point(rings, *pt)
        }

        // Polygon contains LineString — all points strictly inside, no edge crossings
        // with exterior.
        (Geometry::Polygon { coordinates: rings }, Geometry::LineString { coordinates: line }) => {
            polygon_contains_linestring(rings, line)
        }

        // Polygon contains Polygon — all vertices of B inside A, no edge crossings.
        (
            Geometry::Polygon {
                coordinates: rings_a,
            },
            Geometry::Polygon {
                coordinates: rings_b,
            },
        ) => polygon_contains_polygon(rings_a, rings_b),

        // Polygon contains Multi* — all components contained.
        (Geometry::Polygon { .. }, Geometry::MultiPoint { coordinates }) => coordinates
            .iter()
            .all(|pt| st_contains(a, &Geometry::Point { coordinates: *pt })),
        (Geometry::Polygon { .. }, Geometry::MultiLineString { coordinates }) => {
            coordinates.iter().all(|ls| {
                st_contains(
                    a,
                    &Geometry::LineString {
                        coordinates: ls.clone(),
                    },
                )
            })
        }
        (Geometry::Polygon { .. }, Geometry::MultiPolygon { coordinates }) => {
            coordinates.iter().all(|poly| {
                st_contains(
                    a,
                    &Geometry::Polygon {
                        coordinates: poly.clone(),
                    },
                )
            })
        }

        // MultiPolygon contains anything — at least one component polygon contains all of B.
        (Geometry::MultiPolygon { coordinates: polys }, _) => polys.iter().any(|poly| {
            st_contains(
                &Geometry::Polygon {
                    coordinates: poly.clone(),
                },
                b,
            )
        }),

        // LineString contains Point — point must be strictly on the line interior
        // (not just an endpoint for DE-9IM T*****FF*, but for practical purposes
        // we check if point lies on any segment).
        (Geometry::LineString { coordinates: line }, Geometry::Point { coordinates: pt }) => {
            super::edge::point_on_ring_boundary(*pt, line)
        }

        // GeometryCollection contains B — any member contains B.
        (Geometry::GeometryCollection { geometries }, _) => {
            geometries.iter().any(|g| st_contains(g, b))
        }

        // Anything else: not supported or always false.
        _ => false,
    }
}

/// Polygon (with holes) contains a point — strict DE-9IM (boundary = false).
fn polygon_contains_point(rings: &[Vec<[f64; 2]>], pt: [f64; 2]) -> bool {
    let Some(exterior) = rings.first() else {
        return false;
    };

    // Point on exterior boundary → NOT contained (DE-9IM).
    if point_on_ring_boundary(pt, exterior) {
        return false;
    }

    // Point must be inside exterior ring.
    if !point_in_polygon(pt[0], pt[1], exterior) {
        return false;
    }

    // Point must not be inside any hole.
    for hole in &rings[1..] {
        if point_in_polygon(pt[0], pt[1], hole) {
            return false;
        }
        // Point on hole boundary is also outside (it's on A's boundary).
        if point_on_ring_boundary(pt, hole) {
            return false;
        }
    }

    true
}

/// Polygon contains a linestring — all vertices inside, no edge crossings with exterior.
fn polygon_contains_linestring(rings: &[Vec<[f64; 2]>], line: &[[f64; 2]]) -> bool {
    if line.is_empty() {
        return true;
    }

    let Some(exterior) = rings.first() else {
        return false;
    };

    // All line vertices must be strictly inside the polygon.
    for pt in line {
        if !polygon_contains_point(rings, *pt) {
            // Allow points on the boundary if the line passes through interior.
            // But for strict DE-9IM, if the entire line is on the boundary, it's
            // NOT contained. We check: if point is on boundary, at least one other
            // point must be in the interior.
            if !point_on_ring_boundary(*pt, exterior) {
                return false;
            }
        }
    }

    // No edge of the linestring may cross any edge of the polygon exterior.
    let poly_edges = ring_edges(exterior);
    for i in 0..line.len() - 1 {
        for &(pe_a, pe_b) in &poly_edges {
            if edges_properly_cross(line[i], line[i + 1], pe_a, pe_b) {
                return false;
            }
        }
    }

    // No edge of the linestring may cross any hole.
    for hole in &rings[1..] {
        let hole_edges = ring_edges(hole);
        for i in 0..line.len() - 1 {
            for &(he_a, he_b) in &hole_edges {
                if edges_properly_cross(line[i], line[i + 1], he_a, he_b) {
                    return false;
                }
            }
        }
        // Line vertices must not be inside holes.
        for pt in line {
            if point_in_polygon(pt[0], pt[1], hole) {
                return false;
            }
        }
    }

    // At least one point must be strictly interior (not just on boundary).
    line.iter().any(|pt| polygon_contains_point(rings, *pt))
}

/// Polygon A contains Polygon B.
fn polygon_contains_polygon(rings_a: &[Vec<[f64; 2]>], rings_b: &[Vec<[f64; 2]>]) -> bool {
    let Some(ext_b) = rings_b.first() else {
        return true;
    };

    // All vertices of B's exterior must be inside A (or on A's boundary,
    // but at least one must be strictly inside).
    let Some(ext_a) = rings_a.first() else {
        return false;
    };

    for pt in ext_b {
        if !point_in_polygon(pt[0], pt[1], ext_a) && !point_on_ring_boundary(*pt, ext_a) {
            return false;
        }
    }

    // B's exterior must not be inside any hole of A.
    for hole in &rings_a[1..] {
        for pt in ext_b {
            if point_in_polygon(pt[0], pt[1], hole) {
                return false;
            }
        }
    }

    // No proper edge crossings between A's exterior and B's exterior.
    let a_edges = ring_edges(ext_a);
    let b_edges = ring_edges(ext_b);
    for &(a1, a2) in &a_edges {
        for &(b1, b2) in &b_edges {
            if edges_properly_cross(a1, a2, b1, b2) {
                return false;
            }
        }
    }

    // At least one vertex of B must be strictly inside A.
    ext_b.iter().any(|pt| polygon_contains_point(rings_a, *pt))
}

/// Whether two segments properly cross (not just touch at endpoints).
fn edges_properly_cross(a1: [f64; 2], a2: [f64; 2], b1: [f64; 2], b2: [f64; 2]) -> bool {
    use super::edge::Orientation;
    use super::edge::orientation;

    let o1 = orientation(a1, a2, b1);
    let o2 = orientation(a1, a2, b2);
    let o3 = orientation(b1, b2, a1);
    let o4 = orientation(b1, b2, a2);

    // Proper crossing requires different orientations on both sides.
    if o1 != o2
        && o3 != o4
        && o1 != Orientation::Collinear
        && o2 != Orientation::Collinear
        && o3 != Orientation::Collinear
        && o4 != Orientation::Collinear
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square_poly() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]])
    }

    fn poly_with_hole() -> Geometry {
        Geometry::polygon(vec![
            vec![
                [0.0, 0.0],
                [20.0, 0.0],
                [20.0, 20.0],
                [0.0, 20.0],
                [0.0, 0.0],
            ],
            vec![
                [5.0, 5.0],
                [15.0, 5.0],
                [15.0, 15.0],
                [5.0, 15.0],
                [5.0, 5.0],
            ],
        ])
    }

    #[test]
    fn point_inside_polygon() {
        assert!(st_contains(&square_poly(), &Geometry::point(5.0, 5.0)));
    }

    #[test]
    fn point_outside_polygon() {
        assert!(!st_contains(&square_poly(), &Geometry::point(15.0, 5.0)));
    }

    #[test]
    fn point_on_edge_not_contained() {
        // DE-9IM: point on boundary is NOT contained.
        assert!(!st_contains(&square_poly(), &Geometry::point(5.0, 0.0)));
    }

    #[test]
    fn point_on_vertex_not_contained() {
        assert!(!st_contains(&square_poly(), &Geometry::point(0.0, 0.0)));
    }

    #[test]
    fn point_in_hole_not_contained() {
        // Point in the hole of a polygon with hole.
        assert!(!st_contains(
            &poly_with_hole(),
            &Geometry::point(10.0, 10.0)
        ));
    }

    #[test]
    fn point_between_exterior_and_hole() {
        // Point between exterior and hole (in the ring area).
        assert!(st_contains(&poly_with_hole(), &Geometry::point(2.0, 2.0)));
    }

    #[test]
    fn linestring_inside_polygon() {
        let line = Geometry::line_string(vec![[2.0, 2.0], [5.0, 5.0], [8.0, 3.0]]);
        assert!(st_contains(&square_poly(), &line));
    }

    #[test]
    fn linestring_crossing_boundary() {
        let line = Geometry::line_string(vec![[5.0, 5.0], [15.0, 5.0]]);
        assert!(!st_contains(&square_poly(), &line));
    }

    #[test]
    fn polygon_contains_smaller_polygon() {
        let inner = Geometry::polygon(vec![vec![
            [2.0, 2.0],
            [8.0, 2.0],
            [8.0, 8.0],
            [2.0, 8.0],
            [2.0, 2.0],
        ]]);
        assert!(st_contains(&square_poly(), &inner));
    }

    #[test]
    fn polygon_does_not_contain_overlapping() {
        let overlapping = Geometry::polygon(vec![vec![
            [5.0, 5.0],
            [15.0, 5.0],
            [15.0, 15.0],
            [5.0, 15.0],
            [5.0, 5.0],
        ]]);
        assert!(!st_contains(&square_poly(), &overlapping));
    }

    #[test]
    fn point_contains_same_point() {
        let p = Geometry::point(5.0, 5.0);
        assert!(st_contains(&p, &p));
    }

    #[test]
    fn multipoint_contained() {
        let mp = Geometry::MultiPoint {
            coordinates: vec![[2.0, 2.0], [5.0, 5.0], [8.0, 8.0]],
        };
        assert!(st_contains(&square_poly(), &mp));
    }

    #[test]
    fn multipoint_not_all_contained() {
        let mp = Geometry::MultiPoint {
            coordinates: vec![[2.0, 2.0], [15.0, 15.0]],
        };
        assert!(!st_contains(&square_poly(), &mp));
    }
}
