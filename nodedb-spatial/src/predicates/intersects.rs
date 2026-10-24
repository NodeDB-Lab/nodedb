// SPDX-License-Identifier: Apache-2.0

//! ST_Intersects — two geometries share any space (interior or boundary).
//!
//! Inverse of ST_Disjoint. A point on a polygon's edge DOES intersect
//! (unlike ST_Contains where it does not). This is the most commonly
//! used spatial predicate.
//!
//! Implementation: bbox pre-filter → exact geometry test.

use nodedb_types::geometry::{Geometry, point_in_polygon};
use nodedb_types::geometry_bbox;

use super::edge::{is_collection, point_on_ring_boundary, ring_edges, segments_intersect};

/// ST_Intersects(a, b) — do geometries A and B share any space? EMPTY
/// intersects nothing.
pub fn st_intersects(a: &Geometry, b: &Geometry) -> bool {
    if a.is_empty() || b.is_empty() {
        return false;
    }
    // Bbox pre-filter. A GeometryCollection skips it: its bbox is a union
    // whose wrap rule depends on the SRID, and the recursion below filters
    // each member on its own.
    if !is_collection(a) && !is_collection(b) {
        let a_bb = geometry_bbox(a);
        let b_bb = geometry_bbox(b);
        if !a_bb.intersects(&b_bb) {
            return false;
        }
    }

    match (a, b) {
        // Point–Point: identical within tolerance.
        (Geometry::Point { coordinates: ca }, Geometry::Point { coordinates: cb }) => {
            (ca[0] - cb[0]).abs() < 1e-12 && (ca[1] - cb[1]).abs() < 1e-12
        }

        // Point–LineString: point on any segment.
        (Geometry::Point { coordinates: pt }, Geometry::LineString { coordinates: line })
        | (Geometry::LineString { coordinates: line }, Geometry::Point { coordinates: pt }) => {
            point_on_ring_boundary(*pt, line)
        }

        // Point–Polygon: point inside OR on boundary.
        (Geometry::Point { coordinates: pt }, Geometry::Polygon { coordinates: rings })
        | (Geometry::Polygon { coordinates: rings }, Geometry::Point { coordinates: pt }) => {
            point_intersects_polygon(*pt, rings)
        }

        // LineString–LineString: any edge crossing or shared point.
        (Geometry::LineString { coordinates: la }, Geometry::LineString { coordinates: lb }) => {
            linestrings_intersect(la, lb)
        }

        // LineString–Polygon: any edge crossing, or any line point inside polygon.
        (Geometry::LineString { coordinates: line }, Geometry::Polygon { coordinates: rings })
        | (Geometry::Polygon { coordinates: rings }, Geometry::LineString { coordinates: line }) => {
            linestring_intersects_polygon(line, rings)
        }

        // Polygon–Polygon: edge crossing or one inside the other.
        (Geometry::Polygon { coordinates: ra }, Geometry::Polygon { coordinates: rb }) => {
            polygons_intersect(ra, rb)
        }

        // Multi* types: any component intersects.
        (Geometry::MultiPoint { coordinates }, other)
        | (other, Geometry::MultiPoint { coordinates }) => coordinates
            .iter()
            .any(|pt| st_intersects(&Geometry::Point { coordinates: *pt }, other)),

        (Geometry::MultiLineString { coordinates }, other)
        | (other, Geometry::MultiLineString { coordinates }) => coordinates.iter().any(|ls| {
            st_intersects(
                &Geometry::LineString {
                    coordinates: ls.clone(),
                },
                other,
            )
        }),

        (Geometry::MultiPolygon { coordinates }, other)
        | (other, Geometry::MultiPolygon { coordinates }) => coordinates.iter().any(|poly| {
            st_intersects(
                &Geometry::Polygon {
                    coordinates: poly.clone(),
                },
                other,
            )
        }),

        (Geometry::GeometryCollection { geometries }, other)
        | (other, Geometry::GeometryCollection { geometries }) => {
            geometries.iter().any(|g| st_intersects(g, other))
        }
    }
}

/// Point intersects polygon — inside OR on boundary.
fn point_intersects_polygon(pt: [f64; 2], rings: &[Vec<[f64; 2]>]) -> bool {
    let Some(exterior) = rings.first() else {
        return false;
    };

    // On exterior boundary → intersects.
    if point_on_ring_boundary(pt, exterior) {
        return true;
    }

    // Must be inside exterior.
    if !point_in_polygon(pt[0], pt[1], exterior) {
        return false;
    }

    // Must not be inside a hole (but on hole boundary counts as intersects).
    for hole in &rings[1..] {
        if point_on_ring_boundary(pt, hole) {
            return true;
        }
        if point_in_polygon(pt[0], pt[1], hole) {
            return false;
        }
    }

    true
}

/// Two linestrings intersect if any of their segments cross.
fn linestrings_intersect(la: &[[f64; 2]], lb: &[[f64; 2]]) -> bool {
    for i in 0..la.len().saturating_sub(1) {
        for j in 0..lb.len().saturating_sub(1) {
            if segments_intersect(la[i], la[i + 1], lb[j], lb[j + 1]) {
                return true;
            }
        }
    }
    false
}

/// LineString intersects polygon — any edge crossing, or any point inside.
fn linestring_intersects_polygon(line: &[[f64; 2]], rings: &[Vec<[f64; 2]>]) -> bool {
    // Check if any line vertex is inside/on polygon.
    for pt in line {
        if point_intersects_polygon(*pt, rings) {
            return true;
        }
    }

    // Check if any line edge crosses any polygon edge.
    let Some(exterior) = rings.first() else {
        return false;
    };
    let ext_edges = ring_edges(exterior);
    for i in 0..line.len().saturating_sub(1) {
        for &(pe_a, pe_b) in &ext_edges {
            if segments_intersect(line[i], line[i + 1], pe_a, pe_b) {
                return true;
            }
        }
    }

    // Check hole edges too.
    for hole in &rings[1..] {
        let hole_edges = ring_edges(hole);
        for i in 0..line.len().saturating_sub(1) {
            for &(he_a, he_b) in &hole_edges {
                if segments_intersect(line[i], line[i + 1], he_a, he_b) {
                    return true;
                }
            }
        }
    }

    false
}

/// Two polygons intersect — edge crossing or containment.
fn polygons_intersect(ra: &[Vec<[f64; 2]>], rb: &[Vec<[f64; 2]>]) -> bool {
    let Some(ext_a) = ra.first() else {
        return false;
    };
    let Some(ext_b) = rb.first() else {
        return false;
    };

    // Check if any vertex of B is inside/on A.
    for pt in ext_b {
        if point_intersects_polygon(*pt, ra) {
            return true;
        }
    }

    // Check if any vertex of A is inside/on B.
    for pt in ext_a {
        if point_intersects_polygon(*pt, rb) {
            return true;
        }
    }

    // Check edge crossings between exteriors.
    let a_edges = ring_edges(ext_a);
    let b_edges = ring_edges(ext_b);
    for &(a1, a2) in &a_edges {
        for &(b1, b2) in &b_edges {
            if segments_intersect(a1, a2, b1, b2) {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::super::{st_disjoint, st_within};
    use super::*;

    fn square() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]])
    }

    #[test]
    fn point_inside_polygon_intersects() {
        assert!(st_intersects(&Geometry::point(5.0, 5.0), &square()));
    }

    #[test]
    fn point_on_edge_intersects() {
        // Unlike ST_Contains, point on edge DOES intersect.
        assert!(st_intersects(&Geometry::point(5.0, 0.0), &square()));
    }

    #[test]
    fn point_on_vertex_intersects() {
        assert!(st_intersects(&Geometry::point(0.0, 0.0), &square()));
    }

    #[test]
    fn point_outside_does_not_intersect() {
        assert!(!st_intersects(&Geometry::point(20.0, 20.0), &square()));
    }

    #[test]
    fn linestring_crosses_polygon() {
        let line = Geometry::line_string(vec![[-5.0, 5.0], [15.0, 5.0]]);
        assert!(st_intersects(&line, &square()));
    }

    #[test]
    fn linestring_inside_polygon() {
        let line = Geometry::line_string(vec![[2.0, 2.0], [8.0, 8.0]]);
        assert!(st_intersects(&line, &square()));
    }

    #[test]
    fn linestring_outside_polygon() {
        let line = Geometry::line_string(vec![[20.0, 20.0], [30.0, 30.0]]);
        assert!(!st_intersects(&line, &square()));
    }

    #[test]
    fn polygons_overlap() {
        let other = Geometry::polygon(vec![vec![
            [5.0, 5.0],
            [15.0, 5.0],
            [15.0, 15.0],
            [5.0, 15.0],
            [5.0, 5.0],
        ]]);
        assert!(st_intersects(&square(), &other));
    }

    #[test]
    fn polygons_shared_boundary() {
        let adjacent = Geometry::polygon(vec![vec![
            [10.0, 0.0],
            [20.0, 0.0],
            [20.0, 10.0],
            [10.0, 10.0],
            [10.0, 0.0],
        ]]);
        // Shared edge → intersects.
        assert!(st_intersects(&square(), &adjacent));
    }

    #[test]
    fn polygons_disjoint() {
        let far = Geometry::polygon(vec![vec![
            [20.0, 20.0],
            [30.0, 20.0],
            [30.0, 30.0],
            [20.0, 30.0],
            [20.0, 20.0],
        ]]);
        assert!(!st_intersects(&square(), &far));
        assert!(st_disjoint(&square(), &far));
    }

    #[test]
    fn linestrings_cross() {
        let a = Geometry::line_string(vec![[0.0, 0.0], [10.0, 10.0]]);
        let b = Geometry::line_string(vec![[0.0, 10.0], [10.0, 0.0]]);
        assert!(st_intersects(&a, &b));
    }

    #[test]
    fn linestrings_parallel_no_cross() {
        let a = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]);
        let b = Geometry::line_string(vec![[0.0, 1.0], [10.0, 1.0]]);
        assert!(!st_intersects(&a, &b));
    }

    #[test]
    fn st_within_works() {
        let inner = Geometry::point(5.0, 5.0);
        assert!(st_within(&inner, &square()));
        assert!(!st_within(&square(), &inner));
    }

    #[test]
    fn st_disjoint_works() {
        assert!(!st_disjoint(&Geometry::point(5.0, 5.0), &square()));
        assert!(st_disjoint(&Geometry::point(20.0, 20.0), &square()));
    }

    #[test]
    fn linestring_endpoint_touches_polygon() {
        let line = Geometry::line_string(vec![[10.0, 5.0], [15.0, 5.0]]);
        // Endpoint touches polygon edge → intersects.
        assert!(st_intersects(&line, &square()));
    }

    #[test]
    fn polygon_inside_polygon_intersects() {
        let inner = Geometry::polygon(vec![vec![
            [2.0, 2.0],
            [8.0, 2.0],
            [8.0, 8.0],
            [2.0, 8.0],
            [2.0, 2.0],
        ]]);
        assert!(st_intersects(&square(), &inner));
    }
}
