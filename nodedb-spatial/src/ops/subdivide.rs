// SPDX-License-Identifier: Apache-2.0

//! `ST_Subdivide(geom, max_vertices)`: cut a geometry along box halves until
//! every piece has at most `max_vertices` vertices. The pieces return as
//! one GEOMETRYCOLLECTION (a scalar function returns one value), each
//! piece a polygon, line or point. Planar in the coordinates.

use geo::{BooleanOps, LineString, MultiLineString, MultiPolygon, Rect};
use nodedb_types::BoundingBox;
use nodedb_types::geometry::Geometry;

use super::convert::{from_geo, rings, to_geo};

/// Smallest vertex budget, as PostGIS requires.
pub const MIN_SUBDIVIDE_VERTICES: usize = 5;

/// Recursion limit: 50 halvings shrink a box below any f64 coordinate
/// step.
const MAX_DEPTH: usize = 50;

/// Cut `geom` into pieces of at most `max_vertices` vertices.
/// `max_vertices` below [`MIN_SUBDIVIDE_VERTICES`] rises to it.
pub fn subdivide(geom: &Geometry, max_vertices: usize) -> Geometry {
    let budget = max_vertices.max(MIN_SUBDIVIDE_VERTICES);
    let mut pieces = Vec::new();
    for part in parts(geom) {
        split(part, budget, MAX_DEPTH, &mut pieces);
    }
    Geometry::GeometryCollection { geometries: pieces }
}

/// Polygons, lines and points of `geom`, one per component.
fn parts(geom: &Geometry) -> Vec<Geometry> {
    match geom {
        g if g.is_empty() => Vec::new(),
        Geometry::MultiPoint { coordinates } => coordinates
            .iter()
            .map(|c| Geometry::point(c[0], c[1]))
            .collect(),
        Geometry::MultiLineString { coordinates } => coordinates
            .iter()
            .map(|l| Geometry::line_string(l.clone()))
            .collect(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .map(|p| Geometry::polygon(p.clone()))
            .collect(),
        Geometry::GeometryCollection { geometries } => geometries.iter().flat_map(parts).collect(),
        other => vec![other.clone()],
    }
}

fn split(geom: Geometry, budget: usize, depth: usize, out: &mut Vec<Geometry>) {
    if geom.is_empty() {
        return;
    }
    if geom.coord_count() <= budget || depth == 0 {
        out.push(geom);
        return;
    }
    let bb = nodedb_types::geometry_bbox_in(&geom, nodedb_types::BboxSpace::Planar);
    let (a, b) = halves(&bb);
    for half in [a, b] {
        for piece in clip(&geom, &half) {
            split(piece, budget, depth - 1, out);
        }
    }
}

/// The two halves of `bb` across its longer side.
fn halves(bb: &BoundingBox) -> (BoundingBox, BoundingBox) {
    if bb.max_lng - bb.min_lng >= bb.max_lat - bb.min_lat {
        let mid = (bb.min_lng + bb.max_lng) / 2.0;
        (
            BoundingBox::new(bb.min_lng, bb.min_lat, mid, bb.max_lat),
            BoundingBox::new(mid, bb.min_lat, bb.max_lng, bb.max_lat),
        )
    } else {
        let mid = (bb.min_lat + bb.max_lat) / 2.0;
        (
            BoundingBox::new(bb.min_lng, bb.min_lat, bb.max_lng, mid),
            BoundingBox::new(bb.min_lng, mid, bb.max_lng, bb.max_lat),
        )
    }
}

/// The parts of one component inside `bb`, each a single geometry.
fn clip(geom: &Geometry, bb: &BoundingBox) -> Vec<Geometry> {
    let rect = Rect::new(
        geo::Coord {
            x: bb.min_lng,
            y: bb.min_lat,
        },
        geo::Coord {
            x: bb.max_lng,
            y: bb.max_lat,
        },
    )
    .to_polygon();
    match to_geo(geom) {
        geo::Geometry::Polygon(p) => MultiPolygon::new(vec![p])
            .intersection(&rect)
            .0
            .iter()
            .map(|p| Geometry::polygon(rings(p)))
            .collect(),
        geo::Geometry::LineString(l) => {
            let lines: MultiLineString<f64> = MultiLineString::new(vec![l]);
            rect.clip(&lines, false)
                .0
                .iter()
                .map(|l: &LineString<f64>| from_geo(&geo::Geometry::LineString(l.clone())))
                .collect()
        }
        // A point never exceeds the budget, so it is never cut.
        _ => vec![geom.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::planar_area;

    fn circle(n: usize) -> Geometry {
        let ring: Vec<[f64; 2]> = (0..=n)
            .map(|i| {
                let a = std::f64::consts::TAU * (i % n) as f64 / n as f64;
                [a.cos() * 10.0, a.sin() * 10.0]
            })
            .collect();
        Geometry::polygon(vec![ring])
    }

    #[test]
    fn pieces_respect_the_budget_and_keep_the_area() {
        let c = circle(200);
        let out = subdivide(&c, 32);
        let Geometry::GeometryCollection { geometries } = &out else {
            panic!();
        };
        assert!(geometries.len() > 1);
        assert!(geometries.iter().all(|g| g.coord_count() <= 32));
        assert!((planar_area(&out) - planar_area(&c)).abs() < 1e-6);
    }

    #[test]
    fn small_geometry_is_one_piece() {
        let out = subdivide(&Geometry::point(1.0, 2.0), 256);
        assert_eq!(
            out,
            Geometry::GeometryCollection {
                geometries: vec![Geometry::point(1.0, 2.0)]
            }
        );
    }
}
