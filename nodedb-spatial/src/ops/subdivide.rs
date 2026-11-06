// SPDX-License-Identifier: Apache-2.0

//! `ST_Subdivide(geom, max_vertices)`: cut a geometry along box halves until
//! every piece has at most `max_vertices` vertices. The pieces return as
//! one GEOMETRYCOLLECTION (a scalar function returns one value), each
//! piece a polygon, line or point. Planar in the coordinates.

use geo::{BooleanOps, MultiPolygon, Rect};
use nodedb_types::BoundingBox;
use nodedb_types::geometry::Geometry;

use super::convert::{rings, to_geo};

/// Smallest vertex budget, as PostGIS requires.
pub const MIN_SUBDIVIDE_VERTICES: usize = 5;

/// Recursion limit. A piece this many cuts deep stops, whatever its vertex
/// count, so a box that no longer halves cannot recurse further.
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
    let pieces = match &geom {
        Geometry::LineString { coordinates } => cut_line(coordinates, &bb),
        _ => {
            let (a, b) = halves(&bb);
            let mut pieces = clip(&geom, &a);
            pieces.extend(clip(&geom, &b));
            pieces
        }
    };
    for piece in pieces {
        split(piece, budget, depth - 1, out);
    }
}

/// Whether `bb` cuts across longitude: its wider side, or a tie.
fn cuts_longitude(bb: &BoundingBox) -> bool {
    bb.max_lng - bb.min_lng >= bb.max_lat - bb.min_lat
}

/// The runs of `line` on each side of the cut of `bb`, in line order. A
/// segment on the cut stays on the low side, so each segment lands in one
/// run. Repeated vertices drop. A line of one repeated vertex keeps two.
///
/// The cut is arithmetic. An overlay clip loses a line whose box has no area.
/// It also returns a segment on the cut from both halves.
fn cut_line(line: &[[f64; 2]], bb: &BoundingBox) -> Vec<Geometry> {
    let (axis, mid) = if cuts_longitude(bb) {
        (0, (bb.min_lng + bb.max_lng) / 2.0)
    } else {
        (1, (bb.min_lat + bb.max_lat) / 2.0)
    };
    let mut cuts = LineCuts::default();
    for pair in line.windows(2) {
        let &[a, b] = pair else { continue };
        if a == b {
            continue;
        }
        let spans = (a[axis] < mid && b[axis] > mid) || (a[axis] > mid && b[axis] < mid);
        if spans {
            let t = (mid - a[axis]) / (b[axis] - a[axis]);
            let mut at = [a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])];
            at[axis] = mid;
            cuts.push_segment(a[axis] > mid, a, at);
            cuts.push_segment(b[axis] > mid, at, b);
        } else {
            cuts.push_segment(a[axis] > mid || b[axis] > mid, a, b);
        }
    }
    let mut pieces = cuts.finish();
    if pieces.is_empty()
        && let Some(&vertex) = line.first()
    {
        pieces.push(Geometry::line_string(vec![vertex, vertex]));
    }
    pieces
}

/// The finished runs of a cut line and the run still growing.
#[derive(Default)]
struct LineCuts {
    pieces: Vec<Geometry>,
    run: Vec<[f64; 2]>,
    run_is_high: bool,
}

impl LineCuts {
    /// Add segment `a`-`b`, on the high side when `high`. A segment on the
    /// other side of the cut starts a new run.
    fn push_segment(&mut self, high: bool, a: [f64; 2], b: [f64; 2]) {
        if self.run.is_empty() {
            self.run = vec![a, b];
            self.run_is_high = high;
        } else if self.run_is_high == high {
            self.run.push(b);
        } else {
            self.pieces
                .push(Geometry::line_string(std::mem::take(&mut self.run)));
            self.run = vec![a, b];
            self.run_is_high = high;
        }
    }

    fn finish(mut self) -> Vec<Geometry> {
        if !self.run.is_empty() {
            self.pieces.push(Geometry::line_string(self.run));
        }
        self.pieces
    }
}

/// The two halves of `bb` across its longer side.
fn halves(bb: &BoundingBox) -> (BoundingBox, BoundingBox) {
    if cuts_longitude(bb) {
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

/// The parts of one polygon inside `bb`, each a single geometry. A polygon
/// with no area has no parts.
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
        // A point never exceeds the budget, so it is never cut.
        _ => vec![geom.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::{planar_area, planar_length};

    fn pieces(out: &Geometry) -> &[Geometry] {
        let Geometry::GeometryCollection { geometries } = out else {
            panic!("{out:?}");
        };
        geometries
    }

    fn assert_line_kept(line: &Geometry, budget: usize) {
        let out = subdivide(line, budget);
        assert!(pieces(&out).len() > 1);
        assert!(pieces(&out).iter().all(|g| g.coord_count() <= budget));
        assert!((planar_length(&out) - planar_length(line)).abs() < 1e-6);
    }

    #[test]
    fn a_long_axis_parallel_line_keeps_every_segment() {
        let horizontal: Vec<[f64; 2]> = (0..1000).map(|i| [f64::from(i), 7.0]).collect();
        assert_line_kept(&Geometry::line_string(horizontal), 32);
        let vertical: Vec<[f64; 2]> = (0..1000).map(|i| [7.0, f64::from(i)]).collect();
        assert_line_kept(&Geometry::line_string(vertical), 32);
    }

    /// The vertical run lies on the first cut, and only one piece holds it.
    #[test]
    fn a_segment_on_the_cut_lands_in_one_piece() {
        let mut line = vec![[-20.0, 0.0]];
        line.extend((0..=50).map(|i| [0.0, f64::from(i) / 10.0]));
        line.push([20.0, 5.0]);
        assert_line_kept(&Geometry::line_string(line), 8);
    }

    #[test]
    fn repeated_vertices_cut_without_loss() {
        let mut line = vec![[0.0, 0.0]; 500];
        line.extend(vec![[10.0, 0.0]; 500]);
        let out = subdivide(&Geometry::line_string(line), 5);
        assert!(pieces(&out).iter().all(|g| g.coord_count() <= 5));
        assert!((planar_length(&out) - 10.0).abs() < 1e-9);
    }

    #[test]
    fn a_line_of_one_repeated_vertex_collapses_to_one_piece() {
        let out = subdivide(&Geometry::line_string(vec![[3.0, 3.0]; 1000]), 5);
        assert_eq!(
            pieces(&out),
            [Geometry::line_string(vec![[3.0, 3.0], [3.0, 3.0]])]
        );
    }

    /// Two adjacent floats leave no coordinate between them, so the box never
    /// halves. The depth limit stops the piece whole.
    #[test]
    fn a_box_that_cannot_halve_stops_at_the_depth_limit() {
        let far = 1.0f64.next_up();
        let line: Vec<[f64; 2]> = (0..1000)
            .map(|i| [if i % 2 == 0 { 1.0 } else { far }, 0.0])
            .collect();
        let out = subdivide(&Geometry::line_string(line), 5);
        assert_eq!(pieces(&out).len(), 1);
        assert_eq!(pieces(&out)[0].coord_count(), 1000);
    }

    #[test]
    fn a_polygon_with_no_area_ends_without_pieces_of_area() {
        let mut ring: Vec<[f64; 2]> = (0..200).map(|i| [f64::from(i), 0.0]).collect();
        ring.extend((0..=200).rev().map(|i| [f64::from(i), 0.0]));
        let out = subdivide(&Geometry::polygon(vec![ring]), 5);
        assert_eq!(planar_area(&out), 0.0);
    }

    #[test]
    fn a_polygon_with_repeated_vertices_keeps_its_area() {
        let mut ring = Vec::new();
        for corner in [[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0]] {
            ring.extend(vec![corner; 100]);
        }
        ring.push([0.0, 0.0]);
        let out = subdivide(&Geometry::polygon(vec![ring]), 5);
        assert!((planar_area(&out) - 16.0).abs() < 1e-6);
    }

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
