// SPDX-License-Identifier: Apache-2.0

//! `ST_Union`, `ST_Intersection`, `ST_Difference`, `ST_SymDifference`.
//!
//! Each operand splits into its areal, linear and punctual parts. Areal
//! parts go through `geo::BooleanOps` (exact for concave and multi
//! polygons, holes kept, overlapping members dissolved). Lines are clipped
//! by the other side's polygons. Points are kept or dropped by containment.
//! Rings are oriented by the right-hand rule and filled non-zero, so a
//! member drawn clockwise and two overlapping members both behave.
//!
//! Contacts between polygons of lower dimension than the operation (two
//! polygons sharing only an edge or a corner under `ST_Intersection`)
//! produce no output part.

use geo::algorithm::bool_ops::FillRule;
use geo::{BooleanOps, LineString, MultiLineString, MultiPolygon};
use nodedb_types::geometry::{Geometry, GeometryKind};

use super::convert::{multipolygon, polygon, simplest};
use super::noding::{Meet, meet};
use crate::orient::orient_rings;
use crate::parts::polygon_covers;
use crate::predicates::edge::point_on_segment;

/// One operand split by dimension.
#[derive(Default)]
struct Split {
    polygons: Vec<Vec<Vec<[f64; 2]>>>,
    lines: Vec<Vec<[f64; 2]>>,
    points: Vec<[f64; 2]>,
}

impl Split {
    fn of(geom: &Geometry) -> Self {
        let mut oriented = geom.clone();
        orient_rings(&mut oriented);
        let mut split = Split::default();
        split.add(&oriented);
        split
    }

    fn add(&mut self, geom: &Geometry) {
        match geom {
            Geometry::Point { .. } if geom.is_empty() => {}
            Geometry::Point { coordinates } => self.points.push(*coordinates),
            Geometry::MultiPoint { coordinates } => self.points.extend_from_slice(coordinates),
            Geometry::LineString { coordinates } if coordinates.len() >= 2 => {
                self.lines.push(coordinates.clone());
            }
            Geometry::LineString { .. } => {}
            Geometry::MultiLineString { coordinates } => self
                .lines
                .extend(coordinates.iter().filter(|l| l.len() >= 2).cloned()),
            Geometry::Polygon { coordinates } if !coordinates.is_empty() => {
                self.polygons.push(coordinates.clone());
            }
            Geometry::Polygon { .. } => {}
            Geometry::MultiPolygon { coordinates } => self
                .polygons
                .extend(coordinates.iter().filter(|p| !p.is_empty()).cloned()),
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().for_each(|g| self.add(g));
            }
        }
    }

    fn areal(&self) -> MultiPolygon<f64> {
        MultiPolygon::new(self.polygons.iter().map(|p| polygon(p)).collect())
    }

    fn linear(&self) -> MultiLineString<f64> {
        MultiLineString::new(
            self.lines
                .iter()
                .map(|l| {
                    LineString::new(l.iter().map(|c| geo::Coord { x: c[0], y: c[1] }).collect())
                })
                .collect(),
        )
    }

    /// Whether `pt` lies in or on any part.
    fn covers(&self, pt: [f64; 2]) -> bool {
        self.polygons.iter().any(|p| polygon_covers(p, pt))
            || self
                .lines
                .iter()
                .any(|l| l.windows(2).any(|w| point_on_segment(pt, w[0], w[1])))
            || self.points.contains(&pt)
    }

    /// The lowest dimension present, for the kind of an EMPTY result.
    fn kind(&self) -> GeometryKind {
        if !self.points.is_empty() {
            GeometryKind::Point
        } else if !self.lines.is_empty() {
            GeometryKind::LineString
        } else if !self.polygons.is_empty() {
            GeometryKind::Polygon
        } else {
            GeometryKind::GeometryCollection
        }
    }
}

fn op(a: &MultiPolygon<f64>, b: &MultiPolygon<f64>, kind: geo::OpType) -> MultiPolygon<f64> {
    a.boolean_op_with_fill_rule(b, kind, FillRule::NonZero)
}

/// Lines of `lines` inside (`invert` false) or outside (`invert` true) the
/// polygons of `by`. No polygons: everything is outside.
fn clip(
    lines: &MultiLineString<f64>,
    by: &MultiPolygon<f64>,
    invert: bool,
) -> MultiLineString<f64> {
    if lines.0.is_empty() {
        return MultiLineString::new(Vec::new());
    }
    if by.0.is_empty() {
        return if invert {
            lines.clone()
        } else {
            MultiLineString::new(Vec::new())
        };
    }
    by.clip_with_fill_rule(lines, invert, FillRule::NonZero)
}

fn from_lines(lines: &MultiLineString<f64>) -> Vec<Vec<[f64; 2]>> {
    lines
        .0
        .iter()
        .map(|l| l.0.iter().map(|c| [c.x, c.y]).collect::<Vec<_>>())
        .filter(|l: &Vec<[f64; 2]>| l.len() >= 2)
        .collect()
}

/// Build the result from its parts. No parts gives EMPTY of `empty`.
fn assemble(
    polygons: &MultiPolygon<f64>,
    lines: Vec<Vec<[f64; 2]>>,
    mut points: Vec<[f64; 2]>,
    empty: GeometryKind,
) -> Geometry {
    let mut parts = Vec::new();
    let areal = multipolygon(polygons);
    if !areal.is_empty() {
        parts.push(simplest(areal));
    }
    if !lines.is_empty() {
        parts.push(simplest(Geometry::MultiLineString { coordinates: lines }));
    }
    points.sort_by(|a, b| a[0].total_cmp(&b[0]).then(a[1].total_cmp(&b[1])));
    points.dedup();
    if !points.is_empty() {
        parts.push(simplest(Geometry::MultiPoint {
            coordinates: points,
        }));
    }
    match parts.len() {
        0 => Geometry::empty(empty),
        1 => parts.remove(0),
        _ => Geometry::GeometryCollection { geometries: parts },
    }
}

/// `ST_Union(a, b)`: the point set of both, polygons dissolved.
pub fn union(a: &Geometry, b: &Geometry) -> Geometry {
    let (sa, sb) = (Split::of(a), Split::of(b));
    let polygons = op(&sa.areal(), &sb.areal(), geo::OpType::Union);
    let mut all_lines = sa.linear();
    all_lines.0.extend(sb.linear().0);
    let lines = from_lines(&clip(&all_lines, &polygons, true));
    let line_parts = Split {
        lines: lines.clone(),
        ..Split::default()
    };
    let areal_parts = Split {
        polygons: multipolygon_rings(&polygons),
        ..Split::default()
    };
    let points = sa
        .points
        .iter()
        .chain(&sb.points)
        .copied()
        .filter(|p| !areal_parts.covers(*p) && !line_parts.covers(*p))
        .collect();
    assemble(&polygons, lines, points, GeometryKind::GeometryCollection)
}

/// `ST_Intersection(a, b)`: the point set the two share.
pub fn intersection(a: &Geometry, b: &Geometry) -> Geometry {
    let (sa, sb) = (Split::of(a), Split::of(b));
    let (pa, pb) = (sa.areal(), sb.areal());
    let polygons = if pa.0.is_empty() || pb.0.is_empty() {
        MultiPolygon::new(Vec::new())
    } else {
        op(&pa, &pb, geo::OpType::Intersection)
    };
    let mut lines = from_lines(&clip(&sa.linear(), &pb, false));
    lines.extend(from_lines(&clip(&sb.linear(), &pa, false)));
    let mut points: Vec<[f64; 2]> = sa
        .points
        .iter()
        .copied()
        .filter(|p| sb.covers(*p))
        .collect();
    points.extend(sb.points.iter().copied().filter(|p| sa.covers(*p)));
    for la in &sa.lines {
        for lb in &sb.lines {
            line_contacts(la, lb, &mut lines, &mut points);
        }
    }
    let empty = match (sa.kind(), sb.kind()) {
        (GeometryKind::GeometryCollection, _) | (_, GeometryKind::GeometryCollection) => {
            GeometryKind::GeometryCollection
        }
        (x, y) => lower(x, y),
    };
    assemble(&polygons, lines, points, empty)
}

/// `ST_Difference(a, b)`: the part of `a` outside `b`. Lower-dimensional
/// parts of `b` remove nothing from the polygons of `a`.
pub fn difference(a: &Geometry, b: &Geometry) -> Geometry {
    let (sa, sb) = (Split::of(a), Split::of(b));
    let pb = sb.areal();
    let polygons = op(&sa.areal(), &pb, geo::OpType::Difference);
    let lines = from_lines(&clip(&sa.linear(), &pb, true));
    let points = sa
        .points
        .iter()
        .copied()
        .filter(|p| !sb.covers(*p))
        .collect();
    assemble(&polygons, lines, points, a.kind())
}

/// `ST_SymDifference(a, b)`: the parts of each outside the other.
pub fn sym_difference(a: &Geometry, b: &Geometry) -> Geometry {
    let (sa, sb) = (Split::of(a), Split::of(b));
    let (pa, pb) = (sa.areal(), sb.areal());
    let polygons = op(&pa, &pb, geo::OpType::Xor);
    let mut lines = from_lines(&clip(&sa.linear(), &pb, true));
    lines.extend(from_lines(&clip(&sb.linear(), &pa, true)));
    let mut points: Vec<[f64; 2]> = sa
        .points
        .iter()
        .copied()
        .filter(|p| !sb.covers(*p))
        .collect();
    points.extend(sb.points.iter().copied().filter(|p| !sa.covers(*p)));
    assemble(&polygons, lines, points, GeometryKind::GeometryCollection)
}

fn lower(a: GeometryKind, b: GeometryKind) -> GeometryKind {
    let rank = |k: GeometryKind| match k {
        GeometryKind::Point | GeometryKind::MultiPoint => 0,
        GeometryKind::LineString | GeometryKind::MultiLineString => 1,
        GeometryKind::Polygon | GeometryKind::MultiPolygon => 2,
        GeometryKind::GeometryCollection => 3,
    };
    if rank(a) <= rank(b) { a } else { b }
}

fn multipolygon_rings(mp: &MultiPolygon<f64>) -> Vec<Vec<Vec<[f64; 2]>>> {
    mp.0.iter()
        .map(super::convert::rings)
        .filter(|r| !r.is_empty())
        .collect()
}

/// Crossing points and shared stretches of two lines.
fn line_contacts(
    a: &[[f64; 2]],
    b: &[[f64; 2]],
    lines: &mut Vec<Vec<[f64; 2]>>,
    points: &mut Vec<[f64; 2]>,
) {
    for wa in a.windows(2) {
        for wb in b.windows(2) {
            match meet(wa[0], wa[1], wb[0], wb[1]) {
                Meet::Disjoint => {}
                Meet::Point(p) => points.push(p),
                Meet::Overlap(s, e) => lines.push(vec![s, e]),
            }
        }
    }
    // A crossing point on a shared stretch is already in the stretch.
    points.retain(|p| {
        !lines
            .iter()
            .any(|l| l.windows(2).any(|w| point_on_segment(*p, w[0], w[1])))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::planar_area;

    fn square(x0: f64, y0: f64, side: f64) -> Geometry {
        Geometry::polygon(vec![vec![
            [x0, y0],
            [x0 + side, y0],
            [x0 + side, y0 + side],
            [x0, y0 + side],
            [x0, y0],
        ]])
    }

    /// A "U": the 3×3 square minus the 1×2 notch at the top middle.
    fn u_shape() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [3.0, 0.0],
            [3.0, 3.0],
            [2.0, 3.0],
            [2.0, 1.0],
            [1.0, 1.0],
            [1.0, 3.0],
            [0.0, 3.0],
            [0.0, 0.0],
        ]])
    }

    /// Areas computed with shapely 2.2.0 (GEOS) on the same inputs.
    #[test]
    fn concave_clip_matches_shapely() {
        // shapely: box(0.5,1.5,2.5,2.5).intersection(U).area == 1.0
        let bar = Geometry::polygon(vec![vec![
            [0.5, 1.5],
            [2.5, 1.5],
            [2.5, 2.5],
            [0.5, 2.5],
            [0.5, 1.5],
        ]]);
        let i = intersection(&u_shape(), &bar);
        assert!((planar_area(&i) - 1.0).abs() < 1e-9, "{i:?}");
        assert!(matches!(i, Geometry::MultiPolygon { ref coordinates } if coordinates.len() == 2));
        // Sutherland–Hodgman with the U as clip polygon gets this wrong.
        let reversed = intersection(&bar, &u_shape());
        assert!((planar_area(&reversed) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn union_dissolves_overlap() {
        // shapely: box(0,0,2,2).union(box(1,1,3,3)).area == 7.0
        let u = union(&square(0.0, 0.0, 2.0), &square(1.0, 1.0, 2.0));
        assert!(matches!(u, Geometry::Polygon { .. }), "{u:?}");
        assert!((planar_area(&u) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn difference_and_sym_difference() {
        let (a, b) = (square(0.0, 0.0, 2.0), square(1.0, 1.0, 2.0));
        // shapely: a.difference(b).area == 3.0, a.symmetric_difference(b).area == 6.0
        assert!((planar_area(&difference(&a, &b)) - 3.0).abs() < 1e-9);
        assert!((planar_area(&sym_difference(&a, &b)) - 6.0).abs() < 1e-9);
    }

    #[test]
    fn multipolygon_with_hole_intersection() {
        let holed = Geometry::polygon(vec![
            vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]],
            vec![[1.0, 1.0], [1.0, 3.0], [3.0, 3.0], [3.0, 1.0], [1.0, 1.0]],
        ]);
        let multi = Geometry::MultiPolygon {
            coordinates: vec![
                vec![vec![
                    [-1.0, -1.0],
                    [2.0, -1.0],
                    [2.0, 2.0],
                    [-1.0, 2.0],
                    [-1.0, -1.0],
                ]],
                vec![vec![
                    [3.5, 3.5],
                    [5.0, 3.5],
                    [5.0, 5.0],
                    [3.5, 5.0],
                    [3.5, 3.5],
                ]],
            ],
        };
        // shapely: holed.intersection(multi).area == 3.25
        let i = intersection(&holed, &multi);
        assert!((planar_area(&i) - 3.25).abs() < 1e-9, "{i:?}");
    }

    #[test]
    fn disjoint_polygons_intersect_to_empty_polygon() {
        let i = intersection(&square(0.0, 0.0, 1.0), &square(5.0, 5.0, 1.0));
        assert!(i.is_empty());
        assert_eq!(i.kind(), GeometryKind::Polygon);
    }

    #[test]
    fn line_is_clipped_by_polygon() {
        let line = Geometry::line_string(vec![[-1.0, 0.5], [3.0, 0.5]]);
        let i = intersection(&line, &square(0.0, 0.0, 2.0));
        let Geometry::LineString { coordinates } = &i else {
            panic!("{i:?}");
        };
        assert_eq!(coordinates.len(), 2);
        let d = difference(&line, &square(0.0, 0.0, 2.0));
        assert!(
            matches!(d, Geometry::MultiLineString { ref coordinates } if coordinates.len() == 2)
        );
    }

    #[test]
    fn crossing_lines_intersect_at_a_point() {
        let a = Geometry::line_string(vec![[0.0, 0.0], [2.0, 2.0]]);
        let b = Geometry::line_string(vec![[0.0, 2.0], [2.0, 0.0]]);
        assert_eq!(intersection(&a, &b), Geometry::point(1.0, 1.0));
    }

    #[test]
    fn points_inside_survive_intersection() {
        let pts = Geometry::MultiPoint {
            coordinates: vec![[0.5, 0.5], [5.0, 5.0]],
        };
        assert_eq!(
            intersection(&pts, &square(0.0, 0.0, 1.0)),
            Geometry::point(0.5, 0.5)
        );
        assert_eq!(
            difference(&pts, &square(0.0, 0.0, 1.0)),
            Geometry::point(5.0, 5.0)
        );
    }
}
