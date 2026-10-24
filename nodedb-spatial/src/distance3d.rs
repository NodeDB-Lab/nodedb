// SPDX-License-Identifier: Apache-2.0

//! 3D distance between geometries in Cartesian metres.
//!
//! Every position maps to Cartesian metres first: a projected CRS scales x
//! and y by its unit and keeps Z, a geographic CRS goes to Earth-centred
//! (ECEF) coordinates on its ellipsoid with Z as ellipsoidal height. A
//! missing Z is 0. A polygon is a planar surface through its first three
//! non-collinear vertices, as PostGIS `ST_3DDistance` treats it.

use nodedb_types::geometry::{Geometry, SridGeometry, point_in_polygon};

use crate::predicates::edge::point_on_ring_boundary;

/// Cross-product length under which three vertices count as collinear.
const COLLINEAR_EPS: f64 = 1e-12;

/// Maps one position (x, y, z) to Cartesian metres.
pub(crate) type ToCartesian<'a> = &'a dyn Fn([f64; 2], f64) -> [f64; 3];

/// One polygon: its 2D rings for containment and its 3D plane.
struct Surface {
    rings: Vec<Vec<[f64; 2]>>,
    /// A point on the plane and its unit normal. `None` for a degenerate
    /// polygon, which then measures by its boundary alone.
    plane: Option<([f64; 3], [f64; 3])>,
}

/// A geometry as 3D edges, 3D vertices with their 2D position, and
/// surfaces.
#[derive(Default)]
struct Parts3 {
    segments: Vec<([f64; 3], [f64; 3])>,
    vertices: Vec<([f64; 2], [f64; 3])>,
    surfaces: Vec<Surface>,
}

struct Builder<'a> {
    g: &'a SridGeometry,
    to_xyz: ToCartesian<'a>,
    index: usize,
    parts: Parts3,
}

impl Builder<'_> {
    fn next(&mut self, c: [f64; 2]) -> [f64; 3] {
        let (z, _) = self.g.ordinates().at(self.index);
        self.index += 1;
        let p = (self.to_xyz)(c, z.unwrap_or(0.0));
        self.parts.vertices.push((c, p));
        p
    }

    fn path(&mut self, coords: &[[f64; 2]], close: bool) -> Vec<[f64; 3]> {
        let points: Vec<[f64; 3]> = coords.iter().map(|c| self.next(*c)).collect();
        match points.as_slice() {
            [] => {}
            [only] => self.parts.segments.push((*only, *only)),
            _ => {
                self.parts
                    .segments
                    .extend(points.windows(2).map(|w| (w[0], w[1])));
                if close && let (Some(first), Some(last)) = (points.first(), points.last()) {
                    self.parts.segments.push((*last, *first));
                }
            }
        }
        points
    }

    fn polygon(&mut self, rings: &[Vec<[f64; 2]>]) {
        let mut exterior = Vec::new();
        for (i, ring) in rings.iter().enumerate() {
            let points = self.path(ring, true);
            if i == 0 {
                exterior = points;
            }
        }
        self.parts.surfaces.push(Surface {
            rings: rings.to_vec(),
            plane: plane_of(&exterior),
        });
    }

    fn geometry(&mut self, geom: &Geometry) {
        match geom {
            Geometry::Point { .. } if geom.is_empty() => {}
            Geometry::Point { coordinates } => {
                let p = self.next(*coordinates);
                self.parts.segments.push((p, p));
            }
            Geometry::MultiPoint { coordinates } => {
                for c in coordinates {
                    let p = self.next(*c);
                    self.parts.segments.push((p, p));
                }
            }
            Geometry::LineString { coordinates } => {
                self.path(coordinates, false);
            }
            Geometry::MultiLineString { coordinates } => {
                for line in coordinates {
                    self.path(line, false);
                }
            }
            Geometry::Polygon { coordinates } => self.polygon(coordinates),
            Geometry::MultiPolygon { coordinates } => {
                for rings in coordinates {
                    self.polygon(rings);
                }
            }
            Geometry::GeometryCollection { geometries } => {
                for member in geometries {
                    self.geometry(member);
                }
            }
        }
    }
}

fn parts_of(g: &SridGeometry, to_xyz: ToCartesian<'_>) -> Parts3 {
    let mut builder = Builder {
        g,
        to_xyz,
        index: 0,
        parts: Parts3::default(),
    };
    builder.geometry(g.geometry());
    builder.parts
}

/// Minimum 3D distance between two geometries, metres. Infinite when
/// either is EMPTY.
pub(crate) fn distance_3d(a: &SridGeometry, b: &SridGeometry, to_xyz: ToCartesian<'_>) -> f64 {
    let pa = parts_of(a, to_xyz);
    let pb = parts_of(b, to_xyz);
    if pa.segments.is_empty() || pb.segments.is_empty() {
        return f64::INFINITY;
    }
    let mut best = f64::INFINITY;
    for &(a1, a2) in &pa.segments {
        for &(b1, b2) in &pb.segments {
            best = best.min(segment_segment(a1, a2, b1, b2));
        }
    }
    best.min(surface_distance(&pa, &pb))
        .min(surface_distance(&pb, &pa))
}

/// Least distance from a vertex of `points` lying over a surface of
/// `surfaces` to that surface's plane.
fn surface_distance(surfaces: &Parts3, points: &Parts3) -> f64 {
    let mut best = f64::INFINITY;
    for surface in &surfaces.surfaces {
        let Some((origin, normal)) = surface.plane else {
            continue;
        };
        for (xy, p) in &points.vertices {
            if covers(&surface.rings, *xy) {
                best = best.min(dot(sub(*p, origin), normal).abs());
            }
        }
    }
    best
}

fn covers(rings: &[Vec<[f64; 2]>], pt: [f64; 2]) -> bool {
    let Some(exterior) = rings.first() else {
        return false;
    };
    if !point_in_polygon(pt[0], pt[1], exterior) && !point_on_ring_boundary(pt, exterior) {
        return false;
    }
    !rings[1..]
        .iter()
        .any(|hole| point_in_polygon(pt[0], pt[1], hole) && !point_on_ring_boundary(pt, hole))
}

/// The plane through the first three non-collinear points.
fn plane_of(points: &[[f64; 3]]) -> Option<([f64; 3], [f64; 3])> {
    let origin = *points.first()?;
    for (i, b) in points.iter().enumerate().skip(1) {
        for c in points.iter().skip(i + 1) {
            let n = cross(sub(*b, origin), sub(*c, origin));
            let len = norm(n);
            if len > COLLINEAR_EPS * norm(sub(*b, origin)).max(1.0) {
                return Some((origin, [n[0] / len, n[1] / len, n[2] / len]));
            }
        }
    }
    None
}

/// Total 3D length of every linear component, metres.
pub(crate) fn length_3d(g: &SridGeometry, to_xyz: ToCartesian<'_>) -> f64 {
    let mut index = 0usize;
    let mut point = |c: [f64; 2]| {
        let (z, _) = g.ordinates().at(index);
        index += 1;
        to_xyz(c, z.unwrap_or(0.0))
    };
    let mut total = 0.0;
    walk_lines(g.geometry(), &mut |coords, linear| {
        let points: Vec<[f64; 3]> = coords.iter().map(|c| point(*c)).collect();
        if linear {
            total += points
                .windows(2)
                .map(|w| norm(sub(w[1], w[0])))
                .sum::<f64>();
        }
    });
    total
}

/// Visit every coordinate path in traversal order, flagging the linear
/// ones. Points and rings are visited so the Z index stays aligned.
fn walk_lines(geom: &Geometry, f: &mut dyn FnMut(&[[f64; 2]], bool)) {
    match geom {
        Geometry::Point { .. } if geom.is_empty() => {}
        Geometry::Point { coordinates } => f(std::slice::from_ref(coordinates), false),
        Geometry::MultiPoint { coordinates } => f(coordinates, false),
        Geometry::LineString { coordinates } => f(coordinates, true),
        Geometry::MultiLineString { coordinates } => {
            coordinates.iter().for_each(|line| f(line, true));
        }
        Geometry::Polygon { coordinates } => coordinates.iter().for_each(|ring| f(ring, false)),
        Geometry::MultiPolygon { coordinates } => {
            coordinates.iter().flatten().for_each(|ring| f(ring, false))
        }
        Geometry::GeometryCollection { geometries } => {
            geometries.iter().for_each(|member| walk_lines(member, f));
        }
    }
}

/// Distance between segments p1–q1 and p2–q2 (Ericson, Real-Time Collision
/// Detection, 5.1.9).
fn segment_segment(p1: [f64; 3], q1: [f64; 3], p2: [f64; 3], q2: [f64; 3]) -> f64 {
    let d1 = sub(q1, p1);
    let d2 = sub(q2, p2);
    let r = sub(p1, p2);
    let a = dot(d1, d1);
    let e = dot(d2, d2);
    let f = dot(d2, r);
    let tiny = f64::EPSILON;
    let (s, t) = if a <= tiny && e <= tiny {
        (0.0, 0.0)
    } else if a <= tiny {
        (0.0, (f / e).clamp(0.0, 1.0))
    } else {
        let c = dot(d1, r);
        if e <= tiny {
            ((-c / a).clamp(0.0, 1.0), 0.0)
        } else {
            let b = dot(d1, d2);
            let denom = a * e - b * b;
            let mut s = if denom > tiny {
                ((b * f - c * e) / denom).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let mut t = (b * s + f) / e;
            if t < 0.0 {
                t = 0.0;
                s = (-c / a).clamp(0.0, 1.0);
            } else if t > 1.0 {
                t = 1.0;
                s = ((b - c) / a).clamp(0.0, 1.0);
            }
            (s, t)
        }
    };
    let c1 = add(p1, scale(d1, s));
    let c2 = add(p2, scale(d2, t));
    norm(sub(c1, c2))
}

fn sub(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

fn add(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

fn scale(a: [f64; 3], k: f64) -> [f64; 3] {
    [a[0] * k, a[1] * k, a[2] * k]
}

fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn norm(a: [f64; 3]) -> f64 {
    dot(a, a).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Ordinates, Srid};

    fn planar(c: [f64; 2], z: f64) -> [f64; 3] {
        [c[0], c[1], z]
    }

    fn with_z(g: Geometry, z: Vec<f64>) -> SridGeometry {
        SridGeometry::with_ordinates(Srid::WGS84, g, Ordinates::new(Some(z), None)).unwrap()
    }

    #[test]
    fn points_measure_the_3d_chord() {
        let a = with_z(Geometry::point(0.0, 0.0), vec![0.0]);
        let b = with_z(Geometry::point(3.0, 0.0), vec![4.0]);
        assert!((distance_3d(&a, &b, &planar) - 5.0).abs() < 1e-12);
    }

    #[test]
    fn point_above_a_polygon_measures_to_its_plane() {
        let square = with_z(
            Geometry::polygon(vec![vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ]]),
            vec![2.0; 5],
        );
        let above = with_z(Geometry::point(5.0, 5.0), vec![9.0]);
        assert!((distance_3d(&square, &above, &planar) - 7.0).abs() < 1e-12);
    }

    #[test]
    fn crossing_segments_at_different_heights() {
        let a = with_z(
            Geometry::line_string(vec![[-1.0, 0.0], [1.0, 0.0]]),
            vec![0.0, 0.0],
        );
        let b = with_z(
            Geometry::line_string(vec![[0.0, -1.0], [0.0, 1.0]]),
            vec![3.0, 3.0],
        );
        assert!((distance_3d(&a, &b, &planar) - 3.0).abs() < 1e-12);
    }

    #[test]
    fn length_counts_z() {
        let line = with_z(
            Geometry::line_string(vec![[0.0, 0.0], [3.0, 0.0], [3.0, 0.0]]),
            vec![0.0, 4.0, 10.0],
        );
        assert!((length_3d(&line, &planar) - 11.0).abs() < 1e-12);
    }
}
