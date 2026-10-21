// SPDX-License-Identifier: Apache-2.0

//! `ST_Split(input, blade)`: cut a line by points, lines or polygon
//! boundaries, or a polygon by lines or polygon boundaries. The pieces
//! return as a GEOMETRYCOLLECTION, as PostGIS returns them. Planar in the
//! coordinates.
//!
//! A polygon splits by polygonizing: its rings and the blade's parts inside
//! it are noded at every crossing, the faces of the planar graph are
//! traced, and every face inside the input becomes a piece. A blade stretch
//! that does not reach across a face leaves that face whole.

use std::collections::{HashMap, HashSet};

use geo::{BooleanOps, InteriorPoint};
use nodedb_types::geometry::{Geometry, point_in_polygon};

use super::convert::{polygon, rings as geo_rings};
use super::noding::{Meet, fraction_along, meet};
use crate::error::SpatialError;
use crate::parts::polygon_covers;
use crate::planar::area::signed_area;
use crate::predicates::edge::{point_on_ring_boundary, point_on_segment, ring_edges};

/// Vertices closer than this fraction of the extent merge when noding.
const SNAP_FRACTION: f64 = 1e-9;

/// A kept polygon face: outer ring, signed area, and the holes it owns.
type Face = (Vec<[f64; 2]>, f64, Vec<Vec<[f64; 2]>>);

/// Blade parts by dimension.
struct Blade {
    points: Vec<[f64; 2]>,
    lines: Vec<Vec<[f64; 2]>>,
}

impl Blade {
    fn of(geom: &Geometry) -> Result<Self, SpatialError> {
        let mut blade = Blade {
            points: Vec::new(),
            lines: Vec::new(),
        };
        blade.add(geom)?;
        Ok(blade)
    }

    fn add(&mut self, geom: &Geometry) -> Result<(), SpatialError> {
        match geom {
            g if g.is_empty() => {}
            Geometry::Point { coordinates } => self.points.push(*coordinates),
            Geometry::MultiPoint { coordinates } => self.points.extend_from_slice(coordinates),
            Geometry::LineString { coordinates } => self.lines.push(coordinates.clone()),
            Geometry::MultiLineString { coordinates } => {
                self.lines.extend(coordinates.iter().cloned())
            }
            Geometry::Polygon { coordinates } => self.lines.extend(closed(coordinates)),
            Geometry::MultiPolygon { coordinates } => {
                self.lines
                    .extend(coordinates.iter().flat_map(|p| closed(p)));
            }
            Geometry::GeometryCollection { geometries } => {
                for member in geometries {
                    self.add(member)?;
                }
            }
        }
        Ok(())
    }
}

/// Rings as closed paths.
fn closed(rings: &[Vec<[f64; 2]>]) -> Vec<Vec<[f64; 2]>> {
    rings
        .iter()
        .map(|r| {
            let mut r = r.clone();
            if let (Some(first), Some(last)) = (r.first().copied(), r.last())
                && first != *last
            {
                r.push(first);
            }
            r
        })
        .collect()
}

fn unsupported(geom: &Geometry) -> SpatialError {
    SpatialError::Unsupported {
        operation: "ST_Split",
        kinds: format!("a {} input", geom.geometry_type()).into(),
    }
}

/// Split `input` by `blade`.
pub fn split(input: &Geometry, blade: &Geometry) -> Result<Geometry, SpatialError> {
    let blade = Blade::of(blade)?;
    let geometries = match input {
        g if g.is_empty() => Vec::new(),
        Geometry::LineString { coordinates } => split_line(coordinates, &blade)
            .into_iter()
            .map(Geometry::line_string)
            .collect(),
        Geometry::MultiLineString { coordinates } => coordinates
            .iter()
            .flat_map(|l| split_line(l, &blade))
            .map(Geometry::line_string)
            .collect(),
        Geometry::Polygon { coordinates } => split_polygon(coordinates, &blade.lines)
            .into_iter()
            .map(Geometry::polygon)
            .collect(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .flat_map(|p| split_polygon(p, &blade.lines))
            .map(Geometry::polygon)
            .collect(),
        other => return Err(unsupported(other)),
    };
    Ok(Geometry::GeometryCollection { geometries })
}

/// Cut one line at every blade point on it and every blade crossing.
fn split_line(line: &[[f64; 2]], blade: &Blade) -> Vec<Vec<[f64; 2]>> {
    let mut cuts: Vec<(usize, f64)> = Vec::new();
    for (i, w) in line.windows(2).enumerate() {
        for p in &blade.points {
            if point_on_segment(*p, w[0], w[1]) {
                cuts.push((i, fraction_along(w[0], w[1], *p)));
            }
        }
        for bl in &blade.lines {
            for bw in bl.windows(2) {
                match meet(w[0], w[1], bw[0], bw[1]) {
                    Meet::Disjoint => {}
                    Meet::Point(p) => cuts.push((i, fraction_along(w[0], w[1], p))),
                    Meet::Overlap(s, e) => {
                        cuts.push((i, fraction_along(w[0], w[1], s)));
                        cuts.push((i, fraction_along(w[0], w[1], e)));
                    }
                }
            }
        }
    }
    cuts.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
    cuts.dedup();
    let mut pieces = Vec::new();
    let mut current = vec![line[0]];
    let mut cut_iter = cuts.into_iter().peekable();
    for (i, w) in line.windows(2).enumerate() {
        while let Some(&(ci, t)) = cut_iter.peek() {
            if ci != i {
                break;
            }
            cut_iter.next();
            let p = [
                w[0][0] + (w[1][0] - w[0][0]) * t,
                w[0][1] + (w[1][1] - w[0][1]) * t,
            ];
            if current.last() != Some(&p) {
                current.push(p);
            }
            if current.len() >= 2 {
                pieces.push(std::mem::replace(&mut current, vec![p]));
            }
        }
        if current.last() != Some(&w[1]) {
            current.push(w[1]);
        }
    }
    if current.len() >= 2 {
        pieces.push(current);
    }
    pieces
}

/// Split one polygon by blade lines. No blade inside it: the polygon
/// comes back whole.
fn split_polygon(rings: &[Vec<[f64; 2]>], blade: &[Vec<[f64; 2]>]) -> Vec<Vec<Vec<[f64; 2]>>> {
    if rings.is_empty() {
        return Vec::new();
    }
    let shape = polygon(rings);
    let blade_lines = geo::MultiLineString::new(
        blade
            .iter()
            .map(|l| {
                geo::LineString::new(l.iter().map(|c| geo::Coord { x: c[0], y: c[1] }).collect())
            })
            .collect(),
    );
    let inside = shape.clip(&blade_lines, false);
    let mut segments: Vec<([f64; 2], [f64; 2])> =
        rings.iter().flat_map(|r| ring_edges(r)).collect();
    let ring_count = segments.len();
    for l in &inside.0 {
        segments.extend(l.0.windows(2).map(|w| ([w[0].x, w[0].y], [w[1].x, w[1].y])));
    }
    if segments.len() == ring_count {
        return vec![rings.to_vec()];
    }
    let graph = Graph::build(&noded(&segments));
    let faces = graph.faces();
    let mut kept: Vec<Face> = Vec::new();
    let mut holes = Vec::new();
    for face in faces {
        let area = signed_area(&face);
        if area > 0.0 {
            let candidate = polygon(std::slice::from_ref(&face));
            let interior = candidate.interior_point();
            if interior.is_some_and(|p| polygon_covers(rings, [p.x(), p.y()])) {
                kept.push((face, area, Vec::new()));
            }
        } else if area < 0.0 {
            holes.push(face);
        }
    }
    for hole in holes {
        let Some(probe) = hole.first().copied() else {
            continue;
        };
        let owner = kept
            .iter_mut()
            .filter(|(outer, _, _)| {
                point_in_polygon(probe[0], probe[1], outer) && !point_on_ring_boundary(probe, outer)
            })
            .min_by(|a, b| a.1.total_cmp(&b.1));
        if let Some((_, _, owned)) = owner {
            let mut ring = hole;
            ring.reverse();
            owned.push(ring);
        }
    }
    kept.into_iter()
        .map(|(outer, _, owned)| {
            let mut out = vec![outer];
            out.extend(owned);
            geo_rings(&polygon(&out))
        })
        .collect()
}

/// Every segment cut at every point another segment meets it.
fn noded(segments: &[([f64; 2], [f64; 2])]) -> Vec<([f64; 2], [f64; 2])> {
    let mut out = Vec::new();
    for (i, &(a, b)) in segments.iter().enumerate() {
        let mut ts = vec![0.0, 1.0];
        for (j, &(c, d)) in segments.iter().enumerate() {
            if i == j {
                continue;
            }
            match meet(a, b, c, d) {
                Meet::Disjoint => {}
                Meet::Point(p) => ts.push(fraction_along(a, b, p)),
                Meet::Overlap(s, e) => {
                    ts.push(fraction_along(a, b, s));
                    ts.push(fraction_along(a, b, e));
                }
            }
        }
        ts.sort_by(f64::total_cmp);
        ts.dedup();
        let at = |t: f64| [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
        out.extend(ts.windows(2).map(|w| (at(w[0]), at(w[1]))));
    }
    out
}

/// A planar graph of noded segments.
struct Graph {
    vertices: Vec<[f64; 2]>,
    /// Neighbours of each vertex, counter-clockwise by angle.
    around: Vec<Vec<usize>>,
}

impl Graph {
    fn build(segments: &[([f64; 2], [f64; 2])]) -> Self {
        let extent = segments
            .iter()
            .flat_map(|(a, b)| [a[0].abs(), a[1].abs(), b[0].abs(), b[1].abs()])
            .fold(1.0f64, f64::max);
        let eps = extent * SNAP_FRACTION;
        let mut ids: HashMap<(i64, i64), usize> = HashMap::new();
        let mut vertices = Vec::new();
        let mut id = |p: [f64; 2]| {
            let key = ((p[0] / eps).round() as i64, (p[1] / eps).round() as i64);
            *ids.entry(key).or_insert_with(|| {
                vertices.push(p);
                vertices.len() - 1
            })
        };
        let mut edges: HashSet<(usize, usize)> = HashSet::new();
        for &(a, b) in segments {
            let (u, v) = (id(a), id(b));
            if u != v {
                edges.insert((u.min(v), u.max(v)));
            }
        }
        let mut around = vec![Vec::new(); vertices.len()];
        for &(u, v) in &edges {
            around[u].push(v);
            around[v].push(u);
        }
        for (u, list) in around.iter_mut().enumerate() {
            let o = vertices[u];
            list.sort_by(|&a, &b| {
                let angle = |p: [f64; 2]| (p[1] - o[1]).atan2(p[0] - o[0]);
                angle(vertices[a]).total_cmp(&angle(vertices[b]))
            });
        }
        Graph { vertices, around }
    }

    /// Every face cycle, as a closed ring with spikes removed. Bounded
    /// faces run counter-clockwise, the outer boundary of each connected
    /// part clockwise.
    fn faces(&self) -> Vec<Vec<[f64; 2]>> {
        let mut seen: HashSet<(usize, usize)> = HashSet::new();
        let mut faces = Vec::new();
        for (u, list) in self.around.iter().enumerate() {
            for &v in list {
                if seen.contains(&(u, v)) {
                    continue;
                }
                let mut cycle = vec![u];
                let (mut a, mut b) = (u, v);
                loop {
                    seen.insert((a, b));
                    let next_list = &self.around[b];
                    let Some(idx) = next_list.iter().position(|&x| x == a) else {
                        break;
                    };
                    let c = next_list[(idx + next_list.len() - 1) % next_list.len()];
                    (a, b) = (b, c);
                    if (a, b) == (u, v) {
                        break;
                    }
                    cycle.push(a);
                }
                let mut ring: Vec<[f64; 2]> = despike(cycle)
                    .into_iter()
                    .map(|i| self.vertices[i])
                    .collect();
                if ring.len() >= 3 {
                    ring.push(ring[0]);
                    faces.push(ring);
                }
            }
        }
        faces
    }
}

/// Remove back-and-forth visits (`a, b, a`) a dangling edge leaves in a
/// cycle.
fn despike(mut cycle: Vec<usize>) -> Vec<usize> {
    let mut changed = true;
    while changed && cycle.len() >= 3 {
        changed = false;
        let n = cycle.len();
        for i in 0..n {
            let prev = cycle[(i + n - 1) % n];
            let next = cycle[(i + 1) % n];
            if prev == next {
                let (lo, hi) = (i, (i + 1) % n);
                if hi > lo {
                    cycle.drain(lo..=hi);
                } else {
                    cycle.remove(lo);
                    cycle.remove(0);
                }
                changed = true;
                break;
            }
        }
    }
    cycle
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::planar_area;

    fn square() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [4.0, 0.0],
            [4.0, 4.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ]])
    }

    fn members(g: Geometry) -> Vec<Geometry> {
        match g {
            Geometry::GeometryCollection { geometries } => geometries,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn line_splits_a_square_in_two() {
        let blade = Geometry::line_string(vec![[2.0, -1.0], [2.0, 5.0]]);
        let parts = members(split(&square(), &blade).unwrap());
        assert_eq!(parts.len(), 2, "{parts:?}");
        for p in &parts {
            assert!((planar_area(p) - 8.0).abs() < 1e-9, "{p:?}");
        }
    }

    #[test]
    fn blade_through_a_hole_keeps_the_hole_halves() {
        let holed = Geometry::polygon(vec![
            vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]],
            vec![[1.0, 1.0], [1.0, 3.0], [3.0, 3.0], [3.0, 1.0], [1.0, 1.0]],
        ]);
        let blade = Geometry::line_string(vec![[2.0, -1.0], [2.0, 5.0]]);
        let parts = members(split(&holed, &blade).unwrap());
        assert_eq!(parts.len(), 2);
        let total: f64 = parts.iter().map(planar_area).sum();
        assert!((total - 12.0).abs() < 1e-9);
    }

    #[test]
    fn blade_missing_the_polygon_returns_it_whole() {
        let blade = Geometry::line_string(vec![[10.0, 10.0], [11.0, 11.0]]);
        let parts = members(split(&square(), &blade).unwrap());
        assert_eq!(parts.len(), 1);
        assert!((planar_area(&parts[0]) - 16.0).abs() < 1e-9);
    }

    #[test]
    fn line_splits_at_points_and_crossings() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]);
        let by_point = members(split(&line, &Geometry::point(4.0, 0.0)).unwrap());
        assert_eq!(
            by_point,
            vec![
                Geometry::line_string(vec![[0.0, 0.0], [4.0, 0.0]]),
                Geometry::line_string(vec![[4.0, 0.0], [10.0, 0.0]]),
            ]
        );
        let cross = Geometry::line_string(vec![[5.0, -1.0], [5.0, 1.0]]);
        assert_eq!(members(split(&line, &cross).unwrap()).len(), 2);
    }

    #[test]
    fn point_input_is_refused() {
        assert!(split(&Geometry::point(0.0, 0.0), &square()).is_err());
    }
}
