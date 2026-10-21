// SPDX-License-Identifier: Apache-2.0

//! Linear referencing and densification.
//!
//! - `ST_LineInterpolatePoint`: the point at a fraction of a line's length,
//!   geodesic on a geographic CRS.
//! - `ST_LocateAlong`, `ST_LocateBetween`: the parts of an M geometry at or
//!   between measures, optionally offset sideways.
//! - `ST_InterpolatePoint`: the M of a line at the point nearest a point.
//! - `ST_Segmentize`: no edge longer than a length, geodesic on a geographic
//!   CRS.
//!
//! Z and M interpolate linearly along each edge.

use nodedb_crs::method::geodesic::GeodesicLine;
use nodedb_types::geometry::{Geometry, Position, SridGeometry};

use super::noding::fraction_along;
use super::pieces::{Assembler, Piece, lerp, multiline, multipoint, pieces};
use crate::error::SpatialError;
use crate::metric::Metric;
use crate::predicates::edge::point_to_segment_dist_sq;

/// Why a linear-referencing call has no answer.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LinearError {
    #[error("{function} takes a LineString, got {kind}")]
    NotALine {
        function: &'static str,
        kind: &'static str,
    },
    #[error("{function} takes a geometry with M ordinates")]
    NoMeasure { function: &'static str },
    #[error("{function}: fraction {fraction} is outside [0, 1]")]
    Fraction {
        function: &'static str,
        fraction: f64,
    },
}

/// The positions of a single LineString.
fn line_positions(g: &SridGeometry, function: &'static str) -> Result<Vec<Position>, LinearError> {
    match (g.geometry(), pieces(g).as_slice()) {
        (Geometry::LineString { .. }, [Piece::Line(ps)]) => Ok(ps.clone()),
        (other, _) => Err(LinearError::NotALine {
            function,
            kind: other.geometry_type(),
        }),
    }
}

/// `ST_LineInterpolatePoint(line, fraction)`: the point `fraction` of the
/// way along `line`, measured in `metric`.
pub fn line_interpolate_point(
    g: &SridGeometry,
    fraction: f64,
    metric: &Metric,
) -> Result<SridGeometry, SpatialError> {
    const F: &str = "ST_LineInterpolatePoint";
    if !(0.0..=1.0).contains(&fraction) {
        return Err(LinearError::Fraction {
            function: F,
            fraction,
        }
        .into());
    }
    let ps = line_positions(g, F)?;
    let mut asm = Assembler::new(g.dimension());
    let Some(first) = ps.first() else {
        return Ok(asm.finish(g.srid, Geometry::empty_point())?);
    };
    let lengths: Vec<f64> = ps
        .windows(2)
        .map(|w| metric.segment_length([w[0].x, w[0].y], [w[1].x, w[1].y]))
        .collect();
    let total: f64 = lengths.iter().sum();
    let target = total * fraction;
    let mut walked = 0.0;
    let mut at = *ps.last().unwrap_or(first);
    for (i, len) in lengths.iter().enumerate() {
        if walked + len >= target && *len > 0.0 {
            let t = (target - walked) / len;
            at = along(&ps[i], &ps[i + 1], t, metric);
            break;
        }
        walked += len;
    }
    let c = asm.coord(&at)?;
    Ok(asm.finish(g.srid, Geometry::point(c[0], c[1]))?)
}

/// The position fraction `t` along edge a–b: a geodesic position on a
/// geographic CRS, a straight one on a projected CRS.
fn along(a: &Position, b: &Position, t: f64, metric: &Metric) -> Position {
    let straight = lerp(a, b, t);
    match metric {
        Metric::Planar { .. } => straight,
        Metric::Geodesic(m) => {
            let g = m.geodesic();
            let (s12, azi1, _) = g.inverse(a.y, a.x, b.y, b.x);
            let pos = GeodesicLine::new(g, a.y, a.x, azi1).position(s12 * t);
            Position {
                x: pos.lon2,
                y: pos.lat2,
                ..straight
            }
        }
    }
}

/// `ST_Segmentize(geom, max_length_m)`: insert vertices so no edge is
/// longer than `max_metres`, measured in `metric`.
pub fn segmentize(
    g: &SridGeometry,
    max_metres: f64,
    metric: &Metric,
) -> Result<SridGeometry, SpatialError> {
    let mut asm = Assembler::new(g.dimension());
    let dense = |ps: &[Position], asm: &mut Assembler| -> Result<Vec<[f64; 2]>, SpatialError> {
        let mut out = Vec::with_capacity(ps.len());
        for (i, p) in ps.iter().enumerate() {
            if i > 0 {
                let prev = &ps[i - 1];
                let len = metric.segment_length([prev.x, prev.y], [p.x, p.y]);
                let parts = (len / max_metres).ceil().clamp(1.0, MAX_SEGMENTS_PER_EDGE) as usize;
                for k in 1..parts {
                    out.push(asm.coord(&along(prev, p, k as f64 / parts as f64, metric))?);
                }
            }
            out.push(asm.coord(p)?);
        }
        Ok(out)
    };
    let shape = rebuild(g.geometry(), &pieces(g), &mut asm, &dense)?;
    Ok(asm.finish(g.srid, shape)?)
}

/// Cap on inserted vertices per edge, so a tiny length cannot allocate
/// without bound.
const MAX_SEGMENTS_PER_EDGE: f64 = 1_000_000.0;

type Densify<'a> = &'a dyn Fn(&[Position], &mut Assembler) -> Result<Vec<[f64; 2]>, SpatialError>;

/// Rebuild `geom`'s shape from its pieces, every path through `f`.
fn rebuild(
    geom: &Geometry,
    all: &[Piece],
    asm: &mut Assembler,
    f: Densify<'_>,
) -> Result<Geometry, SpatialError> {
    let mut iter = all.iter();
    rebuild_inner(geom, &mut iter, asm, f)
}

fn rebuild_inner<'p>(
    geom: &Geometry,
    iter: &mut impl Iterator<Item = &'p Piece>,
    asm: &mut Assembler,
    f: Densify<'_>,
) -> Result<Geometry, SpatialError> {
    let next_points = |n: usize, iter: &mut dyn Iterator<Item = &'p Piece>, asm: &mut Assembler| {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(Piece::Point(p)) = iter.next() {
                out.push(asm.coord(p)?);
            }
        }
        Ok::<_, SpatialError>(out)
    };
    Ok(match geom {
        g if g.is_empty() && !matches!(g, Geometry::GeometryCollection { .. }) => g.clone(),
        Geometry::Point { .. } => {
            let c = next_points(1, iter, asm)?;
            c.first()
                .map_or_else(Geometry::empty_point, |c| Geometry::point(c[0], c[1]))
        }
        Geometry::MultiPoint { coordinates } => Geometry::MultiPoint {
            coordinates: next_points(coordinates.len(), iter, asm)?,
        },
        Geometry::LineString { .. } => match iter.next() {
            Some(Piece::Line(ps)) => Geometry::line_string(f(ps, asm)?),
            _ => geom.clone(),
        },
        Geometry::MultiLineString { coordinates } => {
            let mut lines = Vec::with_capacity(coordinates.len());
            for _ in coordinates {
                if let Some(Piece::Line(ps)) = iter.next() {
                    lines.push(f(ps, asm)?);
                }
            }
            Geometry::MultiLineString { coordinates: lines }
        }
        Geometry::Polygon { .. } => match iter.next() {
            Some(Piece::Polygon(rings)) => {
                Geometry::polygon(rings.iter().map(|r| f(r, asm)).collect::<Result<_, _>>()?)
            }
            _ => geom.clone(),
        },
        Geometry::MultiPolygon { coordinates } => {
            let mut polygons = Vec::with_capacity(coordinates.len());
            for _ in coordinates {
                if let Some(Piece::Polygon(rings)) = iter.next() {
                    polygons.push(rings.iter().map(|r| f(r, asm)).collect::<Result<_, _>>()?);
                }
            }
            Geometry::MultiPolygon {
                coordinates: polygons,
            }
        }
        Geometry::GeometryCollection { geometries } => Geometry::GeometryCollection {
            geometries: geometries
                .iter()
                .map(|member| rebuild_inner(member, iter, asm, f))
                .collect::<Result<_, _>>()?,
        },
    })
}

fn require_m(g: &SridGeometry, function: &'static str) -> Result<(), LinearError> {
    if g.has_m() {
        Ok(())
    } else {
        Err(LinearError::NoMeasure { function })
    }
}

/// `p` moved `offset` to the left of the direction a → b.
fn offset_left(p: Position, a: &Position, b: &Position, offset: f64) -> Position {
    if offset == 0.0 {
        return p;
    }
    let (dx, dy) = (b.x - a.x, b.y - a.y);
    let len = dx.hypot(dy);
    if len == 0.0 {
        return p;
    }
    Position {
        x: p.x - dy / len * offset,
        y: p.y + dx / len * offset,
        ..p
    }
}

/// `ST_LocateAlong(geom, m [, offset])`: every point where the measure
/// equals `m`, moved `offset` coordinate units to the left of the line.
/// The result is a MULTIPOINT in the input's dimension.
pub fn locate_along(g: &SridGeometry, m: f64, offset: f64) -> Result<SridGeometry, SpatialError> {
    require_m(g, "ST_LocateAlong")?;
    let mut found = Vec::new();
    for piece in pieces(g) {
        match piece {
            Piece::Point(p) if p.m == Some(m) => found.push(p),
            Piece::Point(_) | Piece::Polygon(_) => {}
            Piece::Line(ps) => {
                for w in ps.windows(2) {
                    let (Some(m0), Some(m1)) = (w[0].m, w[1].m) else {
                        continue;
                    };
                    let (lo, hi) = (m0.min(m1), m0.max(m1));
                    if !(lo..=hi).contains(&m) {
                        continue;
                    }
                    let t = if m1 == m0 { 0.0 } else { (m - m0) / (m1 - m0) };
                    let p = Position {
                        m: Some(m),
                        ..lerp(&w[0], &w[1], t)
                    };
                    found.push(offset_left(p, &w[0], &w[1], offset));
                }
            }
        }
    }
    found.dedup();
    Ok(multipoint(g.srid, g.dimension(), &found)?)
}

/// `ST_LocateBetween(geom, m_start, m_end [, offset])`: the parts whose
/// measures lie in [m_start, m_end]. Points come back as a MULTIPOINT,
/// line pieces as a MULTILINESTRING, both when both occur in a
/// GEOMETRYCOLLECTION. Each line piece moves `offset` to its left.
pub fn locate_between(
    g: &SridGeometry,
    m_start: f64,
    m_end: f64,
    offset: f64,
) -> Result<SridGeometry, SpatialError> {
    require_m(g, "ST_LocateBetween")?;
    let (lo, hi) = (m_start.min(m_end), m_start.max(m_end));
    let inside = |m: Option<f64>| m.is_some_and(|m| (lo..=hi).contains(&m));
    let mut points = Vec::new();
    let mut lines: Vec<Vec<Position>> = Vec::new();
    for piece in pieces(g) {
        match piece {
            Piece::Point(p) if inside(p.m) => points.push(p),
            Piece::Point(_) | Piece::Polygon(_) => {}
            Piece::Line(ps) => {
                let mut current: Vec<Position> = Vec::new();
                for w in ps.windows(2) {
                    let (a, b) = (&w[0], &w[1]);
                    let (Some(ma), Some(mb)) = (a.m, b.m) else {
                        continue;
                    };
                    let clamp = |m: f64| {
                        if mb == ma {
                            0.0
                        } else {
                            ((m - ma) / (mb - ma)).clamp(0.0, 1.0)
                        }
                    };
                    let (t0, t1) = {
                        let (u, v) = (clamp(lo), clamp(hi));
                        (u.min(v), u.max(v))
                    };
                    let overlaps = ma.min(mb) <= hi && ma.max(mb) >= lo;
                    if !overlaps {
                        flush(&mut current, &mut lines, offset);
                        continue;
                    }
                    let (s, e) = (lerp(a, b, t0), lerp(a, b, t1));
                    if current.last() != Some(&s) {
                        flush(&mut current, &mut lines, offset);
                        current.push(s);
                    }
                    current.push(e);
                    if t1 < 1.0 {
                        flush(&mut current, &mut lines, offset);
                    }
                }
                flush(&mut current, &mut lines, offset);
            }
        }
    }
    let dimension = g.dimension();
    Ok(match (points.is_empty(), lines.is_empty()) {
        (_, true) => multipoint(g.srid, dimension, &points)?,
        (true, false) => multiline(g.srid, dimension, &lines)?,
        (false, false) => {
            let mut asm = Assembler::new(dimension);
            let pts = asm.path(&points)?;
            let ls = lines
                .iter()
                .map(|l| asm.path(l))
                .collect::<Result<_, _>>()?;
            asm.finish(
                g.srid,
                Geometry::GeometryCollection {
                    geometries: vec![
                        Geometry::MultiPoint { coordinates: pts },
                        Geometry::MultiLineString { coordinates: ls },
                    ],
                },
            )?
        }
    })
}

/// Close the running piece: a single position becomes nothing, two or
/// more become a line moved `offset` to the left.
fn flush(current: &mut Vec<Position>, lines: &mut Vec<Vec<Position>>, offset: f64) {
    let piece = std::mem::take(current);
    if piece.len() < 2 {
        return;
    }
    let moved = piece
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let (a, b) = if i + 1 < piece.len() {
                (&piece[i], &piece[i + 1])
            } else {
                (&piece[i - 1], &piece[i])
            };
            offset_left(*p, a, b, offset)
        })
        .collect();
    lines.push(moved);
}

/// `ST_InterpolatePoint(line, point)`: the M of `line` at the position
/// nearest `point`.
pub fn interpolate_point(
    line: &SridGeometry,
    point: [f64; 2],
) -> Result<Option<f64>, SpatialError> {
    const F: &str = "ST_InterpolatePoint";
    require_m(line, F)?;
    let ps = line_positions(line, F)?;
    let mut best: Option<(f64, f64)> = None;
    for w in ps.windows(2) {
        let (a, b) = ([w[0].x, w[0].y], [w[1].x, w[1].y]);
        let d = point_to_segment_dist_sq(point, a, b);
        if best.is_none_or(|(bd, _)| d < bd) {
            let t = fraction_along(a, b, point);
            best = lerp(&w[0], &w[1], t).m.map(|m| (d, m));
        }
    }
    Ok(best
        .map(|(_, m)| m)
        .or_else(|| ps.first().and_then(|p| p.m)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Ordinates, Srid};

    fn line_m(coords: Vec<[f64; 2]>, m: Vec<f64>) -> SridGeometry {
        SridGeometry::with_ordinates(
            Srid::new(32648).unwrap(),
            Geometry::line_string(coords),
            Ordinates::new(None, Some(m)),
        )
        .unwrap()
    }

    fn planar() -> Metric {
        Metric::Planar {
            metres_per_unit: 1.0,
        }
    }

    #[test]
    fn interpolate_along_a_planar_line() {
        let line = line_m(
            vec![[0.0, 0.0], [10.0, 0.0], [10.0, 10.0]],
            vec![0.0, 10.0, 20.0],
        );
        let p = line_interpolate_point(&line, 0.75, &planar()).unwrap();
        assert_eq!(p.geometry(), &Geometry::point(10.0, 5.0));
        assert_eq!(p.ordinates().m(), Some(&[15.0][..]));
        assert!(line_interpolate_point(&line, 1.5, &planar()).is_err());
    }

    #[test]
    fn geodesic_interpolation_lies_on_the_geodesic() {
        let m = Metric::wgs84();
        let line = SridGeometry::wgs84(Geometry::line_string(vec![[0.0, 0.0], [0.0, 10.0]]));
        let mid = line_interpolate_point(&line, 0.5, &m).unwrap();
        let Geometry::Point { coordinates } = mid.geometry() else {
            panic!();
        };
        let d0 = m.segment_length([0.0, 0.0], *coordinates);
        let d1 = m.segment_length(*coordinates, [0.0, 10.0]);
        assert!((d0 - d1).abs() < 1e-6, "{d0} {d1}");
    }

    #[test]
    fn locate_along_and_between() {
        let line = line_m(vec![[0.0, 0.0], [10.0, 0.0]], vec![0.0, 100.0]);
        let at = locate_along(&line, 25.0, 0.0).unwrap();
        assert_eq!(
            at.geometry(),
            &Geometry::MultiPoint {
                coordinates: vec![[2.5, 0.0]]
            }
        );
        let offset = locate_along(&line, 25.0, 1.0).unwrap();
        assert_eq!(
            offset.geometry(),
            &Geometry::MultiPoint {
                coordinates: vec![[2.5, 1.0]]
            }
        );
        let between = locate_between(&line, 20.0, 40.0, 0.0).unwrap();
        assert_eq!(
            between.geometry(),
            &Geometry::MultiLineString {
                coordinates: vec![vec![[2.0, 0.0], [4.0, 0.0]]]
            }
        );
        assert_eq!(between.ordinates().m(), Some(&[20.0, 40.0][..]));
    }

    #[test]
    fn interpolate_point_reads_m() {
        let line = line_m(vec![[0.0, 0.0], [10.0, 0.0]], vec![0.0, 100.0]);
        assert_eq!(interpolate_point(&line, [3.0, 4.0]).unwrap(), Some(30.0));
        let xy = SridGeometry::wgs84(Geometry::line_string(vec![[0.0, 0.0], [1.0, 0.0]]));
        assert!(interpolate_point(&xy, [0.0, 0.0]).is_err());
    }

    #[test]
    fn segmentize_bounds_edge_length_and_keeps_m() {
        let line = line_m(vec![[0.0, 0.0], [10.0, 0.0]], vec![0.0, 10.0]);
        let dense = segmentize(&line, 3.0, &planar()).unwrap();
        assert_eq!(dense.geometry().coord_count(), 5);
        assert_eq!(dense.ordinates().m(), Some(&[0.0, 2.5, 5.0, 7.5, 10.0][..]));
        let m = Metric::wgs84();
        let geo = SridGeometry::wgs84(Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]));
        let dense = segmentize(&geo, 10_000.0, &m).unwrap();
        let coords: Vec<_> = dense.geometry().coords().collect();
        for w in coords.windows(2) {
            assert!(m.segment_length(w[0], w[1]) <= 10_000.0 + 1e-6);
        }
    }
}
