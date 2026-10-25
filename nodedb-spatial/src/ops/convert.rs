// SPDX-License-Identifier: Apache-2.0

//! Conversion between NodeDB shapes and `geo` types.
//!
//! `geo` has no EMPTY point, so an EMPTY point converts to an empty
//! `GeometryCollection`. `geo` closes rings on construction.

use geo::{
    Coord, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point,
    Polygon,
};
use nodedb_types::geometry::Geometry;

fn coord(c: [f64; 2]) -> Coord<f64> {
    Coord { x: c[0], y: c[1] }
}

fn line(coords: &[[f64; 2]]) -> LineString<f64> {
    LineString::new(coords.iter().map(|c| coord(*c)).collect())
}

pub(crate) fn polygon(rings: &[Vec<[f64; 2]>]) -> Polygon<f64> {
    let mut iter = rings.iter();
    let exterior = iter
        .next()
        .map_or_else(|| LineString::new(Vec::new()), |r| line(r));
    Polygon::new(exterior, iter.map(|r| line(r)).collect())
}

/// A NodeDB shape as a `geo` geometry.
pub(crate) fn to_geo(geom: &Geometry) -> geo::Geometry<f64> {
    match geom {
        Geometry::Point { .. } if geom.is_empty() => {
            geo::Geometry::GeometryCollection(GeometryCollection::new_from(Vec::new()))
        }
        Geometry::Point { coordinates } => geo::Geometry::Point(Point(coord(*coordinates))),
        Geometry::LineString { coordinates } => geo::Geometry::LineString(line(coordinates)),
        Geometry::Polygon { coordinates } => geo::Geometry::Polygon(polygon(coordinates)),
        Geometry::MultiPoint { coordinates } => geo::Geometry::MultiPoint(MultiPoint::new(
            coordinates.iter().map(|c| Point(coord(*c))).collect(),
        )),
        Geometry::MultiLineString { coordinates } => geo::Geometry::MultiLineString(
            MultiLineString::new(coordinates.iter().map(|l| line(l)).collect()),
        ),
        Geometry::MultiPolygon { coordinates } => geo::Geometry::MultiPolygon(MultiPolygon::new(
            coordinates.iter().map(|p| polygon(p)).collect(),
        )),
        Geometry::GeometryCollection { geometries } => geo::Geometry::GeometryCollection(
            GeometryCollection::new_from(geometries.iter().map(to_geo).collect()),
        ),
    }
}

fn path(l: &LineString<f64>) -> Vec<[f64; 2]> {
    l.0.iter().map(|c| [c.x, c.y]).collect()
}

/// A `geo` polygon as rings. A polygon with an empty exterior is EMPTY.
pub(crate) fn rings(p: &Polygon<f64>) -> Vec<Vec<[f64; 2]>> {
    if p.exterior().0.is_empty() {
        return Vec::new();
    }
    std::iter::once(path(p.exterior()))
        .chain(p.interiors().iter().map(path))
        .collect()
}

/// A `geo` geometry as a NodeDB shape.
pub(crate) fn from_geo(geom: &geo::Geometry<f64>) -> Geometry {
    match geom {
        geo::Geometry::Point(p) => Geometry::point(p.x(), p.y()),
        geo::Geometry::Line(l) => {
            Geometry::line_string(vec![[l.start.x, l.start.y], [l.end.x, l.end.y]])
        }
        geo::Geometry::LineString(l) => Geometry::line_string(path(l)),
        geo::Geometry::Polygon(p) => Geometry::polygon(rings(p)),
        geo::Geometry::MultiPoint(mp) => Geometry::MultiPoint {
            coordinates: mp.0.iter().map(|p| [p.x(), p.y()]).collect(),
        },
        geo::Geometry::MultiLineString(ml) => Geometry::MultiLineString {
            coordinates: ml.0.iter().map(path).collect(),
        },
        geo::Geometry::MultiPolygon(mp) => multipolygon(mp),
        geo::Geometry::GeometryCollection(gc) => Geometry::GeometryCollection {
            geometries: gc.0.iter().map(from_geo).collect(),
        },
        geo::Geometry::Rect(r) => Geometry::polygon(rings(&r.to_polygon())),
        geo::Geometry::Triangle(t) => Geometry::polygon(rings(&t.to_polygon())),
    }
}

/// A `geo` multipolygon as a MultiPolygon shape, empty members dropped.
pub(crate) fn multipolygon(mp: &MultiPolygon<f64>) -> Geometry {
    Geometry::MultiPolygon {
        coordinates: mp.0.iter().map(rings).filter(|r| !r.is_empty()).collect(),
    }
}

/// Collapse a multi with one member to that member, as PostGIS returns a
/// `POLYGON` rather than a one-member `MULTIPOLYGON`.
pub(crate) fn simplest(geom: Geometry) -> Geometry {
    match geom {
        Geometry::MultiPolygon { mut coordinates } if coordinates.len() == 1 => Geometry::Polygon {
            coordinates: coordinates.remove(0),
        },
        Geometry::MultiLineString { mut coordinates } if coordinates.len() == 1 => {
            Geometry::LineString {
                coordinates: coordinates.remove(0),
            }
        }
        Geometry::MultiPoint { coordinates } if coordinates.len() == 1 => Geometry::Point {
            coordinates: coordinates[0],
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_keeps_shapes() {
        for g in [
            Geometry::point(1.0, 2.0),
            Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            Geometry::polygon(vec![
                vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]],
                vec![[1.0, 1.0], [1.0, 2.0], [2.0, 2.0], [2.0, 1.0], [1.0, 1.0]],
            ]),
            Geometry::MultiPoint {
                coordinates: vec![[1.0, 2.0], [3.0, 4.0]],
            },
        ] {
            assert_eq!(from_geo(&to_geo(&g)), g);
        }
    }

    #[test]
    fn simplest_unwraps_single_members() {
        let mp = Geometry::MultiPolygon {
            coordinates: vec![vec![vec![[0.0, 0.0], [1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]]],
        };
        assert!(matches!(simplest(mp), Geometry::Polygon { .. }));
    }
}
