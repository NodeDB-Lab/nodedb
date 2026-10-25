// SPDX-License-Identifier: Apache-2.0

//! GeoJSON-compatible geometry shapes.
//!
//! Supports Point, LineString, Polygon, MultiPoint, MultiLineString,
//! MultiPolygon, and GeometryCollection. A shape holds coordinates only. The
//! reference system those coordinates are in lives on
//! [`SridGeometry`](super::SridGeometry).

use serde::{Deserialize, Serialize};

/// A 2D coordinate (x, y) following GeoJSON order.
///
/// In a geographic reference system x is longitude and y is latitude, in
/// degrees. In a projected one x is easting and y is northing, in metres.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Coord {
    pub lng: f64,
    pub lat: f64,
}

impl Coord {
    pub fn new(lng: f64, lat: f64) -> Self {
        Self { lng, lat }
    }
}

/// The kind of a geometry, without its coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeometryKind {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl GeometryKind {
    /// The OGC type name, as `ST_GeometryType` and GeoJSON spell it.
    pub fn name(self) -> &'static str {
        match self {
            GeometryKind::Point => "Point",
            GeometryKind::LineString => "LineString",
            GeometryKind::Polygon => "Polygon",
            GeometryKind::MultiPoint => "MultiPoint",
            GeometryKind::MultiLineString => "MultiLineString",
            GeometryKind::MultiPolygon => "MultiPolygon",
            GeometryKind::GeometryCollection => "GeometryCollection",
        }
    }

    /// The upper-case WKT keyword.
    pub fn wkt_keyword(self) -> &'static str {
        match self {
            GeometryKind::Point => "POINT",
            GeometryKind::LineString => "LINESTRING",
            GeometryKind::Polygon => "POLYGON",
            GeometryKind::MultiPoint => "MULTIPOINT",
            GeometryKind::MultiLineString => "MULTILINESTRING",
            GeometryKind::MultiPolygon => "MULTIPOLYGON",
            GeometryKind::GeometryCollection => "GEOMETRYCOLLECTION",
        }
    }

    /// The ISO WKB base type code, 1 to 7.
    pub fn wkb_code(self) -> u32 {
        match self {
            GeometryKind::Point => 1,
            GeometryKind::LineString => 2,
            GeometryKind::Polygon => 3,
            GeometryKind::MultiPoint => 4,
            GeometryKind::MultiLineString => 5,
            GeometryKind::MultiPolygon => 6,
            GeometryKind::GeometryCollection => 7,
        }
    }

    /// The kind with WKB base type code `code`.
    pub fn from_wkb_code(code: u32) -> Option<Self> {
        Some(match code {
            1 => GeometryKind::Point,
            2 => GeometryKind::LineString,
            3 => GeometryKind::Polygon,
            4 => GeometryKind::MultiPoint,
            5 => GeometryKind::MultiLineString,
            6 => GeometryKind::MultiPolygon,
            7 => GeometryKind::GeometryCollection,
            _ => return None,
        })
    }

    /// The kind GeoJSON names `name`, case-sensitive as RFC 7946 requires.
    pub fn from_geojson_name(name: &str) -> Option<Self> {
        Some(match name {
            "Point" => GeometryKind::Point,
            "LineString" => GeometryKind::LineString,
            "Polygon" => GeometryKind::Polygon,
            "MultiPoint" => GeometryKind::MultiPoint,
            "MultiLineString" => GeometryKind::MultiLineString,
            "MultiPolygon" => GeometryKind::MultiPolygon,
            "GeometryCollection" => GeometryKind::GeometryCollection,
            _ => return None,
        })
    }
}

/// Whether a coordinate is the NaN pair of an EMPTY point.
pub fn is_empty_coord(c: [f64; 2]) -> bool {
    c[0].is_nan() && c[1].is_nan()
}

/// GeoJSON-compatible geometry types.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "type")]
pub enum Geometry {
    Point {
        coordinates: [f64; 2],
    },
    LineString {
        coordinates: Vec<[f64; 2]>,
    },
    Polygon {
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    MultiPoint {
        coordinates: Vec<[f64; 2]>,
    },
    MultiLineString {
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    MultiPolygon {
        coordinates: Vec<Vec<Vec<[f64; 2]>>>,
    },
    GeometryCollection {
        geometries: Vec<Geometry>,
    },
}

impl Geometry {
    /// Create a Point from (x, y).
    pub fn point(lng: f64, lat: f64) -> Self {
        Geometry::Point {
            coordinates: [lng, lat],
        }
    }

    /// Create a LineString from a series of [x, y] pairs.
    pub fn line_string(coords: Vec<[f64; 2]>) -> Self {
        Geometry::LineString {
            coordinates: coords,
        }
    }

    /// Create a Polygon from exterior ring (and optional holes).
    ///
    /// The first ring is the exterior, subsequent rings are holes.
    /// Each ring must be a closed loop (first point == last point).
    pub fn polygon(rings: Vec<Vec<[f64; 2]>>) -> Self {
        Geometry::Polygon { coordinates: rings }
    }

    /// Get the type name of this geometry.
    pub fn geometry_type(&self) -> &'static str {
        self.kind().name()
    }

    /// The EMPTY point. Its two ordinates are NaN, the encoding WKB uses.
    pub fn empty_point() -> Self {
        Geometry::Point {
            coordinates: [f64::NAN, f64::NAN],
        }
    }

    /// An EMPTY geometry of `kind`.
    pub fn empty(kind: GeometryKind) -> Self {
        match kind {
            GeometryKind::Point => Self::empty_point(),
            GeometryKind::LineString => Geometry::LineString {
                coordinates: Vec::new(),
            },
            GeometryKind::Polygon => Geometry::Polygon {
                coordinates: Vec::new(),
            },
            GeometryKind::MultiPoint => Geometry::MultiPoint {
                coordinates: Vec::new(),
            },
            GeometryKind::MultiLineString => Geometry::MultiLineString {
                coordinates: Vec::new(),
            },
            GeometryKind::MultiPolygon => Geometry::MultiPolygon {
                coordinates: Vec::new(),
            },
            GeometryKind::GeometryCollection => Geometry::GeometryCollection {
                geometries: Vec::new(),
            },
        }
    }

    /// The kind of this geometry.
    pub fn kind(&self) -> GeometryKind {
        match self {
            Geometry::Point { .. } => GeometryKind::Point,
            Geometry::LineString { .. } => GeometryKind::LineString,
            Geometry::Polygon { .. } => GeometryKind::Polygon,
            Geometry::MultiPoint { .. } => GeometryKind::MultiPoint,
            Geometry::MultiLineString { .. } => GeometryKind::MultiLineString,
            Geometry::MultiPolygon { .. } => GeometryKind::MultiPolygon,
            Geometry::GeometryCollection { .. } => GeometryKind::GeometryCollection,
        }
    }

    /// Whether the geometry has no coordinates (OGC EMPTY). A point is
    /// EMPTY when both its ordinates are NaN.
    pub fn is_empty(&self) -> bool {
        match self {
            Geometry::Point { coordinates } => is_empty_coord(*coordinates),
            Geometry::LineString { coordinates } | Geometry::MultiPoint { coordinates } => {
                coordinates.is_empty()
            }
            Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
                coordinates.iter().all(Vec::is_empty)
            }
            Geometry::MultiPolygon { coordinates } => coordinates
                .iter()
                .all(|polygon| polygon.iter().all(Vec::is_empty)),
            Geometry::GeometryCollection { geometries } => geometries.iter().all(Self::is_empty),
        }
    }

    /// Number of XY coordinates, in the order [`Geometry::coords`] yields
    /// them. An EMPTY point has none.
    pub fn coord_count(&self) -> usize {
        match self {
            Geometry::Point { coordinates } => usize::from(!is_empty_coord(*coordinates)),
            Geometry::LineString { coordinates } | Geometry::MultiPoint { coordinates } => {
                coordinates.len()
            }
            Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
                coordinates.iter().map(Vec::len).sum()
            }
            Geometry::MultiPolygon { coordinates } => coordinates
                .iter()
                .flat_map(|polygon| polygon.iter())
                .map(Vec::len)
                .sum(),
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().map(Self::coord_count).sum()
            }
        }
    }

    /// Every XY coordinate in traversal order: rings in order, polygons in
    /// order, collection members in order. Z and M arrays index the same
    /// sequence.
    pub fn coords(&self) -> Box<dyn Iterator<Item = [f64; 2]> + '_> {
        match self {
            Geometry::Point { coordinates } => Box::new(
                (!is_empty_coord(*coordinates))
                    .then_some(*coordinates)
                    .into_iter(),
            ),
            Geometry::LineString { coordinates } | Geometry::MultiPoint { coordinates } => {
                Box::new(coordinates.iter().copied())
            }
            Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
                Box::new(coordinates.iter().flatten().copied())
            }
            Geometry::MultiPolygon { coordinates } => {
                Box::new(coordinates.iter().flatten().flatten().copied())
            }
            Geometry::GeometryCollection { geometries } => {
                Box::new(geometries.iter().flat_map(Self::coords))
            }
        }
    }

    /// Map every coordinate through `f`, keeping the shape. An EMPTY point
    /// stays EMPTY and `f` never sees it.
    ///
    /// Stops at the first coordinate `f` rejects and returns its error.
    pub fn try_map_coords<E>(
        &self,
        f: &mut impl FnMut([f64; 2]) -> Result<[f64; 2], E>,
    ) -> Result<Geometry, E> {
        Ok(match self {
            Geometry::Point { coordinates } if is_empty_coord(*coordinates) => self.clone(),
            Geometry::Point { coordinates } => Geometry::Point {
                coordinates: f(*coordinates)?,
            },
            Geometry::LineString { coordinates } => Geometry::LineString {
                coordinates: map_path(coordinates, f)?,
            },
            Geometry::Polygon { coordinates } => Geometry::Polygon {
                coordinates: map_rings(coordinates, f)?,
            },
            Geometry::MultiPoint { coordinates } => Geometry::MultiPoint {
                coordinates: map_path(coordinates, f)?,
            },
            Geometry::MultiLineString { coordinates } => Geometry::MultiLineString {
                coordinates: map_rings(coordinates, f)?,
            },
            Geometry::MultiPolygon { coordinates } => Geometry::MultiPolygon {
                coordinates: coordinates
                    .iter()
                    .map(|polygon| map_rings(polygon, f))
                    .collect::<Result<_, E>>()?,
            },
            Geometry::GeometryCollection { geometries } => Geometry::GeometryCollection {
                geometries: geometries
                    .iter()
                    .map(|g| g.try_map_coords(f))
                    .collect::<Result<_, E>>()?,
            },
        })
    }
}

fn map_path<E>(
    path: &[[f64; 2]],
    f: &mut impl FnMut([f64; 2]) -> Result<[f64; 2], E>,
) -> Result<Vec<[f64; 2]>, E> {
    path.iter().map(|c| f(*c)).collect()
}

fn map_rings<E>(
    rings: &[Vec<[f64; 2]>],
    f: &mut impl FnMut([f64; 2]) -> Result<[f64; 2], E>,
) -> Result<Vec<Vec<[f64; 2]>>, E> {
    rings.iter().map(|ring| map_path(ring, f)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_creation() {
        let p = Geometry::point(-73.9857, 40.7484);
        assert_eq!(p.geometry_type(), "Point");
        if let Geometry::Point { coordinates } = &p {
            assert!((coordinates[0] - (-73.9857)).abs() < 1e-6);
            assert!((coordinates[1] - 40.7484).abs() < 1e-6);
        }
    }

    #[test]
    fn geojson_serialize() {
        let p = Geometry::point(1.0, 2.0);
        let json = sonic_rs::to_string(&p).unwrap();
        assert!(json.contains("\"type\":\"Point\""));
        assert!(json.contains("\"coordinates\":[1.0,2.0]"));
    }

    #[test]
    fn geojson_roundtrip() {
        let original = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]);
        let json = sonic_rs::to_string(&original).unwrap();
        let parsed: Geometry = sonic_rs::from_str(&json).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn try_map_coords_keeps_shape() {
        let gc = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(1.0, 2.0),
                Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]]),
            ],
        };
        let shifted = gc
            .try_map_coords(&mut |[x, y]| Ok::<_, ()>([x + 10.0, y]))
            .unwrap();
        let Geometry::GeometryCollection { geometries } = shifted else {
            panic!("shape changed");
        };
        assert_eq!(geometries[0], Geometry::point(11.0, 2.0));
    }

    #[test]
    fn coords_follow_traversal_order_and_skip_an_empty_point() {
        let gc = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::empty_point(),
                Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]]),
                Geometry::point(5.0, 6.0),
            ],
        };
        let coords: Vec<_> = gc.coords().collect();
        assert_eq!(coords.len(), gc.coord_count());
        assert_eq!(coords, vec![[0.0, 0.0], [1.0, 0.0], [0.0, 0.0], [5.0, 6.0]]);
    }

    #[test]
    fn every_kind_has_an_empty_form() {
        for kind in [
            GeometryKind::Point,
            GeometryKind::LineString,
            GeometryKind::Polygon,
            GeometryKind::MultiPoint,
            GeometryKind::MultiLineString,
            GeometryKind::MultiPolygon,
            GeometryKind::GeometryCollection,
        ] {
            let g = Geometry::empty(kind);
            assert!(g.is_empty(), "{kind:?}");
            assert_eq!(g.kind(), kind);
            assert_eq!(g.coord_count(), 0);
            assert_eq!(GeometryKind::from_wkb_code(kind.wkb_code()), Some(kind));
        }
        assert!(!Geometry::point(0.0, 0.0).is_empty());
    }

    #[test]
    fn try_map_coords_stops_at_first_error() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]);
        let r = line.try_map_coords(&mut |[x, _]| if x > 0.5 { Err(x) } else { Ok([x, 0.0]) });
        assert_eq!(r, Err(1.0));
    }
}
