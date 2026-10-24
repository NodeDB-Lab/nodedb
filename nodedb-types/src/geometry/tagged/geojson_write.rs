// SPDX-License-Identifier: Apache-2.0

//! GeoJSON writer: serde `Serialize` / `Deserialize` for [`SridGeometry`].
//!
//! The writer emits Z as the third number of each position and drops M,
//! as PostGIS `ST_AsGeoJSON` does. An EMPTY geometry writes an empty
//! `coordinates` (or `geometries`) array.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value as Json};

use super::super::shape::Geometry;
use super::super::srid::Srid;
use super::crs_member::CrsMember;
use super::geojson_read::decode_stored_tree;
use super::srid_geometry::SridGeometry;

/// The GeoJSON object of `g`, `crs` member included for a non-WGS 84 SRID.
pub fn geojson_tree(g: &SridGeometry) -> Json {
    let mut z = g.ordinates().z().map(|z| z.iter().copied());
    let mut object = geometry_object(g.geometry(), &mut z);
    if g.srid != Srid::WGS84
        && let Ok(member) = serde_json::to_value(CrsMember::for_srid(g.srid))
    {
        object.insert("crs".into(), member);
    }
    Json::Object(object)
}

/// Z values in traversal order, consumed as positions are written.
type ZIter<'a> = Option<std::iter::Copied<std::slice::Iter<'a, f64>>>;

fn geometry_object(geometry: &Geometry, z: &mut ZIter<'_>) -> Map<String, Json> {
    let mut object = Map::new();
    object.insert("type".into(), Json::from(geometry.geometry_type()));
    let coordinates = match geometry {
        Geometry::Point { .. } if geometry.is_empty() => Json::Array(Vec::new()),
        Geometry::Point { coordinates } => position(*coordinates, z),
        Geometry::LineString { coordinates } | Geometry::MultiPoint { coordinates } => {
            path(coordinates, z)
        }
        Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
            paths(coordinates, z)
        }
        Geometry::MultiPolygon { coordinates } => {
            Json::Array(coordinates.iter().map(|p| paths(p, z)).collect())
        }
        Geometry::GeometryCollection { geometries } => {
            let members = geometries
                .iter()
                .map(|member| Json::Object(geometry_object(member, z)))
                .collect();
            object.insert("geometries".into(), Json::Array(members));
            return object;
        }
    };
    object.insert("coordinates".into(), coordinates);
    object
}

fn position(c: [f64; 2], z: &mut ZIter<'_>) -> Json {
    let mut numbers = vec![Json::from(c[0]), Json::from(c[1])];
    if let Some(value) = z.as_mut().and_then(Iterator::next) {
        numbers.push(Json::from(value));
    }
    Json::Array(numbers)
}

fn path(coords: &[[f64; 2]], z: &mut ZIter<'_>) -> Json {
    Json::Array(coords.iter().map(|c| position(*c, z)).collect())
}

fn paths(rings: &[Vec<[f64; 2]>], z: &mut ZIter<'_>) -> Json {
    Json::Array(rings.iter().map(|ring| path(ring, z)).collect())
}

impl Serialize for SridGeometry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        geojson_tree(self).serialize(serializer)
    }
}

/// Deserialization is a stored-data decode: the `crs` name is checked for
/// form and code range only.
impl<'de> Deserialize<'de> for SridGeometry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let tree = Json::deserialize(deserializer)?;
        decode_stored_tree(&tree).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::ordinates::Ordinates;
    use super::super::geojson_read::parse_geojson;
    use super::*;

    fn ctx() -> nodedb_crs::CrsContext<'static> {
        nodedb_crs::CrsContext::builtin_only()
    }

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    #[test]
    fn wgs84_writes_no_crs_member() {
        let g = SridGeometry::wgs84(Geometry::point(1.0, 2.0));
        let json = sonic_rs::to_string(&g).unwrap();
        assert!(!json.contains("crs"), "{json}");
        assert!(json.contains("\"type\":\"Point\""), "{json}");
    }

    #[test]
    fn projected_round_trip_keeps_srid() {
        let g = SridGeometry::new(srid(32648), Geometry::point(134445.5, 349624.7));
        let json = sonic_rs::to_string(&g).unwrap();
        assert!(json.contains("\"name\":\"EPSG:32648\""), "{json}");
        assert_eq!(parse_geojson(&json, &ctx()), Ok(g.clone()));
        let derived: SridGeometry = sonic_rs::from_str(&json).unwrap();
        assert_eq!(derived, g);
    }

    #[test]
    fn writer_emits_z_and_drops_m() {
        let zm = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            Ordinates::new(Some(vec![5.0, 6.0]), Some(vec![7.0, 8.0])),
        )
        .unwrap();
        let json = sonic_rs::to_string(&zm).unwrap();
        assert!(json.contains("[1.0,1.0,6.0]"), "{json}");
        let back = parse_geojson(&json, &ctx()).unwrap();
        assert_eq!(back.ordinates().z(), Some(&[5.0, 6.0][..]));
        assert_eq!(back.ordinates().m(), None);
    }

    #[test]
    fn empty_point_round_trips() {
        let g = SridGeometry::wgs84(Geometry::empty_point());
        let json = sonic_rs::to_string(&g).unwrap();
        assert!(json.contains("\"coordinates\":[]"), "{json}");
        assert!(parse_geojson(&json, &ctx()).unwrap().is_empty());
    }

    #[test]
    fn collection_z_follows_traversal_order() {
        let g = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::GeometryCollection {
                geometries: vec![
                    Geometry::point(1.0, 1.0),
                    Geometry::line_string(vec![[2.0, 2.0], [3.0, 3.0]]),
                ],
            },
            Ordinates::new(Some(vec![10.0, 20.0, 30.0]), None),
        )
        .unwrap();
        let json = sonic_rs::to_string(&g).unwrap();
        assert_eq!(parse_geojson(&json, &ctx()), Ok(g));
    }
}
