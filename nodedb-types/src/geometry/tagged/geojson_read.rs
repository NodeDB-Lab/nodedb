// SPDX-License-Identifier: Apache-2.0

//! GeoJSON geometry reader.
//!
//! A position holds 2 or 3 numbers: x, y and an optional Z. GeoJSON has no
//! M. More than 3 numbers, or 2 and 3 mixed in one geometry, is an error.
//! An empty `coordinates` array is the EMPTY form of its kind.

use nodedb_srid::{SridResolver, UnknownSrid};
use serde_json::{Map, Value as Json};

use super::super::ordinates::{OrdinateError, Ordinates, OrdinatesBuilder};
use super::super::shape::{Geometry, GeometryKind};
use super::crs_member::{CrsMember, srid_from_crs, stored_srid_from_crs};
use super::srid_geometry::SridGeometry;

/// Nesting limit for `GeometryCollection` members, the same bound the WKB
/// reader keeps.
const MAX_GEOJSON_DEPTH: usize = 64;

/// Why a GeoJSON text does not yield a [`SridGeometry`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GeoJsonError {
    /// The text is not JSON, or the object has no geometry `type`.
    #[error("not a GeoJSON geometry")]
    NotGeometry,
    /// The object names a geometry type but its members do not fit it.
    #[error("malformed GeoJSON geometry: {reason}")]
    Malformed { reason: Box<str> },
    /// Positions mix 2 and 3 numbers.
    #[error("malformed GeoJSON geometry: {0}")]
    Ordinates(#[from] OrdinateError),
    /// The `crs` member is not a supported form, or does not resolve to a
    /// defined SRID.
    #[error(transparent)]
    Srid(#[from] UnknownSrid),
}

fn malformed(reason: impl Into<Box<str>>) -> GeoJsonError {
    GeoJsonError::Malformed {
        reason: reason.into(),
    }
}

/// The parts of a GeoJSON geometry object before its `crs` resolves.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoJsonParts {
    pub geometry: Geometry,
    pub ordinates: Ordinates,
    pub crs: Option<CrsMember>,
}

/// Read a GeoJSON geometry object.
pub fn read_geojson_value(value: &Json) -> Result<GeoJsonParts, GeoJsonError> {
    let object = value.as_object().ok_or(GeoJsonError::NotGeometry)?;
    let kind = geometry_kind(object)?;
    let crs = match object.get("crs") {
        None | Some(Json::Null) => None,
        Some(member) => Some(serde_json::from_value::<CrsMember>(member.clone()).map_err(
            |_| UnknownSrid::InvalidCrsMember {
                reason: "not an object with type and properties".into(),
            },
        )?),
    };
    let mut ordinates = OrdinatesBuilder::default();
    let geometry = read_geometry(kind, object, &mut ordinates, MAX_GEOJSON_DEPTH)?;
    Ok(GeoJsonParts {
        geometry,
        ordinates: ordinates.finish(),
        crs,
    })
}

fn geometry_kind(object: &Map<String, Json>) -> Result<GeometryKind, GeoJsonError> {
    let name = object
        .get("type")
        .and_then(Json::as_str)
        .ok_or(GeoJsonError::NotGeometry)?;
    GeometryKind::from_geojson_name(name).ok_or(GeoJsonError::NotGeometry)
}

fn read_geometry(
    kind: GeometryKind,
    object: &Map<String, Json>,
    ordinates: &mut OrdinatesBuilder,
    depth: usize,
) -> Result<Geometry, GeoJsonError> {
    if depth == 0 {
        return Err(malformed("GeometryCollection nesting exceeds 64 levels"));
    }
    if kind == GeometryKind::GeometryCollection {
        let members = object
            .get("geometries")
            .and_then(Json::as_array)
            .ok_or_else(|| malformed("GeometryCollection has no geometries array"))?;
        let geometries = members
            .iter()
            .map(|member| {
                let member = member
                    .as_object()
                    .ok_or_else(|| malformed("GeometryCollection member is not an object"))?;
                let member_kind = geometry_kind(member)
                    .map_err(|_| malformed("GeometryCollection member has no geometry type"))?;
                read_geometry(member_kind, member, ordinates, depth - 1)
            })
            .collect::<Result<_, _>>()?;
        return Ok(Geometry::GeometryCollection { geometries });
    }
    let coords = object
        .get("coordinates")
        .ok_or_else(|| malformed(format!("{} has no coordinates", kind.name())))?;
    Ok(match kind {
        GeometryKind::Point => {
            let items = array(coords, "Point coordinates")?;
            if items.is_empty() {
                Geometry::empty_point()
            } else {
                Geometry::Point {
                    coordinates: position(coords, ordinates)?,
                }
            }
        }
        GeometryKind::LineString => Geometry::LineString {
            coordinates: path(coords, ordinates)?,
        },
        GeometryKind::MultiPoint => Geometry::MultiPoint {
            coordinates: path(coords, ordinates)?,
        },
        GeometryKind::Polygon => Geometry::Polygon {
            coordinates: paths(coords, ordinates)?,
        },
        GeometryKind::MultiLineString => Geometry::MultiLineString {
            coordinates: paths(coords, ordinates)?,
        },
        GeometryKind::MultiPolygon => Geometry::MultiPolygon {
            coordinates: array(coords, "MultiPolygon coordinates")?
                .iter()
                .map(|polygon| paths(polygon, ordinates))
                .collect::<Result<_, _>>()?,
        },
        GeometryKind::GeometryCollection => {
            return Err(malformed("GeometryCollection takes geometries"));
        }
    })
}

fn array<'a>(value: &'a Json, what: &str) -> Result<&'a Vec<Json>, GeoJsonError> {
    value
        .as_array()
        .ok_or_else(|| malformed(format!("{what} is not an array")))
}

fn position(value: &Json, ordinates: &mut OrdinatesBuilder) -> Result<[f64; 2], GeoJsonError> {
    let items = array(value, "position")?;
    if !(2..=3).contains(&items.len()) {
        return Err(malformed(format!(
            "position has {} numbers; GeoJSON takes 2 or 3",
            items.len()
        )));
    }
    let mut numbers = [0.0f64; 3];
    for (slot, item) in numbers.iter_mut().zip(items) {
        *slot = item
            .as_f64()
            .filter(|n| n.is_finite())
            .ok_or_else(|| malformed("position holds a value that is not a finite number"))?;
    }
    let z = (items.len() == 3).then_some(numbers[2]);
    ordinates.push(z, None)?;
    Ok([numbers[0], numbers[1]])
}

fn path(value: &Json, ordinates: &mut OrdinatesBuilder) -> Result<Vec<[f64; 2]>, GeoJsonError> {
    array(value, "coordinates")?
        .iter()
        .map(|p| position(p, ordinates))
        .collect()
}

fn paths(
    value: &Json,
    ordinates: &mut OrdinatesBuilder,
) -> Result<Vec<Vec<[f64; 2]>>, GeoJsonError> {
    array(value, "coordinates")?
        .iter()
        .map(|p| path(p, ordinates))
        .collect()
}

/// GeoJSON text as a JSON tree. Uses `sonic_rs` per workspace policy.
fn parse_tree(text: &str) -> Result<Json, GeoJsonError> {
    sonic_rs::from_str::<Json>(text).map_err(|_| GeoJsonError::NotGeometry)
}

/// Parse GeoJSON input text into a [`SridGeometry`]. The `crs` member must
/// resolve in `ctx`.
pub fn parse_geojson(text: &str, ctx: &dyn SridResolver) -> Result<SridGeometry, GeoJsonError> {
    let parts = read_geojson_value(&parse_tree(text)?)?;
    let srid = srid_from_crs(parts.crs.as_ref(), ctx)?;
    Ok(SridGeometry::with_ordinates(
        srid,
        parts.geometry,
        parts.ordinates,
    )?)
}

/// [`parse_geojson`] over raw UTF-8 bytes.
pub fn parse_geojson_slice(
    bytes: &[u8],
    ctx: &dyn SridResolver,
) -> Result<SridGeometry, GeoJsonError> {
    let text = std::str::from_utf8(bytes).map_err(|_| GeoJsonError::NotGeometry)?;
    parse_geojson(text, ctx)
}

/// Decode stored GeoJSON bytes. The `crs` name is checked for form and
/// code range only: whether the SRID is defined is a question for the
/// reader that uses the coordinates.
pub fn decode_stored_geojson(bytes: &[u8]) -> Result<SridGeometry, GeoJsonError> {
    let text = std::str::from_utf8(bytes).map_err(|_| GeoJsonError::NotGeometry)?;
    decode_stored_tree(&parse_tree(text)?)
}

/// A stored GeoJSON tree as a [`SridGeometry`], `crs` checked for syntax
/// only.
pub(super) fn decode_stored_tree(tree: &Json) -> Result<SridGeometry, GeoJsonError> {
    let parts = read_geojson_value(tree)?;
    let srid = stored_srid_from_crs(parts.crs.as_ref())?;
    Ok(SridGeometry::with_ordinates(
        srid,
        parts.geometry,
        parts.ordinates,
    )?)
}

#[cfg(test)]
mod tests {
    use super::super::super::ordinates::Dimension;
    use super::super::super::srid::Srid;
    use super::*;

    fn ctx() -> nodedb_crs::CrsContext<'static> {
        nodedb_crs::CrsContext::builtin_only()
    }

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    #[test]
    fn missing_crs_member_is_wgs84() {
        let g = parse_geojson(r#"{"type":"Point","coordinates":[1.0,2.0]}"#, &ctx()).unwrap();
        assert_eq!(g.srid, Srid::WGS84);
        assert_eq!(g.dimension(), Dimension::Xy);
    }

    #[test]
    fn three_numbers_are_z() {
        let g = parse_geojson(
            r#"{"type":"LineString","coordinates":[[1,2,3],[4,5,6]]}"#,
            &ctx(),
        )
        .unwrap();
        assert_eq!(g.ordinates().z(), Some(&[3.0, 6.0][..]));
        assert_eq!(g.ordinates().m(), None);
    }

    #[test]
    fn four_numbers_and_mixed_arity_fail() {
        assert!(matches!(
            parse_geojson(r#"{"type":"Point","coordinates":[1,2,3,4]}"#, &ctx()),
            Err(GeoJsonError::Malformed { .. })
        ));
        assert!(matches!(
            parse_geojson(
                r#"{"type":"LineString","coordinates":[[1,2,3],[4,5]]}"#,
                &ctx()
            ),
            Err(GeoJsonError::Ordinates(
                OrdinateError::MixedDimension { .. }
            ))
        ));
        assert!(matches!(
            parse_geojson(r#"{"type":"Point","coordinates":[1]}"#, &ctx()),
            Err(GeoJsonError::Malformed { .. })
        ));
    }

    #[test]
    fn collection_members_share_one_dimension() {
        let mixed = r#"{"type":"GeometryCollection","geometries":[
            {"type":"Point","coordinates":[1,2,3]},
            {"type":"Point","coordinates":[1,2]}]}"#;
        assert!(matches!(
            parse_geojson(mixed, &ctx()),
            Err(GeoJsonError::Ordinates(_))
        ));
    }

    #[test]
    fn empty_coordinates_are_empty_geometries() {
        for (text, kind) in [
            (r#"{"type":"Point","coordinates":[]}"#, GeometryKind::Point),
            (
                r#"{"type":"LineString","coordinates":[]}"#,
                GeometryKind::LineString,
            ),
            (
                r#"{"type":"MultiPolygon","coordinates":[]}"#,
                GeometryKind::MultiPolygon,
            ),
            (
                r#"{"type":"GeometryCollection","geometries":[]}"#,
                GeometryKind::GeometryCollection,
            ),
        ] {
            let g = parse_geojson(text, &ctx()).unwrap();
            assert!(g.is_empty(), "{text}");
            assert_eq!(g.geometry().kind(), kind);
        }
    }

    #[test]
    fn non_geometry_json_is_not_geometry() {
        assert_eq!(
            parse_geojson(r#"{"a":1}"#, &ctx()),
            Err(GeoJsonError::NotGeometry)
        );
        assert_eq!(
            parse_geojson(r#"{"type":"Feature"}"#, &ctx()),
            Err(GeoJsonError::NotGeometry)
        );
        assert_eq!(
            parse_geojson("nope", &ctx()),
            Err(GeoJsonError::NotGeometry)
        );
    }

    #[test]
    fn non_finite_and_non_numeric_positions_fail() {
        assert!(matches!(
            parse_geojson(r#"{"type":"Point","coordinates":["a",2]}"#, &ctx()),
            Err(GeoJsonError::Malformed { .. })
        ));
    }

    /// The legacy `EPSG` member GDAL and old PostGIS write resolves its
    /// code, as a number or a decimal string.
    #[test]
    fn legacy_epsg_member_resolves() {
        let ctx = ctx();
        let numeric = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"EPSG","properties":{"code":32648}}}"#;
        assert_eq!(
            parse_geojson(numeric, &ctx).map(|g| g.srid),
            Ok(srid(32648))
        );
        let text = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"EPSG","properties":{"code":"3375"}}}"#;
        assert_eq!(parse_geojson(text, &ctx).map(|g| g.srid), Ok(srid(3375)));
        assert_eq!(
            decode_stored_geojson(numeric.as_bytes()).map(|g| g.srid),
            Ok(srid(32648))
        );
        let undefined = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"EPSG","properties":{"code":999}}}"#;
        assert_eq!(
            parse_geojson(undefined, &ctx),
            Err(GeoJsonError::Srid(UnknownSrid::Undefined { code: 999 }))
        );
        let no_code =
            r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"EPSG","properties":{}}}"#;
        assert!(matches!(
            parse_geojson(no_code, &ctx),
            Err(GeoJsonError::Srid(UnknownSrid::InvalidCrsMember { .. }))
        ));
    }

    /// A stored decode checks syntax only, so an undefined code decodes.
    #[test]
    fn stored_decode_checks_syntax_only() {
        let text = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"name","properties":{"name":"EPSG:999"}}}"#;
        let g = decode_stored_geojson(text.as_bytes()).unwrap();
        assert_eq!(g.srid, srid(999));
        let zero = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"name","properties":{"name":"EPSG:0"}}}"#;
        assert_eq!(
            decode_stored_geojson(zero.as_bytes()),
            Err(GeoJsonError::Srid(UnknownSrid::OutOfRange { code: 0 }))
        );
    }
}
