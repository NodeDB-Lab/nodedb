// SPDX-License-Identifier: Apache-2.0

//! GeoJSON-compatible geometry types, Z / M ordinates, spatial reference
//! systems, and spherical geo math.

pub mod math;
pub mod ordinates;
pub mod shape;
pub mod srid;
pub mod tagged;

pub use math::{EARTH_RADIUS_M, haversine_bearing, haversine_distance, point_in_polygon};
pub use ordinates::{Dimension, Ordinate, OrdinateError, Ordinates, OrdinatesBuilder, Position};
pub use shape::{Coord, Geometry, GeometryKind, is_empty_coord};
pub use srid::{Srid, SridResolver};
pub use tagged::{
    CrsCode, CrsMember, CrsProperties, GeoJsonError, GeoJsonParts, SridGeometry,
    decode_stored_geojson, geojson_object, geojson_tree, parse_geojson, parse_geojson_slice,
    read_geojson_value, srid_from_crs, stored_geojson_object,
};
