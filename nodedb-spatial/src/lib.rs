// SPDX-License-Identifier: Apache-2.0

//! Spatial engine primitives shared by Origin, Lite, and WASM: R*-tree with
//! bulk load and nearest-neighbor / range queries, geohash and H3 hex
//! indexes, OGC predicates and DE-9IM, geodesic measures on a CRS's own
//! ellipsoid, boolean overlay and buffer, WKB / WKT / GeoJSON interchange
//! with Z / M / ZM and EMPTY, and hybrid spatial-vector composition.
//!
//! Spatial collections are picked per-collection via `WITH (engine='spatial')`
//! and use the columnar storage core; this crate provides the index
//! structures and geometry algorithms only.

pub mod crs;
pub mod distance3d;
pub mod error;
pub mod geo_meta;
pub mod geodesic;
pub mod geohash;
pub mod geohash_index;
pub mod h3;
pub mod hybrid;
pub mod measures;
pub mod metric;
pub mod operations;
pub mod ops;
pub mod orient;
pub(crate) mod parts;
pub mod persist;
pub mod planar;
pub mod predicates;
pub mod rtree;
pub mod spatial_join;
#[cfg(test)]
mod test_support;
pub mod text;
pub mod validate;
pub mod value;
pub mod wkb;
pub mod wkt;

pub use crs::{transform_geometry, transform_geometry_at};
pub use error::SpatialError;
pub use geodesic::GeodesicMeasure;
pub use geohash::{geohash_decode, geohash_encode, geohash_neighbors};
pub use geohash_index::GeohashIndex;
pub use h3::{
    h3_encode, h3_encode_string, h3_is_valid, h3_neighbors, h3_parent, h3_resolution,
    h3_to_boundary, h3_to_center,
};
pub use hybrid::{SpatialPreFilterResult, bitmap_contains, ids_to_bitmap, spatial_prefilter};
pub use measures::st_centroid;
pub use metric::Metric;
pub use operations::{st_envelope, st_envelope_in};
pub use ops::{
    difference as st_difference, intersection as st_intersection,
    sym_difference as st_symdifference, union as st_union,
};
pub use orient::orient_rings;
pub use persist::{
    RTreeCheckpointError, SpatialIndexMeta, SpatialIndexType, deserialize_meta, meta_storage_key,
    rtree_storage_key, serialize_meta,
};
pub use planar::{planar_area, planar_distance, planar_dwithin, planar_length, planar_perimeter};
pub use predicates::{st_contains, st_disjoint, st_distance, st_dwithin, st_intersects, st_within};
pub use rtree::{RTree, RTreeEntry, SridMismatch, TreeSrid};
pub use text::{SridCheck, geometry_from_bytes, geometry_from_text};
pub use validate::{ValidityReason, is_valid, validate_geometry, validity_reason};
pub use value::{ewkb_of_value, geometry_from_value, value_of_ewkb};
pub use wkb::{
    ByteOrder, Wkb, WkbError, geometry_from_wkb, geometry_to_ewkb, geometry_to_ewkb_ordered,
    geometry_to_wkb, geometry_to_wkb_ordered,
};
pub use wkt::{
    Ewkt, WktError, geometry_from_ewkt, geometry_from_wkt, geometry_to_ewkt, geometry_to_wkt,
    shape_to_wkt,
};
