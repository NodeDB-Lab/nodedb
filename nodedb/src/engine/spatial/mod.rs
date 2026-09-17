// SPDX-License-Identifier: BUSL-1.1

// Re-export shared spatial engine from nodedb-spatial crate.
// Origin's spatial handlers (data/executor/handlers/spatial/) and
// checkpoint logic (data/executor/spatial_checkpoint/) use these directly.
pub use nodedb_spatial::GeohashIndex;
pub use nodedb_spatial::RTree;
pub use nodedb_spatial::RTreeEntry;
pub use nodedb_spatial::geo_meta;
pub use nodedb_spatial::geohash;
pub use nodedb_spatial::geohash_index;
pub use nodedb_spatial::h3;
pub use nodedb_spatial::hybrid;
pub use nodedb_spatial::operations;
pub use nodedb_spatial::persist;
pub use nodedb_spatial::predicates;
pub use nodedb_spatial::rtree;
pub use nodedb_spatial::spatial_join;
pub use nodedb_spatial::validate;
pub use nodedb_spatial::wkb;
pub use nodedb_spatial::wkt;
pub use nodedb_spatial::{
    SpatialPreFilterResult, bitmap_contains, geohash_decode, geohash_encode, geohash_neighbors,
    geometry_from_wkb, geometry_from_wkt, geometry_to_wkb, geometry_to_wkt, h3_encode,
    h3_encode_string, h3_is_valid, h3_neighbors, h3_parent, h3_resolution, h3_to_boundary,
    h3_to_center, ids_to_bitmap, is_valid, spatial_prefilter, st_buffer, st_contains, st_disjoint,
    st_distance, st_dwithin, st_envelope, st_intersection, st_intersects, st_union, st_within,
    validate_geometry,
};
