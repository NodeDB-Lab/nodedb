// SPDX-License-Identifier: Apache-2.0

//! The geospatial function catalog.
//!
//! This table is the single source of truth for the geo/spatial SQL surface.
//! It drives three consumers that previously each kept their own partial list:
//!
//! * `nodedb-sql`'s function registry (the plan-time existence gate, arity
//!   check, and return typing) is generated from it;
//! * `nodedb-sql`'s geometry-expression resolver reads it to decide whether a
//!   call is geometry-valued and may appear in geometry position;
//! * this crate's evaluator dispatches on the canonical name it declares.
//!
//! Because all three read the same rows, a function cannot resolve in one
//! syntactic position and be unknown in another — the failure mode that let
//! `ST_GeomFromText(...)` work in `INSERT ... VALUES` while being rejected
//! inside `ST_DWithin(...)`, and let `geo_x` evaluate while `ST_X` did not
//! exist. Adding a capability means adding one row here.

use super::spec::{GeoArgShape::*, GeoFunctionSpec, GeoReturn::*, f};

/// Every geospatial SQL function, with all of its accepted spellings.
pub static GEO_FUNCTIONS: &[GeoFunctionSpec] = &[
    // ── Topological predicates ──
    f("st_contains", &[], Geometry2, Bool),
    f("st_intersects", &[], Geometry2, Bool),
    f("st_within", &[], Geometry2, Bool),
    f("st_disjoint", &[], Geometry2, Bool),
    f("st_dwithin", &[], Geometry2Distance, Bool),
    f("st_touches", &[], Geometry2, Bool),
    f("st_crosses", &[], Geometry2, Bool),
    f("st_overlaps", &[], Geometry2, Bool),
    f("st_equals", &[], Geometry2, Bool),
    f("st_covers", &[], Geometry2, Bool),
    f("st_coveredby", &[], Geometry2, Bool),
    f("st_relate", &[], Geometry2TextOpt, ByArity),
    f("st_3ddwithin", &[], Geometry2Distance, Bool),
    f("st_3dintersects", &[], Geometry2, Bool),
    f("st_isvalid", &["geo_is_valid"], Geometry1, Bool),
    f("st_isvalidreason", &[], Geometry1, Text),
    f("st_isempty", &[], Geometry1, Bool),
    // ── Measures ──
    f("st_distance", &[], Geometry2, Float),
    f("st_length", &["geo_length"], Geometry1, Float),
    f("st_perimeter", &["geo_perimeter"], Geometry1, Float),
    f("st_area", &[], Geometry1, Float),
    f("st_3ddistance", &[], Geometry2, Float),
    f("st_3dlength", &[], Geometry1, Float),
    f("geo_distance", &["haversine_distance"], Haversine4, Float),
    f("geo_bearing", &["haversine_bearing"], Haversine4, Float),
    // ── Accessors ──
    f("st_x", &["geo_x"], Geometry1, Float),
    f("st_y", &["geo_y"], Geometry1, Float),
    f("st_z", &[], Geometry1, Float),
    f("st_m", &[], Geometry1, Float),
    f("st_zmin", &[], Geometry1, Float),
    f("st_zmax", &[], Geometry1, Float),
    f("st_coorddim", &[], Geometry1, Int),
    f("st_ndims", &[], Geometry1, Int),
    f("st_zmflag", &[], Geometry1, Int),
    f("st_hasz", &[], Geometry1, Bool),
    f("st_hasm", &[], Geometry1, Bool),
    f("st_geometrytype", &["geo_type"], Geometry1, Text),
    f("st_npoints", &["geo_num_points"], Geometry1, Int),
    // ── Encodings ──
    f("st_astext", &["geo_as_wkt"], Geometry1, Text),
    f("st_asewkt", &[], Geometry1, Text),
    f("st_asgeojson", &["geo_as_geojson"], Geometry1, Text),
    f("st_asbinary", &[], GeometryTextOpt, Bytes),
    f("st_asewkb", &[], GeometryTextOpt, Bytes),
    // ── Spatial reference systems ──
    f("st_srid", &[], Geometry1, Int),
    f("st_setsrid", &[], GeometrySrid, Geometry),
    f("st_transform", &[], GeometrySridEpoch, Geometry),
    f("st_shiftlongitude", &[], Geometry1, Geometry),
    // ── Constructors ──
    f("st_point", &["geo_point"], LngLat, Geometry),
    f("st_makepoint", &[], Xyzm, Geometry),
    f("st_makepointm", &[], Xym, Geometry),
    f(
        "st_geomfromtext",
        &["geo_from_wkt", "st_geomfromewkt"],
        TextSrid,
        Geometry,
    ),
    f("st_geomfromgeojson", &["geo_from_geojson"], Text1, Geometry),
    f("st_geomfromwkb", &["st_geomfromewkb"], BytesSrid, Geometry),
    f("st_makeline", &["geo_line"], PointsVariadic, Geometry),
    f("st_makepolygon", &["geo_polygon"], RingsVariadic, Geometry),
    f("st_makeenvelope", &["geo_bbox"], Bbox, Geometry),
    f("st_tileenvelope", &[], TileEnvelope, Geometry),
    f("geo_circle", &[], Circle, Geometry),
    // ── Dimension ──
    f("st_force2d", &[], Geometry1, Geometry),
    f("st_force3d", &["st_force3dz"], GeometryNumberOpt, Geometry),
    f("st_force3dm", &[], GeometryNumberOpt, Geometry),
    f("st_force4d", &[], GeometryNumbers2Opt, Geometry),
    // ── Geometry-returning operations ──
    f("st_buffer", &[], GeometryBuffer, Geometry),
    f("st_envelope", &[], Geometry1, Geometry),
    f("st_union", &[], Geometry2, Geometry),
    f("st_intersection", &[], Geometry2, Geometry),
    f("st_difference", &[], Geometry2, Geometry),
    f("st_symdifference", &[], Geometry2, Geometry),
    f("st_centroid", &[], Geometry1, Geometry),
    f("st_convexhull", &[], Geometry1, Geometry),
    f("st_concavehull", &[], GeometryNumber, Geometry),
    f("st_simplify", &[], GeometryNumber, Geometry),
    f("st_simplifypreservetopology", &[], GeometryNumber, Geometry),
    f("st_makevalid", &[], Geometry1, Geometry),
    f("st_subdivide", &[], GeometryNumberOpt, Geometry),
    f("st_segmentize", &[], GeometryNumber, Geometry),
    f("st_closestpoint", &[], Geometry2, Geometry),
    f("st_split", &[], Geometry2, Geometry),
    // ── Linear referencing ──
    f("st_lineinterpolatepoint", &[], GeometryNumber, Geometry),
    f("st_locatealong", &[], GeometryNumberNumberOpt, Geometry),
    f("st_locatebetween", &[], GeometryRange, Geometry),
    f("st_interpolatepoint", &[], Geometry2, Float),
    // ── Geohash ──
    f("st_geohash", &["geo_geohash"], LngLatPrecision, Text),
    f("st_geohashdecode", &["geo_geohash_decode"], Text1, Object),
    f("geo_geohash_neighbors", &[], Text1, Array),
    // ── H3 ──
    // `geo_h3` and `h3_latlngtocell` are NOT aliases: `geo_h3` takes
    // (lng, lat, resolution) while the H3 surface name takes (lat, lng,
    // resolution). One row for both transposes coordinates, so they stay
    // separate rows with separate shapes.
    f("geo_h3", &[], LngLatPrecision, Text),
    f("h3_latlngtocell", &[], LatLngResolution, Text),
    f("h3_celltolatlng", &[], Text1, Object),
    f("geo_h3_to_boundary", &[], Text1, Geometry),
    f("geo_h3_resolution", &[], Text1, Int),
];
