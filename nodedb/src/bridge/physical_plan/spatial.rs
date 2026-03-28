//! Spatial engine operations dispatched to the Data Plane.

/// Spatial predicate type for R-tree index scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialPredicate {
    /// ST_DWithin: geometry within distance (meters).
    DWithin,
    /// ST_Contains: query geometry contains document geometry.
    Contains,
    /// ST_Intersects: query geometry intersects document geometry.
    Intersects,
    /// ST_Within: document geometry is within query geometry.
    Within,
}

/// Spatial engine physical operations.
#[derive(Debug, Clone)]
pub enum SpatialOp {
    /// R-tree index scan with spatial predicate and exact refinement.
    Scan {
        collection: String,
        field: String,
        predicate: SpatialPredicate,
        /// Query geometry serialized as GeoJSON bytes.
        query_geometry: Vec<u8>,
        /// Distance threshold in meters (for ST_DWithin). 0 for non-distance predicates.
        distance_meters: f64,
        /// Additional attribute filters applied after spatial candidates.
        attribute_filters: Vec<u8>,
        limit: usize,
        projection: Vec<String>,
        /// RLS post-candidate filters.
        rls_filters: Vec<u8>,
    },
}
