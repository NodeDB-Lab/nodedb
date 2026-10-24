// SPDX-License-Identifier: Apache-2.0

//! Spatial engine operations dispatched to the Data Plane.

use nodedb_types::{QualifiedCollection, Surrogate, SurrogateBitmap, geometry::SridGeometry};

/// Spatial predicate type for R-tree index scan.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum SpatialPredicate {
    /// ST_DWithin: geometry within distance (meters).
    DWithin,
    /// ST_3DDWithin: 3D distance within metres, Z in metres.
    DWithin3D,
    /// ST_Contains: query geometry contains document geometry.
    Contains,
    /// ST_Intersects: query geometry intersects document geometry.
    Intersects,
    /// ST_Within: document geometry is within query geometry.
    Within,
}

/// Spatial engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SpatialOp {
    /// Insert a geometry into the R-tree index for a document.
    ///
    /// Used by the sync inbound path to replicate spatial data from Lite to Origin.
    /// The R-tree and sparse document store are keyed by the hex-encoded
    /// `surrogate` (matching direct `INSERT INTO` semantics), so the
    /// cross-engine prefilter bitmap intersect works without translation.
    Insert {
        collection: QualifiedCollection,
        field: String,
        /// Stable global surrogate for the row, assigned on the Control Plane.
        surrogate: Surrogate,
        /// Typed geometry and its SRID, deserialised on the Control Plane
        /// from wire bytes.
        geometry: SridGeometry,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },
    /// Remove a document's geometry from the R-tree index.
    ///
    /// Used by the sync inbound path. Keyed by the same hex-encoded surrogate
    /// used at insert time.
    Delete {
        collection: QualifiedCollection,
        field: String,
        /// Stable global surrogate for the row. `None` when the key's home
        /// binds none: the delete removes nothing and still commits its sync
        /// provenance.
        surrogate: Option<Surrogate>,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },
    /// R-tree index scan with spatial predicate and exact refinement.
    Scan {
        collection: QualifiedCollection,
        field: String,
        predicate: SpatialPredicate,
        /// Typed query geometry and its SRID, parsed and validated on the
        /// Control Plane. An SRID other than the index's fails the scan.
        query_geometry: SridGeometry,
        /// Distance threshold in meters (for ST_DWithin). 0 for non-distance predicates.
        distance_meters: f64,
        /// Additional attribute filters applied after spatial candidates.
        attribute_filters: Vec<u8>,
        limit: usize,
        projection: Vec<String>,
        /// RLS post-candidate filters.
        rls_filters: Vec<u8>,
        /// Optional surrogate prefilter injected by a cross-engine sub-plan.
        /// When present, only candidates whose surrogate is in this bitmap
        /// are returned. `None` = no prefilter; all R-tree candidates pass.
        prefilter: Option<SurrogateBitmap>,
    },
}
