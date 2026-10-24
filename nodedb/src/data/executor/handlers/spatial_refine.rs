// SPDX-License-Identifier: BUSL-1.1

//! Geometry / predicate / projection helpers shared by the spatial scan
//! handler (`spatial.rs`) and the transaction overlay merge
//! (`transaction/overlay/spatial_merge.rs`).
//!
//! These are pure, storage-agnostic functions: they operate on already-decoded
//! `nodedb_types::Value` documents, so the same refinement logic applies to a
//! document-collection row (fetched from the sparse engine) and a
//! `spatial` / columnar-family row (fetched from columnar) alike.

use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_spatial::{Metric, SridMismatch};
use nodedb_types::Value;
use nodedb_types::geometry::SridGeometry;

/// Extract geometry from a document field.
///
/// Handles three storage forms:
/// - `Value::Geometry(g)` — native geometry (columnar path preserves type)
/// - `Value::String(s)` — GeoJSON string (from SQL ST_Point → serialized)
/// - `Value::Object(_)` — GeoJSON object (from schemaless doc storage)
///
/// GeoJSON keeps the SRID its `crs` member names; no member is WGS 84.
/// `Ok(None)` when the field is absent or holds no geometry. `Err` when the
/// geometry names an SRID `crs` does not define.
pub(in crate::data::executor) fn extract_geometry(
    doc: &Value,
    field: &str,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<Option<SridGeometry>, nodedb_crs::UnknownSrid> {
    match doc.get(field) {
        Some(value) => SridGeometry::from_value(value, crs),
        None => Ok(None),
    }
}

/// Apply the spatial predicate.
///
/// The stored and query geometries must share an SRID: a mismatch is
/// [`SridMismatch`] (`index` the stored SRID, `entry` the query's), which
/// the caller raises as `Error::MixedSrid` instead of skipping the row.
/// `metric` is the shared SRID's metric: `ST_DWithin` measures geodesic
/// metres on the CRS ellipsoid in a geographic CRS, and grid metres in a
/// projected one. `ST_3DDWithin` adds Z in metres. An EMPTY operand
/// matches no predicate.
pub(in crate::data::executor) fn apply_predicate(
    predicate: &SpatialPredicate,
    query: &SridGeometry,
    doc: &SridGeometry,
    distance_meters: f64,
    metric: &Metric,
) -> Result<bool, SridMismatch> {
    if query.srid != doc.srid {
        return Err(SridMismatch {
            index: doc.srid,
            entry: query.srid,
        });
    }
    let (q, d) = (query.geometry(), doc.geometry());
    Ok(match predicate {
        SpatialPredicate::DWithin => metric.dwithin(q, d, distance_meters),
        SpatialPredicate::DWithin3D => metric.dwithin_3d(query, doc, distance_meters),
        // `ST_Contains(loc, q)` asks whether the *stored* geometry contains
        // the query geometry — the geofencing shape, where `loc` is a zone
        // polygon and `q` a point. `ST_Within(loc, q)` is its converse. Both
        // pass the stored geometry in the position SQL named first.
        SpatialPredicate::Contains => crate::engine::spatial::st_contains(d, q),
        SpatialPredicate::Intersects => crate::engine::spatial::st_intersects(q, d),
        SpatialPredicate::Within => crate::engine::spatial::st_within(d, q),
    })
}

/// The `Error::MixedSrid` a spatial scan of `collection.field` raises for
/// `mismatch`.
pub(in crate::data::executor) fn mixed_srid_error(
    collection: &str,
    field: &str,
    mismatch: SridMismatch,
) -> crate::Error {
    crate::Error::MixedSrid {
        collection: collection.to_string(),
        field: field.to_string(),
        index_srid: mismatch.index.code(),
        row_srid: mismatch.entry.code(),
    }
}

/// One spatial scan result: the projected row and the surrogate it was read
/// under. The overlay merge keys rows by `surrogate`, never by a row field,
/// so the row names its identity by the collection's identity column alone.
pub(in crate::data::executor) struct SpatialHit {
    /// The row's surrogate, `None` when its `doc_id` is no hex surrogate.
    pub surrogate: Option<u32>,
    pub row: Value,
}

impl SpatialHit {
    /// A hit read under `doc_id`: a document row's hex storage key, or a
    /// columnar-family row's `id` value.
    pub(in crate::data::executor) fn new(doc_id: &str, row: Value) -> Self {
        Self {
            surrogate: u32::from_str_radix(doc_id, 16).ok(),
            row,
        }
    }
}

/// The rows of `hits`, in order, for the response encoder.
pub(in crate::data::executor) fn hit_rows(hits: Vec<SpatialHit>) -> Vec<Value> {
    hits.into_iter().map(|hit| hit.row).collect()
}

/// Apply projection to a document, returning `nodedb_types::Value`.
///
/// The row names its identity under `identity_column`: the collection's
/// declared key, else `id`. A document that holds that column keeps its own
/// value. One that lacks it gains `doc_id` there. A declared-key row never
/// gains an `id` beside its key.
pub(in crate::data::executor) fn project_doc(
    doc: &Value,
    doc_id: &str,
    projection: &[String],
    identity_column: &str,
) -> Value {
    let identity = doc
        .get(identity_column)
        .cloned()
        .unwrap_or_else(|| Value::String(doc_id.to_string()));
    if projection.is_empty() {
        if let Value::Object(mut map) = doc.clone() {
            map.entry(identity_column.to_string()).or_insert(identity);
            Value::Object(map)
        } else {
            doc.clone()
        }
    } else {
        let mut map = std::collections::HashMap::new();
        map.insert(identity_column.to_string(), identity);
        for col in projection {
            if let Some(v) = doc.get(col) {
                map.insert(col.clone(), v.clone());
            }
        }
        Value::Object(map)
    }
}

/// The R-tree search box of `predicate`: the query box, widened to cover
/// every point within `distance_meters` for the distance predicates.
pub(in crate::data::executor) fn search_bbox(
    predicate: &SpatialPredicate,
    query_bbox: nodedb_types::BoundingBox,
    distance_meters: f64,
    metric: &Metric,
) -> nodedb_types::BoundingBox {
    match predicate {
        SpatialPredicate::DWithin => metric.widen(&query_bbox, distance_meters),
        SpatialPredicate::DWithin3D => metric.widen(&query_bbox, metric.reach_3d(distance_meters)),
        SpatialPredicate::Contains | SpatialPredicate::Intersects | SpatialPredicate::Within => {
            query_bbox
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Geometry, Ordinates, Srid};

    fn metric(code: u32) -> Metric {
        let crs = nodedb_crs::CrsContext::builtin_only();
        Metric::resolve(&crs, Srid::new(code).unwrap()).unwrap()
    }

    /// EPSG:2263 is in US survey feet: a 100 m search widens the box by
    /// about 328 ft, and a metre CRS by 100 m.
    #[test]
    fn planar_expansion_is_in_the_crs_unit() {
        let bbox = nodedb_types::BoundingBox::from_point(1_000_000.0, 200_000.0);
        let feet = search_bbox(&SpatialPredicate::DWithin, bbox, 100.0, &metric(2263));
        let expected_ft = 100.0 / (1200.0 / 3937.0);
        assert!(
            (feet.max_lng - 1_000_000.0 - expected_ft).abs() < 1e-6,
            "{feet:?}"
        );
        let metres = search_bbox(&SpatialPredicate::DWithin, bbox, 100.0, &metric(32618));
        assert!((metres.max_lng - 1_000_100.0).abs() < 1e-9, "{metres:?}");
    }

    /// The geographic search box covers the true geodesic reach in
    /// latitude: 100 km north of the equator is 0.9043 degrees, more than
    /// 100 km over the equatorial degree of longitude.
    #[test]
    fn geographic_expansion_covers_latitude_reach() {
        let bbox = nodedb_types::BoundingBox::from_point(0.0, 0.0);
        let wide = search_bbox(&SpatialPredicate::DWithin, bbox, 100_000.0, &metric(4326));
        assert!(wide.max_lat >= 0.9043, "{wide:?}");
        let a = SridGeometry::wgs84(Geometry::point(0.0, 0.0));
        let b = SridGeometry::wgs84(Geometry::point(0.0, 0.904));
        assert!(
            apply_predicate(&SpatialPredicate::DWithin, &a, &b, 100_000.0, &metric(4326)).unwrap()
        );
    }

    /// `ST_DWithin` in a feet CRS compares metres, not feet.
    #[test]
    fn planar_dwithin_converts_metres_to_the_crs_unit() {
        let srid = Srid::new(2263).unwrap();
        let a = SridGeometry::new(srid, Geometry::point(0.0, 0.0));
        // 300 ft is about 91.44 m.
        let b = SridGeometry::new(srid, Geometry::point(300.0, 0.0));
        let within =
            |m: f64| apply_predicate(&SpatialPredicate::DWithin, &a, &b, m, &metric(2263)).unwrap();
        assert!(within(92.0));
        assert!(!within(91.0));
    }

    /// `ST_3DDWithin` counts the Z gap: 30 m apart in plan, 40 m in Z.
    #[test]
    fn dwithin_3d_counts_z() {
        let srid = Srid::new(32618).unwrap();
        let at = |x: f64, z: f64| {
            SridGeometry::with_ordinates(
                srid,
                Geometry::point(x, 0.0),
                Ordinates::new(Some(vec![z]), None),
            )
            .unwrap()
        };
        let (a, b) = (at(0.0, 0.0), at(30.0, 40.0));
        let within = |m: f64| {
            apply_predicate(&SpatialPredicate::DWithin3D, &a, &b, m, &metric(32618)).unwrap()
        };
        assert!(within(50.0));
        assert!(!within(49.9));
        assert!(apply_predicate(&SpatialPredicate::DWithin, &a, &b, 30.0, &metric(32618)).unwrap());
    }

    /// An EMPTY stored geometry matches no predicate.
    #[test]
    fn empty_matches_nothing() {
        let q = SridGeometry::wgs84(Geometry::point(0.0, 0.0));
        let empty = SridGeometry::wgs84(Geometry::empty_point());
        for predicate in [
            SpatialPredicate::DWithin,
            SpatialPredicate::DWithin3D,
            SpatialPredicate::Contains,
            SpatialPredicate::Intersects,
            SpatialPredicate::Within,
        ] {
            assert!(!apply_predicate(&predicate, &q, &empty, 1e9, &metric(4326)).unwrap());
        }
    }
}
