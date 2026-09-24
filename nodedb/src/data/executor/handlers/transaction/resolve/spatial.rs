// SPDX-License-Identifier: BUSL-1.1

//! Spatial serializer for transaction resolve.
//!
//! **Plan-driven, not overlay-driven.** `SpatialOp::Insert` / `SpatialOp::Delete`
//! plan nodes already carry the complete absolute post-image (collection,
//! field, surrogate, geometry, provenance) needed to reconstruct the
//! engine-native `SpatialPut` / `SpatialDelete` WAL sub-record. Unlike KV
//! `Incr` or a document field merge, there is no accumulation across the plan
//! set to resolve — each op is already the final value — so no overlay walk
//! is needed; this mirrors the vector serializer's plan-driven shape, not the
//! KV / document / graph overlay-driven family.
//!
//! `stage_spatial_insert` / `stage_spatial_delete` (`stage_write/stage_spatial.rs`)
//! DO write into the shared per-transaction overlay, but only so a later
//! same-transaction spatial `SELECT` observes the write before COMMIT
//! (`overlay::spatial_merge`); that overlay entry is a read-your-own-writes
//! aid and is never consulted here.
//!
//! Reuses [`encode_spatial_put_payload`] / [`encode_spatial_delete_payload`]
//! (`control::server::wal_dispatch_fts_spatial`), the SAME builders the sync
//! ingest autocommit path (`control::server::sync::spatial_handler`) uses to
//! append its `SpatialPut` / `SpatialDelete` WAL records, so producer and
//! `replay_spatial_wal` never drift.
//!
//! ## Provenance
//!
//! A spatial write from the Lite sync path carries its producer provenance. A
//! write from SQL carries none, and resolve writes the empty provenance
//! (`producer_id` 0) in its place, as the autocommit spatial WAL path does.
//! Replay and the sync gate treat producer 0 as "no producer": no
//! high-water mark moves and no undo is captured for one.
//!
//! ## Reads
//!
//! `SpatialOp::Scan` is read-only and emits no redo sub-record.

use nodedb_physical::physical_plan::SpatialOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    encode_spatial_delete_payload, encode_spatial_put_payload,
};
use crate::wal::RedoSubRecord;

/// Append the redo sub-record for a single spatial plan op to `ops`.
///
/// `Insert` / `Delete` serialize to their engine-native `SpatialPut` /
/// `SpatialDelete` shape. `Scan` emits nothing.
pub(super) fn serialize_spatial_op(
    op: &SpatialOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        } => {
            let prov = provenance.clone().unwrap_or_default();
            let payload = encode_spatial_put_payload(
                collection.as_str(),
                field,
                *surrogate,
                geometry,
                &prov,
            )?;
            let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::SpatialPut as u32,
                payload: bytes,
            });
            Ok(())
        }
        SpatialOp::Delete {
            collection,
            field,
            surrogate,
            provenance,
        } => {
            let prov = provenance.clone().unwrap_or_default();
            let payload =
                encode_spatial_delete_payload(collection.as_str(), field, *surrogate, &prov);
            let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::SpatialDelete as u32,
                payload: bytes,
            });
            Ok(())
        }

        // Read-only: no persisted post-image.
        SpatialOp::Scan { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_types::Surrogate;
    use nodedb_types::geometry::Geometry;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    fn prov(seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 1,
            seq,
        }
    }

    fn point(x: f64, y: f64) -> Geometry {
        Geometry::point(x, y)
    }

    #[test]
    fn insert_emits_spatial_put_sub_record() {
        let op = SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(7),
            geometry: point(10.0, 20.0),
            provenance: Some(prov(1)),
        };
        let mut ops = Vec::new();
        serialize_spatial_op(&op, &mut ops).expect("serialize insert");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::SpatialPut as u32);

        let decoded = nodedb_wal::record::SpatialPutPayload::from_bytes(&ops[0].payload)
            .expect("decode SpatialPutPayload");
        assert_eq!(decoded.collection, "places");
        assert_eq!(decoded.field, "loc");
        let geom: Geometry =
            zerompk::from_msgpack(&decoded.geometry_bytes).expect("decode geometry");
        assert_eq!(geom, point(10.0, 20.0));
    }

    #[test]
    fn delete_emits_spatial_delete_sub_record() {
        let op = SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(7),
            provenance: Some(prov(2)),
        };
        let mut ops = Vec::new();
        serialize_spatial_op(&op, &mut ops).expect("serialize delete");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::SpatialDelete as u32);

        let decoded = nodedb_wal::record::SpatialDeletePayload::from_bytes(&ops[0].payload)
            .expect("decode SpatialDeletePayload");
        assert_eq!(decoded.collection, "places");
        assert_eq!(decoded.field, "loc");
    }

    #[test]
    fn scan_emits_nothing() {
        let op = SpatialOp::Scan {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            predicate: nodedb_physical::physical_plan::SpatialPredicate::Intersects,
            query_geometry: point(0.0, 0.0),
            distance_meters: 0.0,
            attribute_filters: Vec::new(),
            limit: 10,
            projection: Vec::new(),
            rls_filters: Vec::new(),
            prefilter: None,
        };
        let mut ops = Vec::new();
        serialize_spatial_op(&op, &mut ops).expect("serialize scan");
        assert!(ops.is_empty(), "read-only scan emits no sub-record");
    }

    /// A SQL spatial insert carries no provenance. It resolves to a
    /// `SpatialPut` with the empty provenance, never a dropped write.
    #[test]
    fn insert_without_provenance_resolves_with_the_empty_provenance() {
        let op = SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(1),
            geometry: point(1.0, 1.0),
            provenance: None,
        };
        let mut ops = Vec::new();
        serialize_spatial_op(&op, &mut ops).expect("serialize insert");
        assert_eq!(ops.len(), 1, "the insert is never dropped");
        assert_eq!(ops[0].record_type, RecordType::SpatialPut as u32);
        let decoded = nodedb_wal::record::SpatialPutPayload::from_bytes(&ops[0].payload)
            .expect("decode SpatialPutPayload");
        assert_eq!(decoded.provenance, SyncProvenance::default());
    }

    /// A SQL spatial delete carries no provenance. It resolves to a
    /// `SpatialDelete` with the empty provenance, never a dropped write.
    #[test]
    fn delete_without_provenance_resolves_with_the_empty_provenance() {
        let op = SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(1),
            provenance: None,
        };
        let mut ops = Vec::new();
        serialize_spatial_op(&op, &mut ops).expect("serialize delete");
        assert_eq!(ops.len(), 1, "the delete is never dropped");
        assert_eq!(ops[0].record_type, RecordType::SpatialDelete as u32);
        let decoded = nodedb_wal::record::SpatialDeletePayload::from_bytes(&ops[0].payload)
            .expect("decode SpatialDeletePayload");
        assert_eq!(decoded.provenance, SyncProvenance::default());
    }
}
