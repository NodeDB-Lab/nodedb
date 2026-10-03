// SPDX-License-Identifier: BUSL-1.1

//! Decode helpers for sync-engine `ReplicatedWrite` variants.
//!
//! Each function maps the destructured fields of one `ReplicatedWrite` variant
//! back to a `PhysicalPlan`, using the leader-assigned surrogates verbatim
//! rather than re-deriving identity through the local assigner. `wal_lsn` is
//! always `None` — followers allocate their own WAL LSN at apply time.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, ReturningSpec, SpatialOp, TextOp, TimeseriesOp, UpdateValue,
};
use nodedb_types::{RlsWriteCheck, Surrogate};

/// Decode optional sync provenance from the wire bytes.
///
/// Provenance carries the producer/epoch/seq that the Data Plane idempotency
/// gate uses to deduplicate replayed writes. A corrupt encoding must fail loud
/// (propagate) — the same contract as `geometry` decoding in
/// [`spatial_insert`] — rather than silently dropping to `None`. A silent drop
/// blinds the gate and risks double-applying the write on a follower.
pub fn decode_provenance(
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<Option<nodedb_types::sync::wire::SyncProvenance>> {
    match prov_bytes {
        Some(b) => zerompk::from_msgpack::<nodedb_types::sync::wire::SyncProvenance>(b)
            .map(Some)
            .map_err(|e| crate::Error::Internal {
                detail: format!("SyncProvenance decode failed: {e}"),
            }),
        None => Ok(None),
    }
}

/// Decode an optional msgpack-encoded RETURNING spec from the wire bytes.
///
/// Same contract as [`decode_provenance`]: a corrupt encoding fails loud
/// rather than silently dropping to `None`, which turns a caller's
/// `RETURNING` request into a silent empty result.
pub fn decode_returning(bytes: &Option<Vec<u8>>) -> crate::Result<Option<ReturningSpec>> {
    match bytes {
        Some(b) => zerompk::from_msgpack::<ReturningSpec>(b)
            .map(Some)
            .map_err(|e| crate::Error::Internal {
                detail: format!("ReturningSpec decode failed: {e}"),
            }),
        None => Ok(None),
    }
}

/// Fields carried on `ReplicatedWrite::ColumnarIngest` needed to reconstruct
/// `ColumnarOp::Insert`. Bundled into a struct — plain positional arguments
/// here exceed clippy's arity lint.
pub struct ColumnarIngestWire<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub schema_bytes: &'a [u8],
    pub surrogates: &'a [u32],
    pub prov_bytes: &'a Option<Vec<u8>>,
    pub returning_bytes: &'a Option<Vec<u8>>,
    pub rls_filters: &'a [u8],
}

pub fn columnar_ingest(wire: ColumnarIngestWire<'_>) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(wire.prov_bytes)?;
    let returning = decode_returning(wire.returning_bytes)?;
    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: nodedb_types::QualifiedCollection::from_stored(wire.collection.to_owned()),
        payload: wire.payload.to_vec(),
        format: wire.format.to_owned(),
        intent: wire.intent,
        on_conflict_updates: wire.on_conflict_updates.to_vec(),
        surrogates: wire
            .surrogates
            .iter()
            .copied()
            .map(Surrogate::new)
            .collect(),
        schema_bytes: wire.schema_bytes.to_vec(),
        provenance,
        wal_lsn: None,
        // No predicate here: this node applies an already-committed sync
        // write. The writing identity is not available on this node.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning,
        rls_filters: wire.rls_filters.to_vec(),
    }))
}

pub fn timeseries_ingest(
    collection: &str,
    payload: &[u8],
    format: &str,
    surrogates: &[u32],
    prov_bytes: &Option<Vec<u8>>,
    returning_bytes: &Option<Vec<u8>>,
    rls_filters: &[u8],
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    let returning = decode_returning(returning_bytes)?;
    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        payload: payload.to_vec(),
        format: format.to_owned(),
        wal_lsn: None,
        surrogates: surrogates.iter().copied().map(Surrogate::new).collect(),
        provenance,
        // No predicate here: this node applies an already-committed sync
        // write. The writing identity is not available on this node.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning,
        rls_filters: rls_filters.to_vec(),
    }))
}

pub fn fts_index(
    collection: &str,
    surrogate: u32,
    fields: &[(String, String)],
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Text(TextOp::FtsIndexDoc {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        surrogate: Surrogate::new(surrogate),
        fields: fields.to_vec(),
        provenance,
    }))
}

pub fn fts_delete(
    collection: &str,
    surrogate: Option<u32>,
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Text(TextOp::FtsDeleteDoc {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        surrogate: surrogate.map(Surrogate::new),
        provenance,
    }))
}

pub fn spatial_insert(
    collection: &str,
    field: &str,
    surrogate: u32,
    geometry_bytes: &[u8],
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let geometry = zerompk::from_msgpack::<nodedb_types::geometry::Geometry>(geometry_bytes)
        .map_err(|e| crate::Error::Internal {
            detail: format!("SpatialInsert geometry decode failed: {e}"),
        })?;
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Spatial(SpatialOp::Insert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field: field.to_owned(),
        surrogate: Surrogate::new(surrogate),
        geometry,
        provenance,
    }))
}

pub fn spatial_delete(
    collection: &str,
    field: &str,
    surrogate: Option<u32>,
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Spatial(SpatialOp::Delete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field: field.to_owned(),
        surrogate: surrogate.map(Surrogate::new),
        provenance,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::{ReplicatedEntry, ReplicatedWrite};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_types::geometry::Geometry;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{QualifiedCollection, RlsWriteCheck};

    /// Decide + encode in one call, so each test names only the plan it encodes.
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<ReplicatedEntry>> {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::encode::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &write,
        )
    }

    #[test]
    fn columnar_ingest_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 11,
            epoch: 3,
            stream_id: 2,
            seq: 77,
        };

        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"[{}]".to_vec(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: vec![
                nodedb_types::Surrogate::new(42),
                nodedb_types::Surrogate::new(43),
            ],
            schema_bytes: Vec::new(),
            provenance: Some(prov.clone()),
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("ColumnarIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
        match &decoded_entry.write {
            ReplicatedWrite::ColumnarIngest { surrogates, .. } => {
                assert_eq!(surrogates, &vec![42u32, 43u32], "surrogates must roundtrip");
            }
            other => panic!("expected ColumnarIngest, got {other:?}"),
        }
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                surrogates,
                provenance,
                wal_lsn,
                intent,
                on_conflict_updates,
                ..
            }) => {
                assert_eq!(
                    surrogates,
                    vec![
                        nodedb_types::Surrogate::new(42),
                        nodedb_types::Surrogate::new(43)
                    ]
                );
                assert_eq!(provenance, Some(prov));
                assert_eq!(wal_lsn, None, "wal_lsn must be None on decode");
                assert_eq!(intent, ColumnarInsertIntent::Insert);
                assert!(on_conflict_updates.is_empty());
            }
            other => panic!("expected Columnar(Insert), got {other:?}"),
        }
    }

    #[test]
    fn timeseries_ingest_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 5,
            epoch: 1,
            stream_id: 0,
            seq: 200,
        };

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "temps"),
            payload: b"data".to_vec(),
            format: crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT.into(),
            wal_lsn: None,
            surrogates: vec![nodedb_types::Surrogate::new(99)],
            provenance: Some(prov.clone()),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("TimeseriesIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                surrogates,
                provenance,
                wal_lsn,
                ..
            }) => {
                assert_eq!(surrogates, vec![nodedb_types::Surrogate::new(99)]);
                assert_eq!(provenance, Some(prov));
                assert_eq!(wal_lsn, None, "wal_lsn must be None on decode");
            }
            other => panic!("expected Timeseries(Ingest), got {other:?}"),
        }
    }

    /// Decode must not hardcode `on_conflict_updates: Vec::new()` — that turns
    /// `ON CONFLICT DO UPDATE` into a plain overwrite on followers.
    #[test]
    fn columnar_ingest_on_conflict_updates_roundtrip() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"[{}]".to_vec(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Put,
            on_conflict_updates: vec![("count".into(), UpdateValue::Literal(b"5".to_vec()))],
            surrogates: vec![nodedb_types::Surrogate::new(1)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("ColumnarIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                on_conflict_updates,
                ..
            }) => {
                assert_eq!(
                    on_conflict_updates,
                    vec![("count".to_owned(), UpdateValue::Literal(b"5".to_vec()))],
                    "ON CONFLICT DO UPDATE assignments must survive replication"
                );
            }
            other => panic!("expected Columnar(Insert), got {other:?}"),
        }
    }

    /// Decode must not hardcode `intent: ColumnarInsertIntent::Insert` — that
    /// silently drops `ON CONFLICT DO NOTHING` on replication.
    #[test]
    fn columnar_ingest_intent_roundtrip() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"[{}]".to_vec(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::InsertIfAbsent,
            on_conflict_updates: Vec::new(),
            surrogates: vec![nodedb_types::Surrogate::new(1)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("ColumnarIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert { intent, .. }) => {
                assert_eq!(
                    intent,
                    ColumnarInsertIntent::InsertIfAbsent,
                    "ON CONFLICT DO NOTHING must not degrade to a plain insert on replication"
                );
            }
            other => panic!("expected Columnar(Insert), got {other:?}"),
        }
    }

    /// Decode must not hardcode `format: "msgpack"` — that mis-tags a
    /// native-protocol JSON payload on replication.
    #[test]
    fn columnar_ingest_json_format_roundtrip() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"[{}]".to_vec(),
            format: "json".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: vec![nodedb_types::Surrogate::new(1)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("ColumnarIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert { format, .. }) => {
                assert_eq!(
                    format, "json",
                    "a JSON payload must not be mis-tagged as msgpack on replication"
                );
            }
            other => panic!("expected Columnar(Insert), got {other:?}"),
        }
    }

    /// Decode must not hardcode `returning: None` — that silently drops
    /// `RETURNING` rows once the write is replicated.
    #[test]
    fn columnar_ingest_returning_roundtrip() {
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Named(vec![
                nodedb_physical::physical_plan::ReturningItem {
                    name: "id".into(),
                    alias: None,
                },
            ]),
        };
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"[{}]".to_vec(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: vec![nodedb_types::Surrogate::new(1)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: Some(spec.clone()),
            rls_filters: Vec::new(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("ColumnarIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert { returning, .. }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING request must not silently yield no rows on replication"
                );
            }
            other => panic!("expected Columnar(Insert), got {other:?}"),
        }
    }

    /// Decode must not hardcode `rls_filters: Vec::new()` — an unreplicated read
    /// policy lets a `RETURNING` row set exceed what a `SELECT` can see.
    #[test]
    fn columnar_ingest_rls_filters_roundtrip() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"[{}]".to_vec(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: vec![nodedb_types::Surrogate::new(1)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: b"rls-predicate".to_vec(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("ColumnarIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert { rls_filters, .. }) => {
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Columnar(Insert), got {other:?}"),
        }
    }

    /// Pins the same `returning` / `rls_filters` bug as the columnar tests above,
    /// on the `TimeseriesOp::Ingest` sibling.
    #[test]
    fn timeseries_ingest_returning_and_rls_filters_roundtrip() {
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "temps"),
            payload: b"data".to_vec(),
            format: crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT.into(),
            wal_lsn: None,
            surrogates: vec![nodedb_types::Surrogate::new(99)],
            provenance: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("TimeseriesIngest should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING request must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Timeseries(Ingest), got {other:?}"),
        }
    }

    #[test]
    fn fts_index_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 7,
            epoch: 4,
            stream_id: 1,
            seq: 33,
        };

        let plan = PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
            surrogate: nodedb_types::Surrogate::new(500),
            fields: vec![
                ("body".to_string(), "hello world".to_string()),
                ("title".to_string(), "greeting".to_string()),
            ],
            provenance: Some(prov.clone()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("FtsIndex should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Text(TextOp::FtsIndexDoc {
                surrogate,
                fields,
                provenance,
                ..
            }) => {
                assert_eq!(surrogate, nodedb_types::Surrogate::new(500));
                assert_eq!(
                    fields,
                    vec![
                        ("body".to_string(), "hello world".to_string()),
                        ("title".to_string(), "greeting".to_string()),
                    ]
                );
                assert_eq!(provenance, Some(prov));
            }
            other => panic!("expected Text(FtsIndexDoc), got {other:?}"),
        }
    }

    #[test]
    fn fts_delete_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 8,
            epoch: 2,
            stream_id: 0,
            seq: 10,
        };

        let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
            surrogate: Some(nodedb_types::Surrogate::new(501)),
            provenance: Some(prov.clone()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("FtsDelete should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Text(TextOp::FtsDeleteDoc {
                surrogate,
                provenance,
                ..
            }) => {
                assert_eq!(surrogate, Some(nodedb_types::Surrogate::new(501)));
                assert_eq!(provenance, Some(prov));
            }
            other => panic!("expected Text(FtsDeleteDoc), got {other:?}"),
        }
    }

    /// A delete of a key its home never bound replicates as `None`, never
    /// as a placeholder surrogate.
    #[test]
    fn unbound_fts_delete_roundtrips_as_none() {
        let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
            surrogate: None,
            provenance: None,
        });
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect("encode must not error")
        .expect("FtsDelete should produce a ReplicatedEntry");
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&entry.to_bytes())
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        assert_eq!(decoded_plan, plan);
    }

    #[test]
    fn spatial_insert_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 3,
            epoch: 9,
            stream_id: 2,
            seq: 44,
        };
        let geometry = Geometry::Point {
            coordinates: [-73.985, 40.758],
        };

        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "location".into(),
            surrogate: nodedb_types::Surrogate::new(700),
            geometry: geometry.clone(),
            provenance: Some(prov.clone()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("SpatialInsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Spatial(SpatialOp::Insert {
                surrogate,
                geometry: decoded_geom,
                provenance,
                ..
            }) => {
                assert_eq!(surrogate, nodedb_types::Surrogate::new(700));
                assert_eq!(decoded_geom, geometry);
                assert_eq!(provenance, Some(prov));
            }
            other => panic!("expected Spatial(Insert), got {other:?}"),
        }
    }

    #[test]
    fn spatial_delete_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 2,
            epoch: 1,
            stream_id: 0,
            seq: 5,
        };

        let plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "location".into(),
            surrogate: Some(nodedb_types::Surrogate::new(701)),
            provenance: Some(prov.clone()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("SpatialDelete should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Spatial(SpatialOp::Delete {
                surrogate,
                provenance,
                ..
            }) => {
                assert_eq!(surrogate, Some(nodedb_types::Surrogate::new(701)));
                assert_eq!(provenance, Some(prov));
            }
            other => panic!("expected Spatial(Delete), got {other:?}"),
        }
    }
}
