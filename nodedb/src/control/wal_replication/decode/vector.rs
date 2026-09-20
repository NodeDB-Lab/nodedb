// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Vector`.
//!
//! Every surrogate is rebuilt verbatim from the record; `entry.rs` binds the
//! whole plan afterwards (a headless row self-keys there).

use super::super::decode_sync_engines;
use super::super::types::ReplicatedWrite;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::Surrogate;

/// Decode the full `ReplicatedWrite::Vector*` variant group: the four
/// original write shapes (`VectorInsert` / `VectorBatchInsert` /
/// `VectorDelete` / `SetVectorParams`) plus the six sparse / multi-vector /
/// direct-upsert / delete-by-surrogate additions, each delegated to its own
/// function below.
///
/// Delegated from `decode/entry.rs`'s single grouped match arm (all ten
/// `Vector*` patterns dispatch here) so that dispatcher stays under the file
/// size limit. `write` is guaranteed by that caller to already be one of
/// these ten variants — every other `ReplicatedWrite` variant is handled by
/// its own arm in `decode/entry.rs`'s exhaustive match and never reaches
/// here; the trailing arm below exists only because `write`'s static type is
/// the full enum, mirroring how `decode/entry.rs` itself handles `ArrayOp` /
/// `ArraySchema` reaching `to_physical_plan` (an internal dispatch-contract
/// violation, not a reachable production state).
pub(super) fn decode_arm(write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes,
            provenance,
        } => insert(InsertFields {
            collection,
            vector,
            dim: *dim,
            field_name,
            surrogate: *surrogate,
            pk_bytes,
            provenance,
        }),
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
            surrogates,
        } => batch_insert(collection, vectors, *dim, surrogates),
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => Ok(delete(collection, *vector_id)),
        ReplicatedWrite::SetVectorParams {
            collection,
            field_name,
            dim,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => Ok(set_params(SetParamsFields {
            collection,
            field_name,
            dim: *dim,
            m: *m,
            ef_construction: *ef_construction,
            metric,
            index_type,
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        })),
        ReplicatedWrite::DropVectorIndex {
            collection,
            field_name,
        } => Ok(drop_index(collection, field_name)),
        ReplicatedWrite::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => Ok(sparse_insert(collection, field_name, doc_id, entries)),
        ReplicatedWrite::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => Ok(sparse_delete(collection, field_name, doc_id)),
        ReplicatedWrite::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => Ok(multi_vector_insert(
            collection,
            field_name,
            *document_surrogate,
            vectors,
            *count,
            *dim,
        )),
        ReplicatedWrite::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => Ok(multi_vector_delete(
            collection,
            field_name,
            *document_surrogate,
        )),
        ReplicatedWrite::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => delete_by_surrogate(collection, *surrogate, field_name, provenance),
        ReplicatedWrite::DirectUpsert {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            intent,
            on_conflict_updates,
            returning,
            rls_filters,
        } => super::vector_direct::direct_upsert(super::vector_direct::DirectUpsertFields {
            collection,
            field,
            surrogate: *surrogate,
            pk_bytes,
            vector,
            payload,
            quantization: *quantization,
            storage_dtype: *storage_dtype,
            payload_indexes,
            intent: *intent,
            on_conflict_updates,
            returning: decode_sync_engines::decode_returning(returning)?,
            rls_filters,
        }),
        ReplicatedWrite::VectorDirectDelete {
            collection,
            field,
            targets,
            returning,
            rls_filters,
        } => Ok(super::vector_direct::direct_delete(
            collection,
            field,
            targets,
            decode_sync_engines::decode_returning(returning)?,
            rls_filters,
        )),
        ReplicatedWrite::VectorDirectTruncate {
            collection,
            field,
            restart_identity,
        } => Ok(super::vector_direct::direct_truncate(
            collection,
            field,
            *restart_identity,
        )),
        ReplicatedWrite::VectorDirectUpdate {
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
            returning,
            rls_filters,
        } => Ok(super::vector_direct::direct_update(
            super::vector_direct::DirectUpdateFields {
                collection,
                field,
                targets,
                new_vector,
                payload_patch,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                returning: decode_sync_engines::decode_returning(returning)?,
                rls_filters,
            },
        )),
        ReplicatedWrite::VectorResolvedDirectWrite {
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            mutations,
            response_payload,
        } => Ok(super::vector_direct::resolved_direct_write(
            super::vector_direct::ResolvedDirectWriteFields {
                collection,
                field,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                mutations,
                response_payload,
            },
        )),
        _ => Err(crate::Error::Internal {
            detail: "vector::decode_arm called with a non-Vector ReplicatedWrite variant \
                (dispatch bug in decode/entry.rs's grouped Vector match arm)"
                .into(),
        }),
    }
}

/// Fields of the `VectorInsert` wire variant, bundled so [`insert`] stays
/// under the `too_many_arguments` clippy threshold.
pub(super) struct InsertFields<'a> {
    pub(super) collection: &'a str,
    pub(super) vector: &'a [f32],
    pub(super) dim: usize,
    pub(super) field_name: &'a str,
    pub(super) surrogate: u32,
    pub(super) pk_bytes: &'a Option<Vec<u8>>,
    pub(super) provenance: &'a Option<Vec<u8>>,
}

pub(super) fn insert(f: InsertFields) -> crate::Result<PhysicalPlan> {
    let provenance = decode_sync_engines::decode_provenance(f.provenance)?;
    Ok(PhysicalPlan::Vector(VectorOp::Insert {
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
        vector: f.vector.to_vec(),
        dim: f.dim,
        field_name: f.field_name.to_owned(),
        surrogate: Surrogate::new(f.surrogate),
        pk_bytes: f.pk_bytes.clone(),
        provenance,
    }))
}

pub(super) fn batch_insert(
    collection: &str,
    vectors: &[Vec<f32>],
    dim: usize,
    surrogates: &[u32],
) -> crate::Result<PhysicalPlan> {
    // The carried surrogate vector MUST be 1:1 with the vectors.
    // A mismatch is a corrupt/incompatible entry — fail loud rather
    // than truncate or zip-shorten (which would silently drop rows
    // or mis-bind identities).
    if surrogates.len() != vectors.len() {
        return Err(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "VectorBatchInsert surrogate/vector count mismatch: {} surrogates, {} vectors",
                surrogates.len(),
                vectors.len()
            ),
        });
    }
    let surrogates: Vec<Surrogate> = surrogates.iter().map(|&raw| Surrogate::new(raw)).collect();
    Ok(PhysicalPlan::Vector(VectorOp::BatchInsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        vectors: vectors.to_vec(),
        dim,
        surrogates,
    }))
}

pub(super) fn delete(collection: &str, vector_id: u32) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::Delete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        vector_id,
    })
}

/// Fields of the `SetVectorParams` wire variant, bundled so [`set_params`]
/// stays under the `too_many_arguments` clippy threshold.
pub(super) struct SetParamsFields<'a> {
    pub(super) collection: &'a str,
    pub(super) field_name: &'a str,
    pub(super) dim: usize,
    pub(super) m: usize,
    pub(super) ef_construction: usize,
    pub(super) metric: &'a str,
    pub(super) index_type: &'a str,
    pub(super) pq_m: usize,
    pub(super) ivf_cells: usize,
    pub(super) ivf_nprobe: usize,
}

pub(super) fn set_params(f: SetParamsFields) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
        field_name: f.field_name.to_owned(),
        dim: f.dim,
        m: f.m,
        ef_construction: f.ef_construction,
        metric: f.metric.to_owned(),
        index_type: f.index_type.to_owned(),
        pq_m: f.pq_m,
        ivf_cells: f.ivf_cells,
        ivf_nprobe: f.ivf_nprobe,
    })
}

pub(super) fn drop_index(collection: &str, field_name: &str) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DropIndex {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field_name: field_name.to_owned(),
    })
}

pub(super) fn sparse_insert(
    collection: &str,
    field_name: &str,
    doc_id: &str,
    entries: &[(u32, f32)],
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SparseInsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field_name: field_name.to_owned(),
        doc_id: doc_id.to_owned(),
        entries: entries.to_vec(),
    })
}

pub(super) fn sparse_delete(collection: &str, field_name: &str, doc_id: &str) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SparseDelete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field_name: field_name.to_owned(),
        doc_id: doc_id.to_owned(),
    })
}

pub(super) fn multi_vector_insert(
    collection: &str,
    field_name: &str,
    document_surrogate: u32,
    vectors: &[f32],
    count: usize,
    dim: usize,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field_name: field_name.to_owned(),
        document_surrogate: Surrogate::new(document_surrogate),
        vectors: vectors.to_vec(),
        count,
        dim,
    })
}

pub(super) fn multi_vector_delete(
    collection: &str,
    field_name: &str,
    document_surrogate: u32,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field_name: field_name.to_owned(),
        // Deletes an already-bound identity; nothing to bind.
        document_surrogate: Surrogate::new(document_surrogate),
    })
}

pub(super) fn delete_by_surrogate(
    collection: &str,
    surrogate: u32,
    field_name: &str,
    provenance: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_sync_engines::decode_provenance(provenance)?;
    Ok(PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        // Deletes an already-bound identity; no re-binding needed.
        surrogate: Surrogate::new(surrogate),
        field_name: field_name.to_owned(),
        provenance,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::ReplicatedEntry;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::ReturningSpec;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{
        PayloadIndexKind, QualifiedCollection, VectorQuantization, VectorStorageDtype,
    };

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
    fn vector_insert_provenance_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 42,
            epoch: 7,
            stream_id: 3,
            seq: 100,
        };

        // With provenance.
        let plan = PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            vector: vec![0.1, 0.2, 0.3],
            dim: 3,
            field_name: "emb".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: None,
            provenance: Some(prov.clone()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("VectorInsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
        let decoded_plan = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        let (_, _, decoded_plan, _) = decoded_plan;
        match decoded_plan {
            PhysicalPlan::Vector(VectorOp::Insert { provenance, .. }) => {
                assert_eq!(
                    provenance,
                    Some(prov.clone()),
                    "VectorInsert provenance should round-trip"
                );
            }
            other => panic!("expected VectorInsert, got {other:?}"),
        }
        drop(decoded_entry);

        // Without provenance.
        let plan_none = PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            vector: vec![0.1, 0.2, 0.3],
            dim: 3,
            field_name: "emb".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        });
        let entry_none = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan_none)
            .expect("encode must not error")
            .expect("VectorInsert(no provenance) should produce a ReplicatedEntry");
        let bytes_none = entry_none.to_bytes();
        let (_, _, decoded_none, _) = decode::from_replicated_entry(&bytes_none, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_none {
            PhysicalPlan::Vector(VectorOp::Insert { provenance, .. }) => {
                assert_eq!(
                    provenance, None,
                    "None provenance should round-trip as None"
                );
            }
            other => panic!("expected VectorInsert, got {other:?}"),
        }
    }

    #[test]
    fn sparse_insert_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let plan = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field_name: "splade".into(),
            doc_id: "doc-42".into(),
            entries: vec![(10, 0.25), (20, 0.75)],
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("SparseInsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        assert_eq!(decoded_plan, plan, "SparseInsert must round-trip exactly");
    }

    #[test]
    fn sparse_delete_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let plan = PhysicalPlan::Vector(VectorOp::SparseDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field_name: "splade".into(),
            doc_id: "doc-42".into(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("SparseDelete should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        assert_eq!(decoded_plan, plan, "SparseDelete must round-trip exactly");
    }

    #[test]
    fn multi_vector_insert_roundtrip_shares_one_surrogate() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let shared_surrogate = Surrogate::new(777);
        // Three vectors, dim 2 each, all bound to the SAME document_surrogate.
        let plan = PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field_name: "colbert".into(),
            document_surrogate: shared_surrogate,
            vectors: vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
            count: 3,
            dim: 2,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("MultiVectorInsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();

        // Wire representation carries the raw u32 verbatim.
        let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
        match &decoded_entry.write {
            ReplicatedWrite::MultiVectorInsert {
                document_surrogate,
                count,
                vectors,
                ..
            } => {
                assert_eq!(
                    *document_surrogate, 777u32,
                    "surrogate must roundtrip on wire"
                );
                assert_eq!(*count, 3);
                assert_eq!(vectors.len(), 6, "flat vector data must roundtrip in full");
            }
            other => panic!("expected MultiVectorInsert, got {other:?}"),
        }

        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match &decoded_plan {
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                document_surrogate,
                count,
                ..
            }) => {
                assert_eq!(
                    *document_surrogate, shared_surrogate,
                    "all vectors of the document must share the one carried surrogate"
                );
                assert_eq!(*count, 3);
            }
            other => panic!("expected Vector(MultiVectorInsert), got {other:?}"),
        }
        assert_eq!(
            decoded_plan, plan,
            "MultiVectorInsert must round-trip exactly"
        );
    }

    #[test]
    fn multi_vector_delete_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let plan = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field_name: "colbert".into(),
            document_surrogate: Surrogate::new(888),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("MultiVectorDelete should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match &decoded_plan {
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                document_surrogate, ..
            }) => {
                assert_eq!(*document_surrogate, Surrogate::new(888));
            }
            other => panic!("expected Vector(MultiVectorDelete), got {other:?}"),
        }
        assert_eq!(
            decoded_plan, plan,
            "MultiVectorDelete must round-trip exactly"
        );
    }

    #[test]
    fn delete_by_surrogate_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 4,
            epoch: 1,
            stream_id: 0,
            seq: 9,
        };
        let plan = PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            surrogate: Surrogate::new(555),
            field_name: "emb".into(),
            provenance: Some(prov.clone()),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("DeleteBySurrogate should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match &decoded_plan {
            PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
                surrogate,
                provenance,
                ..
            }) => {
                assert_eq!(*surrogate, Surrogate::new(555));
                assert_eq!(*provenance, Some(prov));
            }
            other => panic!("expected Vector(DeleteBySurrogate), got {other:?}"),
        }
        assert_eq!(
            decoded_plan, plan,
            "DeleteBySurrogate must round-trip exactly"
        );
    }

    #[test]
    fn direct_upsert_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "primary_vecs"),
            field: "embedding".into(),
            surrogate: Surrogate::new(999),
            pk_bytes: Vec::new(),
            vector: vec![0.1, 0.2, 0.3, 0.4],
            payload: b"\x81\xa4name\xa5alice".to_vec(),
            quantization: VectorQuantization::Bbq,
            storage_dtype: VectorStorageDtype::BF16,
            payload_indexes: vec![
                ("category".into(), PayloadIndexKind::Equality),
                ("price".into(), PayloadIndexKind::Range),
            ],
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("DirectUpsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match &decoded_plan {
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                surrogate,
                quantization,
                storage_dtype,
                payload_indexes,
                ..
            }) => {
                assert_eq!(*surrogate, Surrogate::new(999));
                assert_eq!(*quantization, VectorQuantization::Bbq);
                assert_eq!(*storage_dtype, VectorStorageDtype::BF16);
                assert_eq!(
                    payload_indexes,
                    &vec![
                        ("category".to_string(), PayloadIndexKind::Equality),
                        ("price".to_string(), PayloadIndexKind::Range),
                    ]
                );
            }
            other => panic!("expected Vector(DirectUpsert), got {other:?}"),
        }
        assert_eq!(decoded_plan, plan, "DirectUpsert must round-trip exactly");
    }

    /// `entry_vector::encode` must not drop `VectorOp::DirectUpsert::returning` /
    /// `rls_filters` — a dropped field silently yields no rows on replication.
    #[test]
    fn vector_direct_upsert_returning_and_rls_filters_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".into(),
            surrogate: Surrogate::new(3),
            pk_bytes: Vec::new(),
            vector: vec![0.5, 0.6],
            payload: vec![1, 2, 3],
            quantization: VectorQuantization::RaBitQ,
            storage_dtype: VectorStorageDtype::F16,
            payload_indexes: vec![("tenant_id".into(), PayloadIndexKind::Equality)],
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("VectorOp::DirectUpsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING upsert must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Vector(DirectUpsert), got {other:?}"),
        }
    }
}
