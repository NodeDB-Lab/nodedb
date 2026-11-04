// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Crdt`.
//!
//! Every surrogate is rebuilt verbatim from the record; `entry.rs` binds the
//! whole plan afterwards. A `CrdtApply` carrying `Surrogate::ZERO` is refused
//! there: every apply carries the surrogate its coordinator bound.

use super::super::decode_sync_engines;
use super::super::types::ConstraintChangeOp;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{CrdtOp, ReturningSpec};
use nodedb_types::QualifiedCollection;

pub(super) struct ApplyArgs<'a> {
    pub(super) collection: &'a str,
    pub(super) document_id: &'a str,
    pub(super) delta: &'a [u8],
    pub(super) peer_id: u64,
    pub(super) provenance_bytes: &'a Option<Vec<u8>>,
    pub(super) constraint_version_required: u64,
    pub(super) expected_frontier_digest: Option<[u8; 32]>,
    pub(super) auth_user_id: u64,
    pub(super) auth_device_id: u64,
    pub(super) auth_seq_no: u64,
    pub(super) delta_signature: [u8; 32],
    pub(super) signing_required: bool,
    pub(super) authenticated: bool,
    /// Leader-assigned surrogate carried on the wire.
    pub(super) carried_surrogate: u32,
}

pub(super) fn apply(database_id: DatabaseId, args: ApplyArgs<'_>) -> crate::Result<PhysicalPlan> {
    let ApplyArgs {
        collection,
        document_id,
        delta,
        peer_id,
        provenance_bytes,
        constraint_version_required,
        expected_frontier_digest,
        auth_user_id,
        auth_device_id,
        auth_seq_no,
        delta_signature,
        signing_required,
        authenticated,
        carried_surrogate,
    } = args;
    let surrogate = nodedb_types::Surrogate::new(carried_surrogate);
    let provenance = decode_sync_engines::decode_provenance(provenance_bytes)?;
    if authenticated {
        let provenance = provenance.ok_or_else(|| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: "authenticated CRDT replay is missing sync provenance".into(),
        })?;
        Ok(PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
            collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        }))
    } else {
        Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        }))
    }
}

/// Per-collection Loro doc import — no surrogate, no provenance. Every
/// replica applies the same snapshot via the same idempotent Loro merge,
/// converging deterministically.
pub(super) fn import_collection(
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
    bytes: &[u8],
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id,
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        bytes: bytes.to_vec(),
    }))
}

/// Narrow a wire `u64` list position/index to the `usize` the live
/// `execute_crdt_list_*` handlers take. `usize::try_from` (never `as`): a
/// value that doesn't fit `usize` on this platform is a corrupt/incompatible
/// wire payload, not a value to silently truncate and replay at the wrong
/// position.
fn list_index(field: &str, value: u64) -> crate::Result<usize> {
    usize::try_from(value).map_err(|_| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("CrdtList {field}={value} does not fit usize on this platform"),
    })
}

/// Reconstruct `CrdtOp::ListInsert` from its wire intent. `surrogate` is the
/// parent document's identity, carried verbatim.
pub(super) fn list_insert(
    database_id: DatabaseId,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: u64,
    fields_json: &str,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Crdt(CrdtOp::ListInsert {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: list_index("index", index)?,
        fields_json: fields_json.to_owned(),
        surrogate,
    }))
}

/// Reconstruct `CrdtOp::ListDelete` from its wire intent. See
/// [`list_insert`] for the surrogate note.
pub(super) fn list_delete(
    database_id: DatabaseId,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Crdt(CrdtOp::ListDelete {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: list_index("index", index)?,
        surrogate,
    }))
}

/// Reconstruct `CrdtOp::ListMove` from its wire intent. `from_index` and
/// `to_index` are narrowed independently so a value that fits one but not
/// the other still surfaces as a typed decode error rather than silently
/// substituting. See [`list_insert`] for the surrogate note.
pub(super) fn list_move(
    database_id: DatabaseId,
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: u64,
    to_index: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Crdt(CrdtOp::ListMove {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        from_index: list_index("from_index", from_index)?,
        to_index: list_index("to_index", to_index)?,
        surrogate,
    }))
}

/// Reconstruct `CrdtOp::DocDelete` from its wire intent. The row's own
/// top-level `surrogate` is carried across the wire and rebuilt via
/// `Surrogate::new`. `None`: the key is unbound, so no row matches.
pub(super) fn doc_delete(
    database_id: DatabaseId,
    collection: &str,
    document_id: &str,
    surrogate: Option<u32>,
    returning: Option<ReturningSpec>,
    rls_filters: &[u8],
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Crdt(CrdtOp::DocDelete {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        document_id: document_id.to_owned(),
        surrogate: surrogate.map(nodedb_types::Surrogate::new),
        // Carried on the record — a replay re-executes this write for the
        // originating request, not only for the follower's own state.
        returning,
        rls_filters: rls_filters.to_vec(),
    }))
}

pub(super) fn constraint_change(
    database_id: DatabaseId,
    collection: &str,
    op: &ConstraintChangeOp,
    constraint_version: u64,
    constraints: &[Vec<u8>],
) -> crate::Result<PhysicalPlan> {
    let collection = QualifiedCollection::from_stored(database_id, collection.to_owned())?;
    Ok(match op {
        ConstraintChangeOp::Set => PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection,
            constraint_version,
            constraints: constraints.to_vec(),
        }),
        ConstraintChangeOp::Drop => PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection,
            constraint_version,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::{ReplicatedEntry, ReplicatedWrite};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::CrdtWriteVerb;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{QualifiedCollection, Surrogate};

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
    fn crdt_apply_fenced_and_unfenced_wire_shapes() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 99,
            epoch: 2,
            stream_id: 1,
            seq: 55,
        };

        // With provenance.
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc-1".into(),
            delta: vec![0xDE, 0xAD],
            peer_id: 7,
            mutation_id: 0,
            surrogate: Surrogate::new(11),
            provenance: Some(prov.clone()),
            constraint_version_required: 42,
            expected_frontier_digest: Some([42; 32]),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtApply should produce a ReplicatedEntry");
        assert!(matches!(
            &entry.write,
            ReplicatedWrite::CrdtApplyFenced { .. }
        ));
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::Apply {
                provenance,
                constraint_version_required,
                expected_frontier_digest,
                ..
            }) => {
                assert_eq!(
                    provenance,
                    Some(prov.clone()),
                    "CrdtApply provenance should round-trip"
                );
                assert_eq!(
                    constraint_version_required, 42,
                    "CrdtApply constraint_version_required should round-trip"
                );
                assert_eq!(
                    expected_frontier_digest,
                    Some([42; 32]),
                    "CrdtApply expected_frontier_digest should round-trip"
                );
            }
            other => panic!("expected CrdtApply, got {other:?}"),
        }

        // An apply with no provenance encodes as the unfenced shape and
        // decodes with no fence.
        let unfenced = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::CrdtApply {
                collection: "docs".into(),
                document_id: "doc-unfenced".into(),
                delta: vec![0xBE, 0xEF],
                peer_id: 8,
                provenance: None,
                constraint_version_required: 0,
                surrogate: 12,
            },
        );
        let unfenced_bytes = unfenced.to_bytes();
        let (_, _, decoded_unfenced, _) = decode::decode_replicated_entry(&unfenced_bytes)
            .expect("unfenced CrdtApply must decode")
            .expect("unfenced CrdtApply must produce a plan");
        match decoded_unfenced {
            PhysicalPlan::Crdt(CrdtOp::Apply {
                provenance,
                expected_frontier_digest,
                ..
            }) => {
                assert_eq!(provenance, None, "unfenced provenance should remain absent");
                assert_eq!(
                    expected_frontier_digest, None,
                    "unfenced CrdtApply must decode without a frontier fence"
                );
            }
            other => panic!("expected unfenced CrdtApply, got {other:?}"),
        }
    }

    #[test]
    fn crdt_list_insert_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "doc-1".into(),
            list_path: "blocks".into(),
            index: 2,
            fields_json: r#"{"type":"text"}"#.into(),
            surrogate: Surrogate::new(1),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::ListInsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::ListInsert {
                collection,
                document_id,
                list_path,
                index,
                fields_json,
                ..
            }) => {
                assert_eq!(collection.as_str(), "notes");
                assert_eq!(document_id, "doc-1");
                assert_eq!(list_path, "blocks");
                assert_eq!(index, 2, "index must round-trip");
                assert_eq!(fields_json, r#"{"type":"text"}"#);
            }
            other => panic!("expected CrdtOp::ListInsert, got {other:?}"),
        }
    }

    #[test]
    fn crdt_list_delete_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "doc-1".into(),
            list_path: "blocks".into(),
            index: 5,
            surrogate: Surrogate::new(1),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::ListDelete should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::ListDelete {
                collection,
                document_id,
                list_path,
                index,
                ..
            }) => {
                assert_eq!(collection.as_str(), "notes");
                assert_eq!(document_id, "doc-1");
                assert_eq!(list_path, "blocks");
                assert_eq!(index, 5, "index must round-trip");
            }
            other => panic!("expected CrdtOp::ListDelete, got {other:?}"),
        }
    }

    /// `from_index` and `to_index` are two distinct required wire fields, not
    /// one `Option<u64>` slot each — they must never collapse to the same value.
    #[test]
    fn crdt_list_move_roundtrip_distinct_indices() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "doc-1".into(),
            list_path: "blocks".into(),
            from_index: 3,
            to_index: 1,
            surrogate: Surrogate::new(1),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::ListMove should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::ListMove {
                collection,
                document_id,
                list_path,
                from_index,
                to_index,
                ..
            }) => {
                assert_eq!(collection.as_str(), "notes");
                assert_eq!(document_id, "doc-1");
                assert_eq!(list_path, "blocks");
                assert_eq!(from_index, 3, "from_index must survive the round trip");
                assert_eq!(to_index, 1, "to_index must survive the round trip");
                assert_ne!(
                    from_index, to_index,
                    "distinct indices must never collapse to the same value"
                );
            }
            other => panic!("expected CrdtOp::ListMove, got {other:?}"),
        }
    }

    #[test]
    fn constraint_change_set_decodes_to_set_constraints() {
        // Decode layer keeps constraint blobs opaque, so raw bytes are sufficient
        // and avoid coupling this test to the constraint wire layout.
        let entry = ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::ConstraintChange {
                collection: "users".into(),
                op: ConstraintChangeOp::Set,
                constraint_version: 12,
                constraints: vec![vec![1, 2, 3]],
            },
        );
        let bytes = entry.to_bytes();
        let (_, _, plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("ConstraintChange(Set) must decode to a plan");
        match plan {
            PhysicalPlan::Crdt(CrdtOp::SetConstraints {
                collection,
                constraint_version,
                constraints,
            }) => {
                assert_eq!(collection.as_str(), "users");
                assert_eq!(constraint_version, 12);
                assert_eq!(constraints.len(), 1);
                assert_eq!(constraints[0], vec![1, 2, 3]);
            }
            other => panic!("expected Crdt(SetConstraints), got {other:?}"),
        }
    }

    #[test]
    fn constraint_change_drop_decodes_to_drop_constraints() {
        let entry = ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::ConstraintChange {
                collection: "users".into(),
                op: ConstraintChangeOp::Drop,
                constraint_version: 8,
                constraints: Vec::new(),
            },
        );
        let bytes = entry.to_bytes();
        let (_, _, plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("ConstraintChange(Drop) must decode to a plan");
        match plan {
            PhysicalPlan::Crdt(CrdtOp::DropConstraints {
                collection,
                constraint_version,
            }) => {
                assert_eq!(collection.as_str(), "users");
                assert_eq!(constraint_version, 8);
            }
            other => panic!("expected Crdt(DropConstraints), got {other:?}"),
        }
    }

    #[test]
    fn crdt_set_constraints_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            constraint_version: 7,
            constraints: vec![vec![1, 2, 3], vec![4, 5]],
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::SetConstraints should replicate as a ConstraintChange");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::SetConstraints {
                collection,
                constraint_version,
                constraints,
            }) => {
                assert_eq!(collection.as_str(), "accounts");
                assert_eq!(constraint_version, 7, "version fence must round-trip");
                assert_eq!(constraints, vec![vec![1, 2, 3], vec![4, 5]]);
            }
            other => panic!("expected CrdtOp::SetConstraints, got {other:?}"),
        }
    }

    #[test]
    fn crdt_drop_constraints_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            constraint_version: 9,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::DropConstraints should replicate as a ConstraintChange");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::DropConstraints {
                collection,
                constraint_version,
            }) => {
                assert_eq!(collection.as_str(), "accounts");
                assert_eq!(constraint_version, 9, "version fence must round-trip");
            }
            other => panic!("expected CrdtOp::DropConstraints, got {other:?}"),
        }
    }

    /// `entry_crdt::encode` must not drop `CrdtOp::DocUpsert::returning` /
    /// `rls_filters` — a dropped field silently yields no rows on replication.
    #[test]
    fn crdt_doc_upsert_returning_and_rls_filters_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let spec = ReturningSpec {
            columns: nodedb_physical::physical_plan::ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".into(),
            fields_json: "{}".into(),
            surrogate: Surrogate::new(4),
            partial: false,
            verb: CrdtWriteVerb::Insert,
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::DocUpsert should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::DocUpsert {
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
            other => panic!("expected Crdt(DocUpsert), got {other:?}"),
        }
    }

    /// Real (non-`Noop`) `SurrogateAssigner` over a temp `redb` catalog — needed
    /// to prove decode binds the carried surrogate instead of allocating fresh.
    fn open_test_assigner() -> (
        tempfile::TempDir,
        crate::control::surrogate::SurrogateAssigner,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let credentials = std::sync::Arc::new(
            crate::control::security::credential::CredentialStore::open(
                &dir.path().join("system.redb"),
            )
            .expect("open credential store"),
        );
        let registry = std::sync::Arc::new(std::sync::RwLock::new(
            crate::control::surrogate::SurrogateRegistry::new(),
        ));
        let wal: std::sync::Arc<dyn crate::control::surrogate::SurrogateWalAppender> =
            std::sync::Arc::new(crate::control::surrogate::NoopWalAppender);
        let assigner =
            crate::control::surrogate::SurrogateAssigner::new(registry, credentials, wal);
        (dir, assigner)
    }

    /// `CrdtOp::Apply` decode binds the carried surrogate (first-wins), never
    /// re-derives via the local allocator. Advances the local allocator past
    /// the carried value first, so a divergent fresh `assign()` is distinguishable.
    #[tokio::test]
    async fn crdt_apply_binds_carried_surrogate_not_fresh_allocation() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let (_dir, assigner) = open_test_assigner();

        // Burn local allocations on unrelated keys so a fresh assign() for
        // "doc-1" diverges from the leader-carried value below.
        for i in 0..5 {
            assigner
                .assign(
                    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "docs"),
                    tenant,
                    format!("burn-{i}").as_bytes(),
                )
                .await
                .expect("burn allocation");
        }

        let leader_surrogate = Surrogate::new(9_001);
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc-1".into(),
            delta: vec![0xDE, 0xAD],
            peer_id: 1,
            mutation_id: 0,
            surrogate: leader_surrogate,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("CrdtOp::Apply should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();

        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, &assigner)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::Apply { surrogate, .. }) => {
                assert_eq!(
                    surrogate, leader_surrogate,
                    "decode must bind the leader-carried surrogate, not allocate a fresh one"
                );
            }
            other => panic!("expected Crdt(Apply), got {other:?}"),
        }

        // A second decode of the same entry (replay/retry) must return the
        // identical value, never re-allocate or overwrite.
        let (_, _, decoded_again, _) = decode::from_replicated_entry(&bytes, &assigner)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_again {
            PhysicalPlan::Crdt(CrdtOp::Apply { surrogate, .. }) => {
                assert_eq!(
                    surrogate, leader_surrogate,
                    "replaying the same entry must resolve to the same bound surrogate"
                );
            }
            other => panic!("expected Crdt(Apply), got {other:?}"),
        }

        assert_eq!(
            assigner
                .lookup_bound(
                    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "docs"),
                    tenant,
                    b"doc-1"
                )
                .expect("catalog lookup"),
            Some(leader_surrogate),
            "the carried surrogate must be installed in the local catalog"
        );
    }

    /// A `CrdtApply` entry that carries `surrogate: 0` has no coordinator value
    /// to bind. Decode refuses it instead of allocating a local identity that
    /// diverges across replicas, and installs nothing in the catalog.
    #[test]
    fn crdt_apply_without_surrogate_is_refused_not_allocated() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let (_dir, assigner) = open_test_assigner();

        let unbound = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::CrdtApply {
                collection: "docs".into(),
                document_id: "doc-unbound".into(),
                delta: vec![0x01],
                peer_id: 3,
                provenance: None,
                constraint_version_required: 0,
                surrogate: 0,
            },
        );
        let result = decode::from_replicated_entry(&unbound.to_bytes(), &assigner);
        assert!(
            result.is_err(),
            "a CRDT apply without a surrogate must be refused, got {result:?}"
        );
        assert_eq!(
            assigner
                .lookup_bound(
                    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "docs"),
                    tenant,
                    b"doc-unbound"
                )
                .expect("catalog lookup"),
            None,
            "a refused apply must not install an identity"
        );
    }

    /// `CrdtOp::ListInsert` / `ListDelete` / `ListMove` carry the parent
    /// document's surrogate; decode binds it first-wins, same identity, no
    /// fresh allocation.
    #[test]
    fn crdt_list_ops_bind_carried_surrogate_not_fresh_allocation() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let (_dir, assigner) = open_test_assigner();

        let leader_surrogate = Surrogate::new(4242);
        let insert_plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "doc-1".into(),
            list_path: "blocks".into(),
            index: 0,
            fields_json: "{}".into(),
            surrogate: leader_surrogate,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &insert_plan)
            .expect("encode must not error")
            .expect("CrdtOp::ListInsert should produce a ReplicatedEntry");
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&entry.to_bytes(), &assigner)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        let parent_surrogate = match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::ListInsert { surrogate, .. }) => {
                assert_eq!(
                    surrogate, leader_surrogate,
                    "ListInsert must bind the leader-carried surrogate"
                );
                surrogate
            }
            other => panic!("expected Crdt(ListInsert), got {other:?}"),
        };

        let delete_plan = PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "doc-1".into(),
            list_path: "blocks".into(),
            index: 0,
            surrogate: parent_surrogate,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &delete_plan)
            .expect("encode must not error")
            .expect("CrdtOp::ListDelete should produce a ReplicatedEntry");
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&entry.to_bytes(), &assigner)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::ListDelete { surrogate, .. }) => {
                assert_eq!(
                    surrogate, parent_surrogate,
                    "ListDelete must resolve to the parent document's existing surrogate"
                );
            }
            other => panic!("expected Crdt(ListDelete), got {other:?}"),
        }

        let move_plan = PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "doc-1".into(),
            list_path: "blocks".into(),
            from_index: 0,
            to_index: 1,
            surrogate: parent_surrogate,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &move_plan)
            .expect("encode must not error")
            .expect("CrdtOp::ListMove should produce a ReplicatedEntry");
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&entry.to_bytes(), &assigner)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Crdt(CrdtOp::ListMove { surrogate, .. }) => {
                assert_eq!(
                    surrogate, parent_surrogate,
                    "ListMove must resolve to the parent document's existing surrogate"
                );
            }
            other => panic!("expected Crdt(ListMove), got {other:?}"),
        }
    }
}
