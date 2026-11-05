// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Kv`.
//!
//! Every surrogate is rebuilt verbatim from the record; `entry.rs` binds the
//! whole plan afterwards.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{KvBatchIntent, KvCounterShape, KvOp, ReturningSpec};
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

/// A decoded RETURNING projection spec plus the read filters gating it — see
/// `ReplicatedWrite::KvPut::returning`. Bundled — plain positional arguments
/// exceed clippy's arity lint once every KV write function carries it.
pub(super) struct ReturningFields<'a> {
    pub returning: Option<ReturningSpec>,
    pub rls_filters: &'a [u8],
}

/// The row a replicated KV put writes, and the identity it is written under.
pub(super) struct PutFields<'a> {
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub surrogate: u32,
}

pub(super) fn put(
    database_id: DatabaseId,
    put: PutFields<'_>,
    returning: ReturningFields<'_>,
    provenance: Option<SyncProvenance>,
) -> crate::Result<PhysicalPlan> {
    let PutFields {
        collection,
        key,
        value,
        ttl_ms,
        surrogate,
    } = put;
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        // Carried on the record — a replay re-executes for the originating request.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        // Every replica gates the write on the same stream mark.
        provenance,
    }))
}

/// Every plan reconstructed here carries `RlsWriteCheck::already_decided_elsewhere()`
/// — the writing identity isn't available on this node, so re-deciding at
/// recovery time makes it non-deterministic.
pub(super) fn delete(
    database_id: DatabaseId,
    collection: &str,
    keys: &[Vec<u8>],
    returning: ReturningFields<'_>,
    provenance: Option<SyncProvenance>,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::Delete {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        keys: keys.to_vec(),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        provenance,
    }))
}

pub(super) fn insert(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::Insert {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        // Carried on the record — see `put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

/// The stored entry an `INSERT ... ON CONFLICT DO UPDATE` record carries.
pub(super) struct ConflictEntry<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    pub surrogate: u32,
}

pub(super) fn insert_on_conflict_update(
    database_id: DatabaseId,
    collection: &str,
    entry: ConflictEntry<'_>,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let ConflictEntry {
        key,
        value,
        ttl_ms,
        updates,
        surrogate,
    } = entry;
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        updates: updates.to_vec(),
        surrogate,
        // No predicate on replay — see `delete`.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        // Carried on the record — see `put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

pub(super) fn batch_put(
    database_id: DatabaseId,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    intent: KvBatchIntent,
    surrogates: &[u32],
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::BatchPut {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        entries: entries.to_vec(),
        ttl_ms,
        intent,
        surrogates: surrogates
            .iter()
            .map(|&raw| nodedb_types::Surrogate::new(raw))
            .collect(),
        // Carried on the record — see `put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

pub(super) fn expire(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    ttl_ms: u64,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::Expire {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        ttl_ms,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

pub(super) fn persist(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::Persist {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

pub(super) fn incr(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
    shape: &KvCounterShape,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::Incr {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        delta,
        ttl_ms,
        surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        shape: shape.clone(),
    }))
}

pub(super) fn incr_float(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    delta: &str,
    surrogate: u32,
    shape: &KvCounterShape,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        delta: delta.to_owned(),
        surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        shape: shape.clone(),
    }))
}

pub(super) fn cas(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::Cas {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        expected: expected.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

pub(super) fn get_set(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
    rls_filters: &[u8],
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::GetSet {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
        // Carried on the record — gated by the originating request's read filters.
        rls_filters: rls_filters.to_vec(),
        // No predicate on replay — see `delete`.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

/// Fields of the `KvRegisterSortedIndex` wire variant, bundled so
/// [`register_sorted_index`] stays under the `too_many_arguments` clippy
/// threshold.
pub(super) struct RegisterSortedIndexFields<'a> {
    pub(super) collection: &'a str,
    pub(super) index_name: &'a str,
    pub(super) sort_columns: &'a [(String, String)],
    pub(super) key_column: &'a str,
    pub(super) window_type: &'a str,
    pub(super) window_timestamp_column: &'a str,
    pub(super) window_start_ms: u64,
    pub(super) window_end_ms: u64,
}

pub(super) fn register_sorted_index(
    database_id: DatabaseId,
    f: RegisterSortedIndexFields,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection: QualifiedCollection::from_stored(database_id, f.collection.to_owned())?,
        index_name: f.index_name.to_owned(),
        sort_columns: f.sort_columns.to_vec(),
        key_column: f.key_column.to_owned(),
        window_type: f.window_type.to_owned(),
        window_timestamp_column: f.window_timestamp_column.to_owned(),
        window_start_ms: f.window_start_ms,
        window_end_ms: f.window_end_ms,
    }))
}

pub(super) fn drop_sorted_index(index_name: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.to_owned(),
    })
}

/// Reconstruct a `RegisterIndex` plan. No surrogate binding — apply re-runs
/// registration (and, if `backfill`, the scan) live on the follower.
pub(super) fn register_index(
    database_id: DatabaseId,
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        field: field.to_owned(),
        field_position,
        backfill,
    }))
}

/// Reconstruct a `DropIndex` plan. Same surrogate-free contract as
/// [`register_index`].
pub(super) fn drop_index(
    database_id: DatabaseId,
    collection: &str,
    field: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::DropIndex {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        field: field.to_owned(),
    }))
}

pub(super) fn field_set(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
    if_present: bool,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::FieldSet {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        key: key.to_vec(),
        updates: updates.to_vec(),
        surrogate,
        if_present,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

/// Reconstruct a `Truncate` plan. Same idempotent-replay contract as
/// `document::truncate` — no surrogate binding, whole-collection clear.
pub(super) fn truncate(
    database_id: DatabaseId,
    collection: &str,
    restart_identity: bool,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::Truncate {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        restart_identity,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::ReplicatedEntry;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, UpdateValue};
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
    fn kv_truncate_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Kv(KvOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            restart_identity: true,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::Truncate should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::Truncate {
                collection,
                restart_identity,
            }) => {
                assert_eq!(collection.as_str(), "kv");
                assert!(restart_identity, "restart_identity must round-trip");
            }
            other => panic!("expected Kv(Truncate), got {other:?}"),
        }
    }

    /// `KvOp::RegisterIndex` must produce a `ReplicatedEntry` and round-trip
    /// verbatim — `backfill` and `field_position` aren't inferable at apply time.
    #[test]
    fn kv_register_index_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Kv(KvOp::RegisterIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            field: "name".into(),
            field_position: 2,
            backfill: true,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::RegisterIndex must now produce a ReplicatedEntry (cluster-replicated)");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, resolved_now_ms) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        assert_eq!(resolved_now_ms, None, "index DDL carries no TTL instant");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection,
                field,
                field_position,
                backfill,
            }) => {
                assert_eq!(collection.as_str(), "players");
                assert_eq!(field, "name");
                assert_eq!(field_position, 2, "field_position must round-trip");
                assert!(
                    backfill,
                    "backfill must round-trip (not inferable at apply)"
                );
            }
            other => panic!("expected Kv(RegisterIndex), got {other:?}"),
        }

        // backfill = false must survive distinctly, not default to true.
        let plan = PhysicalPlan::Kv(KvOp::RegisterIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            field: "name".into(),
            field_position: 0,
            backfill: false,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::RegisterIndex must produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::RegisterIndex { backfill, .. }) => {
                assert!(!backfill, "backfill = false must round-trip distinctly");
            }
            other => panic!("expected Kv(RegisterIndex), got {other:?}"),
        }
    }

    /// `KvOp::DropIndex` (KV secondary-index DDL) — same cluster-replication
    /// regression guard as [`kv_register_index_roundtrip`].
    #[test]
    fn kv_drop_index_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Kv(KvOp::DropIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            field: "name".into(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::DropIndex must now produce a ReplicatedEntry (cluster-replicated)");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::DropIndex { collection, field }) => {
                assert_eq!(collection.as_str(), "players");
                assert_eq!(field, "name");
            }
            other => panic!("expected Kv(DropIndex), got {other:?}"),
        }
    }

    /// A Lite KV push carries its sync provenance to every replica, so each
    /// one gates the write on the same stream mark.
    #[test]
    fn kv_put_and_delete_provenance_roundtrips() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let prov = SyncProvenance {
            producer_id: 7,
            epoch: 2,
            stream_id: 99,
            seq: 5,
        };
        let put = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: Some(prov.clone()),
        });
        let delete = PhysicalPlan::Kv(KvOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            keys: vec![b"k1".to_vec()],
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
            provenance: Some(prov.clone()),
        });
        for plan in [put, delete] {
            let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
                .expect("encode must not error")
                .expect("a KV write produces a ReplicatedEntry");
            let (_, _, decoded, _) = decode::decode_replicated_entry(&entry.to_bytes())
                .expect("decode_replicated_entry error")
                .expect("decode_replicated_entry returned None");
            match decoded {
                PhysicalPlan::Kv(KvOp::Put { provenance, .. })
                | PhysicalPlan::Kv(KvOp::Delete { provenance, .. }) => {
                    assert_eq!(provenance, Some(prov.clone()));
                }
                other => panic!("expected a KV write, got {other:?}"),
            }
        }
    }

    /// `entry_kv::kv_write` must not drop `KvOp::Put::returning` / `rls_filters`
    /// — the leader re-derives its plan from the committed entry, so a drop here
    /// loses `RETURNING` for the originating request too, not only followers.
    #[test]
    fn kv_put_returning_and_rls_filters_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
            provenance: None,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::Put should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::Put {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING write must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Kv(Put), got {other:?}"),
        }
    }

    /// See `kv_put_returning_and_rls_filters_roundtrip`; same bug,
    /// `KvOp::InsertOnConflictUpdate`.
    #[test]
    fn kv_insert_on_conflict_update_returning_and_rls_filters_roundtrip() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![ReturningItem {
                name: "balance".into(),
                alias: None,
            }]),
        };
        let plan = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            key: b"a1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            updates: vec![("balance".into(), UpdateValue::Literal(b"100".to_vec()))],
            surrogate: Surrogate::new(2),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: Some(spec.clone()),
            rls_filters: b"rls-predicate".to_vec(),
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::InsertOnConflictUpdate should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                returning,
                rls_filters,
                ..
            }) => {
                assert_eq!(
                    returning,
                    Some(spec),
                    "a RETURNING write must not silently yield no rows on replication"
                );
                assert_eq!(
                    rls_filters,
                    b"rls-predicate".to_vec(),
                    "the RETURNING read-policy filter must survive replication"
                );
            }
            other => panic!("expected Kv(InsertOnConflictUpdate), got {other:?}"),
        }
    }
}
