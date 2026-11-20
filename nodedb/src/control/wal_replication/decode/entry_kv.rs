// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Kv`. Returns `(PhysicalPlan, Option<u64>)`: TTL-bearing
//! `Kv*` variants stamp `resolved_now_ms` so every replica installs the same
//! `expire_at_ms`. Every re-typing variant carries the declared columns the
//! proposer resolved, and decodes them as resolved. See
//! `entry_document::decode_arm` for the trailing-arm contract.

use super::super::decode_sync_engines::{decode_provenance, decode_returning};
use super::super::types::ReplicatedWrite;
use super::ctx::DecodeCtx;
use super::kv::{PutFields, ReturningFields, Target};
use super::{kv, kv_resolved};
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(
    ctx: &DecodeCtx,
    write: &ReplicatedWrite,
) -> crate::Result<(PhysicalPlan, Option<u64>)> {
    let database_id = ctx.database_id;
    let mut resolved_now_ms: Option<u64> = None;
    let plan = match write {
        ReplicatedWrite::KvTruncate {
            collection,
            restart_identity,
        } => kv::truncate(database_id, collection, *restart_identity)?,
        ReplicatedWrite::KvPut {
            collection,
            declared_columns,
            key,
            value,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
            returning,
            rls_filters,
            provenance,
        } => {
            resolved_now_ms = *rn;
            kv::put(
                database_id,
                PutFields {
                    target: Target {
                        collection,
                        declared_columns,
                    },
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                },
                ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
                decode_provenance(provenance)?,
            )?
        }
        ReplicatedWrite::KvDelete {
            collection,
            keys,
            returning,
            rls_filters,
            provenance,
        } => kv::delete(
            database_id,
            collection,
            keys,
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
            decode_provenance(provenance)?,
        )?,
        ReplicatedWrite::KvInsert {
            collection,
            declared_columns,
            key,
            value,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
            returning,
            rls_filters,
        } => {
            resolved_now_ms = *rn;
            kv::insert(
                database_id,
                PutFields {
                    target: Target {
                        collection,
                        declared_columns,
                    },
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                },
                ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
            )?
        }
        ReplicatedWrite::KvInsertOnConflictUpdate {
            collection,
            declared_columns,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            resolved_now_ms: rn,
            returning,
            rls_filters,
        } => {
            resolved_now_ms = *rn;
            kv::insert_on_conflict_update(
                database_id,
                Target {
                    collection,
                    declared_columns,
                },
                kv::ConflictEntry {
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    updates,
                    surrogate: *surrogate,
                },
                ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
            )?
        }
        ReplicatedWrite::KvBatchPut {
            collection,
            declared_columns,
            entries,
            ttl_ms,
            intent,
            surrogates,
            resolved_now_ms: rn,
            returning,
            rls_filters,
        } => {
            resolved_now_ms = *rn;
            kv::batch_put(
                database_id,
                Target {
                    collection,
                    declared_columns,
                },
                entries,
                *ttl_ms,
                intent.clone(),
                surrogates,
                ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
            )?
        }
        ReplicatedWrite::KvExpire {
            collection,
            key,
            ttl_ms,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::expire(database_id, collection, key, *ttl_ms)?
        }
        ReplicatedWrite::KvPersist { collection, key } => {
            kv::persist(database_id, collection, key)?
        }
        ReplicatedWrite::KvIncr {
            collection,
            declared_columns,
            key,
            delta,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
            shape,
        } => {
            resolved_now_ms = *rn;
            kv::incr(
                database_id,
                Target {
                    collection,
                    declared_columns,
                },
                key,
                *delta,
                *ttl_ms,
                *surrogate,
                shape,
            )?
        }
        ReplicatedWrite::KvIncrFloat {
            collection,
            declared_columns,
            key,
            delta,
            surrogate,
            shape,
        } => kv::incr_float(
            database_id,
            Target {
                collection,
                declared_columns,
            },
            key,
            delta,
            *surrogate,
            shape,
        )?,
        ReplicatedWrite::KvCas {
            collection,
            declared_columns,
            key,
            expected,
            new_value,
            surrogate,
        } => kv::cas(
            database_id,
            Target {
                collection,
                declared_columns,
            },
            key,
            expected,
            new_value,
            *surrogate,
        )?,
        ReplicatedWrite::KvGetSet {
            collection,
            declared_columns,
            key,
            new_value,
            surrogate,
            rls_filters,
        } => kv::get_set(
            database_id,
            Target {
                collection,
                declared_columns,
            },
            key,
            new_value,
            *surrogate,
            rls_filters,
        )?,
        ReplicatedWrite::KvRegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => kv::register_sorted_index(
            database_id,
            kv::RegisterSortedIndexFields {
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms: *window_start_ms,
                window_end_ms: *window_end_ms,
            },
        )?,
        ReplicatedWrite::KvDropSortedIndex { index_name } => kv::drop_sorted_index(index_name),
        ReplicatedWrite::KvRegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        } => kv::register_index(database_id, collection, field, *field_position, *backfill)?,
        ReplicatedWrite::KvDropIndex { collection, field } => {
            kv::drop_index(database_id, collection, field)?
        }
        ReplicatedWrite::KvFieldSet {
            collection,
            declared_columns,
            key,
            updates,
            surrogate,
            if_present,
            returning,
            rls_filters,
        } => kv::field_set(
            database_id,
            Target {
                collection,
                declared_columns,
            },
            key,
            updates,
            *surrogate,
            *if_present,
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        )?,
        ReplicatedWrite::KvTransfer {
            collection,
            declared_columns,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        } => kv_resolved::transfer(
            database_id,
            kv_resolved::TransferFields {
                target: Target {
                    collection,
                    declared_columns,
                },
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: *debit_surrogate,
                credit_surrogate: *credit_surrogate,
            },
        )?,
        ReplicatedWrite::KvResolvedWrite {
            mutations,
            response_payload,
            resolved_now_ms: rn,
        } => {
            // Every replica judges the drift checks at the proposer's instant.
            resolved_now_ms = Some(*rn);
            kv_resolved::resolved_write(database_id, mutations, response_payload)?
        }
        ReplicatedWrite::KvTransferItem {
            source_collection,
            dest_collection,
            declared_columns,
            item_key,
            dest_key,
            surrogate,
        } => kv_resolved::transfer_item(
            database_id,
            source_collection,
            Target {
                collection: dest_collection,
                declared_columns,
            },
            item_key,
            dest_key,
            *surrogate,
        )?,
        _ => {
            return Err(crate::Error::Internal {
                detail: "entry_kv::decode_arm called with a non-Kv ReplicatedWrite variant \
                    (dispatch bug in decode/entry.rs's grouped Kv match arm)"
                    .into(),
            });
        }
    };
    Ok((plan, resolved_now_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::ReplicatedEntry;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::KvOp;

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
    fn kv_put_resolved_now_ms_roundtrips_verbatim_not_a_fresh_clock_read() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let ttl_ms = 5_000u64;
        let resolved_now_ms = 1_000u64;

        let entry = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::KvPut {
                collection: "sessions".into(),
                declared_columns: std::sync::Arc::default(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ttl_ms,
                surrogate: 1,
                resolved_now_ms: Some(resolved_now_ms),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            },
        );
        let bytes = entry.to_bytes();

        let (_, _, plan, decoded_resolved_now_ms) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");

        assert_eq!(
            decoded_resolved_now_ms,
            Some(resolved_now_ms),
            "decoded resolved_now_ms must be the exact instant the proposing node carried, \
             not a fresh clock read"
        );
        match plan {
            PhysicalPlan::Kv(KvOp::Put {
                ttl_ms: decoded_ttl_ms,
                ..
            }) => {
                assert_eq!(decoded_ttl_ms, ttl_ms);
                let installed_expire_at_ms =
                    decoded_resolved_now_ms.expect("resolved") + decoded_ttl_ms;
                assert_eq!(installed_expire_at_ms, resolved_now_ms + ttl_ms);
            }
            other => panic!("expected Kv(Put), got {other:?}"),
        }
    }

    #[test]
    fn kv_put_ttl_zero_carries_no_resolved_now_ms() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let entry = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::KvPut {
                collection: "sessions".into(),
                declared_columns: std::sync::Arc::default(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ttl_ms: 0,
                surrogate: 1,
                resolved_now_ms: None,
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            },
        );
        let bytes = entry.to_bytes();

        let (_, _, plan, decoded_resolved_now_ms) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");

        assert_eq!(
            decoded_resolved_now_ms, None,
            "a ttl_ms == 0 write must carry no resolved instant on either side of the wire"
        );
        match plan {
            PhysicalPlan::Kv(KvOp::Put { ttl_ms, .. }) => assert_eq!(ttl_ms, 0),
            other => panic!("expected Kv(Put), got {other:?}"),
        }
    }

    #[test]
    fn kv_expire_resolved_now_ms_is_always_present() {
        // `ttl_ms == 0` is a legitimate "expire now" request, so resolved instant is always `Some`.
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let ttl_ms = 0u64;
        let resolved_now_ms = 1_000u64;

        let entry = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::KvExpire {
                collection: "sessions".into(),
                key: b"k1".to_vec(),
                ttl_ms,
                resolved_now_ms: Some(resolved_now_ms),
            },
        );
        let bytes = entry.to_bytes();

        let (_, _, plan, decoded_resolved_now_ms) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");

        assert_eq!(
            decoded_resolved_now_ms,
            Some(resolved_now_ms),
            "KvExpire must always carry the proposing node's resolved instant verbatim"
        );
        match plan {
            PhysicalPlan::Kv(KvOp::Expire {
                ttl_ms: decoded_ttl_ms,
                ..
            }) => assert_eq!(decoded_ttl_ms, ttl_ms),
            other => panic!("expected Kv(Expire), got {other:?}"),
        }
    }

    #[test]
    fn kv_incr_resolved_now_ms_only_when_ttl_positive() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        // ttl_ms > 0: resolved_now_ms must round-trip verbatim.
        let entry_with_ttl = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::KvIncr {
                collection: "counters".into(),
                declared_columns: std::sync::Arc::default(),
                key: b"c1".to_vec(),
                delta: 5,
                ttl_ms: 60_000,
                surrogate: 1,
                resolved_now_ms: Some(1_000),
                shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
            },
        );
        let bytes = entry_with_ttl.to_bytes();
        let (_, _, plan, decoded_resolved_now_ms) = decode::decode_replicated_entry(&bytes)
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        assert_eq!(decoded_resolved_now_ms, Some(1_000));
        match plan {
            PhysicalPlan::Kv(KvOp::Incr { ttl_ms, .. }) => assert_eq!(ttl_ms, 60_000),
            other => panic!("expected Kv(Incr), got {other:?}"),
        }

        // ttl_ms == 0 ("preserve existing TTL"): no instant to carry.
        let entry_no_ttl = ReplicatedEntry::new(
            tenant.as_u64(),
            DatabaseId::DEFAULT.as_u64(),
            vshard.as_u32(),
            ReplicatedWrite::KvIncr {
                collection: "counters".into(),
                declared_columns: std::sync::Arc::default(),
                key: b"c1".to_vec(),
                delta: 5,
                ttl_ms: 0,
                surrogate: 1,
                resolved_now_ms: None,
                shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
            },
        );
        let bytes_no_ttl = entry_no_ttl.to_bytes();
        let (_, _, _, decoded_resolved_now_ms_no_ttl) =
            decode::decode_replicated_entry(&bytes_no_ttl)
                .expect("decode_replicated_entry error")
                .expect("decode_replicated_entry returned None");
        assert_eq!(decoded_resolved_now_ms_no_ttl, None);
    }

    #[test]
    fn kv_encoders_resolve_the_instant_once_at_proposal_time() {
        // Exercises the real encode path, not a hand-built `ReplicatedWrite` — pins
        // that the leader-side encoder itself populates `resolved_now_ms`.
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::none(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 5_000,
            surrogate: nodedb_types::Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("Kv(Put) should produce a ReplicatedEntry");
        match entry.write {
            ReplicatedWrite::KvPut {
                resolved_now_ms, ..
            } => assert!(
                resolved_now_ms.is_some(),
                "a ttl_ms > 0 Put must carry a resolved instant"
            ),
            other => panic!("expected KvPut, got {other:?}"),
        }

        let plan_no_ttl = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::none(),
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::new(2),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        let entry_no_ttl = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan_no_ttl)
            .expect("encode must not error")
            .expect("Kv(Put) should produce a ReplicatedEntry");
        match entry_no_ttl.write {
            ReplicatedWrite::KvPut {
                resolved_now_ms, ..
            } => assert_eq!(
                resolved_now_ms, None,
                "a ttl_ms == 0 Put must carry no resolved instant"
            ),
            other => panic!("expected KvPut, got {other:?}"),
        }
    }

    fn proposer_columns() -> nodedb_physical::physical_plan::KvDeclaredColumns {
        use nodedb_physical::physical_plan::{DeclaredColumn, KvDeclaredColumns};
        KvDeclaredColumns::resolved(vec![
            DeclaredColumn::from_declared("value", "DECIMAL(5,2)").expect("decimal"),
            DeclaredColumn::from_declared("n", "SMALLINT").expect("smallint"),
        ])
    }

    /// Encode `plan` for Raft, decode it as an applier does, and return the
    /// declared columns the decoded op carries.
    fn columns_after_round_trip(
        plan: &PhysicalPlan,
    ) -> nodedb_physical::physical_plan::KvDeclaredColumns {
        let entry = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            plan,
        )
        .expect("encode must not error")
        .expect("a replicated KV write");
        let (_, _, decoded, _) = decode::decode_replicated_entry(&entry.to_bytes())
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        let PhysicalPlan::Kv(op) = decoded else {
            panic!("expected a KV plan, got {decoded:?}");
        };
        let (_, columns) = op.declared_columns().expect("a re-typing KV write");
        columns.clone()
    }

    #[test]
    fn kv_writes_carry_the_proposer_columns_through_raft_resolved() {
        use nodedb_physical::physical_plan::{KvCounterShape, TransferAmount};
        use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

        let carried = proposer_columns();
        let kv = || QualifiedCollection::new(DatabaseId::DEFAULT, "kv");
        let plans = [
            PhysicalPlan::Kv(KvOp::Put {
                collection: kv(),
                declared_columns: carried.clone(),
                key: b"k".to_vec(),
                value: b"1.005".to_vec(),
                ttl_ms: 0,
                surrogate: Surrogate::new(1),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            }),
            PhysicalPlan::Kv(KvOp::Incr {
                collection: kv(),
                declared_columns: carried.clone(),
                key: b"n".to_vec(),
                delta: 1,
                ttl_ms: 0,
                surrogate: Surrogate::new(2),
                rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
                shape: KvCounterShape::Raw,
            }),
            PhysicalPlan::Kv(KvOp::Transfer {
                collection: kv(),
                declared_columns: carried.clone(),
                source_key: b"a".to_vec(),
                dest_key: b"b".to_vec(),
                field: "value".into(),
                amount: TransferAmount::Float(1.0),
                debit_surrogate: Surrogate::new(3),
                credit_surrogate: Surrogate::new(4),
                rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            }),
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
                dest_collection: kv(),
                declared_columns: carried.clone(),
                item_key: b"i".to_vec(),
                dest_key: b"d".to_vec(),
                surrogate: Surrogate::new(5),
                source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
                dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            }),
        ];
        for plan in &plans {
            assert_eq!(
                columns_after_round_trip(plan),
                carried,
                "the applier re-types under the proposer's columns: {plan:?}"
            );
        }
    }

    #[test]
    fn a_kv_write_with_pending_columns_is_refused_before_it_is_proposed() {
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::pending(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        let err = to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &plan,
        )
        .expect_err("pending columns never reach the Raft wire");
        assert!(matches!(err, crate::Error::Internal { .. }), "{err:?}");
    }
}
