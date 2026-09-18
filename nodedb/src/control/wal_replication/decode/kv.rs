// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Kv`.

use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{KvOp, ReturningSpec};
use nodedb_types::RlsWriteCheck;

/// A decoded RETURNING projection spec plus the read filters gating it — see
/// `ReplicatedWrite::KvPut::returning`. Bundled — plain positional arguments
/// exceed clippy's arity lint once every KV write function carries it.
pub(super) struct ReturningFields<'a> {
    pub returning: Option<ReturningSpec>,
    pub rls_filters: &'a [u8],
}

pub(super) fn put(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::Put {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        // Carried on the record — a replay re-executes for the originating request.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

/// Every plan reconstructed here carries `RlsWriteCheck::already_decided_elsewhere()`
/// — the writing identity isn't available on this node, so re-deciding at
/// recovery time would make it non-deterministic.
pub(super) fn delete(
    collection: &str,
    keys: &[Vec<u8>],
    returning: ReturningFields<'_>,
) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Delete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        keys: keys.to_vec(),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    })
}

/// Reconstruct a KV predicate `UPDATE` plan. The predicate is re-scanned
/// against this node's own committed state — see `delete` above for why the
/// check slot is stamped rather than re-derived.
pub(super) fn predicate_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
    returning: ReturningFields<'_>,
) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::PredicateUpdate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        filters: filters.to_vec(),
        updates: updates.to_vec(),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    })
}

/// Reconstruct a KV predicate `DELETE` plan — see [`predicate_update`].
pub(super) fn predicate_delete(
    collection: &str,
    filters: &[u8],
    returning: ReturningFields<'_>,
) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::PredicateDelete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        filters: filters.to_vec(),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    })
}

pub(super) fn insert(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::Insert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        // Carried on the record — see `put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

pub(super) fn insert_if_absent(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::InsertIfAbsent {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
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
    ctx: &DecodeCtx,
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
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
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
    ctx: &DecodeCtx,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    surrogates: &[u32],
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let resolved = entries
        .iter()
        .zip(surrogates.iter())
        .map(|((key, _value), carried)| {
            let carried = nodedb_types::Surrogate::new(*carried);
            match ctx.assigner {
                Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried),
                None => Ok(carried),
            }
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PhysicalPlan::Kv(KvOp::BatchPut {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        entries: entries.to_vec(),
        ttl_ms,
        surrogates: resolved,
        // Carried on the record — see `put`.
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

pub(super) fn expire(collection: &str, key: &[u8], ttl_ms: u64) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Expire {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        ttl_ms,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    })
}

pub(super) fn persist(collection: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Persist {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    })
}

pub(super) fn incr(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::Incr {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        delta,
        ttl_ms,
        surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

pub(super) fn incr_float(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    delta: f64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        delta,
        surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

pub(super) fn cas(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::Cas {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        expected: expected.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

pub(super) fn get_set(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
    rls_filters: &[u8],
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::GetSet {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
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

pub(super) fn register_sorted_index(f: RegisterSortedIndexFields) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
        index_name: f.index_name.to_owned(),
        sort_columns: f.sort_columns.to_vec(),
        key_column: f.key_column.to_owned(),
        window_type: f.window_type.to_owned(),
        window_timestamp_column: f.window_timestamp_column.to_owned(),
        window_start_ms: f.window_start_ms,
        window_end_ms: f.window_end_ms,
    })
}

pub(super) fn drop_sorted_index(index_name: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.to_owned(),
    })
}

/// Reconstruct a `RegisterIndex` plan. No surrogate binding — apply re-runs
/// registration (and, if `backfill`, the scan) live on the follower.
pub(super) fn register_index(
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field: field.to_owned(),
        field_position,
        backfill,
    })
}

/// Reconstruct a `DropIndex` plan. Same surrogate-free contract as
/// [`register_index`].
pub(super) fn drop_index(collection: &str, field: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::DropIndex {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field: field.to_owned(),
    })
}

pub(super) fn field_set(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
    if_present: bool,
    returning: ReturningFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::FieldSet {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        key: key.to_vec(),
        updates: updates.to_vec(),
        surrogate,
        if_present,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }))
}

/// Fields of the `KvTransfer` wire variant, bundled so [`transfer`] stays
/// under the `too_many_arguments` clippy threshold.
pub(super) struct TransferFields<'a> {
    pub(super) collection: &'a str,
    pub(super) source_key: &'a [u8],
    pub(super) dest_key: &'a [u8],
    pub(super) field: &'a str,
    pub(super) amount: f64,
    pub(super) debit_surrogate: u32,
    pub(super) credit_surrogate: u32,
}

pub(super) fn transfer(ctx: &DecodeCtx, f: TransferFields) -> crate::Result<PhysicalPlan> {
    let carried_debit = nodedb_types::Surrogate::new(f.debit_surrogate);
    let debit_surrogate = bind_or_lookup(ctx, f.collection, f.source_key, carried_debit)?;
    let carried_credit = nodedb_types::Surrogate::new(f.credit_surrogate);
    let credit_surrogate = bind_or_lookup(ctx, f.collection, f.dest_key, carried_credit)?;
    Ok(PhysicalPlan::Kv(KvOp::Transfer {
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
        source_key: f.source_key.to_vec(),
        dest_key: f.dest_key.to_vec(),
        field: f.field.to_owned(),
        amount: f.amount,
        debit_surrogate,
        credit_surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

/// Reconstruct a `Truncate` plan. Same idempotent-replay contract as
/// `document::truncate` — no surrogate binding, whole-collection clear.
pub(super) fn truncate(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Truncate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
    })
}

/// Reconstruct a resolved KV write plan (`KvOp::ResolvedWrite`). Every `Put`
/// mutation's surrogate binds against its own `(collection, key)`.
pub(super) fn resolved_write(
    ctx: &DecodeCtx,
    mutations: &[super::super::types::KvResolvedMutationWire],
    response_payload: &[u8],
) -> crate::Result<PhysicalPlan> {
    use super::super::types::KvResolvedMutationWire as W;
    use nodedb_physical::physical_plan::KvResolvedMutation as M;

    let decoded = mutations
        .iter()
        .map(|m| -> crate::Result<M> {
            Ok(match m {
                W::Put {
                    collection,
                    key,
                    value,
                    ttl_ms,
                    expire_at_ms,
                    surrogate,
                    precondition,
                } => {
                    let carried = nodedb_types::Surrogate::new(*surrogate);
                    M::Put {
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            collection.clone(),
                        ),
                        key: key.clone(),
                        value: value.clone(),
                        ttl_ms: *ttl_ms,
                        expire_at_ms: *expire_at_ms,
                        surrogate: bind_or_lookup(ctx, collection, key, carried)?,
                        precondition: precondition.clone(),
                    }
                }
                W::Delete {
                    collection,
                    key,
                    precondition,
                } => M::Delete {
                    collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                    key: key.clone(),
                    precondition: precondition.clone(),
                },
                W::Expire {
                    collection,
                    key,
                    ttl_ms,
                    resolved_now_ms,
                    precondition,
                } => M::Expire {
                    collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                    key: key.clone(),
                    ttl_ms: *ttl_ms,
                    // Stamped from the wire, mirroring the `KvExpire` arm.
                    resolved_now_ms: *resolved_now_ms,
                    precondition: precondition.clone(),
                },
                W::Persist {
                    collection,
                    key,
                    precondition,
                } => M::Persist {
                    collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                    key: key.clone(),
                    precondition: precondition.clone(),
                },
            })
        })
        .collect::<crate::Result<Vec<M>>>()?;

    Ok(PhysicalPlan::Kv(KvOp::ResolvedWrite {
        mutations: decoded,
        response_payload: response_payload.to_vec(),
        // Decided before this entry was proposed — see `delete` for why.
        rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
    }))
}

pub(super) fn transfer_item(
    ctx: &DecodeCtx,
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, dest_collection, dest_key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection: nodedb_types::QualifiedCollection::from_stored(
            source_collection.to_owned(),
        ),
        dest_collection: nodedb_types::QualifiedCollection::from_stored(dest_collection.to_owned()),
        item_key: item_key.to_vec(),
        dest_key: dest_key.to_vec(),
        surrogate,
        source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
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
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::Truncate should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::Truncate { collection }) => {
                assert_eq!(collection.as_str(), "kv");
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
        let (_, _, decoded_plan, resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
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
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
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
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Kv(KvOp::DropIndex { collection, field }) => {
                assert_eq!(collection.as_str(), "players");
                assert_eq!(field, "name");
            }
            other => panic!("expected Kv(DropIndex), got {other:?}"),
        }
    }

    /// `entry_kv::kv_write` must not drop `KvOp::Put::returning` / `rls_filters`
    /// — the leader re-derives its plan from the committed entry, so a drop here
    /// loses `RETURNING` for the originating request too, not just followers.
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
        });
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("KvOp::Put should produce a ReplicatedEntry");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
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
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
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
