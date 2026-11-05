// SPDX-License-Identifier: BUSL-1.1

//! Decode the `ReplicatedWrite` variants that produce a resolved or
//! cross-key `PhysicalPlan::Kv`: `KvResolvedWrite`, `KvTransfer`, and
//! `KvTransferItem`.
//!
//! Every surrogate is rebuilt verbatim from the record; `entry.rs` binds the
//! whole plan afterwards.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

/// Fields of the `KvTransfer` wire variant, bundled so [`transfer`] stays
/// under the `too_many_arguments` clippy threshold.
pub(super) struct TransferFields<'a> {
    pub(super) collection: &'a str,
    pub(super) source_key: &'a [u8],
    pub(super) dest_key: &'a [u8],
    pub(super) field: &'a str,
    pub(super) amount: nodedb_physical::physical_plan::TransferAmount,
    pub(super) debit_surrogate: u32,
    pub(super) credit_surrogate: u32,
}

pub(super) fn transfer(database_id: DatabaseId, f: TransferFields) -> crate::Result<PhysicalPlan> {
    let debit_surrogate = nodedb_types::Surrogate::new(f.debit_surrogate);
    let credit_surrogate = nodedb_types::Surrogate::new(f.credit_surrogate);
    Ok(PhysicalPlan::Kv(KvOp::Transfer {
        collection: QualifiedCollection::from_stored(database_id, f.collection.to_owned())?,
        source_key: f.source_key.to_vec(),
        dest_key: f.dest_key.to_vec(),
        field: f.field.to_owned(),
        amount: f.amount,
        debit_surrogate,
        credit_surrogate,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}

/// Reconstruct a resolved KV write plan (`KvOp::ResolvedWrite`). Every `Put`
/// mutation's surrogate binds against its own `(collection, key)`.
pub(super) fn resolved_write(
    database_id: DatabaseId,
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
                } => M::Put {
                    collection: QualifiedCollection::from_stored(database_id, collection.clone())?,
                    key: key.clone(),
                    value: value.clone(),
                    ttl_ms: *ttl_ms,
                    expire_at_ms: *expire_at_ms,
                    surrogate: nodedb_types::Surrogate::new(*surrogate),
                    precondition: precondition.clone(),
                },
                W::Rewrite {
                    collection,
                    key,
                    value,
                    ttl_ms,
                    expire_at_ms,
                    precondition,
                } => M::Rewrite {
                    collection: QualifiedCollection::from_stored(database_id, collection.clone())?,
                    key: key.clone(),
                    value: value.clone(),
                    ttl_ms: *ttl_ms,
                    expire_at_ms: *expire_at_ms,
                    precondition: precondition.clone(),
                },
                W::Delete {
                    collection,
                    key,
                    precondition,
                } => M::Delete {
                    collection: QualifiedCollection::from_stored(database_id, collection.clone())?,
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
                    collection: QualifiedCollection::from_stored(database_id, collection.clone())?,
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
                    collection: QualifiedCollection::from_stored(database_id, collection.clone())?,
                    key: key.clone(),
                    precondition: precondition.clone(),
                },
                W::Reap {
                    collection,
                    key,
                    expire_at_ms,
                } => M::Reap {
                    collection: QualifiedCollection::from_stored(database_id, collection.clone())?,
                    key: key.clone(),
                    expire_at_ms: *expire_at_ms,
                },
            })
        })
        .collect::<crate::Result<Vec<M>>>()?;

    Ok(PhysicalPlan::Kv(KvOp::ResolvedWrite {
        mutations: decoded,
        response_payload: response_payload.to_vec(),
        // Decided before this entry was proposed — see `kv::delete` for why.
        rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
    }))
}

pub(super) fn transfer_item(
    database_id: DatabaseId,
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = nodedb_types::Surrogate::new(surrogate);
    Ok(PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection: QualifiedCollection::from_stored(
            database_id,
            source_collection.to_owned(),
        )?,
        dest_collection: QualifiedCollection::from_stored(database_id, dest_collection.to_owned())?,
        item_key: item_key.to_vec(),
        dest_key: dest_key.to_vec(),
        surrogate,
        source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }))
}
