// SPDX-License-Identifier: BUSL-1.1

//! Encode a committed transaction's redo as a `ReplicatedWrite::TransactionRedo`
//! entry for its vShard's data-group log.
//!
//! A redo is not built from a `PhysicalPlan`, so it has no arm in
//! `to_replicated_entry`: the commit that resolved it encodes it here.

use super::super::transaction_redo::TransactionRedoPayload;
use super::super::types::{
    ReplicatedEntry, ReplicatedEventSource, ReplicatedIdentity, ReplicatedWrite,
};
use crate::types::{DatabaseId, TenantId, VShardId};

/// The Raft entry that carries `payload` to every replica of `vshard_id`.
pub fn transaction_redo_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    payload: &TransactionRedoPayload,
) -> ReplicatedEntry {
    ReplicatedEntry::new(
        tenant_id.as_u64(),
        database_id.as_u64(),
        vshard_id.as_u32(),
        ReplicatedWrite::TransactionRedo {
            redo: payload.redo.clone(),
            collections: payload.collections.clone(),
            sum_targets: payload.sum_targets.clone(),
            identities: payload
                .identities
                .iter()
                .map(|identity| ReplicatedIdentity {
                    collection: identity.collection.clone(),
                    pk_bytes: identity.pk_bytes.clone(),
                    surrogate: identity.surrogate.as_u32(),
                })
                .collect(),
            event_source: ReplicatedEventSource::from(payload.event_source),
            origin: payload.origin,
        },
    )
    .with_event_source(payload.event_source)
}
