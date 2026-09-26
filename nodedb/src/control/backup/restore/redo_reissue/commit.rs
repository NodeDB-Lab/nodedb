// SPDX-License-Identifier: BUSL-1.1

//! Commit restored units as redo records through their vShard's apply log.
//!
//! A restored row installs exactly as a committed transaction's row does. With
//! Raft the record is proposed to the collection's data group, and every
//! replica binds its identities, appends it to its own WAL and installs it.
//! With no Raft this node runs the same apply alone. Either way the call
//! returns once the record is durable and installed here.

use std::collections::HashSet;

use nodedb_physical::physical_plan::RedoOrigin;

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::control::wal_replication::encode::transaction_redo_entry;
use crate::control::wal_replication::propose_replicated_entry;
use crate::control::wal_replication::transaction_redo::{
    RedoTarget, TransactionRedoPayload, apply_transaction_redo,
};
use crate::event::EventSource;
use crate::types::{TenantId, VShardId};
use crate::wal::{RedoRecord, RedoSubRecord};

use super::units::{CollectionUnits, RowUnit};

/// Most sub-records one restore record carries.
const MAX_OPS_PER_RECORD: usize = 512;

/// Most encoded bytes one restore record carries. A single unit larger than
/// this still commits, alone in its own record.
const MAX_BYTES_PER_RECORD: usize = 4 * 1024 * 1024;

/// Split `units` into record-sized batches, in order. A unit never splits.
fn batch_units(units: Vec<RowUnit>) -> Vec<Vec<RowUnit>> {
    let mut batches = Vec::new();
    let mut current: Vec<RowUnit> = Vec::new();
    let (mut ops, mut bytes) = (0usize, 0usize);
    for unit in units {
        let (unit_ops, unit_bytes) = (unit.ops.len(), unit.byte_len());
        if !current.is_empty()
            && (ops + unit_ops > MAX_OPS_PER_RECORD || bytes + unit_bytes > MAX_BYTES_PER_RECORD)
        {
            batches.push(std::mem::take(&mut current));
            (ops, bytes) = (0, 0);
        }
        ops += unit_ops;
        bytes += unit_bytes;
        current.push(unit);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

/// One batch as the payload every replica applies.
fn batch_payload(collection: &str, batch: Vec<RowUnit>) -> TransactionRedoPayload {
    let mut ops: Vec<RedoSubRecord> = Vec::new();
    let mut identities: Vec<CarriedIdentity> = Vec::new();
    let mut seen: HashSet<(String, Vec<u8>)> = HashSet::new();
    for unit in batch {
        ops.extend(unit.ops);
        for identity in unit.identities {
            if seen.insert((identity.collection.clone(), identity.pk_bytes.clone())) {
                identities.push(identity);
            }
        }
    }
    TransactionRedoPayload {
        redo: RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        },
        collections: vec![collection.to_string()],
        // The backup holds every target row with its total already folded in.
        sum_targets: Vec::new(),
        identities,
        // Every replica applies the rows as restored: AFTER triggers fired
        // when the rows were first written, and do not fire again.
        event_source: EventSource::Restore,
        origin: RedoOrigin::Restore,
    }
}

/// Commit one record and wait until it is durable and installed here.
async fn commit_record(
    state: &SharedState,
    target: RedoTarget,
    payload: &TransactionRedoPayload,
) -> crate::Result<()> {
    super::super::durable::log_reissue_step(
        state,
        "redo",
        payload.collections.first().map_or("", String::as_str),
        target.vshard_id,
        payload.redo.ops.len(),
    );
    if let Some(proposer) = state.async_raft_proposer() {
        let entry = transaction_redo_entry(
            target.tenant_id,
            target.database_id,
            target.vshard_id,
            payload,
        );
        propose_replicated_entry(state, proposer, entry).await?;
        return Ok(());
    }
    let outcome = apply_transaction_redo(state, target, payload, 0, None).await?;
    if outcome.response.status == Status::Ok {
        return Ok(());
    }
    Err(crate::Error::DataPlane(
        outcome
            .response
            .error_code
            .as_deref()
            .cloned()
            .unwrap_or_else(|| ErrorCode::Internal {
                detail: "restore redo apply returned an error status with no error code".into(),
            }),
    ))
}

/// Commit every unit of `units` in order. Returns the records committed.
pub(super) async fn commit_collection(
    state: &SharedState,
    tenant_id: TenantId,
    units: CollectionUnits,
) -> crate::Result<usize> {
    let CollectionUnits {
        database_id,
        collection,
        units,
    } = units;
    let target = RedoTarget {
        tenant_id,
        database_id,
        vshard_id: VShardId::from_collection_in_database(database_id, &collection),
    };
    let mut records = 0usize;
    for batch in batch_units(units) {
        let payload = batch_payload(&collection, batch);
        commit_record(state, target, &payload)
            .await
            .map_err(|e| crate::Error::Internal {
                detail: format!("restore: re-issuing rows of '{collection}' failed: {e}"),
            })?;
        records += 1;
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    fn unit(ops: usize, payload_len: usize, pk: &str) -> RowUnit {
        RowUnit {
            ops: (0..ops)
                .map(|_| RedoSubRecord {
                    record_type: 0,
                    payload: vec![0; payload_len],
                })
                .collect(),
            identities: vec![CarriedIdentity {
                collection: "c".into(),
                pk_bytes: pk.as_bytes().to_vec(),
                surrogate: Surrogate::new(1),
            }],
        }
    }

    #[test]
    fn batches_cut_between_units_at_the_op_limit() {
        let units = (0..3)
            .map(|i| unit(MAX_OPS_PER_RECORD / 2, 1, &i.to_string()))
            .collect();
        let batches = batch_units(units);
        let sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![2, 1]);
    }

    #[test]
    fn an_oversized_unit_commits_alone() {
        let units = vec![
            unit(1, 1, "a"),
            unit(1, MAX_BYTES_PER_RECORD + 1, "b"),
            unit(1, 1, "c"),
        ];
        let sizes: Vec<usize> = batch_units(units).iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
    }

    #[test]
    fn a_payload_carries_each_identity_once_and_restores_without_folds() {
        let payload = batch_payload("c", vec![unit(1, 1, "a"), unit(1, 1, "a")]);
        assert_eq!(payload.redo.ops.len(), 2);
        assert_eq!(payload.identities.len(), 1);
        assert!(payload.sum_targets.is_empty());
        assert_eq!(payload.origin, RedoOrigin::Restore);
    }

    #[test]
    fn a_restored_record_carries_the_restore_source_to_every_replica() {
        let payload = batch_payload("c", vec![unit(1, 1, "a")]);
        assert_eq!(payload.event_source, EventSource::Restore);
        let entry = transaction_redo_entry(
            TenantId::new(1),
            crate::types::DatabaseId::DEFAULT,
            VShardId::new(0),
            &payload,
        );
        assert_eq!(
            crate::event::EventSource::from(entry.event_source),
            EventSource::Restore
        );
        match entry.write {
            crate::control::wal_replication::ReplicatedWrite::TransactionRedo {
                event_source,
                ..
            } => assert_eq!(EventSource::from(event_source), EventSource::Restore),
            other => panic!("expected a transaction redo, got {other:?}"),
        }
    }
}
