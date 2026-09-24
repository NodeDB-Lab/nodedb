// SPDX-License-Identifier: BUSL-1.1

//! Decode a committed `ReplicatedWrite::TransactionRedo` entry back into the
//! payload every replica applies.
//!
//! The apply loop intercepts these entries before the generic decode: the
//! apply stamps the redo with the entry's Raft coordinates, which the generic
//! `from_replicated_entry` result has no place for.

use nodedb_types::Surrogate;

use super::super::transaction_redo::TransactionRedoPayload;
use super::super::types::ReplicatedWrite;
use crate::control::surrogate::CarriedIdentity;

/// The payload `write` carries, or an error when `write` is another variant.
pub fn transaction_redo_payload(write: &ReplicatedWrite) -> crate::Result<TransactionRedoPayload> {
    let ReplicatedWrite::TransactionRedo {
        redo,
        collections,
        sum_targets,
        identities,
        event_source,
    } = write
    else {
        return Err(crate::Error::Internal {
            detail: "transaction redo decode called with a different ReplicatedWrite variant"
                .into(),
        });
    };
    Ok(TransactionRedoPayload {
        redo: redo.clone(),
        collections: collections.clone(),
        sum_targets: sum_targets.clone(),
        identities: identities
            .iter()
            .map(|identity| CarriedIdentity {
                collection: identity.collection.clone(),
                pk_bytes: identity.pk_bytes.clone(),
                surrogate: Surrogate::new(identity.surrogate),
            })
            .collect(),
        event_source: (*event_source).into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::ReplicatedEntry;
    use crate::control::wal_replication::encode::transaction_redo_entry;
    use crate::event::EventSource;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::{CalvinStamp, RedoRecord, RedoSubRecord};
    use nodedb_physical::physical_plan::{RedoSumTargets, ResolvedSumTarget};

    fn payload() -> TransactionRedoPayload {
        TransactionRedoPayload {
            redo: RedoRecord {
                version: 1,
                ops: vec![RedoSubRecord {
                    record_type: nodedb_wal::record::RecordType::Put as u32,
                    payload: vec![4, 5, 6],
                }],
                calvin_stamp: Some(CalvinStamp {
                    epoch: 11,
                    position: 2,
                    vshard_id: 7,
                    collections: Vec::new(),
                    sum_targets: Vec::new(),
                }),
            },
            collections: vec!["accounts".into(), "entries".into()],
            sum_targets: vec![RedoSumTargets {
                collection: "entries".into(),
                resolved: vec![ResolvedSumTarget::new("accounts", "a1", Surrogate::new(9))],
                deferred: vec!["audit".into()],
            }],
            identities: vec![CarriedIdentity {
                collection: "entries".into(),
                pk_bytes: b"e1".to_vec(),
                surrogate: Surrogate::new(3),
            }],
            event_source: EventSource::Trigger,
        }
    }

    #[test]
    fn transaction_redo_entry_round_trips_through_the_raft_bytes() {
        let original = payload();
        let entry = transaction_redo_entry(
            TenantId::new(1),
            DatabaseId::new(5),
            VShardId::new(7),
            &original,
        );
        let bytes = entry.to_bytes();
        let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("entry decodes");
        assert_eq!(decoded_entry.tenant_id, 1);
        assert_eq!(decoded_entry.database_id, 5);
        assert_eq!(decoded_entry.vshard_id, 7);
        assert_eq!(decoded_entry.idempotency_key, entry.idempotency_key);

        let decoded = transaction_redo_payload(&decoded_entry.write).expect("payload decodes");
        assert_eq!(decoded.redo, original.redo);
        assert_eq!(decoded.collections, original.collections);
        assert_eq!(decoded.sum_targets, original.sum_targets);
        assert_eq!(decoded.identities, original.identities);
        assert_eq!(decoded.event_source, EventSource::Trigger);
    }

    #[test]
    fn apply_plan_carries_the_committed_redo_bytes_unchanged() {
        let original = payload();
        let plan = original.apply_plan().expect("plan builds");
        let nodedb_physical::physical_plan::PhysicalPlan::Meta(
            nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo { redo, .. },
        ) = plan
        else {
            panic!("apply plan must be ApplyTransactionRedo");
        };
        let redo = RedoRecord::from_bytes(&redo).expect("redo decodes");
        assert_eq!(redo, original.redo);
    }

    #[test]
    fn a_different_variant_is_refused() {
        let write = ReplicatedWrite::KvTruncate {
            collection: "kv".into(),
            restart_identity: false,
        };
        assert!(transaction_redo_payload(&write).is_err());
    }
}
