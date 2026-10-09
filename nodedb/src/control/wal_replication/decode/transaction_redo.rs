// SPDX-License-Identifier: BUSL-1.1

//! Decode a committed `ReplicatedWrite::TransactionRedo` entry back into the
//! payload every replica applies, or into the stream a chunked body names.
//!
//! The apply loop intercepts these entries before the generic decode: the
//! apply stamps the redo with the entry's Raft coordinates, which the generic
//! `from_replicated_entry` result has no place for.

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::Surrogate;

use super::super::transaction_redo::TransactionRedoPayload;
use super::super::types::{RedoBody, RedoContent, ReplicatedEventSource, ReplicatedWrite};
use crate::control::surrogate::CarriedIdentity;
use crate::wal::RedoStreamId;

/// A decoded `TransactionRedo` entry.
#[derive(Debug, Clone)]
pub enum DecodedTransactionRedo {
    /// The entry carries the whole payload.
    Inline(Box<TransactionRedoPayload>),
    /// The entry names the stream whose chunks carry the content.
    Chunked(ChunkedRedo),
}

/// The final entry of a chunked redo: its stream and the fields the entry
/// itself carries.
#[derive(Debug, Clone)]
pub struct ChunkedRedo {
    pub stream: RedoStreamId,
    /// How many chunks the stream holds.
    pub count: u32,
    /// How many bytes the chunks hold in all.
    pub len: u64,
    collections: Vec<String>,
    event_source: ReplicatedEventSource,
    origin: RedoOrigin,
}

impl ChunkedRedo {
    /// The payload of the stream's assembled `content`.
    pub fn payload(&self, content: RedoContent) -> TransactionRedoPayload {
        payload_of(content, &self.collections, self.event_source, self.origin)
    }

    /// Every collection the transaction wrote.
    pub fn collections(&self) -> &[String] {
        &self.collections
    }
}

fn payload_of(
    content: RedoContent,
    collections: &[String],
    event_source: ReplicatedEventSource,
    origin: RedoOrigin,
) -> TransactionRedoPayload {
    TransactionRedoPayload {
        redo: content.redo,
        collections: collections.to_vec(),
        sum_targets: content.sum_targets,
        identities: content
            .identities
            .into_iter()
            .map(|identity| CarriedIdentity {
                collection: identity.collection,
                pk_bytes: identity.pk_bytes,
                surrogate: Surrogate::new(identity.surrogate),
            })
            .collect(),
        event_source: event_source.into(),
        origin,
        // The wire entry carries no Calvin meta.
        calvin: None,
    }
}

/// What `write` carries, or an error when `write` is another variant.
pub fn decode_transaction_redo(write: &ReplicatedWrite) -> crate::Result<DecodedTransactionRedo> {
    let ReplicatedWrite::TransactionRedo {
        body,
        collections,
        event_source,
        origin,
    } = write
    else {
        return Err(crate::Error::Internal {
            detail: "transaction redo decode called with a different ReplicatedWrite variant"
                .into(),
        });
    };
    Ok(match body {
        RedoBody::Inline(content) => DecodedTransactionRedo::Inline(Box::new(payload_of(
            (**content).clone(),
            collections,
            *event_source,
            *origin,
        ))),
        RedoBody::Chunked { stream, count, len } => DecodedTransactionRedo::Chunked(ChunkedRedo {
            stream: *stream,
            count: *count,
            len: *len,
            collections: collections.clone(),
            event_source: *event_source,
            origin: *origin,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::ReplicatedEntry;
    use crate::control::wal_replication::encode::{
        RedoEntryTarget, RedoProposal, session_redo_proposal, transaction_redo_entry,
    };
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
                cross_shard_applied: None,
                row_sources: Vec::new(),
                publishes: vec![crate::wal::RedoPublish {
                    owner: "trigger/5/notify".into(),
                    database_id: 5,
                    tenant_id: 1,
                    topic: "orders_feed".into(),
                    payload: "created".into(),
                    metadata_floor: 0,
                    position: None,
                }],
                row_changes: Vec::new(),
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
            origin: RedoOrigin::Restore,
            calvin: None,
        }
    }

    fn inline(write: &ReplicatedWrite) -> TransactionRedoPayload {
        match decode_transaction_redo(write).expect("payload decodes") {
            DecodedTransactionRedo::Inline(payload) => *payload,
            DecodedTransactionRedo::Chunked(chunked) => {
                panic!("expected an inline body, got stream {:?}", chunked.stream)
            }
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

        let decoded = inline(&decoded_entry.write);
        assert_eq!(decoded.redo, original.redo);
        assert_eq!(decoded.collections, original.collections);
        assert_eq!(decoded.sum_targets, original.sum_targets);
        assert_eq!(decoded.identities, original.identities);
        assert_eq!(decoded.event_source, EventSource::Trigger);
        assert_eq!(decoded.origin, RedoOrigin::Restore);
        // Every collection the redo writes travels for its incarnation stamp.
        let named: Vec<&str> = decoded_entry
            .incarnations
            .iter()
            .map(|named| named.collection.as_str())
            .collect();
        assert_eq!(named, vec!["accounts", "entries"]);
    }

    #[test]
    fn a_chunked_final_entry_round_trips_and_rebuilds_the_payload() {
        let mut original = payload();
        original.redo.ops[0].payload = vec![9; 200_000];
        let RedoProposal::Chunked {
            stream,
            chunks,
            last,
        } = session_redo_proposal(
            RedoEntryTarget {
                tenant_id: TenantId::new(1),
                database_id: DatabaseId::new(5),
                vshard_id: VShardId::new(7),
            },
            &original,
            64 * 1024,
        )
        .expect("build")
        else {
            panic!("a 200 KB redo travels chunked");
        };
        let decoded_last = ReplicatedEntry::from_bytes(&last.to_bytes()).expect("entry decodes");
        let DecodedTransactionRedo::Chunked(chunked) =
            decode_transaction_redo(&decoded_last.write).expect("decodes")
        else {
            panic!("the final entry names its stream");
        };
        assert_eq!(chunked.stream, stream);
        assert_eq!(chunked.count as usize, chunks.len());
        assert_eq!(chunked.collections(), original.collections.as_slice());
        let joined: Vec<u8> = chunks
            .iter()
            .flat_map(|chunk| match &chunk.write {
                ReplicatedWrite::RedoChunk { bytes, .. } => bytes.clone(),
                other => panic!("expected a chunk, got {other:?}"),
            })
            .collect();
        let rebuilt = chunked.payload(RedoContent::from_bytes(&joined).expect("content"));
        assert_eq!(rebuilt.redo, original.redo);
        assert_eq!(rebuilt.sum_targets, original.sum_targets);
        assert_eq!(rebuilt.identities, original.identities);
        assert_eq!(rebuilt.event_source, EventSource::Trigger);
        assert_eq!(rebuilt.origin, RedoOrigin::Restore);
    }

    #[test]
    fn apply_plan_carries_the_committed_redo_bytes_unchanged() {
        let original = payload();
        let plan = original.apply_plan().expect("plan builds");
        let nodedb_physical::physical_plan::PhysicalPlan::Meta(
            nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo { redo, origin, .. },
        ) = plan
        else {
            panic!("apply plan must be ApplyTransactionRedo");
        };
        let redo = RedoRecord::from_bytes(&redo).expect("redo decodes");
        assert_eq!(redo, original.redo);
        assert_eq!(origin, RedoOrigin::Restore);
    }

    #[test]
    fn a_different_variant_is_refused() {
        let write = ReplicatedWrite::KvTruncate {
            collection: "kv".into(),
            restart_identity: false,
        };
        assert!(decode_transaction_redo(&write).is_err());
    }
}
