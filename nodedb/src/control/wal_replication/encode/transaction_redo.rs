// SPDX-License-Identifier: BUSL-1.1

//! Encode a committed transaction's redo as `ReplicatedWrite::TransactionRedo`
//! entries for its vShard's data-group log.
//!
//! A redo is not built from a `PhysicalPlan`, so it has no arm in
//! `to_replicated_entry`: the commit that resolved it encodes it here. A redo
//! whose entry passes the redo entry limit travels as `RedoChunk` entries of
//! one stream, then a final entry whose body names the stream.

use super::super::transaction_redo::TransactionRedoPayload;
use super::super::types::{
    RedoBody, RedoContent, ReplicatedEntry, ReplicatedEventSource, ReplicatedIdentity,
    ReplicatedWrite,
};
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::RedoStreamId;

/// Room a chunk entry keeps for its envelope beside the chunk bytes: the
/// routing fields, the stream header, and the stamps the proposer adds.
pub const CHUNK_ENTRY_ENVELOPE_BYTES: usize = 4 * 1024;

/// Room an inline entry keeps for its fixed fields and the stamps the
/// proposer adds after the size check: the commit HLC and the metadata floor.
const ENTRY_STAMP_BYTES: usize = 1024;

/// Room each named collection's incarnation and wire framing take, beside
/// its name.
const COLLECTION_ENTRY_BYTES: usize = 64;

/// Where a committed redo's entries go.
#[derive(Debug, Clone, Copy)]
pub struct RedoEntryTarget {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
}

/// How one committed redo travels through its vShard's data-group log.
pub enum RedoProposal {
    /// One entry carries the whole redo.
    Inline(ReplicatedEntry),
    /// `chunks` carry the redo in order, then `last` names their stream.
    Chunked {
        stream: RedoStreamId,
        chunks: Vec<ReplicatedEntry>,
        last: ReplicatedEntry,
    },
}

/// The content of `payload`: the part a chunked redo splits.
fn content_of(payload: &TransactionRedoPayload) -> RedoContent {
    RedoContent {
        redo: payload.redo.clone(),
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
    }
}

/// The `TransactionRedo` entry of `payload` with `body`.
fn redo_entry(
    target: RedoEntryTarget,
    payload: &TransactionRedoPayload,
    body: RedoBody,
) -> ReplicatedEntry {
    ReplicatedEntry::new(
        target.tenant_id.as_u64(),
        target.database_id.as_u64(),
        target.vshard_id.as_u32(),
        ReplicatedWrite::TransactionRedo {
            body,
            collections: payload.collections.clone(),
            event_source: ReplicatedEventSource::from(payload.event_source),
            origin: payload.origin,
        },
    )
    .with_event_source(payload.event_source)
    .naming(payload.collections.iter().map(String::as_str))
}

/// The one Raft entry that carries `payload` to every replica of `vshard_id`.
pub fn transaction_redo_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    payload: &TransactionRedoPayload,
) -> ReplicatedEntry {
    redo_entry(
        RedoEntryTarget {
            tenant_id,
            database_id,
            vshard_id,
        },
        payload,
        RedoBody::Inline(Box::new(content_of(payload))),
    )
}

/// The entries of a session COMMIT's redo. The redo travels inline when its
/// entry fits `max_entry_bytes`, else chunked under a stream the final
/// entry's idempotency key names.
pub fn session_redo_proposal(
    target: RedoEntryTarget,
    payload: &TransactionRedoPayload,
    max_entry_bytes: usize,
) -> crate::Result<RedoProposal> {
    let content = content_of(payload);
    let bytes = content.to_bytes()?;
    // The entry around the content: the collections it names twice, once in
    // the write and once with their incarnations, and the fixed fields.
    let envelope = payload
        .collections
        .iter()
        .map(|collection| 2 * collection.len() + COLLECTION_ENTRY_BYTES)
        .sum::<usize>()
        .saturating_add(ENTRY_STAMP_BYTES);
    let mut last = redo_entry(target, payload, RedoBody::Inline(Box::new(content)));
    if bytes.len().saturating_add(envelope) <= max_entry_bytes {
        return Ok(RedoProposal::Inline(last));
    }
    let stream = RedoStreamId::Session {
        vshard: target.vshard_id.as_u32(),
        idempotency_key: last.idempotency_key,
    };
    let chunks = chunk_entries(
        target,
        stream,
        &bytes,
        max_entry_bytes,
        payload.event_source,
    )?;
    let count = u32::try_from(chunks.len()).map_err(|_| crate::Error::Internal {
        detail: format!(
            "a redo of {} bytes splits into too many chunks",
            bytes.len()
        ),
    })?;
    last.write = ReplicatedWrite::TransactionRedo {
        body: RedoBody::Chunked {
            stream,
            count,
            len: bytes.len() as u64,
        },
        collections: payload.collections.clone(),
        event_source: ReplicatedEventSource::from(payload.event_source),
        origin: payload.origin,
    };
    Ok(RedoProposal::Chunked {
        stream,
        chunks,
        last,
    })
}

/// The `RedoChunk` entries that carry `bytes` under `stream`, each encoded
/// within `max_entry_bytes`.
pub fn chunk_entries(
    target: RedoEntryTarget,
    stream: RedoStreamId,
    bytes: &[u8],
    max_entry_bytes: usize,
    event_source: crate::event::EventSource,
) -> crate::Result<Vec<ReplicatedEntry>> {
    let room = max_entry_bytes
        .checked_sub(CHUNK_ENTRY_ENVELOPE_BYTES)
        .filter(|room| *room > 0)
        .ok_or_else(|| crate::Error::Config {
            detail: format!(
                "tuning.calvin.max_redo_entry_bytes {max_entry_bytes} leaves no room for a \
                 redo chunk beside its {CHUNK_ENTRY_ENVELOPE_BYTES}-byte envelope"
            ),
        })?;
    let len = bytes.len() as u64;
    Ok((0u32..)
        .zip(bytes.chunks(room))
        .map(|(index, part)| {
            ReplicatedEntry::new(
                target.tenant_id.as_u64(),
                target.database_id.as_u64(),
                target.vshard_id.as_u32(),
                ReplicatedWrite::RedoChunk {
                    stream,
                    index,
                    len,
                    bytes: part.to_vec(),
                },
            )
            .with_event_source(event_source)
        })
        .collect())
}

/// The entry that drops `stream` on every replica.
pub fn redo_abandon_entry(target: RedoEntryTarget, stream: RedoStreamId) -> ReplicatedEntry {
    ReplicatedEntry::new(
        target.tenant_id.as_u64(),
        target.database_id.as_u64(),
        target.vshard_id.as_u32(),
        ReplicatedWrite::RedoAbandon { stream },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventSource;
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_physical::physical_plan::RedoOrigin;

    fn payload(op_bytes: usize) -> TransactionRedoPayload {
        TransactionRedoPayload {
            redo: RedoRecord {
                version: 1,
                ops: vec![RedoSubRecord {
                    record_type: nodedb_wal::record::RecordType::Put as u32,
                    payload: vec![7; op_bytes],
                }],
                calvin_stamp: None,
                cross_shard_applied: None,
                row_sources: Vec::new(),
                publishes: Vec::new(),
                row_changes: Vec::new(),
            },
            collections: vec!["orders".into()],
            sum_targets: Vec::new(),
            identities: Vec::new(),
            event_source: EventSource::User,
            origin: RedoOrigin::Commit,
            calvin: None,
        }
    }

    fn target() -> RedoEntryTarget {
        RedoEntryTarget {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(3),
        }
    }

    #[test]
    fn a_redo_within_the_limit_travels_inline() {
        let proposal = session_redo_proposal(target(), &payload(100), 64 * 1024).expect("build");
        assert!(matches!(proposal, RedoProposal::Inline(_)));
    }

    #[test]
    fn a_redo_past_the_limit_travels_in_chunks_that_fit_the_limit() {
        let limit = 64 * 1024;
        let proposal = session_redo_proposal(target(), &payload(300_000), limit).expect("build");
        let RedoProposal::Chunked {
            stream,
            chunks,
            last,
        } = proposal
        else {
            panic!("a 300 KB redo must travel chunked");
        };
        assert_eq!(
            stream,
            RedoStreamId::Session {
                vshard: 3,
                idempotency_key: last.idempotency_key
            }
        );
        assert!(chunks.len() >= 5);
        for chunk in &chunks {
            assert!(chunk.encode().expect("encode").len() + ENTRY_STAMP_BYTES <= limit);
        }
        let ReplicatedWrite::TransactionRedo {
            body: RedoBody::Chunked { count, len, .. },
            ..
        } = last.write
        else {
            panic!("the final entry names its stream");
        };
        assert_eq!(count as usize, chunks.len());
        let joined: Vec<u8> = chunks
            .iter()
            .flat_map(|chunk| match &chunk.write {
                ReplicatedWrite::RedoChunk { bytes, .. } => bytes.clone(),
                other => panic!("expected a chunk, got {other:?}"),
            })
            .collect();
        assert_eq!(joined.len() as u64, len);
        assert_eq!(
            RedoContent::from_bytes(&joined).expect("content").redo,
            payload(300_000).redo
        );
    }
}
