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
    CollectionIncarnation, RedoBody, RedoContent, RedoContentView, ReplicatedEntry,
    ReplicatedEventSource, ReplicatedIdentity, ReplicatedWrite,
};
use crate::control::surrogate::CarriedIdentity;
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
        identities: payload.identities.iter().map(replicated_identity).collect(),
        reads: payload.reads.clone(),
    }
}

/// The identity every replica binds, as a redo entry carries it.
fn replicated_identity(identity: &CarriedIdentity) -> ReplicatedIdentity {
    ReplicatedIdentity {
        collection: identity.collection.clone(),
        pk_bytes: identity.pk_bytes.clone(),
        surrogate: identity.surrogate.as_u32(),
    }
}

/// The content of `payload` as a framer reads it, by reference.
fn view_of(
    payload: &TransactionRedoPayload,
) -> RedoContentView<'_, impl Iterator<Item = ReplicatedIdentity> + '_> {
    RedoContentView {
        redo: &payload.redo,
        sum_targets: &payload.sum_targets,
        identities: payload.identities.iter().map(replicated_identity),
        reads: &payload.reads,
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
            calvin: payload.calvin.clone(),
            snapshot_ordinal: payload.snapshot_ordinal,
            adds_storage: payload.adds_storage,
        },
    )
    .with_event_source(payload.event_source)
    .naming_kinds(payload.incarnation_names.iter().cloned())
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
    redo_proposal(target, payload, max_entry_bytes, |last| {
        RedoStreamId::Session {
            vshard: target.vshard_id.as_u32(),
            idempotency_key: last.idempotency_key,
        }
    })
}

/// What the leader stamps on a committed Calvin slice's final entry. Every
/// re-proposal of the slice carries the same stamps.
#[derive(Debug, Clone)]
pub struct CalvinEntryStamps {
    /// The slice's commit HLC: every replica records it on the tenant's
    /// write mark.
    pub write_hlc: u64,
    /// The RESTORE that re-issued the slice, `0` for any other.
    pub restore_id: u64,
    /// The metadata index the coordinator planned the transaction against.
    pub metadata_floor: u64,
    /// The incarnation the coordinator planned against for each collection
    /// the slice writes.
    pub incarnations: Vec<CollectionIncarnation>,
}

/// The entries of a committed Calvin slice's redo, for one proposal
/// attempt. The redo travels inline when its entry fits `max_entry_bytes`,
/// else chunked under `stream`.
pub fn calvin_redo_proposal(
    target: RedoEntryTarget,
    payload: &TransactionRedoPayload,
    stamps: &CalvinEntryStamps,
    stream: RedoStreamId,
    max_entry_bytes: usize,
) -> crate::Result<RedoProposal> {
    let proposal = redo_proposal(target, payload, max_entry_bytes, |_| stream)?;
    let stamp = |mut last: ReplicatedEntry| {
        last.write_hlc = stamps.write_hlc;
        last.restore_id = stamps.restore_id;
        last.metadata_floor = stamps.metadata_floor;
        last.incarnations = stamps.incarnations.clone();
        last
    };
    Ok(match proposal {
        RedoProposal::Inline(last) => RedoProposal::Inline(stamp(last)),
        RedoProposal::Chunked {
            stream,
            chunks,
            last,
        } => RedoProposal::Chunked {
            stream,
            chunks,
            last: stamp(last),
        },
    })
}

/// The entries of `payload`: inline when its entry fits `max_entry_bytes`,
/// else chunks under the stream `stream_of` names for the final entry.
fn redo_proposal(
    target: RedoEntryTarget,
    payload: &TransactionRedoPayload,
    max_entry_bytes: usize,
    stream_of: impl FnOnce(&ReplicatedEntry) -> RedoStreamId,
) -> crate::Result<RedoProposal> {
    // The framer reads the payload by reference. Only a redo that fits one
    // entry is copied, into that entry.
    let bytes = view_of(payload).to_framed()?;
    if fits_inline(payload, bytes.len() as u64, max_entry_bytes) {
        return Ok(RedoProposal::Inline(redo_entry(
            target,
            payload,
            RedoBody::Inline(Box::new(content_of(payload))),
        )));
    }
    let count = chunk_count(bytes.len() as u64, max_entry_bytes)?;
    let last = chunked_final_entry(target, payload, count, bytes.len() as u64, stream_of);
    let stream = final_entry_stream(&last)?;
    let room = chunk_room(max_entry_bytes)?;
    let len = bytes.len() as u64;
    let chunks = (0u32..)
        .zip(bytes.chunks(room))
        .map(|(index, part)| {
            chunk_entry(
                target,
                RedoChunkSlot { stream, index, len },
                part.to_vec(),
                payload.event_source,
            )
        })
        .collect();
    Ok(RedoProposal::Chunked {
        stream,
        chunks,
        last,
    })
}

/// Whether a redo whose framed content holds `content_len` bytes travels
/// inline in one entry of at most `max_entry_bytes`.
pub fn fits_inline(
    payload: &TransactionRedoPayload,
    content_len: u64,
    max_entry_bytes: usize,
) -> bool {
    // The entry around the content: the collections the write names, the
    // names it carries with their incarnations, and the fixed fields.
    let named_bytes = payload
        .collections
        .iter()
        .map(String::len)
        .chain(payload.incarnation_names.iter().map(|(name, _)| name.len()))
        .sum::<usize>();
    let named_count = payload
        .collections
        .len()
        .max(payload.incarnation_names.len());
    let envelope = named_bytes
        .saturating_add(named_count.saturating_mul(COLLECTION_ENTRY_BYTES))
        .saturating_add(ENTRY_STAMP_BYTES);
    content_len.saturating_add(envelope as u64) <= max_entry_bytes as u64
}

/// The header frames of `payload`: the first bytes of its framed content,
/// ahead of the resolved lists.
pub fn redo_header_frames(payload: &TransactionRedoPayload) -> crate::Result<Vec<u8>> {
    let mut out = Vec::new();
    view_of(payload).push_header_frames(&mut out)?;
    Ok(out)
}

/// The final entry of a chunked redo of `count` chunks and `len` bytes,
/// under the stream `stream_of` names for it.
pub fn chunked_final_entry(
    target: RedoEntryTarget,
    payload: &TransactionRedoPayload,
    count: u32,
    len: u64,
    stream_of: impl FnOnce(&ReplicatedEntry) -> RedoStreamId,
) -> ReplicatedEntry {
    // The stream can name the final entry's own idempotency key, so the
    // entry exists before its body names the stream.
    let mut last = redo_entry(
        target,
        payload,
        RedoBody::Chunked {
            stream: RedoStreamId::Session {
                vshard: target.vshard_id.as_u32(),
                idempotency_key: 0,
            },
            count,
            len,
        },
    );
    let stream = stream_of(&last);
    if let ReplicatedWrite::TransactionRedo { body, .. } = &mut last.write {
        *body = RedoBody::Chunked { stream, count, len };
    }
    last
}

/// The stream the body of `last` names.
pub fn final_entry_stream(last: &ReplicatedEntry) -> crate::Result<RedoStreamId> {
    match &last.write {
        ReplicatedWrite::TransactionRedo {
            body: RedoBody::Chunked { stream, .. },
            ..
        } => Ok(*stream),
        other => Err(crate::Error::Internal {
            detail: format!("a chunked redo's final entry carries {other:?}, not a chunked body"),
        }),
    }
}

/// Bytes a chunk entry of `max_entry_bytes` carries beside its envelope.
pub fn chunk_room(max_entry_bytes: usize) -> crate::Result<usize> {
    max_entry_bytes
        .checked_sub(CHUNK_ENTRY_ENVELOPE_BYTES)
        .filter(|room| *room > 0)
        .ok_or_else(|| crate::Error::Config {
            detail: format!(
                "tuning.calvin.max_redo_entry_bytes {max_entry_bytes} leaves no room for a \
                 redo chunk beside its {CHUNK_ENTRY_ENVELOPE_BYTES}-byte envelope"
            ),
        })
}

/// How many chunk entries of `max_entry_bytes` carry `len` bytes: every
/// chunk but the last is full.
pub fn chunk_count(len: u64, max_entry_bytes: usize) -> crate::Result<u32> {
    let room = chunk_room(max_entry_bytes)? as u64;
    u32::try_from(len.div_ceil(room)).map_err(|_| crate::Error::Internal {
        detail: format!(
            "a redo of {len} bytes splits into more than {} chunks",
            u32::MAX
        ),
    })
}

/// Where one chunk sits in its stream.
#[derive(Debug, Clone, Copy)]
pub struct RedoChunkSlot {
    pub stream: RedoStreamId,
    pub index: u32,
    /// The byte length of the whole stream.
    pub len: u64,
}

/// The `RedoChunk` entry that carries `bytes` at `slot`.
pub fn chunk_entry(
    target: RedoEntryTarget,
    slot: RedoChunkSlot,
    bytes: Vec<u8>,
    event_source: crate::event::EventSource,
) -> ReplicatedEntry {
    ReplicatedEntry::new(
        target.tenant_id.as_u64(),
        target.database_id.as_u64(),
        target.vshard_id.as_u32(),
        ReplicatedWrite::RedoChunk {
            stream: slot.stream,
            index: slot.index,
            len: slot.len,
            bytes,
        },
    )
    .with_event_source(event_source)
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
                user_roles: Vec::new(),
            },
            collections: vec!["orders".into()],
            incarnation_names: vec![(
                "orders".into(),
                nodedb_cluster::calvin::types::IncarnationKind::Collection,
            )],
            sum_targets: Vec::new(),
            identities: Vec::new(),
            event_source: EventSource::User,
            origin: RedoOrigin::Commit,
            calvin: None,
            reads: Vec::new(),
            snapshot_ordinal: None,
            adds_storage: false,
        }
    }

    fn target() -> RedoEntryTarget {
        RedoEntryTarget {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(3),
        }
    }

    /// The payload of a committed Calvin slice.
    fn calvin_payload(op_bytes: usize) -> TransactionRedoPayload {
        let mut payload = payload(op_bytes);
        payload.calvin = Some(super::super::super::types::CalvinRedoMeta {
            epoch_system_ms: 7,
            reply: nodedb_physical::physical_plan::CalvinReplySpec::Count(Vec::new()),
            primary_write: true,
            user_write: true,
            returning: false,
        });
        payload
    }

    fn calvin_stamps() -> CalvinEntryStamps {
        CalvinEntryStamps {
            write_hlc: 41,
            restore_id: 0,
            metadata_floor: 9,
            incarnations: vec![CollectionIncarnation {
                collection: "orders".into(),
                incarnation: nodedb_types::Hlc::ZERO,
                kind: nodedb_cluster::calvin::types::IncarnationKind::Collection,
            }],
        }
    }

    fn calvin_stream() -> RedoStreamId {
        RedoStreamId::Calvin {
            vshard: 3,
            epoch: 5,
            position: 1,
            attempt: 2,
        }
    }

    /// The final entry of a Calvin slice's redo carries the leader's stamps
    /// and the slice's meta, inline or chunked. Chunks carry no stamp.
    #[test]
    fn a_calvin_proposal_stamps_its_final_entry() {
        let inline = calvin_redo_proposal(
            target(),
            &calvin_payload(100),
            &calvin_stamps(),
            calvin_stream(),
            64 * 1024,
        )
        .expect("build");
        let RedoProposal::Inline(last) = inline else {
            panic!("a small redo travels inline");
        };
        assert_eq!(last.write_hlc, 41);
        assert_eq!(last.metadata_floor, 9);
        assert_eq!(last.incarnations, calvin_stamps().incarnations);
        assert!(matches!(
            last.write,
            ReplicatedWrite::TransactionRedo {
                calvin: Some(_),
                ..
            }
        ));

        let chunked = calvin_redo_proposal(
            target(),
            &calvin_payload(300_000),
            &calvin_stamps(),
            calvin_stream(),
            64 * 1024,
        )
        .expect("build");
        let RedoProposal::Chunked {
            stream,
            chunks,
            last,
        } = chunked
        else {
            panic!("a 300 KB redo travels chunked");
        };
        assert_eq!(stream, calvin_stream(), "the attempt names its stream");
        assert_eq!(last.write_hlc, 41);
        assert!(chunks.iter().all(|chunk| chunk.write_hlc == 0));
        assert!(matches!(
            last.write,
            ReplicatedWrite::TransactionRedo {
                body: RedoBody::Chunked { stream: named, .. },
                calvin: Some(_),
                ..
            } if named == calvin_stream()
        ));
    }

    #[test]
    fn the_entry_carries_the_payloads_storage_flag_inline_and_chunked() {
        for adds_storage in [true, false] {
            let mut redo = payload(300_000);
            redo.adds_storage = adds_storage;
            let RedoProposal::Chunked { last, .. } =
                session_redo_proposal(target(), &redo, 64 * 1024).expect("chunked proposal")
            else {
                panic!("a 300 KB redo over a 64 KiB limit chunks");
            };
            assert!(matches!(
                last.write,
                ReplicatedWrite::TransactionRedo { adds_storage: flag, .. } if flag == adds_storage
            ));
            redo.redo.ops[0].payload = vec![7; 8];
            let inline = transaction_redo_entry(
                target().tenant_id,
                target().database_id,
                target().vshard_id,
                &redo,
            );
            assert!(matches!(
                inline.write,
                ReplicatedWrite::TransactionRedo { adds_storage: flag, .. } if flag == adds_storage
            ));
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
            RedoContent::from_framed(&joined).expect("content").redo,
            payload(300_000).redo
        );
    }
}
