// SPDX-License-Identifier: BUSL-1.1

//! The Control Plane's reads of a chunked redo's content, one page at a
//! time.
//!
//! A chunked redo's final entry installs a marker record whose only
//! sub-record names the stream (see `crate::wal::redo::stream_ref`). The
//! content stays in this node's WAL. The Control Plane asks the owning core
//! for it through `MetaOp::ReadRedoStreamPage`, one bounded page at a time,
//! and holds one page at a time:
//!
//! - before the install: the header and every identity. Each page's
//!   identities bind into this node's catalog before the next page reads,
//!   and every identity binds before the install that stores the rows.
//! - once the install applied: the row changes. Each page stages its change
//!   events under the entry, as the write funnel stages a write's events,
//!   and the apply loop publishes them when it settles the entry.

use std::sync::Arc;

use nodedb_physical::physical_plan::{MetaOp, RedoStreamPage, RedoStreamSection};
use nodedb_types::Surrogate;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, PendingChanges, SubmitWrite, WalDurability, WriteChangeSet, WriteOrdering,
    submit_write,
};
use crate::control::state::SharedState;
use crate::control::surrogate::{CarriedIdentity, bind_carried_identities};
use crate::control::wal_replication::decode::ChunkedRedo;
use crate::control::wal_replication::types::ReplicatedIdentity;
use crate::types::{Lsn, TraceId};
use crate::wal::redo::frame::{RedoContentHeader, RedoFrameTag, RedoFrames, decode_frame};
use crate::wal::{RedoStreamId, RedoStreamRef};

use super::apply::RedoTarget;
use super::payload::TransactionRedoPayload;

/// Bytes of records one page carries, or one record past the bound alone.
const PAGE_BYTES: u64 = 1024 * 1024;

/// The final entry of a chunked redo, with the reference of its content.
#[derive(Debug, Clone)]
pub struct StreamedFinal {
    pub chunked: ChunkedRedo,
    pub reference: RedoStreamRef,
    /// The mirror database that applies the entry, when one does.
    pub mirror_database: Option<u64>,
}

/// What the row-change read of an installed stream needs, and how far it
/// staged.
pub(crate) struct StreamedChanges {
    stream: RedoStreamId,
    /// The reference, as the page request carries it.
    encoded: Vec<u8>,
    /// The content offset the next page reads from. Every row change before
    /// it is staged.
    next: u64,
    /// Whether every row change is staged.
    done: bool,
    /// The LSN of the stream's first chunk record, which the row-change
    /// reads need in the WAL.
    first: Option<Lsn>,
}

impl StreamedChanges {
    /// The row-change read of the stream `reference` names, from the start
    /// of its content.
    pub(crate) fn of_reference(reference: &RedoStreamRef) -> crate::Result<Self> {
        let stream = reference.stream;
        let encoded = zerompk::to_msgpack_vec(reference)
            .map_err(|e| malformed(stream, format!("the reference does not encode: {e}")))?;
        Ok(Self {
            stream,
            encoded,
            next: 0,
            done: false,
            first: reference.first_lsn().map(Lsn::new),
        })
    }

    /// The LSN of the stream's first chunk record, when the stream has one.
    pub(crate) fn first_chunk_lsn(&self) -> Option<Lsn> {
        self.first
    }
}

/// Where an installed stream's change events stage.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChangesAt {
    /// The data group of the committed entry.
    pub group_id: u64,
    /// The entry's log index in its group.
    pub log_index: u64,
    /// The commit HLC the entry carries, which dates its events.
    pub commit_hlc: u64,
}

fn malformed(stream: RedoStreamId, detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "framed redo content".into(),
        detail: format!("redo stream {stream:?}: {detail}"),
    }
}

/// Ask the owning core for the page of `section` that starts at `from`.
async fn read_page(
    state: &SharedState,
    target: RedoTarget,
    stream: RedoStreamId,
    encoded: &[u8],
    section: RedoStreamSection,
    from: u64,
) -> crate::Result<RedoStreamPage> {
    let outcome = submit_write(
        state,
        SubmitWrite {
            tenant_id: target.tenant_id,
            database_id: target.database_id,
            vshard_id: target.vshard_id,
            // A replicated redo apply orders against no client connection.
            stream: crate::bridge::envelope::RequestStream::Internal,
            plan: PhysicalPlan::Meta(MetaOp::ReadRedoStreamPage {
                stream: encoded.to_vec(),
                section,
                from,
                max_page_bytes: PAGE_BYTES,
            }),
            trace_id: TraceId::generate(),
            event_source: crate::event::EventSource::User,
            txn_id: None,
            user_id: None,
            user_roles: Vec::new(),
            // The read appends nothing.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
                minted: None,
                system_ordinal: None,
            },
            // The apply loop asks it while the entry applies in log order.
            ordering: WriteOrdering::AlreadyOrdered,
            change_feed: ChangeFeedOwner::Unowned,
        },
    )
    .await?;
    let response = outcome.response;
    if response.status != Status::Ok {
        return Err(match response.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Internal {
                detail: format!(
                    "the page of redo stream {stream:?} at byte {from} was refused with no code"
                ),
            },
        });
    }
    let page: RedoStreamPage = zerompk::from_msgpack(response.payload.as_bytes()).map_err(|e| {
        malformed(
            stream,
            format!("the page at byte {from} does not decode: {e}"),
        )
    })?;
    if page.next < from || (page.next == from && !page.done) {
        return Err(crate::Error::Internal {
            detail: format!(
                "the page of redo stream {stream:?} at byte {from} names byte {} next and is \
                 not the last",
                page.next
            ),
        });
    }
    Ok(page)
}

/// The identities one page of the identity section carries. The content's
/// first page opens with its header, which lands in `header`.
fn page_identities(
    stream: RedoStreamId,
    frames: &[u8],
    header: &mut Option<RedoContentHeader>,
) -> crate::Result<Vec<CarriedIdentity>> {
    let mut identities = Vec::new();
    for frame in RedoFrames::new(frames) {
        let (tag, body) = frame?;
        match (tag, header.is_some()) {
            (RedoFrameTag::Header, false) => *header = Some(decode_frame(tag, body)?),
            (RedoFrameTag::Identity, true) => {
                let identity: ReplicatedIdentity = decode_frame(tag, body)?;
                identities.push(CarriedIdentity {
                    collection: identity.collection,
                    pk_bytes: identity.pk_bytes,
                    surrogate: Surrogate::new(identity.surrogate),
                });
            }
            (tag, false) => {
                return Err(malformed(
                    stream,
                    format!("the first record is a {tag:?} record, not the header"),
                ));
            }
            (tag, true) => {
                return Err(malformed(
                    stream,
                    format!("the identity section holds a {tag:?} record"),
                ));
            }
        }
    }
    Ok(identities)
}

/// The marker payload `streamed` applies with, once every identity of its
/// content is bound on this node, and what its row-change read needs.
pub(crate) async fn streamed_payload(
    state: &SharedState,
    target: RedoTarget,
    streamed: StreamedFinal,
) -> crate::Result<(TransactionRedoPayload, StreamedChanges)> {
    let StreamedFinal {
        chunked,
        reference,
        mirror_database,
    } = streamed;
    let stream = reference.stream;
    let mut changes = StreamedChanges::of_reference(&reference)?;
    let mut header: Option<RedoContentHeader> = None;
    let mut from = 0u64;
    loop {
        let page = read_page(
            state,
            target,
            stream,
            &changes.encoded,
            RedoStreamSection::Identities,
            from,
        )
        .await?;
        let identities = page_identities(stream, &page.frames, &mut header)?;
        if !identities.is_empty() {
            bind_carried_identities(
                &state.surrogate_assigner,
                target.database_id,
                target.tenant_id,
                &identities,
            )?;
        }
        from = page.next;
        if page.done {
            break;
        }
    }
    let header = header.ok_or_else(|| malformed(stream, "the content holds no record".into()))?;
    let marker = reference.marker(header.calvin_stamp, header.cross_shard_applied, Vec::new())?;
    let payload = chunked.marker_payload(marker);
    let payload = match mirror_database {
        Some(database) => payload.in_mirror(database),
        None => payload,
    };
    changes.next = from;
    Ok((payload, changes))
}

/// Stage the change events of an installed stream under its entry, one
/// page of row changes at a time, at `lsn`, the LSN of the entry's change
/// marker.
///
/// Each staged page moves `changes` past it, so a call after an error
/// resumes at the page that failed and stages no page twice.
pub(crate) async fn stage_streamed_changes(
    state: &Arc<SharedState>,
    target: RedoTarget,
    changes: &mut StreamedChanges,
    at: ChangesAt,
    lsn: Lsn,
) -> crate::Result<()> {
    while !changes.done {
        let page = read_page(
            state,
            target,
            changes.stream,
            &changes.encoded,
            RedoStreamSection::RowChanges,
            changes.next,
        )
        .await?;
        let mut row_changes = Vec::new();
        for frame in RedoFrames::new(&page.frames) {
            let (tag, body) = frame?;
            if tag != RedoFrameTag::RowChange {
                return Err(malformed(
                    changes.stream,
                    format!("the row-change section holds a {tag:?} record"),
                ));
            }
            row_changes.push(decode_frame(tag, body)?);
        }
        #[cfg(feature = "failpoints")]
        let collections = page_collections(target, &row_changes);
        // A page past the stream's spill bound moves to the change journal
        // as it stages, so the entry holds at most one page in memory. A
        // page refused leaves `changes` here: the retry stages it again.
        PendingChanges::staged(
            WriteChangeSet::of_row_changes(row_changes),
            at.group_id,
            at.log_index,
            lsn,
        )
        .committed_at(at.commit_hlc)
        .publish_page(state, target.tenant_id, target.database_id)
        .await?;
        changes.next = page.next;
        changes.done = page.done;
        #[cfg(feature = "failpoints")]
        crate::control::fail_gate::after_spill(state, at.group_id, at.log_index, &collections)
            .await;
    }
    Ok(())
}

/// The bare names of the collections `row_changes` write, for the
/// collection-named fail gates.
#[cfg(feature = "failpoints")]
fn page_collections(
    target: RedoTarget,
    row_changes: &[crate::wal::RedoRowChange],
) -> std::collections::BTreeSet<String> {
    row_changes
        .iter()
        .filter_map(|change| {
            nodedb_types::CollectionKey::from_qualified_str(target.database_id, &change.collection)
                .ok()
                .map(|key| key.name().to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_types::CollectionKey;

    use super::*;
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::{NoopWalAppender, SurrogateAssigner, SurrogateWalAppender};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::{RedoChunkPlacement, WalManager};
    use crate::wal::redo::frame::push_frame;
    use crate::wal::redo::read_stream_page;
    use crate::wal::{RedoChunkHeader, RedoChunkRef, RedoSubRecord, chunk_crc};

    const IDENTITIES: u32 = 60;

    fn assigner(dir: &std::path::Path) -> SurrogateAssigner {
        let credentials =
            Arc::new(CredentialStore::open(&dir.join("system.redb")).expect("credentials"));
        let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        SurrogateAssigner::new(registry, credentials, wal)
    }

    fn identity(index: u32) -> ReplicatedIdentity {
        ReplicatedIdentity {
            collection: "orders".into(),
            pk_bytes: format!("order-{index}").into_bytes(),
            surrogate: 1_000 + index,
        }
    }

    /// Content with a header, every identity, and a few sub-records.
    fn content() -> Vec<u8> {
        let header = RedoContentHeader {
            version: 1,
            calvin_stamp: None,
            cross_shard_applied: None,
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        };
        let mut out = Vec::new();
        push_frame(&mut out, RedoFrameTag::Header, &header).expect("header");
        for index in 0..IDENTITIES {
            push_frame(&mut out, RedoFrameTag::Identity, &identity(index)).expect("identity");
        }
        for byte in 0..4u8 {
            let op = RedoSubRecord {
                record_type: 7,
                payload: vec![byte; 40],
            };
            push_frame(&mut out, RedoFrameTag::Op, &op).expect("op");
        }
        out
    }

    fn append(wal: &WalManager, content: &[u8]) -> RedoStreamRef {
        let stream = RedoStreamId::Session {
            vshard: 0,
            idempotency_key: 12,
        };
        let placement = RedoChunkPlacement {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
        };
        let mut chunks = Vec::new();
        for (index, bytes) in (0u32..).zip(content.chunks(150)) {
            let header = RedoChunkHeader {
                stream,
                group_id: 1,
                term: 1,
                len: content.len() as u64,
                index,
            };
            let lsns = wal
                .append_redo_chunk(1 + u64::from(index), placement, header, bytes)
                .expect("chunk");
            chunks.push(RedoChunkRef {
                index,
                first_lsn: lsns.first.as_u64(),
                last_lsn: lsns.last.as_u64(),
                len: bytes.len() as u64,
                crc: chunk_crc(bytes),
            });
        }
        wal.sync().expect("sync");
        RedoStreamRef {
            stream,
            len: content.len() as u64,
            chunks,
            publish_position: None,
            entry: None,
            stamp_floor: None,
        }
    }

    /// Binding the identities one page at a time leaves the catalog as one
    /// bind of every identity leaves it.
    #[test]
    fn a_paged_bind_matches_a_whole_bind() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append(&wal, &content());
        let tenant = TenantId::new(1);
        let database = DatabaseId::DEFAULT;

        let paged_dir = tempfile::tempdir().expect("tempdir");
        let paged = assigner(paged_dir.path());
        let mut header = None;
        let mut from = 0;
        let mut pages = 0;
        loop {
            let page = read_stream_page::<nodedb_wal::LazyWalReader>(
                &wal.read_source(),
                reference.clone(),
                RedoStreamSection::Identities,
                from,
                256,
            )
            .expect("page");
            let identities =
                page_identities(reference.stream, &page.frames, &mut header).expect("identities");
            if !identities.is_empty() {
                bind_carried_identities(&paged, database, tenant, &identities).expect("bind");
            }
            pages += 1;
            from = page.next;
            if page.done {
                break;
            }
        }
        assert!(pages > 3, "{pages} pages");
        assert!(header.is_some(), "the first page carries the header");

        let whole_dir = tempfile::tempdir().expect("tempdir");
        let whole = assigner(whole_dir.path());
        let every: Vec<CarriedIdentity> = (0..IDENTITIES)
            .map(|index| {
                let identity = identity(index);
                CarriedIdentity {
                    collection: identity.collection,
                    pk_bytes: identity.pk_bytes,
                    surrogate: Surrogate::new(identity.surrogate),
                }
            })
            .collect();
        bind_carried_identities(&whole, database, tenant, &every).expect("bind");

        let key = CollectionKey::from_bare(database, "orders");
        for index in 0..IDENTITIES {
            let pk = identity(index).pk_bytes;
            let from_pages = paged.lookup_bound(key, tenant, &pk).expect("lookup");
            let from_whole = whole.lookup_bound(key, tenant, &pk).expect("lookup");
            assert_eq!(from_pages, from_whole);
            assert_eq!(from_pages, Some(Surrogate::new(1_000 + index)));
        }
    }

    /// A page whose first record is not the header is refused.
    #[test]
    fn an_identity_page_without_the_header_first_is_refused() {
        let mut frames = Vec::new();
        push_frame(&mut frames, RedoFrameTag::Identity, &identity(0)).expect("identity");
        let stream = RedoStreamId::Session {
            vshard: 0,
            idempotency_key: 1,
        };
        assert!(page_identities(stream, &frames, &mut None).is_err());
    }
}
