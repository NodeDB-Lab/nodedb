// SPDX-License-Identifier: BUSL-1.1

//! Apply path for the entries of a chunked redo stream: a `RedoChunk`, a
//! `RedoAbandon`, and the final `TransactionRedo` entry that names the
//! stream.
//!
//! Every replica decides each of them from the log alone (see
//! `wal_replication::transaction_redo::chunks`). A chunk and an abandon
//! apply with nothing else of their group in flight, so the stream a later
//! entry reads is the one every earlier entry left. A refusal applied
//! nothing on any replica, so it advances the durable prefix like a success.
//!
//! A group whose replica here owes a snapshot install is the exception (see
//! `chunks::owed`). This node dropped streams of it that the other replicas
//! still hold. A refusal there can be this replica's alone, so it holds the
//! group's durable floor until the snapshot installs.

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::control::wal_replication::decode::ChunkedRedo;
use crate::control::wal_replication::transaction_redo::RedoTarget;
use crate::control::wal_replication::transaction_redo::chunks::{ChunkApply, RedoChunkError};
use crate::control::wal_replication::types::RedoContent;
use crate::control::wal_replication::{CollectionIncarnation, ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::RedoStreamId;
use crate::wal::manager::RedoChunkPlacement;

use super::calvin_redo::{ClaimOutcome, claim_chunked};
use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;
use super::transaction_redo::{
    RedoApply, conclude_held, conclude_held_claimed, conclude_installed_copy, enqueue_payload,
    report_then_conclude,
};

/// Prepare a committed `RedoChunk` or `RedoAbandon` entry of term `term`.
pub(super) fn prepare_stream_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    term: u64,
    entry: ReplicatedEntry,
) -> Prepared<'a> {
    let placement = RedoChunkPlacement {
        tenant_id: TenantId::new(entry.tenant_id),
        vshard_id: VShardId::new(entry.vshard_id),
        database_id: DatabaseId::new(entry.database_id),
    };
    match entry.write {
        ReplicatedWrite::RedoChunk {
            stream,
            index,
            len,
            bytes,
        } => {
            let chunk = ChunkApply {
                group_id: pos.group_id,
                term,
                apply_key: pos.applied_key,
                placement,
                stream,
                index,
                len,
                bytes,
            };
            Prepared::Exclusive(Box::pin(async move {
                let applied = ctx.state.redo_chunks.apply_chunk(chunk).await;
                // The chunk's records are durable here, its entry not yet
                // concluded.
                crate::fail_point!(
                    crate::fail_point::FailScope::Node(ctx.state.node_id),
                    "redo_chunk::after_chunk_durable"
                );
                finished(ctx, pos, stream, applied)
            }))
        }
        ReplicatedWrite::RedoAbandon { stream } => Prepared::Exclusive(Box::pin(async move {
            let abandoned = ctx
                .state
                .redo_chunks
                .abandon(stream, pos.applied_key, placement)
                .await;
            finished(ctx, pos, stream, abandoned)
        })),
        other => conclude_held(
            ctx,
            pos,
            crate::Error::Internal {
                detail: format!("redo stream apply called with another write: {other:?}"),
            },
            None,
        ),
    }
}

/// Resolve a chunk or abandon entry of `stream`'s waiter and conclude it.
fn finished(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    stream: RedoStreamId,
    applied: Result<(), RedoChunkError>,
) -> FinishedApply {
    let outcome = match applied {
        Err(refusal)
            if refusal.is_refusal() && ctx.state.redo_chunks.owes_snapshot(pos.group_id) =>
        {
            let error = stream_lost_here(pos, stream, LostEntry::Chunk, &refusal);
            held(ctx.tracker, pos, error)
        }
        applied => concluded(ctx.tracker, pos, applied),
    };
    FinishedApply {
        group_id: pos.group_id,
        log_index: pos.log_index,
        outcome,
    }
}

/// Conclude a chunk or abandon entry by its stream's verdict: a success or
/// a refusal passes the entry, a storage error holds it.
fn concluded(
    tracker: &ProposeTracker,
    pos: AppliedPosition,
    applied: Result<(), RedoChunkError>,
) -> EntryOutcome {
    let durable = applied
        .as_ref()
        .err()
        .is_none_or(RedoChunkError::is_refusal);
    let result = applied
        .map(|()| AppliedWrite::unversioned(Vec::new()))
        .map_err(crate::Error::from);
    let outcome = EntryOutcome::Applied {
        durable,
        result: Some(ledger_outcome(&result)),
    };
    tracker.complete(pos.group_id, pos.log_index, pos.applied_key, result);
    outcome
}

/// Hold a chunk entry this replica cannot decide as the other replicas do.
/// No ledger outcome records it, so a re-delivery applies it again.
fn held(tracker: &ProposeTracker, pos: AppliedPosition, error: crate::Error) -> EntryOutcome {
    tracker.complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
    EntryOutcome::Applied {
        durable: false,
        result: None,
    }
}

/// Which entry of a stream refused in a group that owes a snapshot.
#[derive(Debug, Clone, Copy)]
enum LostEntry {
    Chunk,
    Final,
}

impl LostEntry {
    fn as_str(self) -> &'static str {
        match self {
            Self::Chunk => "chunk",
            Self::Final => "final",
        }
    }
}

/// Report a refusal of `stream`'s `entry` at `pos` on a replica that owes
/// its group a snapshot install. Returns the error the entry's waiter gets.
fn stream_lost_here(
    pos: AppliedPosition,
    stream: RedoStreamId,
    entry: LostEntry,
    refusal: &RedoChunkError,
) -> crate::Error {
    tracing::error!(
        group_id = pos.group_id,
        log_index = pos.log_index,
        ?stream,
        entry = entry.as_str(),
        %refusal,
        "redo stream: this replica dropped streams of the group when it left it; the entry \
         holds until a snapshot of the group installs"
    );
    crate::diag::redo_stream_lost_here(
        pos.group_id,
        pos.log_index,
        stream,
        entry.as_str(),
        refusal,
    );
    crate::Error::Internal {
        detail: format!(
            "group {}: entry {} of redo stream {stream:?} is held on this replica until a \
             snapshot of the group installs: {refusal}",
            pos.group_id, pos.log_index
        ),
    }
}

/// Prepare the final entry of a chunked redo: take its stream, assemble the
/// content, and apply it as an inline redo. A stream that is gone or that
/// holds other chunks than the entry names refuses the entry. In a group
/// whose replica here owes a snapshot install, it holds the entry instead.
pub(super) fn prepare_chunked_final<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    target: RedoTarget,
    chunked: ChunkedRedo,
    incarnations: Vec<CollectionIncarnation>,
) -> Prepared<'a> {
    let claim = claim_chunked(ctx.state, chunked.calvin(), &chunked.stream);
    let taken = ctx.state.redo_chunks.take_for_final(&chunked.stream);
    let calvin = match claim {
        ClaimOutcome::NotCalvin => None,
        ClaimOutcome::Claimed(claim) => Some(claim),
        // Another copy of the position installed, or installs now: this
        // copy's stream drops.
        ClaimOutcome::Refused => return conclude_installed_copy(ctx, pos, taken),
        ClaimOutcome::Malformed(error) => return conclude_held(ctx, pos, error, taken),
    };
    // Every chunk is durable and the final entry committed; nothing of the
    // final entry applied yet.
    crate::fail_point!(
        crate::fail_point::FailScope::Node(ctx.state.node_id),
        "redo_chunk::before_final_install"
    );
    let assembled = match &taken {
        Some(open) => open.assemble(chunked.count, chunked.len),
        None => Err(RedoChunkError::NotOpen {
            stream: chunked.stream,
        }),
    };
    let bytes = match assembled {
        Ok(bytes) => bytes,
        Err(refusal) if ctx.state.redo_chunks.owes_snapshot(pos.group_id) => {
            let error = stream_lost_here(pos, chunked.stream, LostEntry::Final, &refusal);
            return match calvin {
                Some(claim) => conclude_held_claimed(ctx, pos, error, taken, claim),
                None => conclude_held(ctx, pos, error, taken),
            };
        }
        Err(refusal) => {
            // Every replica refuses the final entry alike. The claim stays,
            // and the scheduler hears the refusal once the entry starts.
            let result = Err(crate::Error::from(refusal));
            let outcome = EntryOutcome::Applied {
                durable: true,
                result: Some(ledger_outcome(&result)),
            };
            ctx.tracker
                .complete(pos.group_id, pos.log_index, pos.applied_key, result);
            return match calvin {
                Some(claim) => {
                    let event =
                        crate::control::cluster::calvin::scheduler::CalvinApplyEvent::RedoRefused {
                            error: "the final entry's chunk stream does not hold the chunks it \
                                    names"
                                .into(),
                        };
                    report_then_conclude(ctx, pos, claim, event, outcome)
                }
                None => Prepared::Concluded(outcome),
            };
        }
    };
    match RedoContent::from_bytes(&bytes) {
        Ok(content) => Prepared::Enqueue(enqueue_payload(
            ctx,
            pos,
            target,
            RedoApply {
                payload: chunked.payload(content),
                incarnations,
                stream: taken,
                calvin,
            },
        )),
        Err(error) => match calvin {
            Some(claim) => conclude_held_claimed(ctx, pos, error, taken, claim),
            None => conclude_held(ctx, pos, error, taken),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::RedoOrigin;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::state::SharedState;
    use crate::control::wal_replication::decode::{
        DecodedTransactionRedo, decode_transaction_redo,
    };
    use crate::control::wal_replication::types::{RedoBody, ReplicatedEventSource};
    use crate::wal::WalManager;

    const GROUP: u64 = 4;

    fn stream() -> RedoStreamId {
        RedoStreamId::Session {
            vshard: 2,
            idempotency_key: 77,
        }
    }

    fn pos(group_id: u64, log_index: u64) -> AppliedPosition {
        AppliedPosition {
            group_id,
            log_index,
            applied_key: 1_000 + log_index,
            commit_hlc: 0,
        }
    }

    fn chunk_entry(index: u32, bytes: &[u8]) -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            0,
            2,
            ReplicatedWrite::RedoChunk {
                stream: stream(),
                index,
                len: 4,
                bytes: bytes.to_vec(),
            },
        )
    }

    fn final_entry() -> ChunkedRedo {
        let write = ReplicatedWrite::TransactionRedo {
            body: RedoBody::Chunked {
                stream: stream(),
                count: 2,
                len: 4,
            },
            collections: Vec::new(),
            event_source: ReplicatedEventSource::User,
            origin: RedoOrigin::Commit,
            calvin: None,
        };
        match decode_transaction_redo(&write).expect("decode") {
            DecodedTransactionRedo::Chunked(chunked) => chunked,
            DecodedTransactionRedo::Inline(_) => panic!("a chunked body decodes chunked"),
        }
    }

    fn target() -> RedoTarget {
        RedoTarget {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(2),
        }
    }

    async fn apply_chunk(ctx: ApplyContext<'_>, pos: AppliedPosition, index: u32) -> EntryOutcome {
        let Prepared::Exclusive(apply) =
            prepare_stream_entry(ctx, pos, 3, chunk_entry(index, b"ab"))
        else {
            panic!("a chunk entry applies exclusive");
        };
        apply.await.outcome
    }

    fn holds(outcome: &EntryOutcome) -> bool {
        matches!(
            outcome,
            EntryOutcome::Applied {
                durable: false,
                result: None
            }
        )
    }

    /// This node applied a stream's first chunk, left the group, and
    /// mounted it again on the group's old log. The next chunk and the
    /// final entry refuse here while every other replica installs the
    /// redo. Each holds the group's durable floor instead of passing.
    #[tokio::test]
    async fn a_final_entry_of_a_stream_this_replica_dropped_holds_the_apply() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };

        let first = apply_chunk(ctx, pos(GROUP, 10), 0).await;
        assert!(matches!(first, EntryOutcome::Applied { durable: true, .. }));
        state
            .redo_chunks
            .drop_unhosted_groups(|_| false, state.credentials.catalog())
            .expect("leave the group");
        assert!(state.redo_chunks.owes_snapshot(GROUP));

        let second = apply_chunk(ctx, pos(GROUP, 11), 1).await;
        assert!(holds(&second), "the dropped stream's next chunk holds");

        let Prepared::Concluded(last) =
            prepare_chunked_final(ctx, pos(GROUP, 12), target(), final_entry(), Vec::new())
        else {
            panic!("a final entry with no stream concludes at prepare");
        };
        assert!(holds(&last), "the dropped stream's final entry holds");
    }

    /// In a group that owes no snapshot, every replica refuses a final entry
    /// with no stream alike, and the refusal passes the entry.
    #[tokio::test]
    async fn a_final_entry_with_no_stream_passes_in_a_group_that_owes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };

        let Prepared::Concluded(outcome) =
            prepare_chunked_final(ctx, pos(GROUP, 12), target(), final_entry(), Vec::new())
        else {
            panic!("a final entry with no stream concludes at prepare");
        };
        assert!(matches!(
            outcome,
            EntryOutcome::Applied {
                durable: true,
                result: Some(_)
            }
        ));
    }
}
