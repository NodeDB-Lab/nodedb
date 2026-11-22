// SPDX-License-Identifier: BUSL-1.1

//! A committed Calvin slice's redo, read from the core in parts and proposed
//! as a chunk stream while it is read.
//!
//! Part `0` of the slice's resolve reports the byte length of every part, so
//! the leader knows the framed content's length before its first chunk. A
//! content that fits one entry arrives in part `0` and travels inline. A
//! larger one travels as chunk entries under a stream that names the slice's
//! position and attempt: the leader frames the header, then proposes every
//! full chunk of the parts it holds before it asks for the next part, then
//! proposes the final entry. It holds at most one chunk and one part.
//!
//! The slice's staged state stays on the core until its install consumes
//! it, and the slice resolves deterministically, so an attempt that does not
//! reach the log starts again from part `0` under a new stream. The stream
//! of an earlier attempt closes when a later term's entry applies or when
//! the position installs through another copy.

use super::super::types::CommitState;
use super::redo_propose::ProposeRefusal;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::wal_replication::encode::{
    RedoChunkSlot, chunk_count, chunk_entry, chunk_room, chunked_final_entry, chunks_complete,
    chunks_overrun, declare_final_keys, fits_inline, redo_header_frames, stamp_calvin_final,
};
use crate::control::wal_replication::types::RedoContent;
use crate::wal::{RedoStreamId, StreamKeysWalk};
use nodedb_physical::physical_plan::CalvinResolvedPart;

/// How a committed slice's redo travels.
#[derive(Debug, Clone)]
pub(in crate::control::cluster::calvin::scheduler::driver) enum OwedBody {
    /// The whole content fits one entry: the payload holds every list.
    Inline,
    /// The content travels as a chunk stream. The payload holds the header
    /// fields alone. `None` between attempts.
    Streamed(Option<Box<StreamProgress>>),
}

/// One attempt of a streamed redo, between parts.
#[derive(Debug, Clone)]
pub(in crate::control::cluster::calvin::scheduler::driver) struct StreamProgress {
    stream: RedoStreamId,
    /// The stamped final entry, which names the stream.
    last: ReplicatedEntry,
    /// Bytes one chunk carries.
    room: usize,
    /// The declared byte length of the content.
    len: u64,
    /// The declared chunk count.
    count: u32,
    /// Bytes read and not yet proposed. Less than `room` past the header
    /// once the full chunks it held are proposed.
    held: Vec<u8>,
    next_index: u32,
    proposed: u64,
    /// The part the next resolve request reads.
    next_part: u32,
    /// Whether the last part was read.
    parts_done: bool,
    /// Whether the node admitted the stream's bytes.
    admitted: bool,
    /// The keys of every record read so far. The final entry declares
    /// them.
    keys: StreamKeysWalk,
}

impl StreamProgress {
    /// The part the next resolve request of the attempt reads.
    pub(in crate::control::cluster::calvin::scheduler::driver) fn next_part(&self) -> u32 {
        self.next_part
    }
}

/// Where a stream advance stopped.
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum StreamStep {
    /// Every chunk of the parts read so far is proposed: read the next part.
    NeedPart,
    /// A crash test holds the stream before its second chunk.
    Held,
    /// The final entry is proposed at `(group_id, log_index)`.
    Final((u64, u64)),
}

impl Scheduler {
    /// Take part `0` of `txn_id`'s resolve: build the slice's owed redo and
    /// start its attempt.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn open_owed_redo(
        &mut self,
        txn_id: TxnId,
        answer: CalvinResolvedPart,
    ) -> crate::Result<()> {
        let missing = |what: &str| crate::Error::Internal {
            detail: format!(
                "calvin txn {}/{}: resolve part 0 carries no {what}",
                txn_id.epoch, txn_id.position
            ),
        };
        let reply = answer.reply.ok_or_else(|| missing("reply"))?;
        let opened = answer
            .part
            .opened
            .ok_or_else(|| missing("resolve summary"))?;
        let mut owed = self.owed_redo(txn_id, reply)?;
        let header = redo_header_frames(&owed.payload)?;
        let len = (header.len() as u64).saturating_add(opened.len);
        let max_entry_bytes = self.shared.redo_chunks.limits().max_entry_bytes;
        if fits_inline(&owed.payload, len, max_entry_bytes) {
            // Part 0 holds up to one entry's bytes, so a content that fits
            // one entry arrives whole.
            if !answer.part.done {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "calvin txn {}/{}: a resolve of {len} bytes answered more than one part",
                        txn_id.epoch, txn_id.position
                    ),
                });
            }
            let mut framed = header;
            framed.extend_from_slice(&answer.part.bytes);
            owed.payload.redo = RedoContent::from_framed(&framed)?.redo;
            owed.body = OwedBody::Inline;
        } else {
            let stream = RedoStreamId::Calvin {
                vshard: self.vshard_id,
                epoch: txn_id.epoch,
                position: txn_id.position,
                attempt: owed.attempts,
            };
            owed.attempts = owed.attempts.saturating_add(1);
            let count = chunk_count(len, max_entry_bytes)?;
            // The parts are walked as they arrive, and the final proposal
            // declares their keys.
            let last = stamp_calvin_final(
                chunked_final_entry(owed.target, &owed.payload, count, len, |_| stream),
                &owed.stamps,
            );
            let mut keys = StreamKeysWalk::new();
            keys.walk(&header)?;
            keys.walk(&answer.part.bytes)?;
            let mut held = header;
            held.extend_from_slice(&answer.part.bytes);
            owed.body = OwedBody::Streamed(Some(Box::new(StreamProgress {
                stream,
                last,
                room: chunk_room(max_entry_bytes)?,
                len,
                count,
                held,
                next_index: 0,
                proposed: 0,
                next_part: 1,
                parts_done: answer.part.done,
                admitted: false,
                keys,
            })));
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.redo = Some(owed);
            pending.commit_state = CommitState::AwaitingRedoApply { proposed: None };
        }
        self.propose_calvin_redo(txn_id);
        Ok(())
    }

    /// Take a later part of `txn_id`'s resolve and advance its stream.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn continue_owed_redo(
        &mut self,
        txn_id: TxnId,
        answer: CalvinResolvedPart,
    ) -> crate::Result<()> {
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return Err(super::commit_redo::missing_pending_error(txn_id));
        };
        let Some(OwedBody::Streamed(Some(progress))) =
            pending.redo.as_mut().map(|owed| &mut owed.body)
        else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "calvin txn {}/{}: resolve part {} arrived with no stream open",
                    txn_id.epoch, txn_id.position, answer.part.index
                ),
            });
        };
        if answer.part.index != progress.next_part {
            return Err(crate::Error::Internal {
                detail: format!(
                    "calvin txn {}/{}: resolve part {} arrived, part {} was asked for",
                    txn_id.epoch, txn_id.position, answer.part.index, progress.next_part
                ),
            });
        }
        progress.keys.walk(&answer.part.bytes)?;
        progress.held.extend_from_slice(&answer.part.bytes);
        progress.next_part += 1;
        progress.parts_done = answer.part.done;
        pending.commit_state = CommitState::AwaitingRedoApply { proposed: None };
        self.propose_calvin_redo(txn_id);
        Ok(())
    }

    /// Propose every full chunk the stream of `txn_id` holds, then its last
    /// chunk and final entry once every part is read. A refusal leaves the
    /// unproposed bytes in the stream, so the next advance goes on from
    /// them.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn advance_stream(
        &mut self,
        txn_id: TxnId,
    ) -> Result<StreamStep, ProposeRefusal> {
        let until = self.request_deadline();
        let Some(owed) = self
            .pending
            .get_mut(&txn_id)
            .and_then(|pending| pending.redo.as_mut())
        else {
            return Err(ProposeRefusal::Unbuildable(
                "the txn owes no redo".to_owned(),
            ));
        };
        let OwedBody::Streamed(slot) = &mut owed.body else {
            return Err(ProposeRefusal::Unbuildable(
                "an inline redo has no stream".to_owned(),
            ));
        };
        let Some(mut progress) = slot.take() else {
            return Err(ProposeRefusal::Unbuildable(
                "the redo has no stream open".to_owned(),
            ));
        };
        let target = owed.target;
        let event_source = owed.payload.event_source;
        let collections = owed.payload.collections.clone();
        let step = self.advance(&mut progress, target, event_source, &collections, until);
        if !matches!(step, Ok(StreamStep::Final(_)))
            && let Some(OwedBody::Streamed(slot)) = self
                .pending
                .get_mut(&txn_id)
                .and_then(|pending| pending.redo.as_mut())
                .map(|owed| &mut owed.body)
        {
            *slot = Some(progress);
        }
        step
    }

    fn advance(
        &self,
        progress: &mut StreamProgress,
        target: crate::control::wal_replication::encode::RedoEntryTarget,
        event_source: crate::event::EventSource,
        collections: &[String],
        until: std::time::Instant,
    ) -> Result<StreamStep, ProposeRefusal> {
        let unbuildable = |error: crate::Error| ProposeRefusal::Unbuildable(error.to_string());
        if !progress.admitted {
            // The leader bounds the node's open stream bytes: a stream past
            // the cap waits for the stall tick.
            self.shared
                .redo_chunks
                .admit_stream(progress.stream, progress.len, until)
                .map_err(|refusal| ProposeRefusal::Transient(refusal.to_string()))?;
            progress.admitted = true;
        }
        loop {
            let take = if progress.held.len() >= progress.room {
                progress.room
            } else if progress.parts_done && !progress.held.is_empty() {
                progress.held.len()
            } else {
                break;
            };
            if progress.next_index == 1 && self.stream_held(collections) {
                return Ok(StreamStep::Held);
            }
            let carried = take as u64;
            if chunks_overrun(progress.proposed, carried, progress.len) {
                return Err(ProposeRefusal::Unbuildable(format!(
                    "redo stream {:?} declared {} bytes, and its parts hold more",
                    progress.stream, progress.len
                )));
            }
            let entry = chunk_entry(
                target,
                RedoChunkSlot {
                    stream: progress.stream,
                    index: progress.next_index,
                    len: progress.len,
                },
                progress.held[..take].to_vec(),
                event_source,
            );
            self.propose_entries(vec![entry.encode().map_err(unbuildable)?])?;
            progress.held.drain(..take);
            progress.next_index += 1;
            progress.proposed += carried;
        }
        if !progress.parts_done {
            return Ok(StreamStep::NeedPart);
        }
        if !chunks_complete(
            progress.proposed,
            progress.next_index,
            progress.len,
            progress.count,
        ) {
            return Err(ProposeRefusal::Unbuildable(format!(
                "redo stream {:?} declared {} bytes in {} chunks, and its parts held {} bytes \
                 in {} chunks",
                progress.stream,
                progress.len,
                progress.count,
                progress.proposed,
                progress.next_index
            )));
        }
        let max_entry_bytes = self.shared.redo_chunks.limits().max_entry_bytes;
        declare_final_keys(&mut progress.last, progress.keys.keys(), max_entry_bytes)
            .map_err(unbuildable)?;
        let last = progress.last.encode().map_err(unbuildable)?;
        self.propose_entries(vec![last]).map(StreamStep::Final)
    }

    /// Whether a crash test holds the stream of a slice that writes
    /// `collections` before its second chunk. Always `false` outside the
    /// `failpoints` feature.
    fn stream_held(&self, collections: &[String]) -> bool {
        #[cfg(feature = "failpoints")]
        {
            crate::control::fail_gate::holds_redo_stream(self.shared.node_id, collections)
        }
        #[cfg(not(feature = "failpoints"))]
        {
            let _ = collections;
            false
        }
    }
}
