// SPDX-License-Identifier: BUSL-1.1

//! Propose a session redo whose framed content arrives in parts.
//!
//! The commit declares the content's byte length before the first chunk,
//! then pushes the parts in order. Each full chunk is proposed and applied
//! on this node before the next one, so the proposer holds at most one
//! chunk and one part. The final entry names the stream once every chunk
//! applied. A stream that stops before its final entry applies is
//! abandoned, so every replica drops its chunks.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::control::wal_replication::encode::{
    RedoChunkSlot, RedoEntryTarget, chunk_count, chunk_entry, chunk_room, chunked_final_entry,
    final_entry_stream,
};
use crate::control::wal_replication::propose_replicated_entry;
use crate::control::wal_replication::{AppliedOutput, AsyncRaftProposer, ReplicatedEntry};
use crate::wal::RedoStreamId;

use super::apply::RedoTarget;
use super::payload::TransactionRedoPayload;
use super::propose::abandon;

/// One session redo stream in flight.
pub(crate) struct RedoStreamProposal<'s> {
    state: &'s SharedState,
    proposer: &'s Arc<AsyncRaftProposer>,
    target: RedoEntryTarget,
    stream: RedoStreamId,
    last: ReplicatedEntry,
    event_source: crate::event::EventSource,
    deadline: tokio::time::Instant,
    /// Bytes one chunk carries.
    room: usize,
    /// The declared byte length of the content.
    len: u64,
    /// The declared chunk count.
    count: u32,
    /// Pushed bytes no chunk carries yet. Less than `room` between pushes.
    held: Vec<u8>,
    next_index: u32,
    proposed: u64,
}

impl<'s> RedoStreamProposal<'s> {
    /// Open the stream of `payload`'s redo, whose framed content holds
    /// `len` bytes. `payload` carries the header fields alone.
    pub(crate) fn open(
        state: &'s SharedState,
        target: RedoTarget,
        payload: &TransactionRedoPayload,
        len: u64,
        deadline: tokio::time::Instant,
    ) -> crate::Result<Self> {
        let proposer = state.async_raft_proposer()?;
        let max_entry_bytes = state.redo_chunks.limits().max_entry_bytes;
        let target = RedoEntryTarget {
            tenant_id: target.tenant_id,
            database_id: target.database_id,
            vshard_id: target.vshard_id,
        };
        let count = chunk_count(len, max_entry_bytes)?;
        let last = chunked_final_entry(target, payload, count, len, |last| RedoStreamId::Session {
            vshard: target.vshard_id.as_u32(),
            idempotency_key: last.idempotency_key,
        });
        let stream = final_entry_stream(&last)?;
        Ok(Self {
            state,
            proposer,
            target,
            stream,
            last,
            event_source: payload.event_source,
            deadline,
            room: chunk_room(max_entry_bytes)?,
            len,
            count,
            held: Vec::new(),
            next_index: 0,
            proposed: 0,
        })
    }

    /// Append `bytes` to the content and propose every full chunk. On error
    /// the caller abandons the stream.
    pub(crate) async fn push(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.held.extend_from_slice(bytes);
        while self.held.len() >= self.room {
            let rest = self.held.split_off(self.room);
            let chunk = std::mem::replace(&mut self.held, rest);
            self.propose_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn propose_chunk(&mut self, bytes: Vec<u8>) -> crate::Result<()> {
        if self.next_index == 1 {
            crate::fail_point_err!(
                crate::fail_point::FailScope::Node(self.state.node_id),
                "redo_stream::before_second_chunk",
                |detail| crate::Error::Internal { detail }
            );
        }
        let carried = bytes.len() as u64;
        if self.proposed.saturating_add(carried) > self.len {
            return Err(self.length_error(self.proposed.saturating_add(carried)));
        }
        let entry = chunk_entry(
            self.target,
            RedoChunkSlot {
                stream: self.stream,
                index: self.next_index,
                len: self.len,
            },
            bytes,
            self.event_source,
        );
        propose_replicated_entry(self.state, self.proposer, entry, self.deadline).await?;
        self.next_index += 1;
        self.proposed += carried;
        Ok(())
    }

    fn length_error(&self, produced: u64) -> crate::Error {
        crate::Error::Internal {
            detail: format!(
                "redo stream {:?} declared {} bytes in {} chunks, and its parts hold at least \
                 {produced} bytes in {} chunks; the commit is refused",
                self.stream, self.len, self.count, self.next_index
            ),
        }
    }

    /// Propose the last partial chunk, then the final entry, and wait until
    /// the final entry applied on this node. Abandons the stream on error.
    pub(crate) async fn finish(mut self) -> crate::Result<AppliedOutput> {
        let tail = std::mem::take(&mut self.held);
        if !tail.is_empty()
            && let Err(error) = self.propose_chunk(tail).await
        {
            self.abandon().await;
            return Err(error);
        }
        if self.proposed != self.len || self.next_index != self.count {
            let error = self.length_error(self.proposed);
            self.abandon().await;
            return Err(error);
        }
        let installed =
            propose_replicated_entry(self.state, self.proposer, self.last, self.deadline).await;
        if installed.is_err() {
            abandon(self.state, self.target, self.stream).await;
        }
        installed
    }

    /// Drop the stream on every replica.
    pub(crate) async fn abandon(self) {
        abandon(self.state, self.target, self.stream).await;
    }
}
