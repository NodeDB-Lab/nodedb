// SPDX-License-Identifier: BUSL-1.1

//! Propose a committed session redo through its vShard's data-group log.
//!
//! A redo whose entry fits `tuning.calvin.max_redo_entry_bytes` travels as
//! one entry. A larger redo travels as chunk entries, proposed in order, each
//! applied on this node before the next, and then a final entry that names
//! their stream. A proposal that stops before its final entry applies
//! proposes an abandon, so every replica drops the stream's chunks. A failed
//! abandon proposal goes to the abandoner, which retries it while the stream
//! is open (see [`super::abandon`]).

use crate::control::state::SharedState;
use crate::control::wal_replication::encode::{
    RedoEntryTarget, RedoProposal, session_redo_proposal,
};
use crate::control::wal_replication::propose_replicated_entry;
use crate::types::Lsn;
use crate::wal::RedoStreamId;

use super::abandon::propose_abandon;
use super::apply::RedoTarget;
use super::payload::TransactionRedoPayload;

/// Propose `payload` to `target`'s data group and wait until it applied on
/// this node. Returns what [`propose_replicated_entry`] returns for the entry
/// that installs the redo.
pub(crate) async fn propose_transaction_redo(
    state: &SharedState,
    target: RedoTarget,
    payload: &TransactionRedoPayload,
    deadline: tokio::time::Instant,
) -> crate::Result<(Vec<u8>, Lsn)> {
    let proposer = state.async_raft_proposer()?;
    let entry_target = RedoEntryTarget {
        tenant_id: target.tenant_id,
        database_id: target.database_id,
        vshard_id: target.vshard_id,
    };
    let proposal = session_redo_proposal(
        entry_target,
        payload,
        state.redo_chunks.limits().max_entry_bytes,
    )?;
    let (stream, chunks, last) = match proposal {
        RedoProposal::Inline(entry) => {
            return propose_replicated_entry(state, proposer, entry, deadline).await;
        }
        RedoProposal::Chunked {
            stream,
            chunks,
            last,
        } => (stream, chunks, last),
    };
    for chunk in chunks {
        if let Err(error) = propose_replicated_entry(state, proposer, chunk, deadline).await {
            abandon(state, entry_target, stream).await;
            return Err(error);
        }
    }
    let installed = propose_replicated_entry(state, proposer, last, deadline).await;
    if installed.is_err() {
        abandon(state, entry_target, stream).await;
    }
    installed
}

/// Propose the abandon of `stream` once. A stream the final entry already
/// took is gone, so the abandon then drops nothing. A failed proposal queues
/// the abandon for the abandoner, so the session's error returns now.
async fn abandon(state: &SharedState, target: RedoEntryTarget, stream: RedoStreamId) {
    if let Err(error) = propose_abandon(state, target, stream).await {
        tracing::warn!(
            ?stream,
            %error,
            "redo stream abandon did not apply; the abandoner retries it while the stream is open"
        );
        state.redo_chunks.queue_abandon(stream, target);
    }
}
