// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for chunked redo streams: an abandon that stopped retrying,
//! and a stream entry held on a replica that dropped the stream.

use faultbox::{Capture, EventKind, error_chain_of};

use crate::control::wal_replication::transaction_redo::chunks::{DueAbandon, RedoChunkError};
use crate::diag::context;
use crate::wal::RedoStreamId;

/// Report a session stream left open when the abandoner stopped. Called from
/// the abandoner's exit, the one site that gives the abandon up.
pub fn redo_abandon_given_up(abandon: &DueAbandon, last_error: Option<&crate::Error>) {
    let ctx = context::RedoAbandonGivenUp {
        stream: format!("{:?}", abandon.stream),
        vshard: abandon.stream.vshard(),
        group_id: abandon.group_id,
        term: abandon.term,
        len: abandon.len,
        last_error: last_error.map(ToString::to_string),
    };
    let _ = Capture::new(
        EventKind::Error,
        "redo stream abandon given up: the stream stays open until its group's next term",
    )
    .domain(&ctx)
    .emit();
}

/// Report a refusal of `stream`'s `entry` (`chunk` or `final`) at log index
/// `log_index` of `group_id`, on a replica that dropped the group's streams
/// when it left the group. The other replicas can hold the stream and
/// install its redo. Called from the data-group apply loop, the one site
/// that holds the entry.
pub fn redo_stream_lost_here(
    group_id: u64,
    log_index: u64,
    stream: RedoStreamId,
    entry: &'static str,
    refusal: &RedoChunkError,
) {
    let ctx = context::RedoStreamLostHere {
        group_id,
        log_index,
        stream: format!("{stream:?}"),
        vshard: stream.vshard(),
        entry,
        refusal: refusal.to_string(),
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "redo stream entry refused on a replica that dropped the stream: held until a snapshot \
         of the group installs",
    )
    .domain(&ctx)
    .error_chain(error_chain_of(refusal))
    .emit();
}

/// Report that the snapshot debt of data groups this node left did not
/// persist. Their streams stay, with their WAL floor holds, until a later
/// membership pass records it. Called from that pass, the one site that
/// writes the debt.
pub fn redo_snapshot_debt_not_recorded(error: &crate::Error) {
    let ctx = context::RedoSnapshotDebtNotRecorded {
        error: error.to_string(),
    };
    let _ = Capture::new(
        EventKind::Error,
        "snapshot debt of left data groups did not persist: their redo streams stay held",
    )
    .domain(&ctx)
    .error_chain(error_chain_of(error))
    .emit();
}
