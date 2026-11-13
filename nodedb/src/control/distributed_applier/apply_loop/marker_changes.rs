// SPDX-License-Identifier: BUSL-1.1

//! The change events of an installed chunked redo, and of an entry a
//! restart delivers again.
//!
//! A chunked redo installs as its stream's marker, and its row changes stay
//! in the stream's content. Once the install applied, the apply loop reads
//! them back page by page and stages each page's events under the entry, as
//! the write funnel stages an inline redo's. A page past the change
//! stream's spill bound moves to the change journal as it stages, so the
//! entry holds at most one page in memory. The entry's settle publishes
//! them in log order. The stream's floor hold keeps its chunks until then,
//! and the entry's hold keeps them until its group's saved floor covers it.
//!
//! - A page read that fails is retried with backoff, from the page that
//!   failed. The chunks are durable in the WAL, so a read error is a
//!   transient or infrastructure fault.
//! - A read that fails on every retry holds the entry's group at the entry
//!   until a snapshot of the group installs. The entry never settles without
//!   its events.
//!
//! A crash after an entry applied and before its group's applied floor
//! passes it loses the events the entry staged. Boot reads them back from
//! the WAL (see `change_stream::unjournaled`). The entry is delivered again,
//! the proposal ledger skips its apply, and its events stage again here at
//! the same positions and the same LSN: its change marker's.

use std::sync::Arc;
use std::time::Duration;

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::change_stream::unjournaled::RecoveredChanges;
use crate::control::server::dispatch_utils::{PendingChanges, WriteChangeSet};
use crate::control::state::SharedState;
use crate::control::wal_replication::transaction_redo::{
    ChangesAt, RedoTarget, StreamedChanges, stage_streamed_changes,
};
use crate::types::Lsn;

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::{EntryOutcome, HoldReason};
use super::start::Prepared;

/// Reads of one stream's row changes before its entry holds.
const STAGE_ATTEMPTS: u32 = 8;

/// The wait after the first failed read. Each later wait doubles it.
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

/// The longest wait between two reads.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Stage an installed marker's change events under its entry, at `lsn`, the
/// LSN of the entry's change marker. `None` names no marker: the install
/// applied no replicated entry, and its events have no LSN to name. A failed
/// read is retried with backoff from the page that failed. Returns the last
/// error once every attempt failed.
pub(super) async fn stage_marker_changes(
    state: &Arc<SharedState>,
    target: RedoTarget,
    pos: AppliedPosition,
    changes: &mut StreamedChanges,
    lsn: Option<Lsn>,
) -> crate::Result<()> {
    let lsn = lsn.ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "internal invariant break: the chunked redo install of group {} entry {} \
             appended no change marker",
            pos.group_id, pos.log_index
        ),
    })?;
    let at = ChangesAt {
        group_id: pos.group_id,
        log_index: pos.log_index,
        commit_hlc: pos
            .carried_commit_hlc()
            .unwrap_or_else(|| state.hlc_clock.now().wall_ns),
    };
    stage_with_retry(state, target, at, changes, lsn).await
}

/// Stage the stream's row changes at `at`, at `lsn`. The stream's chunks
/// stay in the WAL until the entry's group saves a floor that covers it.
async fn stage_with_retry(
    state: &Arc<SharedState>,
    target: RedoTarget,
    at: ChangesAt,
    changes: &mut StreamedChanges,
    lsn: Lsn,
) -> crate::Result<()> {
    if let Some(first) = changes.first_chunk_lsn() {
        state.change_stream.unjournaled().hold_from(
            at.group_id,
            at.log_index,
            state.wal.floor_holds().hold(first),
        );
    }
    let mut backoff = FIRST_BACKOFF;
    let mut attempt = 1;
    loop {
        let error = match stage_streamed_changes(state, target, changes, at, lsn).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if attempt >= STAGE_ATTEMPTS {
            return Err(error);
        }
        tracing::warn!(
            group_id = at.group_id,
            log_index = at.log_index,
            attempt,
            %error,
            "an installed chunked redo's row changes did not read back; the read is retried"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
        attempt += 1;
    }
}

/// The outcome of an entry whose change events did not stage after every
/// retry: it holds its group's lane until a snapshot of the group installs.
pub(super) fn changes_unstaged(pos: AppliedPosition, error: &crate::Error) -> EntryOutcome {
    tracing::error!(
        group_id = pos.group_id,
        log_index = pos.log_index,
        %error,
        "an installed chunked redo's row changes did not read back after every retry; the \
         entry holds its group's apply until a snapshot of the group installs"
    );
    crate::diag::redo_changes_unstaged(pos.group_id, pos.log_index, error);
    EntryOutcome::Held {
        reason: HoldReason::ChangesUnstaged,
    }
}

/// Stage again the change events of the entry at `pos`, delivered again
/// after a restart, from what boot read for it: the rows its change marker
/// names, then the row changes of its stream when it installed a chunked
/// redo. The entry concludes as `repeat` once they staged.
///
/// Every event names the marker's LSN and the commit HLC the marker carries,
/// and takes the next ordinal under the entry, as the first apply's did. The
/// change ring drops a position it already holds, so an event the journal
/// kept publishes once.
pub(super) fn restage_recovered<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    recovered: RecoveredChanges,
    repeat: EntryOutcome,
) -> Prepared<'a> {
    let RecoveredChanges {
        lsn,
        tenant_id,
        database_id,
        vshard_id,
        commit_hlc,
        rows,
        stream,
    } = recovered;
    PendingChanges::staged(
        WriteChangeSet::of_row_changes(rows),
        pos.group_id,
        pos.log_index,
        lsn,
    )
    .committed_at(commit_hlc)
    .publish(ctx.state, tenant_id, database_id);
    let Some(reference) = stream else {
        return Prepared::Concluded(repeat);
    };
    let target = RedoTarget {
        tenant_id,
        database_id,
        vshard_id,
        system_ordinal: pos.system_ordinal,
    };
    let mut changes = match StreamedChanges::of_reference(&reference) {
        Ok(changes) => changes,
        Err(error) => return Prepared::Concluded(changes_unstaged(pos, &error)),
    };
    let at = ChangesAt {
        group_id: pos.group_id,
        log_index: pos.log_index,
        commit_hlc,
    };
    Prepared::Exclusive(Box::pin(async move {
        let outcome = match stage_with_retry(ctx.state, target, at, &mut changes, lsn).await {
            Ok(()) => repeat,
            // The entry keeps its hold: the floor that covers it, or the
            // snapshot install that releases the hold, releases it.
            Err(error) => changes_unstaged(pos, &error),
        };
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}
