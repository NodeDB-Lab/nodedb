// SPDX-License-Identifier: BUSL-1.1

//! The change events of an installed chunked redo.
//!
//! A chunked redo installs as its stream's marker, and its row changes stay
//! in the stream's content. Once the install applied, the apply loop reads
//! them back page by page and stages each page's events under the entry, as
//! the write funnel stages an inline redo's. The entry's settle publishes
//! them in log order. The stream's floor hold keeps its chunks until then.
//!
//! - A page read that fails is retried with backoff, from the page that
//!   failed. The chunks are durable in the WAL, so a read error is a
//!   transient or infrastructure fault.
//! - A read that fails on every retry holds the entry's group at the entry
//!   until a snapshot of the group installs. The entry never settles without
//!   its events.
//! - A crash after the install and before the group's applied floor passes
//!   the entry loses the staged events. Boot keeps the marker (see
//!   `transaction_redo::chunks::unstaged`). The entry is delivered again,
//!   the proposal ledger skips its install, and its events stage again here
//!   at the same positions.

use std::time::Duration;

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::wal_replication::transaction_redo::chunks::RecoveredMarker;
use crate::control::wal_replication::transaction_redo::{
    ChangesAt, RedoTarget, StreamedChanges, stage_streamed_changes,
};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

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
/// marker record's LSN. A failed read is retried with backoff from the page
/// that failed. Returns the last error once every attempt failed.
pub(super) async fn stage_marker_changes(
    state: &SharedState,
    target: RedoTarget,
    pos: AppliedPosition,
    changes: &mut StreamedChanges,
    lsn: Lsn,
) -> crate::Result<()> {
    let at = ChangesAt {
        group_id: pos.group_id,
        log_index: pos.log_index,
        commit_hlc: pos
            .carried_commit_hlc()
            .unwrap_or_else(|| state.hlc_clock.now().wall_ns),
    };
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
            group_id = pos.group_id,
            log_index = pos.log_index,
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

/// Stage again the change events of `replicated`, an entry delivered again
/// after a restart whose first apply installed `marker`. The entry concludes
/// as `repeat` once they staged. The marker's floor hold keeps its chunks
/// until then.
pub(super) fn restage_recovered<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    replicated: &ReplicatedEntry,
    marker: RecoveredMarker,
    repeat: EntryOutcome,
) -> Prepared<'a> {
    let target = RedoTarget {
        tenant_id: TenantId::new(replicated.tenant_id),
        database_id: DatabaseId::new(replicated.database_id),
        vshard_id: VShardId::new(replicated.vshard_id),
        system_ordinal: replicated.carried_system_ordinal(),
    };
    let mut changes = match StreamedChanges::of_reference(&marker.reference) {
        Ok(changes) => changes,
        Err(error) => return Prepared::Concluded(changes_unstaged(pos, &error)),
    };
    Prepared::Exclusive(Box::pin(async move {
        let staged = stage_marker_changes(ctx.state, target, pos, &mut changes, marker.lsn).await;
        let outcome = match staged {
            Ok(()) => {
                ctx.state
                    .redo_chunks
                    .release_unstaged(pos.group_id, pos.log_index);
                repeat
            }
            // The marker stays kept: the floor that covers the install
            // releases it.
            Err(error) => changes_unstaged(pos, &error),
        };
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}
