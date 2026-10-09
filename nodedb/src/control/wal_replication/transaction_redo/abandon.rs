// SPDX-License-Identifier: BUSL-1.1

//! Retry the abandon of a session redo stream until the stream closes.
//!
//! A session proposes the abandon of its stream once. When that proposal
//! fails, the stream joins the store's abandon queue. One task per node
//! retries each queued abandon while the stream is open on this node.
//!
//! The stream closes when its abandon or final entry applies, when its group
//! applies an entry of a later term, or when this node leaves the group. So
//! the proposer's term bounds the retries, not a clock. A stream that opens
//! here only after its abandon left the queue closes at its group's next
//! term.
//!
//! The task stops only at shutdown. Each queued stream still open then gets
//! a capture.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;
use crate::control::wal_replication::encode::{RedoEntryTarget, redo_abandon_entry};
use crate::control::wal_replication::propose_replicated_entry;
use crate::wal::RedoStreamId;

/// How long one abandon proposal waits for its apply on this node.
const ATTEMPT_BUDGET: Duration = Duration::from_secs(5);

/// Wait before the first retry. Each later wait doubles it, up to
/// [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Propose the abandon of `stream` to `target`'s data group, and wait until
/// it applied on this node.
pub(super) async fn propose_abandon(
    state: &SharedState,
    target: RedoEntryTarget,
    stream: RedoStreamId,
) -> crate::Result<()> {
    let proposer = state.async_raft_proposer()?;
    let entry = redo_abandon_entry(target, stream);
    let deadline = tokio::time::Instant::now() + ATTEMPT_BUDGET;
    propose_replicated_entry(state, proposer, entry, deadline)
        .await
        .map(|_| ())
}

/// Spawn the task that retries `shared`'s queued abandons. `start_raft` calls
/// it once the async raft proposer is installed. A second call spawns
/// nothing.
pub(crate) fn spawn_redo_abandoner(shared: &Arc<SharedState>) {
    if !shared.redo_chunks.claim_abandoner() {
        tracing::warn!("redo abandoner already running; start_raft appears to have run twice");
        return;
    }
    let weak = Arc::downgrade(shared);
    let wake = shared.redo_chunks.abandon_wake();
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "redo_abandoner",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            // The last error of each queued abandon that has not applied.
            let mut failed: BTreeMap<RedoStreamId, crate::Error> = BTreeMap::new();
            let mut backoff = FIRST_BACKOFF;
            loop {
                let Some(state) = weak.upgrade() else {
                    return;
                };
                let due = state.redo_chunks.due_abandons();
                failed.retain(|stream, _| due.iter().any(|abandon| abandon.stream == *stream));
                for abandon in &due {
                    tokio::select! {
                        biased;
                        _ = shutdown.wait_cancelled() => {
                            give_up(&weak, &failed);
                            return;
                        }
                        result = propose_abandon(&state, abandon.target, abandon.stream) => {
                            match result {
                                Ok(()) => {
                                    state.redo_chunks.settle_abandon(&abandon.stream);
                                    failed.remove(&abandon.stream);
                                }
                                Err(error) => {
                                    tracing::debug!(
                                        stream = ?abandon.stream,
                                        %error,
                                        "redo stream abandon did not apply; retrying"
                                    );
                                    failed.insert(abandon.stream, error);
                                }
                            }
                        }
                    }
                }
                drop(state);
                if failed.is_empty() {
                    backoff = FIRST_BACKOFF;
                    tokio::select! {
                        biased;
                        _ = shutdown.wait_cancelled() => {
                            give_up(&weak, &failed);
                            return;
                        }
                        _ = wake.notified() => {}
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = shutdown.wait_cancelled() => {
                            give_up(&weak, &failed);
                            return;
                        }
                        _ = tokio::time::sleep(backoff) => {}
                        _ = wake.notified() => {}
                    }
                    backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                }
            }
        },
    );
}

/// Record every queued abandon whose stream is still open: no retry runs
/// after this, so the stream closes only at its group's next term.
fn give_up(state: &Weak<SharedState>, failed: &BTreeMap<RedoStreamId, crate::Error>) {
    let Some(state) = state.upgrade() else {
        return;
    };
    for abandon in state.redo_chunks.due_abandons() {
        tracing::warn!(
            stream = ?abandon.stream,
            group_id = abandon.group_id,
            term = abandon.term,
            "redo stream abandon given up at shutdown; the stream closes at its group's next term"
        );
        crate::diag::redo_abandon_given_up(&abandon, failed.get(&abandon.stream));
    }
}
