// SPDX-License-Identifier: BUSL-1.1

//! Replay of a cursor the bounded ring no longer serves.
//!
//! The ring evicts its oldest events as new ones arrive. A cursor whose
//! next event the ring evicted catches up from what this node holds
//! outside the ring:
//!
//! - the journal's rows, counted ones and the spilled pages of settled
//!   entries;
//! - the settled events pending for the journal, which the next floor save
//!   journals.
//!
//! Every event the ring evicted is in one of them: a settled event is
//! spilled or pending before the ring takes it, and a forwarded run is
//! journaled before the ring takes it. The journal's retention bounds the
//! catch-up: a cursor below the journal's `after` expires.
//!
//! One replay serves at most [`CATCH_UP_EVENTS`] events and reports
//! `has_more`, so a catch-up holds a bounded page in memory. Once every
//! lagging partition reached the ring, the replay goes on from the ring.

use std::sync::Arc;

use crate::control::change_stream::journal::ChangeJournal;
use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::types::{DatabaseId, TenantId};

use super::bus::ChangeStream;
use super::ring::{Lag, ReplayError, ReplayScope, ReplaySnapshot, ReplayStart, ReplayedChange};
use super::{ChangeCursor, ChangePartition, SequencedChangeEvent};

/// Events one catch-up replay serves at most.
pub const CATCH_UP_EVENTS: usize = 10_000;

/// What one replay page asks for.
#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: Option<String>,
    pub start: ReplayStart,
    pub limit: usize,
}

/// Run `request` on the blocking pool: a catch-up reads the change journal
/// from disk, and an async handler never blocks its reactor on it.
///
/// The outer error is the blocking task's own failure: it was cancelled at
/// runtime shutdown. A panic in the task resumes in the caller.
pub async fn replay_blocking(
    shared: &Arc<SharedState>,
    request: ReplayRequest,
) -> crate::Result<Result<ReplaySnapshot, ReplayError>> {
    let shared = Arc::clone(shared);
    let task = tokio::task::spawn_blocking(move || {
        shared.change_stream.query_changes_in_database(
            request.tenant_id,
            request.database_id,
            request.collection.as_deref(),
            request.start,
            request.limit,
        )
    });
    match task.await {
        Ok(replay) => Ok(replay),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(crate::Error::Internal {
            detail: format!("the change-feed replay task did not finish: {error}"),
        }),
    }
}

impl ChangeStream {
    /// Replay the events in `scope` from `start`, at most `limit`: from the
    /// ring, after a catch-up of every partition the ring evicted unconsumed
    /// events of.
    pub(super) fn replay(
        &self,
        scope: ReplayScope<'_>,
        start: &ReplayStart,
        limit: usize,
    ) -> Result<ReplaySnapshot, ReplayError> {
        let state = self.lock();
        let ReplayStart::Cursor(cursor) = start else {
            return state.ring.replay(scope, start, limit);
        };
        let lagging = state.ring.lagging(cursor)?;
        if lagging.is_empty() {
            return state.ring.replay(scope, start, limit);
        }
        drop(state);
        let journal = self.journal.get().ok_or(ReplayError::Expired)?;
        let limit = limit.min(CATCH_UP_EVENTS);
        let mut cursor = cursor.clone();
        let mut events = Vec::new();
        for lag in lagging {
            let remaining = limit - events.len();
            let reached = if remaining == 0 {
                lag.from
            } else {
                self.catch_up(journal, scope, lag, remaining, &mut cursor, &mut events)?
            };
            if reached < lag.to {
                return Ok(ReplaySnapshot {
                    events,
                    cursor,
                    has_more: true,
                });
            }
        }
        let remaining = limit - events.len();
        if remaining == 0 {
            return Ok(ReplaySnapshot {
                events,
                cursor,
                has_more: true,
            });
        }
        let rest = self
            .lock()
            .ring
            .replay(scope, &ReplayStart::Cursor(cursor.clone()), remaining);
        match rest {
            Ok(rest) => {
                events.extend(rest.events);
                Ok(ReplaySnapshot {
                    events,
                    cursor: rest.cursor,
                    has_more: rest.has_more,
                })
            }
            // The ring evicted past the cursor meanwhile, or a hole opened:
            // the next replay catches up again, or expires.
            Err(ReplayError::Expired) => Ok(ReplaySnapshot {
                events,
                cursor,
                has_more: true,
            }),
        }
    }

    /// Serve `lag` from the pending events and the journal, at most `limit`
    /// events in scope, raising `cursor` past each. Returns the position up
    /// to which `cursor` now covers the partition.
    fn catch_up(
        &self,
        journal: &ChangeJournal,
        scope: ReplayScope<'_>,
        lag: Lag,
        limit: usize,
        cursor: &mut ChangeCursor,
        events: &mut Vec<ReplayedChange>,
    ) -> Result<CdcOffset, ReplayError> {
        let Lag {
            partition,
            from,
            to,
            floor,
        } = lag;
        // Pending first: an event the journal takes meanwhile is in the
        // journal read below.
        let (pending, pending_through) = self.pending_in(partition, scope, from, to, limit);
        let read = journal
            .read(partition, from, to, Some(scope), limit)
            .map_err(|error| {
                tracing::error!(
                    ?partition,
                    %error,
                    "a catch-up read of the change journal failed; the cursor resets"
                );
                ReplayError::Expired
            })?;
        if read.after.is_some_and(|after| from < after) {
            return Err(ReplayError::Expired);
        }
        let mut through = pending_through.min(read.scanned_through);
        let journaled = read
            .changes
            .into_iter()
            .map(|change| {
                SequencedChangeEvent::new(
                    partition,
                    change.position,
                    floor,
                    change.database_id,
                    change.event,
                )
            })
            .collect();
        let mut merged = merge(pending, journaled);
        merged.retain(|event| event.position() <= through);
        if merged.len() >= limit {
            merged.truncate(limit);
            if let Some(last) = merged.last() {
                through = last.position();
            }
        }
        for event in merged {
            cursor.raise(partition, event.position());
            events.push(ReplayedChange {
                event,
                cursor: cursor.clone(),
            });
        }
        // Past every event the read passed over, in scope or not.
        cursor.raise(partition, through);
        Ok(through)
    }

    /// The pending events of `partition` in `(from, to]` in scope, at most
    /// `limit`, and the position up to which they are every such event.
    fn pending_in(
        &self,
        partition: ChangePartition,
        scope: ReplayScope<'_>,
        from: CdcOffset,
        to: CdcOffset,
        limit: usize,
    ) -> (Vec<SequencedChangeEvent>, CdcOffset) {
        let ChangePartition(group_id) = partition;
        let state = self.lock();
        let mut taken: Vec<SequencedChangeEvent> = Vec::new();
        let Some(pending) = state.group_pending.get(&group_id) else {
            return (taken, to);
        };
        let start = pending.partition_point(|event| event.position() <= from);
        for event in &pending[start..] {
            if event.position() > to {
                break;
            }
            if !scope.matches(event) {
                continue;
            }
            if taken.len() == limit {
                let through = taken.last().map_or(from, SequencedChangeEvent::position);
                return (taken, through);
            }
            taken.push(event.clone());
        }
        (taken, to)
    }
}

/// `left` and `right` merged in position order, each position once. Each
/// input is in position order.
fn merge(
    left: Vec<SequencedChangeEvent>,
    right: Vec<SequencedChangeEvent>,
) -> Vec<SequencedChangeEvent> {
    let mut out = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    loop {
        let next = match (left.peek(), right.peek()) {
            (Some(l), Some(r)) if l.position() < r.position() => left.next(),
            (Some(l), Some(r)) if l.position() > r.position() => right.next(),
            (Some(_), Some(_)) => {
                right.next();
                left.next()
            }
            (Some(_), None) => left.next(),
            (None, Some(_)) => right.next(),
            (None, None) => break,
        };
        if let Some(event) = next {
            out.push(event);
        }
    }
    out
}
