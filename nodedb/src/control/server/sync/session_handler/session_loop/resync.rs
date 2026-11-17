// SPDX-License-Identifier: BUSL-1.1

//! Server-side resync of the collections whose pushes a full queue dropped.
//!
//! A delta that finds the session's queue full is dropped, and the drop
//! marks its collection. The session then sends one collection resync per
//! marked collection: a snapshot covering every document shape the session
//! holds on it, flagged so the client replaces the collection's rows with
//! it. A row deleted, or moved out of the session's view, while its push was
//! dropped leaves the client too.
//!
//! The session reads its row queue's end position before the snapshot read
//! begins. After the snapshot succeeds, the collection's pushes queued before
//! that position are skipped: each was queued after its write applied, so
//! the snapshot holds its effect, and pushed after the snapshot it moves the
//! client's row back. A push queued at or past the position goes out after
//! the snapshot, so the newest write lands last.
//!
//! The cutoff is a queue position, not an LSN. A delta's LSN is the applying
//! core's watermark, and a snapshot served by the group's leader on another
//! node reads under that node's WAL, so the two LSNs share no order.
//!
//! Every resync is a snapshot read on the session's behalf. It draws from the
//! session's snapshot bucket, at most [`MAX_RESYNCS_PER_WAKE`] run per wake,
//! and each read races the session's kill. A collection the bucket or the
//! per-wake cap has no room for, or whose snapshot did not complete, stays
//! marked, and the loop wakes again to retry it.
//!
//! A dropped definition notice marks its family the same way, and the
//! session sends the family's full current set.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tracing::{debug, info, warn};

use crate::control::security::sessions::kill_signal_ended;
use crate::control::server::sync::async_dispatch::{CollectionResyncFailure, collection_resync};
use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;
use crate::event::crdt_sync::types::DefinitionFamily;

use super::channels::{Flow, SessionChannels};
use super::send::{kill_of, send_frame, send_message};

type Ws = WebSocketStream<tokio::net::TcpStream>;

/// The most resync snapshots one wake reads. Each is a full collection scan,
/// so a session with many marked collections spreads them over wakes and
/// keeps answering its inbound frames.
const MAX_RESYNCS_PER_WAKE: usize = 4;

/// How long the loop waits before it retries a resync that did not complete
/// or that the per-wake cap had no room for.
const RESYNC_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The most resyncs a session gets within [`SLOW_CONSUMER_WINDOW`].
///
/// A resync follows a full push queue. A client that fills its queue this
/// often takes pushes slower than its collections change. Each resync costs
/// a full collection scan and leaves the client as far behind as before, so
/// the session closes. The client reconnects and catches up from its
/// handshake.
const SLOW_CONSUMER_RESYNCS: usize = 8;

/// The window [`SLOW_CONSUMER_RESYNCS`] counts in.
const SLOW_CONSUMER_WINDOW: Duration = Duration::from_secs(60);

/// The close reason a slow consumer receives, with close code 1013 (try
/// again later).
const SLOW_CONSUMER_CLOSE_REASON: &str =
    "the client takes pushes slower than its collections change; reconnect to catch up";

/// Resync the collections and definition families the session's queues
/// dropped pushes of.
pub(super) async fn resync_dropped(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
) -> Flow {
    let Some(state) = shared.as_ref() else {
        return Flow::Continue;
    };
    channels.resync_retry_at = None;
    let mut budget = MAX_RESYNCS_PER_WAKE;

    let collections = state.crdt_sync_delivery.take_resync(&session.session_id);
    let mut pending = collections.into_iter();
    while let Some(collection) = pending.next() {
        let step = resync_collection(ws, session, channels, state, &collection, &mut budget).await;
        match step {
            Step::Done => {}
            Step::Retry(retry_at) => {
                schedule_retry(channels, retry_at);
                state
                    .crdt_sync_delivery
                    .mark_resync(&session.session_id, &collection);
            }
            Step::Deferred(retry_at) => {
                requeue(state, session, channels, retry_at, &collection, pending);
                break;
            }
            Step::Close => return Flow::Break,
        }
    }

    let families = state
        .crdt_sync_delivery
        .take_admin_resync(&session.session_id);
    let mut pending = families.into_iter();
    while let Some(family) = pending.next() {
        let step = resync_family(ws, session, channels, state, &family, &mut budget).await;
        match step {
            Step::Done => {}
            Step::Retry(retry_at) => {
                schedule_retry(channels, retry_at);
                state
                    .crdt_sync_delivery
                    .mark_admin_resync(&session.session_id, &family);
            }
            Step::Deferred(retry_at) => {
                schedule_retry(channels, retry_at);
                state
                    .crdt_sync_delivery
                    .mark_admin_resync(&session.session_id, &family);
                for family in pending {
                    state
                        .crdt_sync_delivery
                        .mark_admin_resync(&session.session_id, &family);
                }
                break;
            }
            Step::Close => return Flow::Break,
        }
    }
    Flow::Continue
}

/// What one resync attempt left to do.
enum Step {
    /// The mark is settled: the resync went out, or there is nothing to
    /// resync.
    Done,
    /// The resync did not complete. Its mark stays, the loop retries it at
    /// the given instant, and the other marks of this wake go on.
    Retry(Instant),
    /// The wake has no room for this resync. Its mark and every later one
    /// stay, and the loop retries them at the given instant.
    Deferred(Instant),
    /// The session closes.
    Close,
}

/// Mark `collection` and every collection after it again, and schedule the
/// retry.
fn requeue(
    state: &SharedState,
    session: &SyncSession,
    channels: &mut SessionChannels,
    retry_at: Instant,
    collection: &str,
    rest: impl Iterator<Item = String>,
) {
    schedule_retry(channels, retry_at);
    let delivery = &state.crdt_sync_delivery;
    delivery.mark_resync(&session.session_id, collection);
    for collection in rest {
        delivery.mark_resync(&session.session_id, &collection);
    }
}

fn schedule_retry(channels: &mut SessionChannels, retry_at: Instant) {
    channels.resync_retry_at = Some(match channels.resync_retry_at {
        Some(at) => at.min(retry_at),
        None => retry_at,
    });
}

/// Take one unit of the wake's resync budget and one snapshot token. The
/// retry instant when either has none left.
fn admit(session: &mut SyncSession, budget: &mut usize) -> Result<(), Instant> {
    if *budget == 0 {
        return Err(Instant::now() + RESYNC_RETRY_DELAY);
    }
    if let Err(retry_after_ms) = session.snapshot_limiter.try_acquire() {
        return Err(Instant::now() + Duration::from_millis(retry_after_ms.max(1)));
    }
    *budget -= 1;
    Ok(())
}

async fn resync_collection(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    state: &SharedState,
    collection: &str,
    budget: &mut usize,
) -> Step {
    if let Err(retry_at) = admit(session, budget) {
        return Step::Deferred(retry_at);
    }

    // Read before the snapshot read begins: every push queued before this
    // position is a write the snapshot holds.
    let fence = channels.crdt_delivery.end();
    let mut kill = kill_of(session);
    let outcome = tokio::select! {
        biased;
        _ = kill_signal_ended(&mut kill) => return Step::Close,
        outcome = collection_resync(state, session, collection) => outcome,
    };

    let resync = match outcome {
        Ok(resync) => resync,
        // No shape: no push of the collection reaches the session, and a
        // later subscribe takes its own snapshot.
        Err(CollectionResyncFailure::NoShapes) => return Step::Done,
        Err(CollectionResyncFailure::Refused(detail)) => {
            debug!(
                session = %session.session_id,
                collection,
                %detail,
                "sync: collection resync refused; the collection stays marked"
            );
            return Step::Retry(Instant::now() + RESYNC_RETRY_DELAY);
        }
        Err(CollectionResyncFailure::Failed(detail)) => {
            warn!(
                session = %session.session_id,
                collection,
                %detail,
                "sync: collection resync did not complete; the collection stays marked"
            );
            return Step::Retry(Instant::now() + RESYNC_RETRY_DELAY);
        }
    };

    raise_fence(&mut channels.resync_fences, collection, fence);
    if !send_frame(ws, session, &resync.frame).await {
        return Step::Close;
    }
    info!(
        session = %session.session_id,
        collection,
        snapshot_lsn = resync.snapshot_lsn,
        fence,
        "sync: collection resynced after the queue dropped pushes"
    );
    record_resync(ws, session, channels).await
}

async fn resync_family(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    state: &SharedState,
    family: &str,
    budget: &mut usize,
) -> Step {
    let Some(definitions) = DefinitionFamily::from_collection(family) else {
        return Step::Done;
    };
    // A principal no longer an admin gets no notice, so it gets no resync
    // of one either.
    let Some(tenant_id) = session
        .identity
        .as_ref()
        .map(|identity| identity.tenant_id.as_u64())
    else {
        return Step::Done;
    };
    if !super::register::is_tenant_admin(session) {
        return Step::Done;
    }
    if let Err(retry_at) = admit(session, budget) {
        return Step::Deferred(retry_at);
    }

    // A notice is queued after its DDL committed, so one queued before the
    // catalog read holds a state the read holds too. One queued after it
    // goes out after the resync.
    let fence = channels.admin_notices.end();
    let frame = match super::admin_resync::family_resync(
        state,
        tenant_id,
        session.database_id(),
        definitions,
    ) {
        Ok(frame) => frame,
        Err(error) => {
            warn!(
                session = %session.session_id,
                family,
                %error,
                "sync: definition resync did not complete; the family stays marked"
            );
            return Step::Retry(Instant::now() + RESYNC_RETRY_DELAY);
        }
    };
    raise_fence(&mut channels.admin_fences, family, fence);
    if !send_frame(ws, session, &frame).await {
        return Step::Close;
    }
    record_resync(ws, session, channels).await
}

/// Raise the fence of `name` to `fence`. A fence never moves back.
fn raise_fence(fences: &mut std::collections::HashMap<String, u64>, name: &str, fence: u64) {
    let at = fences.entry(name.to_string()).or_insert(0);
    *at = (*at).max(fence);
}

/// Count a resync that went out. A session past [`SLOW_CONSUMER_RESYNCS`]
/// in the window gets a close frame, and closes.
async fn record_resync(ws: &mut Ws, session: &SyncSession, channels: &mut SessionChannels) -> Step {
    let now = Instant::now();
    if !note_resync(&mut channels.recent_resyncs, now) {
        return Step::Done;
    }
    warn!(
        session = %session.session_id,
        resyncs = channels.recent_resyncs.len(),
        window_secs = SLOW_CONSUMER_WINDOW.as_secs(),
        "sync: closing a slow consumer"
    );
    let close = CloseFrame {
        code: CloseCode::Again,
        reason: Utf8Bytes::from_static(SLOW_CONSUMER_CLOSE_REASON),
    };
    // The session closes whether or not the close frame goes out.
    send_message(ws, session, Message::Close(Some(close))).await;
    Step::Close
}

/// Record a resync at `now` and drop the ones older than the window.
/// Returns whether the window holds more than [`SLOW_CONSUMER_RESYNCS`].
fn note_resync(recent: &mut std::collections::VecDeque<Instant>, now: Instant) -> bool {
    recent.push_back(now);
    while recent
        .front()
        .is_some_and(|at| now.duration_since(*at) > SLOW_CONSUMER_WINDOW)
    {
        recent.pop_front();
    }
    recent.len() > SLOW_CONSUMER_RESYNCS
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_slow_consumer_is_closed() {
        let mut recent = VecDeque::new();
        for _ in 0..SLOW_CONSUMER_RESYNCS {
            assert!(!note_resync(&mut recent, Instant::now()));
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert!(
            note_resync(&mut recent, Instant::now()),
            "one resync past the limit in the window closes the session"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resyncs_spread_past_the_window_are_not_slow() {
        let mut recent = VecDeque::new();
        for _ in 0..SLOW_CONSUMER_RESYNCS * 3 {
            assert!(!note_resync(&mut recent, Instant::now()));
            tokio::time::advance(
                SLOW_CONSUMER_WINDOW / SLOW_CONSUMER_RESYNCS as u32 + Duration::from_secs(1),
            )
            .await;
        }
    }

    #[test]
    fn a_resync_flood_is_limited() {
        let mut session = SyncSession::new("resync-flood".into());
        let mut budget = usize::MAX;
        let admitted = (0..1000)
            .filter(|_| admit(&mut session, &mut budget).is_ok())
            .count();
        let burst = crate::control::server::sync::async_dispatch::snapshot_rate().burst as usize;
        assert!(
            admitted <= burst + 1,
            "{admitted} resyncs passed a bucket of burst {burst}"
        );
    }

    #[test]
    fn a_wake_runs_at_most_its_resync_budget() {
        let mut session = SyncSession::new("resync-budget".into());
        let mut budget = MAX_RESYNCS_PER_WAKE;
        let admitted = (0..MAX_RESYNCS_PER_WAKE * 2)
            .filter(|_| admit(&mut session, &mut budget).is_ok())
            .count();
        assert_eq!(admitted, MAX_RESYNCS_PER_WAKE);
    }
}
