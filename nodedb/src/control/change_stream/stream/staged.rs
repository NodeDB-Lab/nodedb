// SPDX-License-Identifier: BUSL-1.1

//! An entry's change events between their stage and the entry's settle.
//!
//! Each event takes its position as it stages: the entry's log index and
//! the next ordinal under the entry. Ordinals continue across every stage
//! call of the entry, so a page staged alone keeps the position it has in
//! the whole entry.
//!
//! Two stages:
//!
//! - [`ChangeStream::stage`] holds the events in memory. The write funnel
//!   stages a write's events with it once per entry: the entry's plan is in
//!   memory already, and its events settle from memory in one run.
//! - [`ChangeStream::stage_page`] stages one page of a large entry. Once the
//!   entry holds [`SPILL_EVENTS`] events, they move to the change journal
//!   as one staged page (see `ChangeJournal::spill`), on the blocking pool,
//!   and the entry keeps only the position of the last one. A large entry
//!   holds at most one page in memory, whatever its size.
//!
//! A spilled page is not visible: a consumer sees an entry's events only
//! once the entry settles, in log order, after every earlier entry.
//!
//! A page the journal does not take stays in memory, ahead of what stages
//! after it, so no event is lost and the order holds. The next spill writes
//! it again. A second page that does not spill while one waits is refused:
//! its events leave the entry, and the caller retries the page, so an
//! entry never holds more than two pages. A caller that keeps failing holds
//! its entry (see `apply_loop::marker_changes`).

use std::sync::Arc;

use crate::control::change_stream::journal::codec::JournalRow;
use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::types::DatabaseId;

use super::bus::ChangeStream;
use super::ring::PositionedChange;
use super::{ChangeEvent, ChangePartition};

/// Events an entry holds in memory before they move to the journal.
pub const SPILL_EVENTS: usize = 1024;

/// One entry's staged events.
#[derive(Debug, Default)]
pub(super) struct StagedEntry {
    /// The ordinal of the entry's last staged event.
    ordinal: u64,
    /// The events held in memory, in position order. Every one sits above
    /// `spilled_through`.
    pub held: Vec<PositionedChange>,
    /// The position of the last event spilled to the journal. The journal
    /// holds every event of the entry up to it.
    pub spilled_through: Option<CdcOffset>,
}

/// Events the stream holds in memory outside its ring, and the most it
/// held at once.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct HeldGauge {
    held: usize,
    peak: usize,
}

impl HeldGauge {
    pub fn add(&mut self, events: usize) {
        self.held += events;
        self.peak = self.peak.max(self.held);
    }

    pub fn sub(&mut self, events: usize) {
        self.held = self.held.saturating_sub(events);
    }
}

/// A page leaving an entry for the journal, and what the entry held before
/// the stage that moves it.
#[derive(Debug)]
struct SpillTicket {
    group_id: u64,
    log_index: u64,
    page: Vec<PositionedChange>,
    /// Events the entry held before this stage: a page an earlier spill
    /// left in memory.
    held_before: usize,
    /// The entry's ordinal before this stage.
    ordinal_before: u64,
}

impl ChangeStream {
    /// Stage `events` under the data-group entry at `(group_id, log_index)`,
    /// in memory, until the apply loop settles it.
    pub(crate) fn stage(
        &self,
        group_id: u64,
        log_index: u64,
        database_id: DatabaseId,
        events: Vec<ChangeEvent>,
    ) {
        self.hold(group_id, log_index, database_id, events, false);
    }

    /// Stage `events`, one page of a large entry, under the entry at
    /// `(group_id, log_index)`. Once the entry holds [`SPILL_EVENTS`]
    /// events they move to the journal on the blocking pool.
    ///
    /// Fails when the page did not spill while an earlier page waits in
    /// memory. The page then left the entry, and the caller stages it
    /// again.
    pub(crate) async fn stage_page(
        shared: &Arc<SharedState>,
        group_id: u64,
        log_index: u64,
        database_id: DatabaseId,
        events: Vec<ChangeEvent>,
    ) -> crate::Result<()> {
        let Some(ticket) =
            shared
                .change_stream
                .hold(group_id, log_index, database_id, events, true)
        else {
            return Ok(());
        };
        let writer = Arc::clone(shared);
        let task = tokio::task::spawn_blocking(move || {
            let written = writer.change_stream.write_spill(&ticket);
            (ticket, written)
        });
        match task.await {
            Ok((ticket, written)) => shared.change_stream.settle_spill(ticket, written),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // Cancelled at runtime shutdown, with the page: the entry never
            // settles in this process, and a restart stages it again.
            Err(error) => Err(crate::Error::Internal {
                detail: format!("the staged page's spill did not finish: {error}"),
            }),
        }
    }

    /// [`Self::stage_page`] on the calling thread.
    #[cfg(test)]
    pub(crate) fn stage_page_now(
        &self,
        group_id: u64,
        log_index: u64,
        database_id: DatabaseId,
        events: Vec<ChangeEvent>,
    ) -> crate::Result<()> {
        let Some(ticket) = self.hold(group_id, log_index, database_id, events, true) else {
            return Ok(());
        };
        let written = self.write_spill(&ticket);
        self.settle_spill(ticket, written)
    }

    /// Hold `events` under the entry at their positions. Returns the page to
    /// spill when `spill` is on, a journal is attached, and the entry holds
    /// [`SPILL_EVENTS`] events.
    fn hold(
        &self,
        group_id: u64,
        log_index: u64,
        database_id: DatabaseId,
        events: Vec<ChangeEvent>,
        spill: bool,
    ) -> Option<SpillTicket> {
        if events.is_empty() {
            return None;
        }
        let mut guard = self.lock();
        let state = &mut *guard;
        let staged = events.len();
        let entry = state.staged.entry((group_id, log_index)).or_default();
        let held_before = entry.held.len();
        let ordinal_before = entry.ordinal;
        for event in events {
            entry.ordinal += 1;
            entry.held.push(PositionedChange {
                position: CdcOffset::data_event(0, log_index, entry.ordinal),
                database_id,
                event,
            });
        }
        state.held.add(staged);
        if !spill || self.journal.get().is_none() || entry.held.len() < SPILL_EVENTS {
            return None;
        }
        let page = std::mem::take(&mut entry.held);
        state.held.sub(page.len());
        Some(SpillTicket {
            group_id,
            log_index,
            page,
            held_before,
            ordinal_before,
        })
    }

    /// Write `ticket`'s page to the journal. Blocks on disk.
    fn write_spill(&self, ticket: &SpillTicket) -> crate::Result<()> {
        let Some(journal) = self.journal.get() else {
            return Err(crate::Error::Internal {
                detail: "a staged page spilled with no change journal attached".into(),
            });
        };
        journal.spill(
            ChangePartition(ticket.group_id),
            ticket.page.iter().map(JournalRow::of_positioned),
        )
    }

    /// Record the outcome of `ticket`'s spill on its entry. The entry stages
    /// nothing else while its page moves: its stages run in order, before it
    /// settles.
    fn settle_spill(&self, ticket: SpillTicket, written: crate::Result<()>) -> crate::Result<()> {
        let SpillTicket {
            group_id,
            log_index,
            page,
            held_before,
            ordinal_before,
        } = ticket;
        let mut guard = self.lock();
        let state = &mut *guard;
        // A snapshot install covered the entry meanwhile: it never settles
        // here, and the install's hole drops its rows.
        let Some(entry) = state.staged.get_mut(&(group_id, log_index)) else {
            return Ok(());
        };
        let error = match written {
            Ok(()) => {
                entry.spilled_through = page.last().map(|change| change.position);
                return Ok(());
            }
            Err(error) => error,
        };
        tracing::error!(
            group_id,
            log_index,
            events = page.len(),
            %error,
            "a staged page of change events did not reach the change journal; the entry holds \
             it in memory"
        );
        crate::diag::change_journal_failed(group_id, "staged page write", &error);
        let mut held = page;
        held.append(&mut entry.held);
        if held_before == 0 {
            state.held.add(held.len());
            entry.held = held;
            return Ok(());
        }
        // A page waits in memory already: this stage's events leave the
        // entry, and the caller stages them again.
        held.truncate(held_before);
        state.held.add(held.len());
        entry.held = held;
        entry.ordinal = ordinal_before;
        Err(error)
    }

    /// Events the stream holds in memory outside its ring: staged entries
    /// and settled events the journal lacks.
    pub fn held_events(&self) -> usize {
        self.lock().held.held
    }

    /// The most events [`Self::held_events`] counted at once.
    pub fn held_events_peak(&self) -> usize {
        self.lock().held.peak
    }

    /// Whether the entry at `(group_id, log_index)` moved staged events to
    /// the journal.
    #[cfg(feature = "failpoints")]
    pub(crate) fn spilled(&self, group_id: u64, log_index: u64) -> bool {
        self.lock()
            .staged
            .get(&(group_id, log_index))
            .is_some_and(|entry| entry.spilled_through.is_some())
    }
}
