// SPDX-License-Identifier: BUSL-1.1

//! Where a rebuild pass ends, read on the owning core when the pass hands
//! its scratch database back.
//!
//! A pass whose journal holds at most `max_docs` documents cuts over: the
//! core replays each noted document in the cutover transaction. A pass
//! whose journal passed the bound runs a catch-up round instead. In one
//! step on the core, with no write between, the journal hands over its
//! noted set, starts recording again, and a fresh read snapshot is
//! pinned. A rebuild thread then copies the ended pass's rows, minus the
//! noted documents, into a new scratch database and derives each noted
//! document again from the fresh snapshot. That scratch then holds what a
//! full pass over the fresh snapshot derives, and the fresh journal holds
//! every write since.
//!
//! A catch-up round re-derives each noted document whole. A filter on
//! system-time ordinals misses writes: an undo of a frame write removes
//! stored frames, and a restore re-issues frames below the newest one.
//!
//! The rounds are bounded. A journal still past its bound after the last
//! round refuses the rebuild with the write rate of that round.

use redb::ReadableDatabase as _;
use roaring::RoaringBitmap;

use super::core::InvertedIndex;
use super::errors::inverted_err;
use super::rebuild_journal::{FtsJournal, JournalState};
use super::rebuild_snapshot::FtsRebuildTicket;

/// Where a pass of rebuild `token` ends.
pub(super) enum PassEnd {
    /// The journal is gone: the rebuild was aborted or replaced.
    Gone,
    /// The collection or its tenant was purged. The journal is closed.
    Purged,
    /// Writes passed the bound after every round. The journal is closed.
    Outpaced { rounds: u32, docs_per_sec: u64 },
    /// The journal is at most at its bound. The journal is closed, and the
    /// cutover replays its noted set.
    CutOver(FtsJournal),
    /// The journal passed its bound. It records again from `ticket`'s
    /// snapshot. `touched` is every document written during the pass.
    CatchUp {
        ticket: FtsRebuildTicket,
        touched: RoaringBitmap,
    },
}

impl InvertedIndex {
    /// End the current pass of rebuild `token`. Errors when redb cannot
    /// pin the fresh snapshot of a catch-up round; the journal then stays
    /// open, and the caller aborts the rebuild.
    pub(super) fn end_rebuild_pass(&self, token: u64) -> crate::Result<PassEnd> {
        let mut journals = self.journals.borrow_mut();
        let Some(pos) = journals.active.iter().position(|j| j.token == token) else {
            return Ok(PassEnd::Gone);
        };
        let journal = &mut journals.active[pos];
        match journal.state {
            JournalState::Purged => {
                journals.active.swap_remove(pos);
                Ok(PassEnd::Purged)
            }
            JournalState::Recording => Ok(PassEnd::CutOver(journals.active.swap_remove(pos))),
            JournalState::Overflowed if journal.rounds >= journal.bounds.max_rounds => {
                let rounds = journal.rounds;
                let docs_per_sec = write_rate(journal);
                journals.active.swap_remove(pos);
                Ok(PassEnd::Outpaced {
                    rounds,
                    docs_per_sec,
                })
            }
            JournalState::Overflowed => {
                let txn = self
                    .inner
                    .backend()
                    .db()
                    .begin_read()
                    .map_err(|e| inverted_err("rebuild catch-up snapshot", e))?;
                let touched = std::mem::take(&mut journal.touched);
                journal.state = JournalState::Recording;
                journal.rounds += 1;
                // no-determinism: the clock only sizes the write rate a
                // refusal names. The rebuilt rows do not depend on it.
                journal.pinned_at = std::time::Instant::now();
                let ticket = FtsRebuildTicket::new(
                    token,
                    journal.database_id,
                    journal.tid,
                    journal.collection.clone(),
                    txn,
                    journal.rounds,
                );
                Ok(PassEnd::CatchUp { ticket, touched })
            }
        }
    }
}

/// Distinct documents written per second since the pass's snapshot.
fn write_rate(journal: &FtsJournal) -> u64 {
    let millis = journal.pinned_at.elapsed().as_millis().max(1);
    let per_sec = u128::from(journal.touched.len()) * 1000 / millis;
    u64::try_from(per_sec).unwrap_or(u64::MAX)
}
