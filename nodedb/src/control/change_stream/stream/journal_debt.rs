// SPDX-License-Identifier: BUSL-1.1

//! Settled change events the journal refused, and the halt when it keeps
//! refusing them.
//!
//! A journal write that fails never drops an event.
//!
//! - A replicated group's settled event goes to the ring, so live consumers
//!   see it, and waits in the group's pending events, which a catch-up also
//!   reads. The write retries on every apply pass, and on the apply loop's
//!   timer while the debt is owed: the group's applied floor waits until
//!   the journal holds every event it covers.
//! - A forwarded partition takes no run its journal refuses. The leader
//!   keeps the run's events and sends the stretch again (see
//!   [`super::forwarded`]). Its refused events count as waiting events.
//!
//! A group whose writes keep failing halts: after [`HALT_FAILURES`] failed
//! writes in a row, or once [`HALT_EVENTS`] events wait. A halted
//! replicated group settles no more entries. Each pass, or each run, still
//! tries the write. The halt marks the node not ready through the node's
//! apply wedge, the same way a cut barrier floor that keeps failing does.
//! The first write that succeeds pays the debt and clears the halt.

use std::collections::BTreeMap;

use crate::control::cluster::metadata_applier::{MetadataApplyWedge, WedgeReport};
use crate::event::cdc::CdcOffset;

use super::bus::ChangeStream;
use super::staged::SPILL_EVENTS;

/// Failed journal writes of one group in a row before it halts.
pub const HALT_FAILURES: u32 = 16;

/// Events of one group waiting for the journal before it halts.
pub const HALT_EVENTS: usize = 16 * SPILL_EVENTS;

/// What a group owes the journal.
#[derive(Debug)]
struct Owed {
    failures: u32,
    error: String,
    /// The report the halt recorded, once the group halted.
    halted: Option<WedgeReport>,
}

/// Every group that owes the journal, and the halts paid since the last
/// clear.
#[derive(Debug, Default)]
pub(super) struct JournalDebt {
    owed: BTreeMap<u64, Owed>,
    cleared: Vec<WedgeReport>,
}

impl JournalDebt {
    /// Note a failed journal write of `group_id`.
    pub fn failed(&mut self, group_id: u64, error: &crate::Error) {
        let owed = self.owed.entry(group_id).or_insert(Owed {
            failures: 0,
            error: String::new(),
            halted: None,
        });
        owed.failures = owed.failures.saturating_add(1);
        owed.error = error.to_string();
    }

    /// Note a journal write of `group_id` that succeeded: its debt is paid.
    pub fn paid(&mut self, group_id: u64) {
        if let Some(owed) = self.owed.remove(&group_id)
            && let Some(report) = owed.halted
        {
            self.cleared.push(report);
        }
    }

    /// Whether any group owes the journal.
    pub fn owes(&self) -> bool {
        !self.owed.is_empty()
    }

    /// The halt of `group_id`, with `pending` events waiting from `oldest`
    /// on. Records the halt the first time it holds.
    fn halt(
        &mut self,
        group_id: u64,
        pending: usize,
        oldest: Option<CdcOffset>,
    ) -> Option<WedgeReport> {
        let owed = self.owed.get_mut(&group_id)?;
        if owed.halted.is_none() && (owed.failures >= HALT_FAILURES || pending >= HALT_EVENTS) {
            let raft_index = oldest.map_or(0, |position| position.index);
            owed.halted = Some(WedgeReport {
                raft_index,
                last_applied_watermark: raft_index.saturating_sub(1),
                entry_kind: "change journal".to_owned(),
                error: format!(
                    "group {group_id}: the change journal refused {} writes in a row with \
                     {pending} events waiting: {}",
                    owed.failures, owed.error
                ),
            });
        }
        owed.halted.clone()
    }
}

impl ChangeStream {
    /// Whether a journal write failed and waits for its retry.
    pub(crate) fn journal_owed(&self) -> bool {
        self.lock().debt.owes()
    }

    /// Whether `group_id` halts: its journal writes keep failing. The first
    /// halt records itself on `wedge`, so the node reports not ready.
    pub(crate) fn journal_halts(&self, group_id: u64, wedge: &MetadataApplyWedge) -> bool {
        self.journal_halts_with(group_id, None, wedge)
    }

    /// [`Self::journal_halts`], counting `refused` with the pending events:
    /// the number of events of a forwarded run the node refused, and the
    /// position of its first one. They wait at the run's sender.
    pub(super) fn journal_halts_with(
        &self,
        group_id: u64,
        refused: Option<(usize, CdcOffset)>,
        wedge: &MetadataApplyWedge,
    ) -> bool {
        let report = {
            let mut guard = self.lock();
            let state = &mut *guard;
            let pending = state.group_pending.get(&group_id);
            let oldest = pending
                .and_then(|events| events.first())
                .map(|event| event.position())
                .or(refused.map(|(_, first)| first));
            let waiting = pending.map_or(0, Vec::len) + refused.map_or(0, |(events, _)| events);
            state.debt.halt(group_id, waiting, oldest)
        };
        let Some(report) = report else {
            return false;
        };
        if wedge.record(report.clone()) {
            tracing::error!(
                group_id,
                error = %report.error,
                "the change journal keeps refusing a group's events; the group halts and this \
                 node is no longer ready"
            );
            crate::diag::change_journal_halted(group_id, &report.error);
        }
        true
    }

    /// Clear on `wedge` every halt whose debt a later journal write paid.
    pub(crate) fn clear_journal_halts(&self, wedge: &MetadataApplyWedge) {
        let cleared = std::mem::take(&mut self.lock().debt.cleared);
        for report in cleared {
            if wedge.clear(&report) {
                tracing::info!(
                    raft_index = report.raft_index,
                    "the change journal took a halted group's events; the group resumes"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused() -> crate::Error {
        crate::Error::Internal {
            detail: "disk refused the write".into(),
        }
    }

    /// A group halts after [`HALT_FAILURES`] failed writes in a row, or
    /// once [`HALT_EVENTS`] events wait. The write that succeeds pays the
    /// debt and hands its halt back for the wedge to clear.
    #[test]
    fn a_group_halts_while_its_writes_keep_failing_and_resumes_once_paid() {
        let mut debt = JournalDebt::default();
        for _ in 1..HALT_FAILURES {
            debt.failed(4, &refused());
        }
        assert!(debt.owes() && debt.owed.contains_key(&4) && !debt.owed.contains_key(&5));
        assert!(debt.halt(4, 10, None).is_none(), "below both bounds");
        assert!(
            debt.halt(4, HALT_EVENTS, Some(CdcOffset::data_event(0, 9, 1)))
                .is_some_and(|report| report.raft_index == 9),
            "the waiting events alone halt it"
        );
        debt.failed(4, &refused());
        let report = debt.halt(4, 0, None).expect("still halted");
        assert!(report.error.contains("disk refused the write"));

        debt.paid(4);
        assert!(!debt.owes());
        assert_eq!(debt.cleared, vec![report]);
        assert!(
            debt.halt(4, HALT_EVENTS, None).is_none(),
            "a paid group runs"
        );
    }

    /// One failed write with [`HALT_EVENTS`] settled events waiting halts
    /// the group and marks the node not ready. The write that succeeds
    /// journals them, and the next clear lifts the halt from the wedge.
    #[test]
    fn waiting_events_alone_halt_a_group_until_the_journal_takes_them() {
        use std::sync::Arc;

        use crate::control::change_stream::journal::ChangeJournal;
        use crate::control::change_stream::{ChangeEvent, ChangeOperation};
        use crate::types::{DatabaseId, Lsn, TenantId};

        const GROUP: u64 = 4;
        let dir = tempfile::tempdir().expect("tempdir");
        let stream = ChangeStream::new(8);
        let journal = Arc::new(ChangeJournal::open(dir.path(), HALT_EVENTS).expect("open"));
        stream.attach_journal(Arc::clone(&journal)).expect("attach");
        let wedge = MetadataApplyWedge::default();
        let events = (0..HALT_EVENTS)
            .map(|row| ChangeEvent {
                lsn: Lsn::new(1),
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: nodedb_types::RowIdentity::from_user_key(format!("e{row}")),
                operation: ChangeOperation::Insert,
                timestamp_ms: 1,
                after: None,
            })
            .collect();
        stream.stage(GROUP, 3, DatabaseId::DEFAULT, events);
        stream.settle_group(GROUP, 3, 3);

        journal.fail_next_persists_for_test(1);
        assert!(stream.journal_group(GROUP, 3).is_err());
        assert_eq!(stream.held_events(), HALT_EVENTS, "no event is dropped");
        assert!(
            stream.journal_halts(GROUP, &wedge),
            "one failure with the bound reached halts the group"
        );
        let report = wedge.report().expect("the node is not ready");
        assert_eq!(report.entry_kind, "change journal");
        assert_eq!(report.raft_index, 3, "the oldest waiting entry");

        stream.journal_group(GROUP, 3).expect("the write succeeds");
        assert_eq!(stream.held_events(), 0);
        assert!(!stream.journal_halts(GROUP, &wedge), "a paid group runs");
        assert!(wedge.is_wedged(), "the halt stays until the clear");
        stream.clear_journal_halts(&wedge);
        assert!(!wedge.is_wedged(), "the clear lifts the halt");
    }
}
