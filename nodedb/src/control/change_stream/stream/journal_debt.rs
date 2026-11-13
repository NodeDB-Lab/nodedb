// SPDX-License-Identifier: BUSL-1.1

//! Settled change events the journal refused, and the halt when it keeps
//! refusing them.
//!
//! A journal write that fails never drops an event. The ring takes it, so
//! live consumers see it, and it waits in the group's pending events, which
//! a catch-up also reads. The write retries:
//!
//! - a replicated group's on every apply pass, and on the apply loop's
//!   timer while the debt is owed: its applied floor waits until the
//!   journal holds every event it covers;
//! - a forwarded partition's with the next run the leader sends.
//!
//! A group whose writes keep failing halts: after [`HALT_FAILURES`] failed
//! writes in a row, or once [`HALT_EVENTS`] events wait. A halted
//! replicated group settles no more entries, and a halted forwarded
//! partition takes no run its journal refuses, so no node delivers events
//! without a durable copy for long. Each pass, or each run, still tries the
//! write. The halt marks the node not ready through the
//! node's apply wedge, the same way a cut barrier floor that keeps failing
//! does. The first write that succeeds pays the debt and clears the halt.

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

    /// Whether `group_id` owes the journal.
    pub fn owes_group(&self, group_id: u64) -> bool {
        self.owed.contains_key(&group_id)
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
        let report = {
            let mut guard = self.lock();
            let state = &mut *guard;
            let pending = state.group_pending.get(&group_id);
            state.debt.halt(
                group_id,
                pending.map_or(0, Vec::len),
                pending
                    .and_then(|events| events.first())
                    .map(|event| event.position()),
            )
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
        assert!(debt.owes() && debt.owes_group(4) && !debt.owes_group(5));
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
}
