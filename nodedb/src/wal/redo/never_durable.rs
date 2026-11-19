// SPDX-License-Identifier: BUSL-1.1

//! Committed redo records whose install never became durable.
//!
//! A committed record's install folds materialized sums into target rows.
//! The fold commits with the store's durability deferred, and becomes
//! durable only with the install's stored write set. A crash before that
//! store reverts the fold, while the record's WAL copy can already be
//! durable. Boot settle finds such a record: it carries sum targets, and no
//! write set and no part of its group is stored. Boot hands its LSN to the
//! cores, and restart replay folds the record as its install did.
//!
//! A committed record with no write set and no part has no outcome: its
//! install never answered. The live install validates a record before it
//! writes, and a refusal cancels the record. So boot replay judges each such
//! record that carries a commit boundary, at its place in replay, as the
//! live install would have: a [`JudgedRedo`]. Its verdict is a
//! [`BootVerdict`]. A Calvin slice carries no boundary: its stage vote
//! validated it.

use std::collections::{BTreeMap, BTreeSet};

use super::boundary::RedoBoundary;
use super::capture::WriteSetCapture;

/// A committed record whose install has no outcome, which boot replay
/// judges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JudgedRedo {
    /// The LSN of the `TransactionRedo` record.
    pub lsn: u64,
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub database_id: u64,
    /// The apply key, event source and commit instant the record's header
    /// carries. The records that close its group carry them too. Settle
    /// checks the event source code once, when it builds the record.
    pub apply_key: u64,
    pub event_source: crate::event::EventSource,
    pub commit_hlc: u64,
    pub boundary: RedoBoundary,
    /// The records a refusal cancels beside the record: its group's
    /// announcement and continuations, and its boundary record. These are
    /// the records the live refusal cancels.
    pub members: Vec<u64>,
    /// The verdict an earlier boot stored for the record, in any core's
    /// store. It decides the record without a second validate.
    pub stored: Option<BootVerdictOutcome>,
}

/// What boot replay decided for a [`JudgedRedo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum BootVerdictOutcome {
    /// The validate passed, or could not run, and replay applied the
    /// record.
    Passed,
    /// The validate refused the record, and replay applied none of it.
    /// `final_refusal` says a redelivery of its entry refuses it again.
    Refused { final_refusal: bool },
}

/// The verdict on one judged record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootVerdict {
    pub judged: JudgedRedo,
    pub outcome: BootVerdictOutcome,
}

/// The LSNs of the committed redo records restart replay folds, and the
/// committed records it judges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NeverDurableRedos {
    lsns: BTreeSet<u64>,
    judged: BTreeMap<u64, JudgedRedo>,
}

impl NeverDurableRedos {
    /// Note the committed record `judged` as one replay judges.
    pub fn insert_judged(&mut self, judged: JudgedRedo) {
        self.judged.insert(judged.lsn, judged);
    }

    /// The judged record at `lsn`, when replay judges it.
    pub fn judged(&self, lsn: u64) -> Option<&JudgedRedo> {
        self.judged.get(&lsn)
    }

    /// Every judged record, in LSN order.
    pub fn judged_records(&self) -> impl Iterator<Item = &JudgedRedo> + '_ {
        self.judged.values()
    }

    /// Hand each judged record the verdict `stored` holds for it. `stored`
    /// holds every core's stored verdicts, so a verdict decides its record
    /// whichever core stored it.
    pub fn attach_stored(&mut self, stored: &BTreeMap<u64, BootVerdictOutcome>) {
        for (lsn, judged) in &mut self.judged {
            judged.stored = stored.get(lsn).copied();
        }
    }

    /// Note the committed record at `lsn` as never durable.
    pub fn insert(&mut self, lsn: u64) {
        self.lsns.insert(lsn);
    }

    /// Whether replay folds the committed record at `lsn`.
    pub fn contains(&self, lsn: u64) -> bool {
        self.lsns.contains(&lsn)
    }

    pub fn is_empty(&self) -> bool {
        self.lsns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.lsns.len()
    }

    /// Every LSN, in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.lsns.iter().copied()
    }
}

/// What a core's restart replay hands boot before the node serves.
#[derive(Debug, Default)]
pub struct ReplayedCaptures {
    /// The write sets a core stored for the records restart replay folded:
    /// the fold target rows of each, with the group the record opened.
    /// Boot journals them as the parts of each record's group.
    pub captures: Vec<WriteSetCapture>,
    /// The verdict on each record this core judged. Boot cancels each
    /// refused record and closes the group of each passed one.
    pub verdicts: Vec<BootVerdict>,
}
