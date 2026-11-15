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

use std::collections::BTreeSet;

use super::capture::WriteSetCapture;

/// The LSNs of the committed redo records restart replay folds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NeverDurableRedos {
    lsns: BTreeSet<u64>,
}

impl NeverDurableRedos {
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

/// The write sets a core stored for the records restart replay folded: the
/// fold target rows of each, with the group the record opened. Boot
/// journals them as the parts of each record's group before the node
/// serves.
pub type ReplayedCaptures = Vec<WriteSetCapture>;
