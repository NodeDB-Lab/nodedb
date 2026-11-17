// SPDX-License-Identifier: BUSL-1.1

//! Envelope origins retained while grouped WAL records are reconstituted.

use std::collections::BTreeSet;

/// Effective LSNs of actual transaction and write-group envelopes.
#[derive(Default)]
pub(crate) struct GroupedReplayOrigins {
    effective_lsns: BTreeSet<u64>,
}

impl GroupedReplayOrigins {
    pub(super) fn insert(&mut self, lsn: u64) {
        self.effective_lsns.insert(lsn);
    }

    pub(crate) fn contains(&self, lsn: u64) -> bool {
        self.effective_lsns.contains(&lsn)
    }
}
