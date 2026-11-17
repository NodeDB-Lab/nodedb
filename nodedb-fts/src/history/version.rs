// SPDX-License-Identifier: Apache-2.0

//! Text versions and the cut predicate.

use nodedb_types::Surrogate;

use crate::scope::IndexScope;

/// The ordinal of a write that changes no history: it records no start and
/// ends no version. An index rebuild and a checkpoint restore write this way.
pub const UNVERSIONED: i64 = i64::MIN;

/// One document version a write ended, visible at the cuts in
/// `[sys_from, sys_to)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedVersion {
    pub surrogate: Surrogate,
    /// The ordinal of the write that indexed the version. [`UNVERSIONED`]
    /// when that write kept no start.
    pub sys_from: i64,
    /// The ordinal of the write that ended the version.
    pub sys_to: i64,
    /// The version's analyzed tokens, in order.
    pub tokens: Vec<String>,
}

/// The system-time ordinal one read takes the index at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextCut {
    at: i64,
}

impl TextCut {
    /// The cut at ordinal `at`.
    pub const fn at(at: i64) -> Self {
        Self { at }
    }

    /// The ordinal of the cut.
    pub const fn ordinal(&self) -> i64 {
        self.at
    }

    /// The lowest ordinal of a write after the cut. `None` at `i64::MAX`:
    /// no write follows it.
    pub const fn first_after(&self) -> Option<i64> {
        self.at.checked_add(1)
    }

    /// Whether a read at the cut sees a version live over `[sys_from, sys_to)`.
    pub const fn sees(&self, sys_from: i64, sys_to: i64) -> bool {
        sys_from <= self.at && self.at < sys_to
    }
}

/// The document a history write addresses: one index of one tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryDoc<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub index: IndexScope<'a>,
    pub surrogate: Surrogate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_sees_the_closed_open_interval() {
        let cut = TextCut::at(20);
        assert!(cut.sees(10, 21), "live across the cut");
        assert!(cut.sees(20, 30), "started at the cut");
        assert!(!cut.sees(10, 20), "ended at the cut");
        assert!(!cut.sees(21, 30), "started after the cut");
    }

    #[test]
    fn the_last_cut_has_no_write_after_it() {
        assert_eq!(TextCut::at(i64::MAX).first_after(), None);
        assert_eq!(TextCut::at(7).first_after(), Some(8));
    }
}
