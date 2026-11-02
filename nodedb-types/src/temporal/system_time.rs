// SPDX-License-Identifier: Apache-2.0

//! System-time scope for bitemporal scans.
//!
//! An explicit enum makes the "all versions" (audit-log) mode, the
//! statement-pinned cut, and the transaction snapshot representable, with no
//! out-of-band sentinel.

use serde::{Deserialize, Serialize};

/// System-time selection for a temporal scan.
///
/// `AsOf` carries the statement's time in milliseconds since Unix epoch.
/// `Pinned` carries the system-time ordinal the Control Plane pinned for
/// one statement. `TxnSnapshot` carries the snapshot ordinal of a
/// transaction block. Valid time is a separate axis and is unaffected by
/// this enum.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SystemTimeScope {
    /// Latest committed version (no `AS OF SYSTEM TIME` clause).
    #[default]
    Current,
    /// Point-in-time snapshot: the version visible AS OF `ms`. A read at
    /// this scope reads at the last ordinal of `ms`, with no cap.
    AsOf(i64),
    /// Point-in-time snapshot at one pinned system-time ordinal: the
    /// statement's `AS OF SYSTEM TIME`, capped at the statement's read cut.
    /// Every read of the statement reads at `ordinal`, so the statement
    /// reads one cut.
    Pinned {
        /// The ordinal every versioned read of the statement reads at.
        ordinal: i64,
        /// The Calvin cut marker whose cut caps `ordinal`, `0` when no
        /// marker caps it. A serving node waits for the marker before it
        /// reads.
        cut_marker: u64,
    },
    /// The snapshot of a transaction block: the version visible at
    /// `ordinal`, the same version `Pinned` selects, with the reading
    /// transaction's own staged writes merged on top. A statement inside a
    /// transaction block that names no system time reads at this scope.
    /// `Pinned` never merges staged writes.
    TxnSnapshot {
        /// The ordinal every read of the transaction block reads at.
        ordinal: i64,
        /// The Calvin cut marker whose cut caps `ordinal`, `0` when no
        /// marker caps it. A serving node waits for the marker before it
        /// reads.
        cut_marker: u64,
    },
    /// `AS OF SYSTEM TIME NULL` — every system-time version of each matching
    /// row, ordered ascending by system time, with the system-time column
    /// projected into the output (audit-log semantics).
    AllVersions,
}

impl SystemTimeScope {
    /// True when this scope selects something other than current state.
    pub const fn is_temporal(&self) -> bool {
        !matches!(self, Self::Current)
    }

    /// The point-in-time cutoff as a system-time ordinal. Version keys
    /// compare against this value. `AsOf` answers the newest ordinal a
    /// write in the cutoff millisecond can carry. `Pinned` and
    /// `TxnSnapshot` answer their ordinal.
    pub fn as_of_ordinal(&self) -> Option<i64> {
        match self {
            Self::AsOf(ms) => Some(super::ordinal::ms_to_ordinal_upper(*ms)),
            Self::Pinned { ordinal, .. } | Self::TxnSnapshot { ordinal, .. } => Some(*ordinal),
            Self::Current | Self::AllVersions => None,
        }
    }

    /// The ordinal and the cut marker that caps it, when a marker caps a
    /// pinned scope or a transaction snapshot.
    pub const fn marked_cut(&self) -> Option<(i64, u64)> {
        match self {
            Self::Pinned {
                ordinal,
                cut_marker,
            }
            | Self::TxnSnapshot {
                ordinal,
                cut_marker,
            } if *cut_marker != 0 => Some((*ordinal, *cut_marker)),
            Self::Pinned { .. }
            | Self::TxnSnapshot { .. }
            | Self::Current
            | Self::AsOf(_)
            | Self::AllVersions => None,
        }
    }

    /// True when a read at this scope merges the reading transaction's own
    /// staged writes on top of the versions it selects: the current state
    /// and a transaction snapshot. A historical read (`AsOf`, `Pinned`,
    /// `AllVersions`) never sees staged writes.
    pub const fn merges_txn_overlay(&self) -> bool {
        matches!(self, Self::Current | Self::TxnSnapshot { .. })
    }

    /// True when this scope reads committed history: `AsOf`, `Pinned`, and
    /// `AllVersions`. A read at such a scope reports its horizon. A current
    /// read and a transaction snapshot read report none.
    pub const fn is_historical(&self) -> bool {
        !self.merges_txn_overlay()
    }

    /// True when this scope reads the newest committed version of each row.
    pub const fn is_current(&self) -> bool {
        matches!(self, Self::Current)
    }

    /// True when this scope requests every system-time version.
    pub const fn is_all_versions(&self) -> bool {
        matches!(self, Self::AllVersions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_current() {
        assert_eq!(SystemTimeScope::default(), SystemTimeScope::Current);
        assert!(!SystemTimeScope::Current.is_temporal());
    }

    #[test]
    fn as_of_ordinal_is_the_last_ordinal_of_the_millisecond() {
        assert_eq!(
            SystemTimeScope::AsOf(42).as_of_ordinal(),
            Some(42 * 1_000_000 + 999_999)
        );
        assert_eq!(SystemTimeScope::Current.as_of_ordinal(), None);
        assert_eq!(SystemTimeScope::AllVersions.as_of_ordinal(), None);
    }

    #[test]
    fn a_pinned_scope_reads_at_its_ordinal() {
        let pinned = SystemTimeScope::Pinned {
            ordinal: 42 * 1_000_000 + 7,
            cut_marker: 0,
        };
        assert_eq!(pinned.as_of_ordinal(), Some(42 * 1_000_000 + 7));
        assert!(pinned.is_temporal());
        assert_eq!(pinned.marked_cut(), None);
        let marked = SystemTimeScope::Pinned {
            ordinal: 9,
            cut_marker: 4,
        };
        assert_eq!(marked.marked_cut(), Some((9, 4)));
        assert_eq!(SystemTimeScope::AsOf(9).marked_cut(), None);
    }

    #[test]
    fn a_txn_snapshot_reads_at_its_ordinal_and_merges_staged_writes() {
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42 * 1_000_000 + 9,
            cut_marker: 0,
        };
        assert_eq!(snapshot.as_of_ordinal(), Some(42 * 1_000_000 + 9));
        assert!(snapshot.is_temporal());
        assert!(!snapshot.is_all_versions());
        assert!(!snapshot.is_current());
        assert!(snapshot.merges_txn_overlay());
        assert!(!snapshot.is_historical());
        assert_eq!(snapshot.marked_cut(), None);
        let marked = SystemTimeScope::TxnSnapshot {
            ordinal: 9,
            cut_marker: 4,
        };
        assert_eq!(marked.marked_cut(), Some((9, 4)));
    }

    #[test]
    fn only_current_and_txn_snapshot_merge_staged_writes() {
        assert!(SystemTimeScope::Current.merges_txn_overlay());
        assert!(!SystemTimeScope::AsOf(1).merges_txn_overlay());
        assert!(
            !SystemTimeScope::Pinned {
                ordinal: 1,
                cut_marker: 0
            }
            .merges_txn_overlay()
        );
        assert!(!SystemTimeScope::AllVersions.merges_txn_overlay());
        assert!(SystemTimeScope::AsOf(1).is_historical());
        assert!(!SystemTimeScope::Current.is_historical());
    }

    #[test]
    fn all_versions_is_temporal() {
        assert!(SystemTimeScope::AllVersions.is_temporal());
        assert!(SystemTimeScope::AllVersions.is_all_versions());
        assert!(!SystemTimeScope::AsOf(1).is_all_versions());
    }
}
