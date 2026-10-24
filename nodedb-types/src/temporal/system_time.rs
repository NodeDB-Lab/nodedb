// SPDX-License-Identifier: Apache-2.0

//! System-time scope for bitemporal scans.
//!
//! An explicit enum makes the "all versions" (audit-log) mode and the
//! statement-pinned cut representable, with no out-of-band sentinel.

use serde::{Deserialize, Serialize};

/// System-time selection for a temporal scan.
///
/// `AsOf` carries the statement's time in milliseconds since Unix epoch.
/// `Pinned` carries the system-time ordinal the Control Plane pinned for
/// one statement. Valid time is a separate axis and is unaffected by this
/// enum.
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

    /// The point-in-time cutoff in milliseconds, if this is a point-in-time
    /// scope. An engine that stamps system time in milliseconds reads at
    /// this value. A pinned scope answers the newest millisecond whose every
    /// ordinal is at or below its ordinal. A write in the cut's own
    /// millisecond can land after the cut, so a cut inside a millisecond
    /// reads the millisecond before it, and every read of the statement
    /// reads the same rows.
    pub fn as_of_ms(&self) -> Option<i64> {
        match self {
            Self::AsOf(ms) => Some(*ms),
            Self::Pinned { ordinal, .. } => Some(whole_ms_at_or_below(*ordinal)),
            Self::Current | Self::AllVersions => None,
        }
    }

    /// The point-in-time cutoff as a system-time ordinal. Version keys
    /// compare against this value. `AsOf` answers the newest ordinal a
    /// write in the cutoff millisecond can carry. `Pinned` answers its
    /// ordinal.
    pub fn as_of_ordinal(&self) -> Option<i64> {
        match self {
            Self::AsOf(ms) => Some(super::ordinal::ms_to_ordinal_upper(*ms)),
            Self::Pinned { ordinal, .. } => Some(*ordinal),
            Self::Current | Self::AllVersions => None,
        }
    }

    /// The pinned ordinal and the cut marker that caps it, when a marker
    /// caps a pinned scope.
    pub const fn marked_cut(&self) -> Option<(i64, u64)> {
        match self {
            Self::Pinned {
                ordinal,
                cut_marker,
            } if *cut_marker != 0 => Some((*ordinal, *cut_marker)),
            Self::Pinned { .. } | Self::Current | Self::AsOf(_) | Self::AllVersions => None,
        }
    }

    /// True when this scope requests every system-time version.
    pub const fn is_all_versions(&self) -> bool {
        matches!(self, Self::AllVersions)
    }
}

/// The newest millisecond whose every ordinal is at or below `ordinal`.
fn whole_ms_at_or_below(ordinal: i64) -> i64 {
    let ms = super::ordinal::ordinal_to_ms(ordinal);
    if super::ordinal::ms_to_ordinal_upper(ms) <= ordinal {
        ms
    } else {
        ms - 1
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
    fn as_of_exposes_ms() {
        assert_eq!(SystemTimeScope::AsOf(42).as_of_ms(), Some(42));
        assert_eq!(SystemTimeScope::Current.as_of_ms(), None);
        assert_eq!(SystemTimeScope::AllVersions.as_of_ms(), None);
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
        assert_eq!(
            pinned.as_of_ms(),
            Some(41),
            "a cut inside 42 reads up to 41"
        );
        let whole = SystemTimeScope::Pinned {
            ordinal: 42 * 1_000_000 + 999_999,
            cut_marker: 0,
        };
        assert_eq!(
            whole.as_of_ms(),
            Some(42),
            "a cut at the end of 42 reads 42"
        );
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
    fn all_versions_is_temporal() {
        assert!(SystemTimeScope::AllVersions.is_temporal());
        assert!(SystemTimeScope::AllVersions.is_all_versions());
        assert!(!SystemTimeScope::AsOf(1).is_all_versions());
    }
}
