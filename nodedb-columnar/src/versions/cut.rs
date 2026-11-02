// SPDX-License-Identifier: Apache-2.0

//! The system-time cut a read selects row versions at.

use nodedb_types::SystemTimeScope;

/// Which row versions a read sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCut {
    /// The newest version of each key: rows no write has ended.
    Current,
    /// The versions visible at this system-time ordinal ([`version_visible`]).
    At(i64),
    /// Every retained version, ended ones included. A purged version is gone.
    All,
}

impl VersionCut {
    /// The cut a read at `scope` selects. `AsOf` reads at the last ordinal
    /// of its millisecond. `Pinned` reads at its ordinal.
    pub fn of_scope(scope: SystemTimeScope) -> Self {
        match scope {
            SystemTimeScope::Current => Self::Current,
            SystemTimeScope::AllVersions => Self::All,
            SystemTimeScope::AsOf(_)
            | SystemTimeScope::Pinned { .. }
            | SystemTimeScope::TxnSnapshot { .. } => {
                scope.as_of_ordinal().map_or(Self::Current, Self::At)
            }
        }
    }

    /// The ordinal of an `At` cut. `None` for `Current` and `All`.
    pub fn ordinal(self) -> Option<i64> {
        match self {
            Self::At(at) => Some(at),
            Self::Current | Self::All => None,
        }
    }
}

/// Whether a version created at `sys_from` and ended at `sys_to` (`None`
/// while no write ended it) is visible at ordinal `at`.
///
/// Visible iff `sys_from <= at` and the version is not ended at or below
/// `at`.
pub fn version_visible(sys_from: i64, sys_to: Option<i64>, at: i64) -> bool {
    sys_from <= at && sys_to.is_none_or(|to| to > at)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_is_visible_from_its_stamp_until_its_end() {
        assert!(!version_visible(10, None, 9));
        assert!(version_visible(10, None, 10));
        assert!(version_visible(10, Some(20), 19));
        assert!(!version_visible(10, Some(20), 20));
        assert!(!version_visible(10, Some(10), 10), "an empty interval");
    }

    #[test]
    fn a_scope_maps_to_its_cut() {
        assert_eq!(
            VersionCut::of_scope(SystemTimeScope::Current),
            VersionCut::Current
        );
        assert_eq!(
            VersionCut::of_scope(SystemTimeScope::AllVersions),
            VersionCut::All
        );
        assert_eq!(
            VersionCut::of_scope(SystemTimeScope::Pinned {
                ordinal: 42,
                cut_marker: 0
            }),
            VersionCut::At(42)
        );
    }
}
