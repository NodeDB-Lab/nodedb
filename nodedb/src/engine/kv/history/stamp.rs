// SPDX-License-Identifier: BUSL-1.1

//! The stamp rule a write applies to a key that already holds a version.

/// What a write at `ordinal` does to a stored version that starts at
/// `sys_from`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionStamp {
    /// The write carries the stored version's own ordinal: the same statement
    /// or transaction writes the key again. The write replaces the version in
    /// place. No read at any ordinal saw the replaced value.
    SameVersion,
    /// The write starts a new version at this ordinal. The stored version
    /// ends there.
    NewVersion(i64),
}

impl VersionStamp {
    /// The stamp of a write at `ordinal` over a version starting at
    /// `sys_from`. A write below the stored version stamps one above it, so
    /// every version of a key sorts after the versions before it.
    pub fn of(ordinal: i64, sys_from: i64) -> Self {
        if ordinal == sys_from {
            Self::SameVersion
        } else if ordinal > sys_from {
            Self::NewVersion(ordinal)
        } else {
            Self::NewVersion(sys_from.saturating_add(1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_later_write_starts_a_new_version_at_its_ordinal() {
        assert_eq!(VersionStamp::of(10, 5), VersionStamp::NewVersion(10));
    }

    #[test]
    fn the_same_ordinal_replaces_in_place() {
        assert_eq!(VersionStamp::of(5, 5), VersionStamp::SameVersion);
    }

    #[test]
    fn an_older_ordinal_stamps_one_above_the_stored_version() {
        assert_eq!(VersionStamp::of(3, 5), VersionStamp::NewVersion(6));
    }
}
