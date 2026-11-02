// SPDX-License-Identifier: BUSL-1.1

//! The versions a RESTORE installs for one restored key.
//!
//! A columnar primary key or a vector row restores as one key. The backup
//! holds every version of the key with its `[sys_from, sys_to)` interval. The
//! resolve reads the destination's newest ordinal for the key at the
//! transaction's turn: the newest `sys_from` or `sys_to` any version holds.
//!
//! * The destination holds nothing: every backed-up version installs at its
//!   own interval.
//! * The destination's newest ordinal is above the RESTORE's cut: a write
//!   after the RESTORE began. The key keeps it, and nothing installs.
//! * Otherwise every backed-up version that starts above the destination's
//!   newest ordinal installs at its own interval. The first one ends the
//!   destination's current version at its `sys_from`.
//! * When no backed-up version starts above it, the backup's current state
//!   is set again once, one above the destination's newest ordinal and at
//!   least the transaction's ordinal. A destination already at the backup's
//!   newest ordinal, or absent where the backup ends absent, stays as it is.
//!
//! The outcome is the RESTORE applied whole at its cut, then every later
//! write of the key. An install never ends a version at an ordinal below its
//! start, so every interval the key holds stays well formed.

/// The interval of one backed-up version: `sys_to` is `None` while open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BackedUpSpan {
    pub sys_from: i64,
    pub sys_to: Option<i64>,
}

/// What a RESTORE installs for one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyRestore {
    /// Nothing installs.
    Keep,
    /// The backed-up versions from index `from` on install at their own
    /// intervals.
    Versions { from: usize },
    /// The backup's current state is set again at `at`. An open newest
    /// version installs open at `at`. A key the backup ends absent installs
    /// its newest version as `[at, at)`, which ends the destination's
    /// current version and is visible at no cut.
    SetAgain { at: i64 },
}

/// The plan for a key whose backed-up versions are `spans`, ascending by
/// `sys_from`. `dest_newest` is the destination's newest ordinal for the
/// key, and `dest_live` whether it holds a current version. `applied` is the
/// transaction's ordinal and `cut` the RESTORE's cut.
pub(super) fn plan_key(
    spans: &[BackedUpSpan],
    dest_newest: Option<i64>,
    dest_live: bool,
    applied: i64,
    cut: i64,
) -> KeyRestore {
    let Some(newest) = spans.last() else {
        return KeyRestore::Keep;
    };
    let Some(dest) = dest_newest else {
        return KeyRestore::Versions { from: 0 };
    };
    if dest > cut {
        return KeyRestore::Keep;
    }
    if let Some(from) = spans.iter().position(|span| span.sys_from > dest) {
        return KeyRestore::Versions { from };
    }
    let backup_newest = newest.sys_to.unwrap_or(newest.sys_from);
    let backup_live = newest.sys_to.is_none();
    if dest == backup_newest || (!backup_live && !dest_live) {
        return KeyRestore::Keep;
    }
    KeyRestore::SetAgain {
        at: applied.max(dest.saturating_add(1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPEN: i64 = i64::MAX;

    fn spans() -> Vec<BackedUpSpan> {
        vec![
            BackedUpSpan {
                sys_from: 10,
                sys_to: Some(20),
            },
            BackedUpSpan {
                sys_from: 20,
                sys_to: None,
            },
        ]
    }

    /// An empty destination takes every backed-up version.
    #[test]
    fn an_empty_destination_takes_every_version() {
        assert_eq!(
            plan_key(&spans(), None, false, 500, OPEN),
            KeyRestore::Versions { from: 0 }
        );
    }

    /// A destination version above the cut is a write after the RESTORE
    /// began: it stays current, and nothing installs.
    #[test]
    fn a_write_after_the_cut_survives() {
        assert_eq!(
            plan_key(&spans(), Some(301), true, 500, 300),
            KeyRestore::Keep
        );
    }

    /// Only the versions above the destination's newest ordinal install.
    #[test]
    fn versions_above_the_destination_install_at_their_stamps() {
        assert_eq!(
            plan_key(&spans(), Some(15), true, 500, 300),
            KeyRestore::Versions { from: 1 }
        );
        assert_eq!(
            plan_key(&spans(), Some(5), true, 500, 300),
            KeyRestore::Versions { from: 0 }
        );
    }

    /// A destination at or below the cut and above every backed-up version
    /// gets the backup's state again, above it and the transaction.
    #[test]
    fn a_newer_destination_below_the_cut_is_set_again() {
        assert_eq!(
            plan_key(&spans(), Some(250), true, 500, 300),
            KeyRestore::SetAgain { at: 500 }
        );
        assert_eq!(
            plan_key(&spans(), Some(250), true, 100, 300),
            KeyRestore::SetAgain { at: 251 }
        );
    }

    /// A destination at the backup's newest ordinal holds the backup's
    /// state: a second run of the batch installs nothing.
    #[test]
    fn a_destination_at_the_backups_newest_stays() {
        assert_eq!(
            plan_key(&spans(), Some(20), true, 500, 300),
            KeyRestore::Keep
        );
    }

    /// A key both ends absent stays absent. One the destination holds live
    /// is ended again at the stamp.
    #[test]
    fn an_absent_key_ends_the_destinations_version() {
        let ended = [BackedUpSpan {
            sys_from: 10,
            sys_to: Some(20),
        }];
        assert_eq!(
            plan_key(&ended, Some(30), false, 500, 300),
            KeyRestore::Keep
        );
        assert_eq!(
            plan_key(&ended, Some(30), true, 500, 300),
            KeyRestore::SetAgain { at: 500 }
        );
    }
}
