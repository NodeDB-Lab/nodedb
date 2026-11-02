// SPDX-License-Identifier: BUSL-1.1

//! The cut a predicate write matches its rows at.
//!
//! A predicate write outside a transaction block matches the current state.
//! Inside a block it matches the rows visible at the block's snapshot, with
//! the block's staged writes merged on top (`SystemTimeScope::TxnSnapshot`).
//! A write never matches committed history: `AS OF SYSTEM TIME` selects
//! rows to read, never rows to write.

use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::ErrorCode;

/// The ordinal a predicate write's match reads at. `None` reads the current
/// state. `Some(ordinal)` reads the versions visible at `ordinal`.
///
/// `what` names the write kind, for the error a historical scope answers.
pub(in crate::data::executor) fn write_match_cut(
    system_time: &SystemTimeScope,
    what: &str,
) -> Result<Option<i64>, ErrorCode> {
    match system_time {
        SystemTimeScope::Current => Ok(None),
        SystemTimeScope::TxnSnapshot { ordinal, .. } => Ok(Some(*ordinal)),
        SystemTimeScope::AsOf(_)
        | SystemTimeScope::Pinned { .. }
        | SystemTimeScope::AllVersions => Err(ErrorCode::Unsupported {
            detail: format!(
                "{what} matches rows at the current state or at its transaction's snapshot, \
                     never at {system_time:?}"
            ),
        }),
    }
}

/// The current-state cut an autocommit predicate write matches at. A write
/// that carries a transaction snapshot outside its transaction's staging has
/// lost the overlay its snapshot reads, so it is refused.
pub(in crate::data::executor) fn autocommit_match_cut(
    system_time: &SystemTimeScope,
    what: &str,
) -> Result<(), ErrorCode> {
    match write_match_cut(system_time, what)? {
        None => Ok(()),
        Some(ordinal) => Err(ErrorCode::Internal {
            detail: format!(
                "{what} carries the transaction snapshot {ordinal} outside its transaction's \
                 staging; a block's predicate write stages, it never applies directly"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_matches_current_state_or_the_block_snapshot() {
        assert_eq!(
            write_match_cut(&SystemTimeScope::Current, "UPDATE"),
            Ok(None)
        );
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 3,
        };
        assert_eq!(write_match_cut(&snapshot, "UPDATE"), Ok(Some(42)));
        for historical in [
            SystemTimeScope::AsOf(7),
            SystemTimeScope::Pinned {
                ordinal: 9,
                cut_marker: 0,
            },
            SystemTimeScope::AllVersions,
        ] {
            assert!(write_match_cut(&historical, "UPDATE").is_err());
        }
    }

    #[test]
    fn an_autocommit_write_refuses_a_snapshot() {
        assert_eq!(
            autocommit_match_cut(&SystemTimeScope::Current, "DELETE"),
            Ok(())
        );
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 0,
        };
        assert!(autocommit_match_cut(&snapshot, "DELETE").is_err());
    }
}
