// SPDX-License-Identifier: BUSL-1.1

//! The system time an index search reads at.
//!
//! A vector, full-text, or spatial search reads its index at one system
//! time: the current state, a pinned cut, or a transaction block's snapshot.
//! An index holds no valid time, so a valid-time qualifier has nothing to
//! select on. A search ranks or matches one state of the collection, so
//! `AS OF SYSTEM TIME NULL` (every version) is no search input.

use nodedb_sql::temporal::{TemporalScope, ValidTime};
use nodedb_types::SystemTimeScope;

/// The system time a search of `collection` (`what` names the search)
/// reads at, from the statement's `temporal`.
///
/// Fails with `PlanError` on a valid-time qualifier and on
/// `AS OF SYSTEM TIME NULL`.
pub(in crate::control::planner::sql_plan_convert) fn search_system_time(
    temporal: &TemporalScope,
    collection: &str,
    what: &str,
) -> crate::Result<SystemTimeScope> {
    if !matches!(temporal.valid_time, ValidTime::Any) {
        return Err(crate::Error::PlanError {
            detail: format!(
                "{what} on '{collection}' cannot read AS OF VALID TIME: the index holds no \
                 valid time; filter the valid-time columns in WHERE"
            ),
        });
    }
    match temporal.system_time {
        scope @ (SystemTimeScope::Current
        | SystemTimeScope::AsOf(_)
        | SystemTimeScope::Pinned { .. }
        | SystemTimeScope::TxnSnapshot { .. }) => Ok(scope),
        SystemTimeScope::AllVersions => Err(crate::Error::PlanError {
            detail: format!(
                "{what} on '{collection}' cannot read AS OF SYSTEM TIME NULL: a search reads \
                 one state of the collection; name a system time"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporal(system_time: SystemTimeScope, valid_time: ValidTime) -> TemporalScope {
        TemporalScope {
            system_time,
            valid_time,
        }
    }

    #[test]
    fn a_system_time_passes_through() {
        for scope in [
            SystemTimeScope::Current,
            SystemTimeScope::AsOf(5),
            SystemTimeScope::Pinned {
                ordinal: 9,
                cut_marker: 1,
            },
            SystemTimeScope::TxnSnapshot {
                ordinal: 9,
                cut_marker: 1,
            },
        ] {
            assert_eq!(
                search_system_time(&temporal(scope, ValidTime::Any), "c", "vector search")
                    .expect("a system time reads"),
                scope
            );
        }
    }

    #[test]
    fn valid_time_and_all_versions_are_refused() {
        assert!(matches!(
            search_system_time(
                &temporal(SystemTimeScope::Current, ValidTime::At(3)),
                "c",
                "text search"
            ),
            Err(crate::Error::PlanError { .. })
        ));
        assert!(matches!(
            search_system_time(
                &temporal(SystemTimeScope::AllVersions, ValidTime::Any),
                "c",
                "spatial scan"
            ),
            Err(crate::Error::PlanError { .. })
        ));
    }
}
