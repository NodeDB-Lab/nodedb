// SPDX-License-Identifier: BUSL-1.1

//! The age limit of a transaction snapshot.
//!
//! A block reads every statement at its snapshot. Every version the block
//! can see stays until no open snapshot is below it, so an old snapshot
//! holds history back. A statement of a block whose snapshot is older than
//! `[tuning.network] txn_snapshot_max_age_secs` fails with SQLSTATE `40001`,
//! and the client retries the block at a fresh snapshot. The cleanup horizon
//! relies on this bound for a node it cannot reach (`SnapshotBoard`).

use std::time::Duration;

use crate::control::state::SharedState;

/// Refuse a statement of a block whose snapshot `cut` is older than the
/// node's snapshot age limit, with [`crate::Error::TxnSnapshotTooOld`].
/// Call it before each statement of the block runs.
pub fn check_snapshot_age(state: &SharedState, cut: i64) -> crate::Result<()> {
    let limit = Duration::from_secs(state.tuning.network.txn_snapshot_max_age_secs);
    snapshot_age_within(cut, nodedb_types::wall_now_ns_i64(), limit)
}

/// The longest any statement reads at one snapshot: the age limit, plus the
/// longest statement deadline, plus the clock skew bound. A snapshot older
/// than this serves no statement on any node.
pub fn snapshot_max_hold(state: &SharedState) -> Duration {
    state.tuning.network.snapshot_max_hold()
}

fn snapshot_age_within(cut: i64, now_ns: i64, limit: Duration) -> crate::Result<()> {
    let age_ns = u64::try_from(now_ns.saturating_sub(cut)).unwrap_or(0);
    let age = Duration::from_nanos(age_ns);
    if age <= limit {
        return Ok(());
    }
    Err(crate::Error::TxnSnapshotTooOld {
        age_ms: u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
        limit_ms: u64::try_from(limit.as_millis()).unwrap_or(u64::MAX),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_NS: i64 = 1_000_000_000;

    #[test]
    fn a_snapshot_within_the_limit_runs() {
        let now = 1_000 * SECOND_NS;
        let limit = Duration::from_secs(300);
        snapshot_age_within(now - 299 * SECOND_NS, now, limit).expect("in time");
        snapshot_age_within(now - 300 * SECOND_NS, now, limit).expect("at the limit");
        snapshot_age_within(now + SECOND_NS, now, limit).expect("a cut ahead of the clock");
    }

    #[test]
    fn an_old_snapshot_fails_naming_its_age_and_the_limit() {
        let now = 1_000 * SECOND_NS;
        let error = snapshot_age_within(now - 301 * SECOND_NS, now, Duration::from_secs(300))
            .expect_err("too old");
        match &error {
            crate::Error::TxnSnapshotTooOld { age_ms, limit_ms } => {
                assert_eq!(*age_ms, 301_000);
                assert_eq!(*limit_ms, 300_000);
            }
            other => panic!("expected TxnSnapshotTooOld, got {other:?}"),
        }
        let message = error.to_string();
        assert!(
            message.contains("301000") && message.contains("300000"),
            "{message}"
        );
    }
}
