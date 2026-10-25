// SPDX-License-Identifier: BUSL-1.1

//! Recognising the refusals a read is *supposed* to get while a group is
//! still settling.
//!
//! A linearizable read proves leadership against a quorum before it answers,
//! so a node that has just started — or just restarted — refuses reads on any
//! range whose Raft group has not finished electing. The refusal is retriable
//! by contract, and a test that polls for a value must treat it as "not yet",
//! not as a failure. Panicking on it turns an ordinary startup window into a
//! flake, and one that lands only under load.

use std::time::{Duration, Instant};

/// Whether `err` is a retriable "this range has no leader yet" refusal.
///
/// Matches both shapes the gate produces: no leader is known for the group,
/// and no quorum confirmed leadership before the deadline. Anything else —
/// a syntax error, a missing collection, a constraint violation — is a real
/// failure and must not be swallowed.
pub fn is_no_serving_leader(err: &tokio_postgres::Error) -> bool {
    err.as_db_error()
        .is_some_and(|db| db.code().code() == nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER)
}

/// Run `query` until it stops being refused for want of a leader, and return
/// its value.
///
/// Polls only on [`is_no_serving_leader`]; every other error is returned to
/// the caller on the spot, so a genuine defect still fails the test at the
/// first attempt rather than after the deadline.
pub async fn read_once_a_leader_exists<F, Fut, T>(
    desc: &str,
    deadline: Duration,
    step: Duration,
    mut query: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tokio_postgres::Error>>,
{
    let start = Instant::now();
    loop {
        match query().await {
            Ok(value) => return value,
            Err(err) if is_no_serving_leader(&err) => {
                if start.elapsed() >= deadline {
                    panic!("timed out after {deadline:?} waiting for a leader to serve: {desc}");
                }
                tokio::time::sleep(step).await;
            }
            Err(err) => match err.as_db_error() {
                Some(db) => panic!("{desc}: {} {}", db.code().code(), db.message()),
                None => panic!("{desc}: {err}"),
            },
        }
    }
}
