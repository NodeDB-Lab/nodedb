// SPDX-License-Identifier: BUSL-1.1

//! Async fail-point gates for Control-Plane code.
//!
//! A crash test sometimes needs one request parked at a precise point while
//! every other request proceeds. A synchronous fail point cannot do that: it
//! blocks the thread, and every task that shares the thread stalls with it.
//! The gate here awaits a file with a Tokio timer, so only the task that
//! reached it waits. The test creates the file to release it.
//!
//! Compiled only with the `failpoints` feature, and called only from async
//! Control-Plane code, never from the Data Plane.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::fail_point::{FailAction, lookup};
use crate::types::Lsn;

/// How often a parked task checks for its release file.
const GATE_POLL: Duration = Duration::from_millis(20);

/// Park until the file armed for `name` with `wait_file(<path>)` exists. No-op
/// when nothing is armed for `name`.
pub(crate) async fn wait(name: &str) {
    let Some(FailAction::WaitForFile(path)) = lookup(name) else {
        return;
    };
    while !path.exists() {
        tokio::time::sleep(GATE_POLL).await;
    }
}

/// The write funnel's gate: after a logged write's WAL record is appended and
/// before any core holds the request. Named per collection,
/// `funnel::before_dispatch::<collection>`, and only a write carrying a WAL
/// LSN reaches it. A committed redo parks on the gate of each collection it
/// writes.
///
/// `funnel::before_dispatch::node<N>::<collection>` parks the write only on
/// node `N`. An in-process cluster test shares one fail-point registry across
/// its nodes, so it names the node to hold one replica's apply.
pub(crate) async fn before_dispatch(node_id: u64, plan: &PhysicalPlan, wal_lsn: Option<Lsn>) {
    if wal_lsn.is_none() {
        return;
    }
    for collection in plan.named_collections() {
        wait(&format!("funnel::before_dispatch::{collection}")).await;
        wait(&format!(
            "funnel::before_dispatch::node{node_id}::{collection}"
        ))
        .await;
    }
}

/// The Calvin scheduler's flush gate: after a committed transaction's redo
/// record is appended and before its flush reaches a core. Named per
/// collection, `calvin::before_flush::<collection>`.
///
/// The scheduler cannot park on a timer, so the gate answers without waiting:
/// `true` while the file armed for one of the flush's collections is absent.
/// The scheduler then keeps the flush in its re-send queue and asks again on
/// its next pass.
pub(crate) fn holds_flush(plan: &PhysicalPlan) -> bool {
    plan.named_collections().iter().any(|collection| {
        matches!(
            lookup(&format!("calvin::before_flush::{collection}")),
            Some(FailAction::WaitForFile(path)) if !path.exists()
        )
    })
}
