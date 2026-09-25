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
/// LSN reaches it.
pub(crate) async fn before_dispatch(plan: &PhysicalPlan, wal_lsn: Option<Lsn>) {
    if wal_lsn.is_none() {
        return;
    }
    let name = format!(
        "funnel::before_dispatch::{}",
        plan.collection().unwrap_or_default()
    );
    wait(&name).await;
}
