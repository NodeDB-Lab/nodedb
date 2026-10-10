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
use crate::fail_point::{FailAction, FailScope, lookup};
use crate::types::Lsn;

/// How often a parked task checks for its release file.
const GATE_POLL: Duration = Duration::from_millis(20);

/// Park until the file armed for `name` in `scope` with `wait_file(<path>)`
/// exists. No-op when nothing is armed for `name` in `scope`.
///
/// On arrival the gate creates `<path>.parked`, so a test can wait until the
/// task has reached the gate before it acts.
pub(crate) async fn wait(scope: FailScope, name: &str) {
    let Some(FailAction::WaitForFile(path)) = lookup(scope, name) else {
        return;
    };
    let mut parked = path.clone().into_os_string();
    parked.push(".parked");
    if let Err(e) = std::fs::write(&parked, name) {
        tracing::warn!(gate = name, error = %e, "fail gate parked marker not created");
    }
    while !path.exists() {
        tokio::time::sleep(GATE_POLL).await;
    }
}

/// The write funnel's gate on node `node_id`: after a logged write's WAL
/// record is appended and before any core holds the request. Named per
/// collection, `funnel::before_dispatch::<collection>`, and only a write
/// carrying a WAL LSN reaches it. A committed redo parks on the gate of each
/// collection it writes. A replicated write parks inside its apply entry's
/// enqueue, so every later entry of its Raft group waits for it. Only a write
/// of another group applies while it is parked.
///
/// A test arms the gate for one node to hold one replica's apply.
pub(crate) async fn before_dispatch(node_id: u64, plan: &PhysicalPlan, wal_lsn: Option<Lsn>) {
    if wal_lsn.is_none() {
        return;
    }
    for collection in plan.named_collections() {
        wait(
            FailScope::Node(node_id),
            &format!("funnel::before_dispatch::{collection}"),
        )
        .await;
    }
}

/// The Calvin submit gate of node `node_id`: after a transaction's collection
/// incarnations are stamped and before the sequencer admits it. Named per
/// collection, `calvin::after_stamp::<collection>`. A test parks a
/// transaction here, then changes a collection it names before the
/// transaction reaches a replica.
pub(crate) async fn after_calvin_stamp(
    node_id: u64,
    tx_class: &nodedb_cluster::calvin::types::TxClass,
) {
    for named in &tx_class.incarnations {
        wait(
            FailScope::Node(node_id),
            &format!("calvin::after_stamp::{}", named.collection),
        )
        .await;
    }
}

/// The Calvin scheduler's redo gate on node `node_id`: after a committed
/// slice's redo is resolved and before the data-group leader proposes it.
/// Named per collection, `calvin::before_redo_propose::<collection>`.
///
/// The scheduler cannot park on a timer, so the gate answers without waiting:
/// `true` while the file armed for one of `collections` is absent. The
/// scheduler then keeps the slice unproposed and asks again on its next stall
/// tick. On its first hold the gate creates `<path>.parked`, so a test can
/// wait until the slice is held.
pub(crate) fn holds_redo_propose(node_id: u64, collections: &[String]) -> bool {
    collections.iter().any(|collection| {
        holds(
            FailScope::Node(node_id),
            &format!("calvin::before_redo_propose::{collection}"),
        )
    })
}

/// The Calvin install gate on node `node_id`: after a stamped redo's install
/// is durable and before its `RedoApplied` event reaches the vShard's
/// scheduler. Named per collection,
/// `calvin::before_redo_applied_push::<collection>`.
///
/// The apply loop cannot park here without holding the entry back. Returns
/// the name of the gate that holds one of `collections`: its armed file is
/// absent. The caller hands the event to its own task, which waits on
/// [`released`] in the same scope. On its first hold the gate creates
/// `<path>.parked`.
pub(crate) fn holds_redo_applied_push(node_id: u64, collections: &[String]) -> Option<String> {
    collections
        .iter()
        .map(|collection| format!("calvin::before_redo_applied_push::{collection}"))
        .find(|name| holds(FailScope::Node(node_id), name))
}

/// Resolves once gate `name` in `scope` stops holding: its armed file
/// exists, or nothing is armed for it.
pub(crate) async fn released(scope: FailScope, name: &str) {
    while armed_and_absent(scope, name).is_some() {
        tokio::time::sleep(GATE_POLL).await;
    }
}

/// The file armed for `name` in `scope` with `wait_file(<path>)`, while it
/// is absent.
fn armed_and_absent(scope: FailScope, name: &str) -> Option<std::path::PathBuf> {
    let Some(FailAction::WaitForFile(path)) = lookup(scope, name) else {
        return None;
    };
    (!path.exists()).then_some(path)
}

/// Whether gate `name` holds in `scope` now: its armed file is absent. On
/// its first hold it creates `<path>.parked`.
fn holds(scope: FailScope, name: &str) -> bool {
    let Some(path) = armed_and_absent(scope, name) else {
        return false;
    };
    let mut parked = path.into_os_string();
    parked.push(".parked");
    let parked = std::path::PathBuf::from(parked);
    if !parked.exists()
        && let Err(e) = std::fs::write(&parked, name)
    {
        tracing::warn!(gate = %name, error = %e, "fail gate parked marker not created");
    }
    true
}

/// The committed-message delivery gates of node `node_id`:
/// `publish::before_delivery` before a delivery pass sends anything, and
/// `publish::before_cursor_commit` after a partition's messages are sent and
/// before its cursor is committed. `point` names the gate.
///
/// Returns `false` when the node shuts down while parked: the pass then stops
/// where it is, as a node killed at that point does.
pub(crate) async fn publish_delivery(
    node_id: u64,
    point: &str,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> bool {
    delivery_gate(node_id, &format!("publish::{point}"), shutdown).await
}

/// The trigger action lane's firing gates of node `node_id`:
/// `trigger::before_firing` before a firing pass fires anything, and
/// `trigger::before_cursor_commit` after a partition's actions fired and
/// before its cursor is committed. `point` names the gate.
///
/// Returns `false` when the node shuts down while parked.
pub(crate) async fn action_firing(
    node_id: u64,
    point: &str,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> bool {
    delivery_gate(node_id, &format!("trigger::{point}"), shutdown).await
}

/// Park on gate `name` of node `node_id` until it is released or the node
/// shuts down.
async fn delivery_gate(
    node_id: u64,
    name: &str,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        () = wait(FailScope::Node(node_id), name) => true,
        _ = shutdown.changed() => false,
    }
}

/// The metadata apply gate of node `node_id`, named
/// [`crate::control::cluster::metadata_applier::METADATA_APPLY_HOLD_POINT`].
///
/// The metadata applier runs on the Raft loop and cannot park, so the gate
/// answers without waiting: `true` while the file armed for it is absent.
/// On its first hold the gate creates `<path>.parked`, so a test can wait
/// until the node's apply is held.
pub(crate) fn holds_metadata_apply(node_id: u64) -> bool {
    holds(
        FailScope::Node(node_id),
        crate::control::cluster::metadata_applier::METADATA_APPLY_HOLD_POINT,
    )
}
