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
///
/// On arrival the gate creates `<path>.parked`, so a test can wait until the
/// task has reached the gate before it acts.
pub(crate) async fn wait(name: &str) {
    let Some(FailAction::WaitForFile(path)) = lookup(name) else {
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

/// The write funnel's gate: after a logged write's WAL record is appended and
/// before any core holds the request. Named per collection,
/// `funnel::before_dispatch::<collection>`, and only a write carrying a WAL
/// LSN reaches it. A committed redo parks on the gate of each collection it
/// writes. A replicated write parks inside its apply entry's enqueue, so
/// every later entry of its Raft group waits for it. Only a write of another
/// group applies while it is parked.
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

/// The Calvin submit gate: after a transaction's collection incarnations are
/// stamped and before the sequencer admits it. Named per collection,
/// `calvin::after_stamp::<collection>`. A test parks a transaction here, then
/// changes a collection it names before the transaction reaches a replica.
pub(crate) async fn after_calvin_stamp(tx_class: &nodedb_cluster::calvin::types::TxClass) {
    for named in &tx_class.incarnations {
        wait(&format!("calvin::after_stamp::{}", named.collection)).await;
    }
}

/// The Calvin scheduler's redo gate: after a committed slice's redo is
/// resolved and before the data-group leader proposes it. Named per
/// collection, `calvin::before_redo_propose::<collection>`.
///
/// The scheduler cannot park on a timer, so the gate answers without waiting:
/// `true` while the file armed for one of `collections` is absent. The
/// scheduler then keeps the slice unproposed and asks again on its next stall
/// tick. On its first hold the gate creates `<path>.parked`, so a test can
/// wait until the slice is held.
pub(crate) fn holds_redo_propose(collections: &[String]) -> bool {
    collections
        .iter()
        .any(|collection| holds(&format!("calvin::before_redo_propose::{collection}")))
}

/// The Calvin install gate: after a stamped redo's install is durable and
/// before its `RedoApplied` event reaches the vShard's scheduler. Named per
/// collection, `calvin::before_redo_applied_push::<collection>`.
///
/// The apply loop cannot park here without holding the entry back. Returns
/// the name of the gate that holds one of `collections`: its armed file is
/// absent. The caller hands the event to its own task, which waits on
/// [`released`]. On its first hold the gate creates `<path>.parked`.
pub(crate) fn holds_redo_applied_push(collections: &[String]) -> Option<String> {
    collections
        .iter()
        .map(|collection| format!("calvin::before_redo_applied_push::{collection}"))
        .find(|name| holds(name))
}

/// Resolves once gate `name` stops holding: its armed file exists, or
/// nothing is armed for it.
pub(crate) async fn released(name: &str) {
    while armed_and_absent(name).is_some() {
        tokio::time::sleep(GATE_POLL).await;
    }
}

/// The file armed for `name` with `wait_file(<path>)`, while it is absent.
fn armed_and_absent(name: &str) -> Option<std::path::PathBuf> {
    let Some(FailAction::WaitForFile(path)) = lookup(name) else {
        return None;
    };
    (!path.exists()).then_some(path)
}

/// Whether gate `name` holds now: its armed file is absent. On its first
/// hold it creates `<path>.parked`.
fn holds(name: &str) -> bool {
    let Some(path) = armed_and_absent(name) else {
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
/// `publish::before_delivery::node<N>` before a delivery pass sends anything,
/// and `publish::before_cursor_commit::node<N>` after a partition's messages
/// are sent and before its cursor is committed. `point` names the gate.
///
/// Returns `false` when the node shuts down while parked: the pass then stops
/// where it is, as a node killed at that point does.
pub(crate) async fn publish_delivery(
    node_id: u64,
    point: &str,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> bool {
    delivery_gate(&format!("publish::{point}::node{node_id}"), shutdown).await
}

/// The trigger action lane's firing gates of node `node_id`:
/// `trigger::before_firing::node<N>` before a firing pass fires anything,
/// and `trigger::before_cursor_commit::node<N>` after a partition's actions
/// fired and before its cursor is committed. `point` names the gate.
///
/// Returns `false` when the node shuts down while parked.
pub(crate) async fn action_firing(
    node_id: u64,
    point: &str,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> bool {
    delivery_gate(&format!("trigger::{point}::node{node_id}"), shutdown).await
}

/// Park on gate `name` until it is released or the node shuts down.
async fn delivery_gate(name: &str, mut shutdown: tokio::sync::watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        () = wait(name) => true,
        _ = shutdown.changed() => false,
    }
}

/// The metadata apply gate of node `node_id`, named by
/// [`crate::control::cluster::metadata_applier::metadata_apply_hold_point`].
///
/// The metadata applier runs on the Raft loop and cannot park, so the gate
/// answers without waiting: `true` while the file armed for it is absent.
/// On its first hold the gate creates `<path>.parked`, so a test can wait
/// until the node's apply is held.
pub(crate) fn holds_metadata_apply(node_id: u64) -> bool {
    holds(&crate::control::cluster::metadata_applier::metadata_apply_hold_point(node_id))
}
