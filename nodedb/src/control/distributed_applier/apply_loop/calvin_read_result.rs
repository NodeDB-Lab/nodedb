// SPDX-License-Identifier: BUSL-1.1

//! Forward a committed `ReplicatedWrite::CalvinReadResult` entry to the local
//! Calvin scheduler's read-result channel for its target vShard.
//!
//! A read result is forwarded to an in-memory Calvin scheduler and writes
//! nothing durable, so the caller neither advances the applied prefix nor
//! breaks it for this entry. Advancing on it would assert a redo record that
//! does not exist; breaking on it would stall the floor behind an entry that a
//! re-delivery could not usefully replay anyway — the epoch it belongs to does
//! not survive a restart — and force every later write in the batch to be
//! applied twice on the next boot.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::types::TenantId;

/// Fields extracted from a `ReplicatedWrite::CalvinReadResult` entry.
pub(super) struct CalvinReadResultFields<'a> {
    pub target_vshard: u32,
    pub epoch: u64,
    pub position: u32,
    pub passive_vshard: u32,
    pub tenant_id: u64,
    pub values: &'a [u8],
}

/// Decode `fields.values`, forward the resulting [`ReadResultEvent`] to the
/// scheduler registered for `fields.target_vshard`, and complete the propose
/// waiter.
pub(super) fn forward_calvin_read_result(
    tracker: &Arc<ProposeTracker>,
    calvin_read_result_senders: &Arc<Mutex<BTreeMap<u32, mpsc::Sender<ReadResultEvent>>>>,
    pos: AppliedPosition,
    fields: CalvinReadResultFields<'_>,
) {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
    } = pos;

    let decoded_values: Vec<(
        nodedb_physical::physical_plan::meta::PassiveReadKeyId,
        nodedb_types::Value,
    )> = match zerompk::from_msgpack(fields.values) {
        Ok(decoded) => decoded,
        Err(e) => {
            tracing::warn!(
                group_id,
                index = log_index,
                error = %e,
                "failed to decode CalvinReadResult payload"
            );
            tracker.complete(
                group_id,
                log_index,
                applied_key,
                Err(crate::Error::Internal {
                    detail: format!("decode CalvinReadResult payload: {e}"),
                }),
            );
            // Prefix-neutral, like the forward below: a read result mints no
            // durable state either way, so there is nothing a re-delivery
            // could restore and nothing later entries must wait behind.
            return;
        }
    };

    let event = ReadResultEvent {
        epoch: fields.epoch,
        position: fields.position,
        passive_vshard: fields.passive_vshard,
        tenant_id: TenantId::new(fields.tenant_id),
        values: decoded_values,
    };

    let send_result = calvin_read_result_senders
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&fields.target_vshard)
        .cloned()
        .map(|sender| sender.try_send(event));

    if let Some(Err(e)) = send_result {
        tracing::warn!(
            group_id,
            index = log_index,
            error = %e,
            "failed to forward CalvinReadResult to scheduler"
        );
    }
    tracker.complete(
        group_id,
        log_index,
        applied_key,
        Ok(AppliedWrite::unversioned(Vec::new())),
    );
}
