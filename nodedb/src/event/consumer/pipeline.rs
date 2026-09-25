// SPDX-License-Identifier: BUSL-1.1

//! Deliver an event to every side effect.
//!
//! Events from the ring and events WAL catch-up rebuilt take the same path:
//! DML audit, the awaited AFTER and DEFINE EVENT actions, then the watermark,
//! CDC, streaming materialized views and CRDT sync. The guard admits each
//! event once, so an event both paths carry is delivered once.
//!
//! The permission cache is not a side effect here. The permission step runs
//! on ring events by their core numbers, before delivery (see
//! `control::security::permission_tree::event_handler`).

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::action::ActionRetryQueue;
use crate::event::cdc::CdcRouter;
use crate::event::sink_ledger::SinkEventKey;
use crate::event::types::WriteEvent;

use super::delivery::DeliveryGuard;

/// Deliver every event of `events` the guard admits, in order. Returns how
/// many were delivered.
pub async fn deliver_events<'a>(
    core_id: usize,
    events: impl IntoIterator<Item = &'a WriteEvent>,
    guard: &mut DeliveryGuard,
    shared_state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
    cdc_router: &Arc<CdcRouter>,
) -> u64 {
    let mut delivered = 0u64;
    for event in events {
        if !guard.admit(event) {
            continue;
        }
        deliver_event(core_id, event, shared_state, retry_queue, cdc_router).await;
        delivered += 1;
    }
    delivered
}

/// Deliver one admitted event.
async fn deliver_event(
    core_id: usize,
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
    cdc_router: &Arc<CdcRouter>,
) {
    if !event.op.is_data_event() {
        shared_state
            .watermark_tracker
            .advance_lsn_only(event.vshard_id.as_u32(), event.lsn.as_u64());
        return;
    }
    // The key the non-idempotent sinks remember the event by, so an event
    // delivered again after a restart reaches each of them once.
    let key = SinkEventKey::of(core_id, event);
    // Recorded before the actions run, as the statement's audit precedes its
    // triggers.
    crate::event::audit_dml::audit_dml_event(event, shared_state, key.as_ref());
    // Every action finishes before the watermark or any other side effect
    // moves past the event.
    dispatch_event_actions(event, shared_state, retry_queue).await;
    accumulate_data_event(event, key.as_ref(), shared_state, cdc_router);
}

/// Run the AFTER-ROW triggers and DEFINE EVENT actions of a data event.
pub async fn dispatch_event_actions(
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
) {
    if !event_actions_required(event) {
        return;
    }
    crate::event::trigger::dispatcher::dispatch_triggers(event, shared_state, retry_queue).await;
    crate::control::event_trigger::process_write_event(
        Arc::clone(shared_state),
        event,
        retry_queue,
    )
    .await;
}

fn event_actions_required(event: &WriteEvent) -> bool {
    event.op.is_data_event()
}

/// Apply the side effects after the actions: the wall-time watermark, CDC
/// routing, streaming materialized views and CRDT sync packaging.
fn accumulate_data_event(
    event: &WriteEvent,
    key: Option<&SinkEventKey>,
    shared_state: &Arc<SharedState>,
    cdc_router: &Arc<CdcRouter>,
) {
    let event_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    shared_state.watermark_tracker.advance(
        event.vshard_id.as_u32(),
        event.lsn.as_u64(),
        event_time_ms,
    );

    match shared_state.sink_ledgers.get() {
        Some(ledgers) => {
            ledgers
                .cdc
                .route_once(key, cdc_router, event, &shared_state.watermark_tracker)
        }
        // Without its ledger (the sink state did not load, and the node is
        // stopping) a change stream is not exactly-once across a restart.
        None => cdc_router.route_event(event, &shared_state.watermark_tracker),
    }
    let matching_streams = shared_state.stream_registry.find_matching(
        event.database_id,
        event.tenant_id.as_u64(),
        &event.collection,
    );
    if !matching_streams.is_empty() {
        shared_state.mv_registry.applied().apply_once(key, || {
            for stream_def in &matching_streams {
                crate::event::streaming_mv::processor::process_write_event_for_mvs(
                    event,
                    &shared_state.mv_registry,
                    &stream_def.name,
                );
            }
        });
    }
    shared_state.delta_packager.package_and_enqueue(
        event,
        key,
        shared_state.sink_ledgers.get().map(|ledgers| &ledgers.crdt),
        &shared_state.crdt_sync_delivery,
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::event_actions_required;
    use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    fn event(op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 1,
            collection: Arc::from("events"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
            lsn: Lsn::new(1),
            record: None,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    #[test]
    fn every_data_event_runs_its_actions() {
        assert!(event_actions_required(&event(WriteOp::Insert)));
        assert!(event_actions_required(&event(WriteOp::BulkDelete {
            count: 2
        })));
        assert!(!event_actions_required(&event(WriteOp::Heartbeat)));
    }
}
