// SPDX-License-Identifier: BUSL-1.1

//! Deliver an event to every side effect.
//!
//! Events from the ring and events WAL catch-up rebuilt take the same path:
//! DML audit, the hold of the event's AFTER and DEFINE EVENT actions, then
//! the watermark, CDC, streaming materialized views and CRDT sync. A
//! committed transaction's publish event is held for delivery to its topic
//! instead. The guard admits each event once, so an event both paths carry
//! is delivered once.
//!
//! Every replica holds an event's actions. Only the partition's owner fires
//! them, from a replicated cursor (see `trigger::lane`).
//!
//! The permission cache is not a side effect here. The permission step runs
//! on ring events by their core numbers, before delivery (see
//! `control::security::permission_tree::event_handler`).

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::CdcRouter;
use crate::event::sink_ledger::SinkEventKey;
use crate::event::types::WriteEvent;

use super::delivery::DeliveryGuard;

/// Deliver every event of `events` the guard admits, in order. Returns how
/// many were delivered.
///
/// `events` is a concrete slice, never a generic iterator: an `impl
/// IntoIterator` parameter held across the awaits below makes the spawned
/// consumer future fail its `Send` check.
pub async fn deliver_events(
    core_id: usize,
    events: &[WriteEvent],
    guard: &mut DeliveryGuard,
    shared_state: &Arc<SharedState>,
    cdc_router: &Arc<CdcRouter>,
) -> u64 {
    let mut delivered = 0u64;
    let mut held = Vec::new();
    for event in events {
        if !guard.admit(event) {
            continue;
        }
        deliver_event(core_id, event, shared_state, cdc_router, &mut held).await;
        delivered += 1;
    }
    // The batch's events are durable in the lane before the consumer's safe
    // LSN, and so its persisted watermark, passes them.
    crate::event::trigger::lane::hold_rows(shared_state, &held).await;
    delivered
}

/// Deliver one admitted event. Its trigger action row joins `held`.
async fn deliver_event(
    core_id: usize,
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    cdc_router: &Arc<CdcRouter>,
    held: &mut Vec<crate::event::trigger::lane::HeldRow>,
) {
    // A committed transaction's message is held for delivery to its topic.
    // It is no row, so no other side effect sees it.
    if event.op == crate::event::types::WriteOp::Publish {
        crate::event::topic::hold_committed_publish(event, shared_state).await;
    }
    if !event.op.is_data_event() {
        shared_state
            .watermark_tracker
            .advance_lsn_only(event.vshard_id.as_u32(), event.lsn.as_u64());
        return;
    }
    if let Some(fault) = event.image_fault {
        dead_letter_unrendered_image(event, fault, shared_state);
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
    // Every replica positions the event in apply order and holds it.
    if event_actions_required(event)
        && let Some(row) = crate::event::trigger::lane::action_row(event, shared_state, cdc_router)
    {
        held.push(row);
    }
    accumulate_data_event(event, key.as_ref(), shared_state, cdc_router);
}

/// Dead-letter an event whose row image the Data Plane could not render.
///
/// The stored row is corrupt, so no side effect can act on the write: a
/// trigger would bind a missing NEW or OLD row, a change stream would carry
/// a row without its image, and a view or CRDT peer would apply it as one.
/// The event runs none of them. A durable audit entry names the collection,
/// row, LSN and image, and the Data Plane already filed the recorder report.
fn dead_letter_unrendered_image(
    event: &WriteEvent,
    fault: crate::event::image_fault::ImageFault,
    shared_state: &SharedState,
) {
    let detail = format!(
        "event dead-lettered: the {} image of row '{}' in '{}' at LSN {} did not render; \
         no trigger, change stream, view or CRDT peer received the write",
        fault.as_str(),
        event.row_id.as_str(),
        event.collection,
        event.lsn.as_u64(),
    );
    tracing::error!(
        collection = %event.collection,
        row = event.row_id.as_str(),
        lsn = event.lsn.as_u64(),
        image = fault.as_str(),
        "{detail}"
    );
    shared_state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(event.tenant_id),
        Some(event.database_id),
        "event_plane",
        &detail,
    );
}

/// Whether `event` is a row write that triggers and event actions act on.
///
/// A graph edge write reaches the stream of the edge's collection for its
/// change streams. It is not a row of that collection, and its image is the
/// edge's properties, not a row map. The document write that mirrors an
/// implicit edge emits its own row event, which fires the collection's
/// triggers once.
fn event_actions_required(event: &WriteEvent) -> bool {
    event.op.is_data_event() && !matches!(event.row_id, crate::event::types::RowId::Edge(_))
}

/// Apply the side effects after the actions: CDC routing, the wall-time
/// watermark, streaming materialized views and CRDT sync packaging.
fn accumulate_data_event(
    event: &WriteEvent,
    key: Option<&SinkEventKey>,
    shared_state: &Arc<SharedState>,
    cdc_router: &Arc<CdcRouter>,
) {
    // Routing runs before the watermark moves: the late-data check compares
    // the event against the watermark of the events before it.
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
    let event_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    shared_state.watermark_tracker.advance(
        event.vshard_id.as_u32(),
        event.lsn.as_u64(),
        event_time_ms,
    );
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
            commit_hlc: Some(crate::event::test_utils::test_commit_hlc()),
            image_fault: None,
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

    #[tokio::test]
    async fn an_event_with_an_unrendered_image_is_dead_lettered() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_wal, _watermarks, shared_state, _dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);
        let mut faulted = event(WriteOp::Update);
        faulted.lsn = Lsn::new(9);
        faulted.new_value = Some(Arc::from(&[0x80u8][..]));
        faulted.image_fault = Some(crate::event::image_fault::ImageFault::Old);
        let mut guard = super::super::delivery::DeliveryGuard::new(Lsn::new(0));

        let delivered = super::deliver_events(
            0,
            std::slice::from_ref(&faulted),
            &mut guard,
            &shared_state,
            &cdc_router,
        )
        .await;

        assert_eq!(delivered, 1);
        let audit = shared_state.audit.lock().expect("read audit log");
        let entry = audit
            .all()
            .iter()
            .find(|entry| entry.detail.contains("event dead-lettered"))
            .expect("the dead-lettered event is audited");
        assert!(
            entry
                .detail
                .contains("old image of row 'row-1' in 'events' at LSN 9")
        );
        assert!(
            cdc_router.stream_buffers().is_empty(),
            "no change stream receives the write"
        );
    }

    #[test]
    fn an_edge_write_runs_no_row_actions() {
        let mut edge = event(WriteOp::Insert);
        edge.row_id = RowId::edge(
            crate::event::types::EdgeEndpoints {
                src: "a",
                src_surrogate: nodedb_types::Surrogate::new(1),
                dst: "b",
                dst_surrogate: nodedb_types::Surrogate::new(2),
            },
            "knows",
        );
        assert!(!event_actions_required(&edge));
    }
}
