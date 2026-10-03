// SPDX-License-Identifier: BUSL-1.1

//! Write events of a timeseries ingest.
//!
//! Timeseries rows are append-only: an ingest only inserts rows, so each
//! stored row is one Insert event. The event carries the row as a scan reads
//! it. A timeseries row has no identity of its own, so its event names no
//! single row ([`RowId::Batch`]). The producer numbers the events of one
//! record in row order, which tells them apart.
//!
//! An ingest builds these events only when some Event Plane consumer reads
//! the collection, so a collection nothing consumes pays nothing for them.
//!
//! A resolved ingest (`ingest_resolved`) records whether a consumer read the
//! collection at resolve and each row's image, and emits those images for the
//! rows that landed. WAL catch-up rebuilds the same events from the record.
//! This module emits for an ingest that reached its core unresolved: a
//! staged write, or a write whose caller appended its record upstream. Such
//! a record carries lines only, so WAL catch-up rebuilds none of its events.
//! Restart replay emits none.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::event_emit::RowWriteEvent;
use crate::data::executor::task::ExecutionTask;
use crate::event::WriteOp;
use crate::event::types::RowId;

use super::ingest_dispatch::TimeseriesApplyMode;

impl CoreLoop {
    /// Whether an unresolved live ingest into `collection` owes the Event
    /// Plane its rows.
    pub(super) fn ts_ingest_emits_events(
        &self,
        task: &ExecutionTask,
        collection: &str,
        mode: TimeseriesApplyMode,
    ) -> bool {
        mode == TimeseriesApplyMode::Immediate
            && self.events.producer.is_some()
            && self
                .events
                .interest
                .consumes(task.request.database_id, collection)
    }

    /// Emit one Insert event per stored row in `rows`, in row order.
    pub(super) fn emit_ts_ingest_events(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        rows: &[rmpv::Value],
    ) {
        let mut image = Vec::new();
        for row in rows {
            image.clear();
            if let Err(error) = rmpv::encode::write_value(&mut image, row) {
                tracing::error!(
                    collection,
                    error = %error,
                    "a stored timeseries row did not encode; no event emitted for it"
                );
                continue;
            }
            self.emit_event_with_row_id_as(
                task,
                task.request.event_source,
                RowWriteEvent {
                    collection,
                    op: WriteOp::Insert,
                    row_id: RowId::Batch,
                    new_value: Some(&image),
                    old_value: None,
                    image_fault: None,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TimeseriesOp;

    use super::super::{TimeseriesApplyMode, TimeseriesIngestExec};
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::task::ExecutionTask;
    use crate::event::WriteOp;
    use crate::event::bus::{EventConsumerRx, create_event_bus_with_capacity};
    use crate::event::interest::{EventInterest, Interest, InterestSlice, InterestSources};
    use crate::event::types::{RecordPosition, RowId, WriteEvent};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    const TENANT: u64 = 1;
    const COLLECTION: &str = "metrics";
    const COMMIT_HLC: u64 = 42_000_000;
    const LINES: &str = "metrics,host=a value=1i\nmetrics,host=b value=2i\n";

    /// A task stamped `lsn`, committed at [`COMMIT_HLC`]. The ingest reads
    /// only the envelope.
    fn task_at(lsn: u64) -> ExecutionTask {
        let mut task = CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                restart_identity: false,
            }),
            Some(Lsn::new(lsn)),
        );
        task.request.commit_hlc = Some(COMMIT_HLC);
        task
    }

    /// Wire `core` to an event bus and to an interest set whose trigger
    /// slice names `consumed`.
    fn wire_events(core: &mut CoreLoop, consumed: &[&str]) -> EventConsumerRx {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        core.set_event_producer(producers.pop().expect("producer"));
        let triggers = InterestSlice::new();
        let mut interest = Interest::default();
        for collection in consumed {
            interest.insert(DatabaseId::DEFAULT, collection);
        }
        triggers.publish(interest);
        let set = EventInterest::new();
        set.install(InterestSources {
            triggers,
            ..InterestSources::default()
        });
        core.set_event_interest(set);
        consumers.pop().expect("consumer")
    }

    fn ingest(core: &mut CoreLoop, lsn: u64, mode: TimeseriesApplyMode) -> Status {
        let task = task_at(lsn);
        core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: TenantId::new(TENANT),
            collection: COLLECTION,
            payload: LINES.as_bytes(),
            format: "ilp",
            wal_lsn: Some(lsn),
            provenance: None,
            mode,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        })
        .status
    }

    fn drain(events: &mut EventConsumerRx) -> Vec<WriteEvent> {
        std::iter::from_fn(|| events.try_recv()).collect()
    }

    fn host(event: &WriteEvent) -> String {
        let row = event.new_value.as_deref().expect("row image");
        let value = nodedb_types::value_from_msgpack(row).expect("row decodes");
        value
            .as_object()
            .and_then(|fields| fields.get("host"))
            .and_then(|host| host.as_str())
            .expect("host tag")
            .to_owned()
    }

    #[test]
    fn a_consumed_collection_emits_one_insert_per_stored_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core, &[COLLECTION]);

        assert_eq!(
            ingest(&mut core, 5, TimeseriesApplyMode::Immediate),
            Status::Ok
        );

        let emitted = drain(&mut events);
        assert_eq!(emitted.len(), 2);
        for (occurrence, event) in emitted.iter().enumerate() {
            assert_eq!(event.collection.as_ref(), COLLECTION);
            assert_eq!(event.op, WriteOp::Insert);
            assert_eq!(event.row_id, RowId::Batch);
            assert_eq!(event.commit_hlc, Some(COMMIT_HLC));
            assert!(event.old_value.is_none());
            assert_eq!(
                event.record,
                Some(RecordPosition {
                    lsn: Lsn::new(5),
                    occurrence: u32::try_from(occurrence).expect("small index"),
                })
            );
        }
        let hosts: Vec<String> = emitted.iter().map(host).collect();
        assert_eq!(hosts, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn a_collection_nothing_consumes_emits_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core, &["other"]);

        assert_eq!(
            ingest(&mut core, 5, TimeseriesApplyMode::Immediate),
            Status::Ok
        );

        assert!(drain(&mut events).is_empty());
    }

    #[test]
    fn restart_replay_emits_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core, &[COLLECTION]);

        assert_eq!(
            ingest(&mut core, 5, TimeseriesApplyMode::Replay),
            Status::Ok
        );

        assert!(drain(&mut events).is_empty());
    }
}
