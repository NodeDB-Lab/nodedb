// SPDX-License-Identifier: BUSL-1.1

//! Shared fixtures for the resolved timeseries ingest tests.

use nodedb_physical::physical_plan::TimeseriesOp;

use super::{TimeseriesApplyMode, TimeseriesIngestExec, TsResolveInput};
use crate::bridge::envelope::{Admission, ExemptReason, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::raw_scan::emit_memtable_rows_at;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::resolved_ingest::{
    RESOLVED_INGEST_FORMAT, ResolvedTsBatch, TsDriftPolicy,
};
use crate::event::bus::{EventConsumerRx, create_event_bus_with_capacity};
use crate::event::interest::{EventInterest, Interest, InterestSlice, InterestSources};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

pub(super) const TENANT: u64 = 1;
pub(super) const COLLECTION: &str = "metrics";

pub(super) fn task_at(lsn: u64) -> ExecutionTask {
    CoreLoop::replay_task(
        TenantId::new(TENANT),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            restart_identity: false,
        }),
        Some(Lsn::new(lsn)),
    )
}

pub(super) fn key() -> (DatabaseId, TenantId, String) {
    (
        DatabaseId::DEFAULT,
        TenantId::new(TENANT),
        COLLECTION.to_string(),
    )
}

/// Wire `core` to an event bus, with a consumer on the collection.
pub(super) fn wire_events(core: &mut CoreLoop) -> EventConsumerRx {
    let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
    core.set_event_producer(producers.pop().expect("producer"));
    let triggers = InterestSlice::new();
    let mut interest = Interest::default();
    interest.insert(DatabaseId::DEFAULT, COLLECTION);
    triggers.publish(interest);
    let set = EventInterest::new();
    set.install(InterestSources {
        triggers,
        ..InterestSources::default()
    });
    core.set_event_interest(set);
    consumers.pop().expect("consumer")
}

pub(super) fn resolve(core: &CoreLoop, line: &str, now_ms: i64) -> ResolvedTsBatch {
    resolve_lines(core, &[line], now_ms)
}

pub(super) fn resolve_lines(core: &CoreLoop, lines: &[&str], now_ms: i64) -> ResolvedTsBatch {
    let lines: Vec<String> = lines.iter().map(|line| (*line).to_string()).collect();
    core.resolve_ts_batch(
        &task_at(0),
        TsResolveInput {
            tid: TenantId::new(TENANT),
            collection: COLLECTION,
            lines: &lines,
            now_ms,
            drift: TsDriftPolicy::Refuse,
            needs_images: false,
            base: None,
        },
    )
    .expect("resolve")
}

/// Install `batch` as a live write that passed the admission gate.
pub(super) fn install(core: &mut CoreLoop, batch: &ResolvedTsBatch, lsn: u64) -> Response {
    install_as(core, batch, lsn, Admission::Admitted)
}

/// Install `batch` as a committed entry's apply: its order was decided
/// by the log.
pub(super) fn install_committed(
    core: &mut CoreLoop,
    batch: &ResolvedTsBatch,
    lsn: u64,
) -> Response {
    install_as(
        core,
        batch,
        lsn,
        Admission::Exempt(ExemptReason::AlreadyOrdered),
    )
}

pub(super) fn install_as(
    core: &mut CoreLoop,
    batch: &ResolvedTsBatch,
    lsn: u64,
    admission: Admission,
) -> Response {
    let payload = batch.to_bytes().expect("encode batch");
    let mut task = task_at(lsn);
    task.request.admission = admission;
    core.execute_timeseries_ingest(TimeseriesIngestExec {
        task: &task,
        tid: TenantId::new(TENANT),
        collection: COLLECTION,
        payload: &payload,
        format: RESOLVED_INGEST_FORMAT,
        wal_lsn: Some(lsn),
        provenance: None,
        mode: TimeseriesApplyMode::Immediate,
        rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: &[],
    })
}

/// The stored row at `index`, as a scan reads it.
pub(super) fn stored_row(core: &CoreLoop, index: usize) -> rmpv::Value {
    let mt = core.columnar_memtables.get(&key()).expect("memtable");
    emit_memtable_rows_at(mt, &[index])
        .expect("read row")
        .pop()
        .expect("one row")
}

/// The images of every event the core emitted since the last drain.
pub(super) fn drained_images(events: &mut EventConsumerRx) -> Vec<rmpv::Value> {
    std::iter::from_fn(|| events.try_recv())
        .map(|event| {
            let image = event.new_value.expect("insert image");
            crate::util::bounded_msgpack::read_value(&image).expect("decode image")
        })
        .collect()
}
