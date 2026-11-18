// SPDX-License-Identifier: BUSL-1.1

//! Root redo capture ordering against timeseries settlement.

use super::*;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
    SOURCE, TARGET, balance, calvin_redo, configure_sum, core_with_sum, sum_targets,
};
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};
use nodedb_physical::physical_plan::{CalvinInstall, CalvinReplySpec, DocumentOp, RedoOrigin};
use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, TenantId};
use nodedb_wal::record::RecordType;

const ORIGIN: u64 = 70;
const METRICS: &str = "capture_metrics";

pub(super) fn task(calvin: bool, refused: bool) -> ExecutionTask {
    let mut redo = RedoRecord::from_bytes(&calvin_redo()).expect("decode redo");
    let line = format!("{METRICS} value=1 1700000000000000000");
    redo.ops.push(RedoSubRecord {
        record_type: RecordType::TimeseriesBatch as u32,
        payload: crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
            crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                collection: METRICS,
                payload: line.as_bytes(),
                provenance: None,
                format: "ilp",
                default_timestamp_ms: 0,
            },
        )
        .expect("encode ingest"),
    });
    if refused {
        redo.ops.push(crate::data::executor::handlers::transaction::redo_apply::test_commit::failing_install_sub_record());
    }
    let mut request = make_default_task().request;
    request.plan = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: redo.to_bytes().expect("encode redo"),
        collections: vec![SOURCE.into(), METRICS.into()],
        sum_targets: sum_targets(),
        origin: RedoOrigin::Commit,
        calvin: calvin.then(|| CalvinInstall {
            epoch: 3,
            position: 0,
            epoch_system_ms: 0,
            reply: CalvinReplySpec::Count(Vec::new()),
            user_write: true,
            mirror: false,
        }),
        reads: Vec::new(),
        snapshot_ordinal: None,
    });
    request.wal_lsn = Some(Lsn::new(ORIGIN));
    ExecutionTask::new(request)
}

pub(in crate::data::executor::core_loop) fn group() -> JournalGroup {
    JournalGroup {
        origin: Lsn::new(ORIGIN),
        collection: SOURCE.into(),
        apply_key: 0,
        commit_hlc: None,
        change_position: None,
    }
}

pub(in crate::data::executor::core_loop) fn captures(core: &CoreLoop) -> Vec<WriteSetCapture> {
    core.sparse
        .load_write_set_captures()
        .expect("load captures")
        .into_iter()
        .map(|(_, bytes)| WriteSetCapture::from_bytes(&bytes).expect("decode capture"))
        .collect()
}

pub(in crate::data::executor::core_loop) fn crash_sparse(
    core_dir: &std::path::Path,
    image: &std::path::Path,
) {
    std::fs::create_dir_all(image.join("sparse")).expect("mkdir");
    std::fs::copy(
        core_dir.join("sparse/core-0.redb"),
        image.join("sparse/core-0.redb"),
    )
    .expect("copy sparse crash image");
}

pub(in crate::data::executor::core_loop) fn event_bus(
    core: &mut CoreLoop,
) -> impl FnMut() -> usize + use<> {
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 64);
    core.set_event_producer(producers.pop().expect("producer"));
    let mut consumer = consumers.pop().expect("consumer");
    move || {
        let mut count = 0;
        while consumer.try_recv().is_some() {
            count += 1;
        }
        count
    }
}

#[test]
fn root_redo_persists_fold_capture_before_timeseries_settlement_and_outer_completion() {
    for calvin in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _ends) = core_with_sum(directory.path());
        core.ts_tuning.memtable_budget_bytes = 1;
        let mut events = event_bus(&mut core);
        let task = task(calvin, false);
        let permit = core
            .begin_task_journal(&task, group())
            .expect("root permit");
        let mut response = core.execute_with_journal(&task, Some(permit));
        assert_eq!(response.status, Status::Ok, "{response:?}");
        assert_eq!(events(), 0, "events wait for outer success");
        let stored = captures(&core);
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].origin, ORIGIN);
        assert_eq!(stored[0].entries.len(), response.write_set.len());
        assert!(!response.write_set.is_empty());
        assert_eq!(balance(&core).as_deref(), Some("125"));
        let key = (DatabaseId::DEFAULT, TenantId::new(1), METRICS.to_string());
        assert_eq!(
            core.columnar_memtables[&key].row_count(),
            0,
            "settlement drained the memtable"
        );
        assert_eq!(core.ts_registries[&key].total_row_count(), 1);
        assert!(directory.path().join("ts/0/1").join(METRICS).is_dir());

        let image = tempfile::tempdir().expect("image");
        crash_sparse(directory.path(), image.path());
        let (mut reopened, _req, _resp) = make_core_with_dir(image.path());
        configure_sum(&mut reopened);
        assert_eq!(captures(&reopened)[0].origin, ORIGIN);
        assert_eq!(balance(&reopened).as_deref(), Some("125"));
        assert!(
            reopened
                .sparse
                .get(
                    0,
                    1,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(Surrogate::new(11))
                )
                .expect("source")
                .is_some()
        );

        // An outer capture store would remove this origin before replacing it.
        core.write_set_journal.settled.push(ORIGIN);
        response.write_set.clear();
        core.finish_journalled_task(&task, &mut response)
            .expect("finish journal");
        assert_eq!(core.write_set_journal.settled, [ORIGIN]);
        assert_eq!(captures(&core)[0].entries.len(), stored[0].entries.len());
        assert_eq!(response.write_set.len(), stored[0].entries.len());
        assert!(events() > 0);
        assert!(core.write_set_journal.running.is_none());
    }
}

#[test]
fn a_timeseries_settlement_error_retains_the_durable_capture_and_accepted_rows() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    core.ts_tuning.memtable_budget_bytes = 1;
    std::fs::write(directory.path().join("ts"), b"blocks the segment directory")
        .expect("obstruct flush");
    let mut events = event_bus(&mut core);
    let task = task(false, false);
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(core.is_fail_stopped());
    assert!(!response.write_set.is_empty());
    assert_eq!(captures(&core).len(), 1);
    let image = tempfile::tempdir().expect("image");
    crash_sparse(directory.path(), image.path());
    let (mut reopened, _req, _resp) = make_core_with_dir(image.path());
    configure_sum(&mut reopened);
    assert_eq!(balance(&reopened).as_deref(), Some("125"));
    assert_eq!(
        captures(&reopened)[0].entries.len(),
        response.write_set.len()
    );
    let key = (DatabaseId::DEFAULT, TenantId::new(1), METRICS.to_string());
    assert_eq!(core.columnar_memtables[&key].row_count(), 1);
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert_eq!(events(), 0);
    assert_eq!(captures(&core).len(), 1);
    assert!(core.write_set_journal.running.is_none());
}

#[test]
fn a_capture_store_error_prevents_timeseries_settlement_and_events() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    core.ts_tuning.memtable_budget_bytes = 1;
    let mut events = event_bus(&mut core);
    reject_captures(&core);
    let task = task(false, false);
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(core.is_fail_stopped());
    assert!(!response.write_set.is_empty());
    assert!(matches!(
        core.write_set_journal.running,
        Some(RunningJournal::Failed)
    ));
    let key = (DatabaseId::DEFAULT, TenantId::new(1), METRICS.to_string());
    assert_eq!(core.columnar_memtables[&key].row_count(), 1);
    assert!(!directory.path().join("ts").exists());
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert_eq!(events(), 0);
    assert!(!core.sparse.db().is_deferred());
    assert!(core.write_set_journal.running.is_none());
}

#[test]
fn an_install_refusal_accepts_no_fold_capture_or_source_effects() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    let mut events = event_bus(&mut core);
    let task = task(false, true);
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(response.write_set.is_empty());
    assert_eq!(balance(&core).as_deref(), Some("100"));
    assert!(captures(&core).is_empty());
    assert!(matches!(
        core.write_set_journal.running,
        Some(RunningJournal::Pending { .. })
    ));
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert!(captures(&core)[0].entries.is_empty());
    assert_eq!(events(), 0);
}

#[test]
fn a_recursive_query_with_the_root_request_id_cannot_consume_the_journal() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    let task = task(false, false);
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let query = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, TARGET),
        document_id: "a1".into(),
        surrogate: Some(Surrogate::new(4242)),
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let response = core.execute_plan(&task, &query);
    assert_eq!(response.status, Status::Ok);
    assert!(captures(&core).is_empty());
    assert!(matches!(
        core.write_set_journal.running,
        Some(RunningJournal::Pending { .. })
    ));
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Ok, "{response:?}");
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert_eq!(captures(&core).len(), 1);
}

#[test]
fn an_early_validation_return_leaves_the_root_group_for_outer_completion() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    let mut task = task(false, false);
    if let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { redo, .. }) = &mut task.request.plan {
        *redo = vec![0xc1];
    }
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(matches!(
        core.write_set_journal.running,
        Some(RunningJournal::Pending { .. })
    ));
    assert!(captures(&core).is_empty());
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert!(captures(&core)[0].entries.is_empty());
    assert!(core.write_set_journal.running.is_none());
    assert!(!core.sparse.db().is_deferred());
}

pub(in crate::data::executor::core_loop) fn ordinary_task(id: u64) -> ExecutionTask {
    let mut request = make_default_task().request;
    request.request_id = crate::types::RequestId::new(id);
    request.wal_lsn = Some(Lsn::new(id));
    request.plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "ordinary_rows"),
        document_id: format!("row-{id}"),
        value: crate::data::executor::doc_format::encode_to_msgpack(
            &serde_json::json!({"value": id}),
        ),
        surrogate: Surrogate::new(id as u32),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
    });
    ExecutionTask::new(request)
}

pub(in crate::data::executor::core_loop) fn ordinary_group(id: u64) -> JournalGroup {
    JournalGroup {
        origin: Lsn::new(id),
        collection: "ordinary_rows".into(),
        ..group()
    }
}

#[test]
fn ordinary_journalled_writes_hold_events_until_their_capture_is_durable() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    let mut events = event_bus(&mut core);
    let task = ordinary_task(1);
    assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
    let mut response = core.execute(&task);
    assert_eq!(response.status, Status::Ok, "{response:?}");
    assert_eq!(events(), 0);
    assert!(captures(&core).is_empty());
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert_eq!(captures(&core).len(), 1);
    assert_eq!(events(), 1);
}

#[test]
fn ordinary_journalled_batches_persist_each_capture_before_releasing_events() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
    let mut events = event_bus(&mut core);
    for id in [1, 2] {
        core.keep_write_set_journal(
            id,
            WriteSetJournal {
                group: Some(ordinary_group(id)),
                ..WriteSetJournal::default()
            },
        );
        core.task_queue.push(ordinary_task(id));
    }
    assert_eq!(events(), 0);
    assert_eq!(core.poll_write_batch(), 2);
    for _ in 0..2 {
        assert_eq!(
            responses.try_pop().expect("response").inner.status,
            Status::Ok
        );
    }
    let stored = captures(&core);
    assert_eq!(stored.len(), 2);
    assert!(stored.iter().all(|capture| capture.entries.len() == 2));
    assert_eq!(events(), 2);
    assert!(!core.sparse.db().is_deferred());
    assert!(core.write_set_journal.held_events.is_none());
}

#[test]
fn a_capture_scope_error_rolls_back_before_any_timeseries_settlement() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    core.ts_tuning.memtable_budget_bytes = 1;
    let root = task(false, false);
    let permit = core.begin_task_journal(&root, group()).expect("permit");
    let mut other = task(false, false);
    other.request.request_id = crate::types::RequestId::new(root.request_id().as_u64() + 1);
    let watermark = core.watermark;
    let mut response = core.execute_with_journal(&other, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(response.write_set.is_empty());
    assert_eq!(balance(&core).as_deref(), Some("100"));
    assert!(captures(&core).is_empty());
    assert_eq!(core.watermark, watermark);
    assert!(!directory.path().join("ts").exists());
    core.finish_journalled_task(&other, &mut response)
        .expect("finish journal");
    assert!(
        captures(&core).is_empty(),
        "failed capture never retries at outer completion"
    );
    assert!(core.write_set_journal.running.is_none());
}

/// A whole record whose capture cannot be prepared rolls back in its
/// task. The journal then fails, and the core fail-stops once, for the
/// unstored write set.
#[cfg(feature = "failpoints")]
#[test]
fn a_failed_whole_record_capture_prepare_rolls_back_and_fail_stops_once() {
    use nodedb_types::fail_point::FailGuard;

    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    let root = task(false, false);
    let permit = core.begin_task_journal(&root, group()).expect("permit");
    let response = {
        let _failing = FailGuard::fail("redo_apply::capture_prepare_fails", "the prepare fails");
        core.execute_with_journal(&root, Some(permit))
    };
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(crate::bridge::envelope::ErrorCode::RetryableRefusal { .. })
        ),
        "the record rolled back: {:?}",
        response.error_code
    );
    assert_eq!(balance(&core).as_deref(), Some("100"), "no fold stays");
    assert!(captures(&core).is_empty());
    assert!(matches!(
        core.fail_stop.cause(),
        Some((FailStopCause::WriteSetUnpersisted, _))
    ));
}

#[test]
fn a_stopped_core_releases_no_ordinary_held_events_after_a_durable_store() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    let mut events = event_bus(&mut core);
    let task = ordinary_task(1);
    assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
    let mut response = core.execute(&task);
    assert_eq!(response.status, Status::Ok);
    core.fail_stop_core(FailStopCause::PostInstallFailed, "publication stopped");
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert_eq!(captures(&core).len(), 1);
    assert_eq!(events(), 0);
}

/// An event bus that keeps every event it received.
fn event_log(core: &mut CoreLoop) -> impl FnMut() -> Vec<crate::event::WriteEvent> + use<> {
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 64);
    core.set_event_producer(producers.pop().expect("producer"));
    let mut consumer = consumers.pop().expect("consumer");
    move || {
        let mut received = Vec::new();
        while let Some(event) = consumer.try_recv() {
            received.push(event);
        }
        received
    }
}

/// What one root redo install sent, its held events spilling past
/// `threshold`, when its outer task answers `outer_ok`. Asserts the held
/// set spilled exactly when `threshold` is 0, and left no directory.
fn held_run(threshold: usize, outer_ok: bool) -> Vec<crate::event::WriteEvent> {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _ends) = core_with_sum(directory.path());
    core.ts_tuning.memtable_budget_bytes = 1;
    core.query_tuning.max_txn_overlay_bytes = threshold;
    let mut events = event_log(&mut core);
    let task = task(false, false);
    let permit = core
        .begin_task_journal(&task, group())
        .expect("root permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Ok, "{response:?}");
    assert!(events().is_empty(), "events wait for outer success");
    let held = core
        .write_set_journal
        .held_events
        .as_ref()
        .expect("held events");
    let spill_dir = held.dir().map(std::path::Path::to_path_buf);
    if threshold == 0 {
        assert!(held.spilled_len() > 0, "the held events spilled");
        assert!(spill_dir.as_ref().is_some_and(|dir| dir.is_dir()));
    } else {
        assert!(spill_dir.is_none(), "an unspilled set names no directory");
    }
    if !outer_ok {
        response.status = Status::Error;
    }
    core.finish_journalled_task(&task, &mut response)
        .expect("finish journal");
    assert!(core.write_set_journal.held_events.is_none());
    if let Some(dir) = spill_dir {
        assert!(!dir.exists(), "the spill directory is gone");
    }
    events()
}

/// Held events that spilled leave on outer success in the order, and with
/// the sequence numbers, of held events that stayed in RAM. On outer
/// failure they leave nothing behind: no event and no spill file.
#[test]
fn spilled_held_events_leave_only_on_outer_success() {
    let in_ram = held_run(usize::MAX, true);
    let spilled = held_run(0, true);
    assert_eq!(spilled.len(), 2);
    // Images compare as decoded documents: a document image writes its
    // keys in hash order.
    assert_eq!(
        crate::data::executor::core_loop::held_events::read_as::as_read_all(&spilled),
        crate::data::executor::core_loop::held_events::read_as::as_read_all(&in_ram)
    );
    assert!(
        spilled
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1),
        "the events take consecutive sequence numbers"
    );
    assert!(held_run(0, false).is_empty());
}

/// A held-event spill that fails answers the journalled write with an
/// error: no write set is stored, and no event is sent.
#[test]
fn a_failed_held_event_spill_answers_the_write_with_an_error() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    core.query_tuning.max_txn_overlay_bytes = 0;
    // A file where the held set's spill directory goes.
    let blocked = crate::data::executor::handlers::spill::cleanup::SpillKind::Txn
        .core_dir(directory.path(), core.core_id)
        .join("held-events-0");
    std::fs::create_dir_all(blocked.parent().expect("parent")).expect("mkdir");
    std::fs::write(&blocked, b"in the way").expect("block the directory");
    let mut events = event_bus(&mut core);
    let task = ordinary_task(1);
    assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
    let mut response = core.execute(&task);
    assert!(
        core.write_set_journal
            .held_events
            .as_ref()
            .is_some_and(|held| held.failure().is_some()),
        "the held set failed"
    );
    assert!(
        core.finish_journalled_task(&task, &mut response).is_err(),
        "the journal answers an error"
    );
    assert!(captures(&core).is_empty(), "no write set is stored");
    assert_eq!(events(), 0, "no event is sent");
    assert!(core.is_fail_stopped());
}

/// A spilled held event that does not read back at the journal's end
/// fail-stops the core.
#[test]
fn a_held_event_that_does_not_read_back_fail_stops_the_core() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    core.query_tuning.max_txn_overlay_bytes = 0;
    let mut events = event_bus(&mut core);
    let task = ordinary_task(1);
    assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
    let mut response = core.execute(&task);
    assert_eq!(response.status, Status::Ok, "{response:?}");
    let dir = core
        .write_set_journal
        .held_events
        .as_ref()
        .and_then(|held| held.dir())
        .map(std::path::Path::to_path_buf)
        .expect("the held events spilled");
    for file in std::fs::read_dir(&dir).expect("spill dir") {
        std::fs::OpenOptions::new()
            .write(true)
            .open(file.expect("entry").path())
            .and_then(|file| file.set_len(2))
            .expect("cut the spill file");
    }
    core.finish_journalled_task(&task, &mut response)
        .expect("the write set is stored");
    assert_eq!(captures(&core).len(), 1);
    assert_eq!(events(), 0, "the cut event never leaves");
    assert!(core.is_fail_stopped(), "the core stops on the lost event");
}

pub(super) fn reject_captures(core: &CoreLoop) {
    let txn = core.sparse.begin_write().expect("transaction");
    txn.delete_table(redb::TableDefinition::<u64, &[u8]>::new(
        "write_set_captures",
    ))
    .expect("remove table");
    txn.open_table(redb::TableDefinition::<u64, u64>::new("write_set_captures"))
        .expect("incompatible table");
    txn.commit().expect("commit table type");
}
