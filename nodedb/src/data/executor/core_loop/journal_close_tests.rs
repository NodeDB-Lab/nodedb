// SPDX-License-Identifier: BUSL-1.1

//! Normal close preserves only effects whose required capture reached disk.

use super::tests::{
    captures, event_bus, group, ordinary_group, ordinary_task, reject_captures, task,
};
use super::*;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
    SOURCE, balance, configure_sum, core_with_sum,
};
use nodedb_types::{StorageKey, Surrogate};

/// Repair only the capture table fixture after the guarded core closed.
fn repair_capture_table(directory: &std::path::Path) {
    let db = redb::Database::create(directory.join("sparse/core-0.redb")).expect("raw reopen");
    let txn = db.begin_write().expect("transaction");
    txn.delete_table(redb::TableDefinition::<u64, u64>::new("write_set_captures"))
        .expect("remove incompatible table");
    txn.open_table(redb::TableDefinition::<u64, &[u8]>::new(
        "write_set_captures",
    ))
    .expect("restore table definition");
    txn.commit().expect("commit table definition");
}

fn assert_row_absent(core: &CoreLoop, collection: &str, surrogate: u32) {
    assert!(
        core.sparse
            .get(
                0,
                1,
                collection,
                &StorageKey::for_surrogate(Surrogate::new(surrogate))
            )
            .expect("read row")
            .is_none()
    );
}

#[test]
fn rejected_root_capture_never_promotes_source_effects_during_normal_close() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, ends) = core_with_sum(directory.path());
    reject_captures(&core);
    let mut events = event_bus(&mut core);
    let task = task(false, false);
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(!response.write_set.is_empty());
    core.finish_journalled_task(&task, &mut response)
        .expect("finish failed scope");
    assert_eq!(events(), 0);
    drop(core);
    drop(ends);
    repair_capture_table(directory.path());
    let (mut reopened, _requests, _responses) = make_core_with_dir(directory.path());
    configure_sum(&mut reopened);
    assert_eq!(balance(&reopened).as_deref(), Some("100"));
    assert_row_absent(&reopened, SOURCE, 11);
    assert!(captures(&reopened).is_empty());
}

#[test]
fn rejected_ordinary_capture_never_promotes_its_row_during_normal_close() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, requests, mut responses) = make_core_with_dir(directory.path());
    reject_captures(&core);
    let mut events = event_bus(&mut core);
    core.keep_write_set_journal(
        1,
        WriteSetJournal {
            group: Some(ordinary_group(1)),
            ..WriteSetJournal::default()
        },
    );
    core.run_task(ordinary_task(1));
    let response = responses.try_pop().expect("response").inner;
    assert_eq!(response.status, Status::Error);
    assert!(!response.write_set.is_empty());
    assert!(core.is_fail_stopped());
    assert_eq!(events(), 0);
    drop(core);
    drop((requests, responses));
    repair_capture_table(directory.path());
    let (reopened, _requests, _responses) = make_core_with_dir(directory.path());
    assert_row_absent(&reopened, "ordinary_rows", 1);
    assert!(captures(&reopened).is_empty());
}

#[test]
fn rejected_batch_captures_never_promote_their_rows_during_normal_close() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, requests, mut responses) = make_core_with_dir(directory.path());
    reject_captures(&core);
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
    assert_eq!(core.poll_write_batch(), 2);
    for _ in 0..2 {
        let response = responses.try_pop().expect("response").inner;
        assert_eq!(response.status, Status::Error);
        assert!(!response.write_set.is_empty());
    }
    assert!(core.is_fail_stopped());
    assert_eq!(events(), 0);
    drop(core);
    drop((requests, responses));
    repair_capture_table(directory.path());
    let (reopened, _requests, _responses) = make_core_with_dir(directory.path());
    for id in [1, 2] {
        assert_row_absent(&reopened, "ordinary_rows", id);
    }
    assert!(captures(&reopened).is_empty());
}

#[test]
fn accepted_root_capture_survives_a_settlement_error_and_normal_close() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, ends) = core_with_sum(directory.path());
    core.ts_tuning.memtable_budget_bytes = 1;
    std::fs::write(directory.path().join("ts"), b"blocks segment persistence")
        .expect("obstruct flush");
    let mut events = event_bus(&mut core);
    let task = task(false, false);
    let permit = core.begin_task_journal(&task, group()).expect("permit");
    let mut response = core.execute_with_journal(&task, Some(permit));
    assert_eq!(response.status, Status::Error);
    assert!(!response.write_set.is_empty());
    let accepted = response.write_set.len();
    core.finish_journalled_task(&task, &mut response)
        .expect("finish accepted scope");
    assert_eq!(events(), 0);
    drop(core);
    drop(ends);
    std::fs::remove_file(directory.path().join("ts")).expect("remove obstruction");
    let (mut reopened, _requests, _responses) = make_core_with_dir(directory.path());
    configure_sum(&mut reopened);
    assert_eq!(balance(&reopened).as_deref(), Some("125"));
    assert_eq!(captures(&reopened)[0].entries.len(), accepted);
}

#[test]
fn healthy_root_completion_keeps_its_capture_after_normal_close() {
    for calvin in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, ends) = core_with_sum(directory.path());
        core.ts_tuning.memtable_budget_bytes = 1;
        let task = task(calvin, false);
        let permit = core.begin_task_journal(&task, group()).expect("permit");
        let mut response = core.execute_with_journal(&task, Some(permit));
        assert_eq!(response.status, Status::Ok, "{response:?}");
        core.finish_journalled_task(&task, &mut response)
            .expect("finish healthy scope");
        drop(core);
        drop(ends);
        let (mut reopened, _requests, _responses) = make_core_with_dir(directory.path());
        configure_sum(&mut reopened);
        assert_eq!(balance(&reopened).as_deref(), Some("125"));
        assert_eq!(captures(&reopened).len(), 1);
    }
}

#[test]
fn unfinished_ordinary_scope_never_promotes_its_row_during_normal_close() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, requests, responses) = make_core_with_dir(directory.path());
    let task = ordinary_task(1);
    assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
    let response = core.execute(&task);
    assert_eq!(response.status, Status::Ok);
    assert!(!response.write_set.is_empty());
    drop(core);
    drop((requests, responses));
    let (reopened, _requests, _responses) = make_core_with_dir(directory.path());
    assert_row_absent(&reopened, "ordinary_rows", 1);
    assert!(captures(&reopened).is_empty());
}

#[test]
fn healthy_ordinary_and_batch_completion_survive_normal_close() {
    for batched in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, requests, mut responses) = make_core_with_dir(directory.path());
        let count = if batched { 2 } else { 1 };
        for id in 1..=count {
            core.keep_write_set_journal(
                id,
                WriteSetJournal {
                    group: Some(ordinary_group(id)),
                    ..WriteSetJournal::default()
                },
            );
            core.task_queue.push(ordinary_task(id));
        }
        if batched {
            assert_eq!(core.poll_write_batch(), 2);
        } else {
            assert!(core.poll_one());
        }
        for _ in 0..count {
            assert_eq!(
                responses.try_pop().expect("response").inner.status,
                Status::Ok
            );
        }
        drop(core);
        drop((requests, responses));
        let (reopened, _requests, _responses) = make_core_with_dir(directory.path());
        assert_eq!(captures(&reopened).len(), count as usize);
        for id in 1..=count {
            assert!(
                reopened
                    .sparse
                    .get(
                        0,
                        1,
                        "ordinary_rows",
                        &StorageKey::for_surrogate(Surrogate::new(id as u32))
                    )
                    .expect("read row")
                    .is_some()
            );
        }
    }
}
