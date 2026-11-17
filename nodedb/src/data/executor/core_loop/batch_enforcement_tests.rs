// SPDX-License-Identifier: BUSL-1.1

//! Image enforcement keeps its canonical per-statement execution boundary.

use super::tests::{captures, event_bus, ordinary_group, ordinary_task};
use super::*;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
    SOURCE, TARGET, balance, core_with_sum, sum_targets,
};
use crate::engine::document::store::CollectionConfig;
use nodedb_physical::physical_plan::{BalancedDef, DocumentOp};
use nodedb_types::{DatabaseId, QualifiedCollection, StorageKey, Surrogate, TenantId};

fn sum_task(id: u64) -> ExecutionTask {
    let mut task = ordinary_task(id);
    let PhysicalPlan::Document(DocumentOp::PointPut {
        collection,
        value,
        resolved_sum_targets,
        ..
    }) = &mut task.request.plan
    else {
        panic!("ordinary fixture must be a PointPut");
    };
    *collection = QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE);
    *value = doc_format::encode_to_msgpack(&serde_json::json!({"account_id": "a1", "amount": 25}));
    *resolved_sum_targets = sum_targets().remove(0).resolved;
    task
}

fn keep_journal(core: &mut CoreLoop, task: &ExecutionTask, collection: &str) {
    let id = task.request_id().as_u64();
    let mut group = ordinary_group(id);
    group.collection = collection.into();
    core.keep_write_set_journal(
        id,
        WriteSetJournal {
            group: Some(group),
            ..WriteSetJournal::default()
        },
    );
}

#[test]
fn queued_sum_puts_run_the_canonical_fold_and_capture_each_target() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, ends) = core_with_sum(directory.path());
    let mut events = event_bus(&mut core);
    for id in [1, 2] {
        let task = sum_task(id);
        keep_journal(&mut core, &task, SOURCE);
        core.task_queue.push(task);
    }
    assert_eq!(core.poll_write_batch(), 0);
    assert_eq!(core.pending_count(), 2);
    assert!(core.poll_one());
    assert_eq!(balance(&core).as_deref(), Some("125"));
    assert_eq!(core.poll_write_batch(), 0);
    assert!(core.poll_one());
    assert_eq!(balance(&core).as_deref(), Some("150"));
    let stored = captures(&core);
    assert_eq!(stored.len(), 2);
    for (capture, expected) in stored.iter().zip(["125", "150"]) {
        let entries = capture.write_set().expect("capture entries");
        let image = entries
            .iter()
            .find(|entry| entry.collection.as_deref() == Some(TARGET))
            .expect("captured target");
        let crate::bridge::envelope::RowEffect::InstalledPut {
            image: installed,
            event,
        } = &image.effect
        else {
            panic!("target capture must be a put");
        };
        let target = doc_format::decode_document(&installed.canonical).expect("decode target");
        assert_eq!(target["balance"].as_str(), Some(expected));
        let actual = core
            .sparse
            .versioned_get_as_of(
                0,
                1,
                TARGET,
                &StorageKey::for_surrogate(Surrogate::new(image.surrogate)),
                Some(installed.version.sys_from),
                None,
            )
            .expect("read target version")
            .expect("target version exists");
        assert_eq!(actual, installed.stored);
        let event_value =
            doc_format::decode_document(&event.new_value).expect("decode target event");
        assert_eq!(
            event_value,
            doc_format::decode_document(&actual).expect("decode stored target")
        );
        assert_eq!(event_value["balance"].as_str(), Some(expected));
        let prior =
            doc_format::decode_document(event.old_value.as_deref().expect("prior target image"))
                .expect("decode prior target");
        assert_eq!(
            prior["balance"].as_str(),
            Some(if expected == "125" { "100" } else { "125" })
        );
    }
    assert!(events() > 0);
    drop(ends);
}

#[test]
fn queued_balanced_puts_keep_separate_statement_boundaries_and_accept_no_rows() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
    let mut config = CollectionConfig::new("balanced_rows");
    config.enforcement.balanced = Some(BalancedDef {
        group_key_column: "journal_id".into(),
        entry_type_column: "entry_type".into(),
        debit_value: "DEBIT".into(),
        credit_value: "CREDIT".into(),
        amount_column: "amount".into(),
    });
    core.doc_configs.insert(
        (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "balanced_rows".into(),
        ),
        config,
    );
    let mut events = event_bus(&mut core);
    for (id, leg) in [(1, "DEBIT"), (2, "CREDIT")] {
        let mut task = ordinary_task(id);
        let PhysicalPlan::Document(DocumentOp::PointPut {
            collection, value, ..
        }) = &mut task.request.plan
        else {
            panic!("ordinary fixture must be a PointPut");
        };
        *collection = QualifiedCollection::new(DatabaseId::DEFAULT, "balanced_rows");
        *value = doc_format::encode_to_msgpack(
            &serde_json::json!({"journal_id": "j1", "entry_type": leg, "amount": 25}),
        );
        keep_journal(&mut core, &task, "balanced_rows");
        core.task_queue.push(task);
    }
    assert_eq!(core.poll_write_batch(), 0);
    assert_eq!(core.pending_count(), 2);
    for id in [1, 2] {
        assert!(core.poll_one());
        let response = responses.try_pop().expect("response").inner;
        assert_eq!(response.request_id.as_u64(), id);
        assert_eq!(response.status, Status::Error);
        assert!(matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::BalanceViolation { .. })
        ));
        assert!(response.write_set.is_empty());
        assert!(
            core.sparse
                .get(
                    0,
                    1,
                    "balanced_rows",
                    &StorageKey::for_surrogate(Surrogate::new(id as u32))
                )
                .expect("read row")
                .is_none()
        );
    }
    assert_eq!(events(), 0);
    let stored = captures(&core);
    assert_eq!(stored.len(), 2);
    assert!(stored.iter().all(|capture| capture.entries.is_empty()));
}

#[test]
fn a_hooked_put_stops_batch_collection_without_reordering_the_plain_prefix() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, ends) = core_with_sum(directory.path());
    core.task_queue.push(ordinary_task(1));
    core.task_queue.push(sum_task(2));
    core.task_queue.push(ordinary_task(3));
    assert_eq!(core.poll_write_batch(), 0);
    assert_eq!(core.pending_count(), 3);
    assert_eq!(
        core.task_queue
            .front()
            .expect("front")
            .request_id()
            .as_u64(),
        1
    );
    assert!(core.poll_one());
    assert_eq!(
        core.task_queue
            .front()
            .expect("front")
            .request_id()
            .as_u64(),
        2
    );
    assert_eq!(core.poll_write_batch(), 0);
    assert!(core.poll_one());
    assert_eq!(balance(&core).as_deref(), Some("125"));
    assert_eq!(
        core.task_queue
            .front()
            .expect("front")
            .request_id()
            .as_u64(),
        3
    );
    assert!(core.poll_one());
    assert_eq!(core.pending_count(), 0);
    drop(ends);
}
