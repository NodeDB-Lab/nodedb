// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::sync::wire::{SyncAckResult, SyncOutcome};

use super::*;
use crate::bridge::envelope::Status;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

const TID: u64 = 1;

fn prov(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: 7,
        epoch: 1,
        stream_id: 42,
        seq,
    }
}

fn assert_applied(response: &Response, seq: u64) {
    assert_eq!(response.status, Status::Ok);
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("sync ack");
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
    assert_eq!(ack.applied_seq, seq);
}

/// A message body whose only string field is `body`.
fn body(text: &str) -> Vec<(String, String)> {
    vec![("body".to_string(), text.to_string())]
}

/// Two documents under bound surrogates.
fn index_documents(core: &mut CoreLoop, task: &ExecutionTask) {
    for surrogate in [Surrogate::new(5), Surrogate::new(6)] {
        let response =
            core.execute_fts_index_doc(task, TID, "notes", surrogate, &body("hello world"), None);
        assert_eq!(response.status, Status::Ok);
    }
}

/// Ids of `notes` documents matching `query` in one index.
fn hits(core: &CoreLoop, index: nodedb_fts::IndexScope<'_>, query: &str) -> Vec<u32> {
    let mut ids: Vec<u32> = core
        .inverted
        .search(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            nodedb_types::TenantId::new(TID),
            index,
            nodedb_fts::FtsSearchParams {
                query,
                top_k: 10,
                fuzzy_enabled: false,
                mode: nodedb_fts::QueryMode::And,
                prefilter: None,
            },
        )
        .expect("search")
        .into_iter()
        .map(|r| r.doc_id.as_u32())
        .collect();
    ids.sort_unstable();
    ids
}

/// A synced document is searchable per field, and a later message with no
/// fields removes it from every index.
#[test]
fn synced_fields_index_per_field_and_empty_fields_remove() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let title = nodedb_fts::IndexScope::field("notes", "title").expect("field scope");
    let body_index = nodedb_fts::IndexScope::field("notes", "body").expect("field scope");
    let doc = vec![
        ("title".to_string(), "rust".to_string()),
        ("body".to_string(), "guide".to_string()),
    ];
    let response = core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &doc, None);
    assert_eq!(response.status, Status::Ok);
    let response =
        core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(6), &body("rust"), None);
    assert_eq!(response.status, Status::Ok);

    assert_eq!(hits(&core, title, "rust"), vec![5]);
    assert_eq!(hits(&core, body_index, "rust"), vec![6]);
    assert_eq!(hits(&core, "notes".into(), "rust"), vec![5, 6]);

    let response =
        core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &[], Some(&prov(1)));
    assert_applied(&response, 1);
    assert!(hits(&core, title, "rust").is_empty());
    assert!(hits(&core, body_index, "guide").is_empty());
    assert_eq!(hits(&core, "notes".into(), "rust"), vec![6]);
    assert_eq!(doc_count(&core), 1);
}

/// The indexed document count of `notes`.
fn doc_count(core: &CoreLoop) -> u32 {
    core.inverted
        .corpus_stats(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            nodedb_types::TenantId::new(TID),
            "notes",
        )
        .expect("corpus stats")
        .0
}

#[test]
fn an_index_without_a_surrogate_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();

    let response =
        core.execute_fts_index_doc(&task, TID, "notes", Surrogate::ZERO, &body("hello"), None);
    assert!(matches!(
        response.error_code.as_deref(),
        Some(crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. })
    ));
    assert_eq!(doc_count(&core), 0, "nothing is indexed");
}

#[test]
fn unbound_delete_with_provenance_commits_the_sequence_and_removes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    index_documents(&mut core, &task);
    let before = doc_count(&core);
    assert_eq!(before, 2);

    let response = core.execute_fts_delete_doc(&task, TID, "notes", None, Some(&prov(1)));
    assert_applied(&response, 1);
    assert_eq!(core.sync_hwm_value(7, 42), 1, "the sequence commits");
    assert_eq!(doc_count(&core), before, "no document is removed");
}

#[test]
fn unbound_delete_without_provenance_is_ok_and_removes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    index_documents(&mut core, &task);

    let response = core.execute_fts_delete_doc(&task, TID, "notes", None, None);
    assert_eq!(response.status, Status::Ok);
    assert!(response.payload.is_empty());
    assert_eq!(doc_count(&core), 2);
}

#[test]
fn an_admitted_unbound_index_consumes_only_its_own_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let response = core.execute_fts_index_doc(
        &task,
        TID,
        "notes",
        Surrogate::ZERO,
        &body("hello"),
        Some(&prov(1)),
    );
    assert!(
        matches!(response.error_code.as_deref(), Some(crate::bridge::envelope::ErrorCode::SyncRejected { applied_seq: 1, provenance, .. }) if provenance == &prov(1))
    );
    assert_eq!(core.sync_hwm_value(7, 42), 1);
    assert_eq!(doc_count(&core), 0);
    let duplicate = core.execute_fts_index_doc(
        &task,
        TID,
        "notes",
        Surrogate::ZERO,
        &body("hello"),
        Some(&prov(1)),
    );
    let ack: SyncAckResult = zerompk::from_msgpack(&duplicate.payload).unwrap();
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Duplicate));
    assert_eq!(ack.applied_seq, 1);
    let next = core.execute_fts_index_doc(
        &task,
        TID,
        "notes",
        Surrogate::new(5),
        &body("hello"),
        Some(&prov(2)),
    );
    assert_applied(&next, 2);
    assert_eq!(doc_count(&core), 1);
}

#[test]
fn a_gap_precedes_unbound_index_validation_without_consuming_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let response = core.execute_fts_index_doc(
        &task,
        TID,
        "notes",
        Surrogate::ZERO,
        &body("hello"),
        Some(&prov(2)),
    );
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).unwrap();
    assert_eq!(
        ack.outcome,
        SyncOutcome::Ack(AckStatus::Gap { expected: 1 })
    );
    assert_eq!(ack.applied_seq, 0);
    assert_eq!(core.sync_hwm_value(7, 42), 0);
    assert_eq!(doc_count(&core), 0);
}

#[test]
fn a_local_sentinel_unbound_index_remains_an_unconsumed_error() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let response = core.execute_fts_index_doc(
        &make_default_task(),
        TID,
        "notes",
        Surrogate::ZERO,
        &body("hello"),
        Some(&SyncProvenance::default()),
    );
    assert!(matches!(
        response.error_code.as_deref(),
        Some(crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. })
    ));
    assert_eq!(doc_count(&core), 0);
}

#[test]
fn a_fenced_unbound_index_does_not_consume_a_new_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let mut current = prov(1);
    current.epoch = 2;
    assert_applied(
        &core.execute_fts_index_doc(
            &task,
            TID,
            "notes",
            Surrogate::new(5),
            &body("hello"),
            Some(&current),
        ),
        1,
    );
    let response = core.execute_fts_index_doc(
        &task,
        TID,
        "notes",
        Surrogate::ZERO,
        &body("hello"),
        Some(&prov(2)),
    );
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).unwrap();
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Fenced));
    assert_eq!(ack.applied_seq, 1);
    assert_eq!(core.sync_hwm_value(7, 42), 1);
    assert_eq!(doc_count(&core), 1);
}
