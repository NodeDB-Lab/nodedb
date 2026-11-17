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

/// Register `notes` with full-text indexes on `title` and `body`.
fn declare_notes(core: &mut CoreLoop) {
    let mut config = crate::engine::document::store::CollectionConfig::new("notes");
    config.text_fields = nodedb_physical::physical_plan::TextFieldSet::all_readable(vec![
        "body".to_string(),
        "title".to_string(),
    ]);
    core.doc_configs.insert(
        (
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::TenantId::new(TID),
            "notes".to_string(),
        ),
        config,
    );
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
    declare_notes(&mut core);
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
    declare_notes(&mut core);
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
    declare_notes(&mut core);
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
    declare_notes(&mut core);
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
    declare_notes(&mut core);
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

/// A collection with no declared text field indexes nothing from a frame.
/// The frame still commits its sequence.
#[test]
fn a_frame_for_an_undeclared_collection_indexes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let response = core.execute_fts_index_doc(
        &task,
        TID,
        "notes",
        Surrogate::new(5),
        &body("hello"),
        Some(&prov(1)),
    );
    assert_applied(&response, 1);
    assert_eq!(core.sync_hwm_value(7, 42), 1);
    assert_eq!(doc_count(&core), 0);
}

/// A frame field the collection does not declare is dropped.
#[test]
fn a_frame_drops_an_undeclared_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    declare_notes(&mut core);
    let task = make_default_task();
    let doc = vec![
        ("body".to_string(), "guide".to_string()),
        ("secret".to_string(), "hidden".to_string()),
    ];
    let response = core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &doc, None);
    assert_eq!(response.status, Status::Ok);
    assert_eq!(hits(&core, "notes".into(), "guide"), vec![5]);
    assert!(hits(&core, "notes".into(), "hidden").is_empty());
}

/// The ordinal and fields of every stored frame version of `notes`.
fn stored_frames(core: &CoreLoop) -> Vec<(u32, i64, usize)> {
    let mut frames = Vec::new();
    core.inverted
        .for_each_synced_text(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            nodedb_types::TenantId::new(TID),
            Some("notes"),
            |frame| {
                frames.push((frame.surrogate.as_u32(), frame.ordinal, frame.fields.len()));
                Ok(())
            },
        )
        .expect("read stored frames");
    frames
}

/// A re-issued frame version installs at its source ordinal. It indexes a
/// row the document store holds no version of. A row with a stored body
/// keeps the body's text, and the frame is only stored.
#[test]
fn a_frame_at_a_sys_from_installs_at_that_ordinal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    declare_notes(&mut core);
    let task = make_default_task();
    let db = nodedb_types::DatabaseId::DEFAULT.as_u64();
    let bodied = nodedb_types::StorageKey::for_surrogate(Surrogate::new(6));
    core.sparse
        .put(db, TID, "notes", &bodied, &[0x80], 3)
        .expect("store a body for row 6");
    assert!(
        core.sparse
            .versioned_latest_sys_from(db, TID, "notes", &bodied)
            .expect("read row 6")
            .is_some()
    );

    for surrogate in [Surrogate::new(5), Surrogate::new(6)] {
        let frame = SyncedFrame {
            collection: "notes",
            surrogate,
            fields: &body("restored"),
            sys_from: Some(10),
        };
        let response = core.execute_fts_index_doc_at(&task, TID, frame, None);
        assert_eq!(response.status, Status::Ok);
    }
    let response = core.execute_fts_delete_doc_at(
        &task,
        TID,
        "notes",
        Some(Surrogate::new(5)),
        None,
        Some(20),
    );
    assert_eq!(response.status, Status::Ok);

    assert_eq!(
        stored_frames(&core),
        vec![(5, 10, 1), (5, 20, 0), (6, 10, 1)],
        "every version is stored at its own ordinal"
    );
    assert!(
        hits(&core, "notes".into(), "restored").is_empty(),
        "row 5 ended at 20 and row 6 keeps its body's text"
    );
    let at_15 = core
        .inverted
        .text_cut_view(db, nodedb_types::TenantId::new(TID), "notes".into(), 15)
        .expect("cut view");
    let readers: Vec<u32> = at_15.docs.iter().map(|d| d.doc_id.as_u32()).collect();
    assert_eq!(
        readers,
        vec![5],
        "only row 5's frame text was indexed, from 10 to 20"
    );
}

/// The number of stored document versions of `notes`.
fn document_versions(core: &CoreLoop) -> usize {
    let mut versions = 0usize;
    core.sparse
        .versioned_for_each_version(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            TID,
            "notes",
            |_, _, _| {
                versions += 1;
                Ok(())
            },
        )
        .expect("read document versions");
    versions
}

/// Frames follow the version rule of the collection's documents. Every
/// sync stores a version, as every document write does. The history purge
/// at a non-bitemporal collection's horizon keeps one frame per row, as it
/// keeps one document version per row.
#[test]
fn a_non_bitemporal_collection_keeps_one_frame_per_row_after_the_purge() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    declare_notes(&mut core);
    let task = make_default_task();
    let db = nodedb_types::DatabaseId::DEFAULT.as_u64();
    let bodied = nodedb_types::StorageKey::for_surrogate(Surrogate::new(9));
    for ordinal in [10, 20, 30] {
        for surrogate in [Surrogate::new(5), Surrogate::new(6)] {
            let text = body(&format!("version {ordinal}"));
            let frame = SyncedFrame {
                collection: "notes",
                surrogate,
                fields: &text,
                sys_from: Some(ordinal),
            };
            let response = core.execute_fts_index_doc_at(&task, TID, frame, None);
            assert_eq!(response.status, Status::Ok);
        }
        core.sparse
            .put(db, TID, "notes", &bodied, &[0x80], ordinal)
            .expect("store a document version");
    }
    assert_eq!(stored_frames(&core).len(), 6, "every sync stores a version");
    assert_eq!(document_versions(&core), 3, "every write stores a version");

    let horizon = 40;
    core.inverted
        .purge_text_versions_below(horizon)
        .expect("purge text history");
    core.sparse
        .purge_document_versions_below(db, TID, "notes", horizon)
        .expect("purge document history");

    assert_eq!(stored_frames(&core), vec![(5, 30, 1), (6, 30, 1)]);
    assert_eq!(document_versions(&core), 1);
}

/// Two live frames of one row under one Calvin ordinal store two versions.
/// The second sorts above the first and holds the current text.
#[test]
fn two_live_frames_of_one_row_under_one_ordinal_both_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    declare_notes(&mut core);
    let task = make_default_task();
    core.apply_scope.calvin_txn_ordinal = Some(100);
    for text in ["first", "second"] {
        let response =
            core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &body(text), None);
        assert_eq!(response.status, Status::Ok);
    }
    core.apply_scope.calvin_txn_ordinal = None;

    assert_eq!(
        stored_frames(&core),
        vec![(5, 100, 1), (5, 101, 1)],
        "the second frame stores above the first"
    );
    assert_eq!(hits(&core, "notes".into(), "second"), vec![5]);
    assert!(hits(&core, "notes".into(), "first").is_empty());
    let at_100 = core
        .inverted
        .text_cut_view(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            nodedb_types::TenantId::new(TID),
            "notes".into(),
            100,
        )
        .expect("cut view");
    let readers: Vec<u32> = at_100.docs.iter().map(|d| d.doc_id.as_u32()).collect();
    assert_eq!(readers, vec![5], "the first frame's text reads at 100");
}

/// A live frame whose ordinal sits below a restored head stores above the
/// head and holds the current text. Its rollback removes it.
#[test]
fn a_rolled_back_live_frame_below_a_restored_head_is_gone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    declare_notes(&mut core);
    let task = make_default_task();
    let restored = SyncedFrame {
        collection: "notes",
        surrogate: Surrogate::new(5),
        fields: &body("restored"),
        sys_from: Some(1_000),
    };
    let response = core.execute_fts_index_doc_at(&task, TID, restored, None);
    assert_eq!(response.status, Status::Ok);

    let undo = core
        .capture_fts_doc_undo(
            nodedb_types::DatabaseId::DEFAULT,
            TID,
            "notes",
            Surrogate::new(5),
            None,
            None,
        )
        .expect("capture undo");
    core.apply_scope.calvin_txn_ordinal = Some(100);
    let response =
        core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &body("live"), None);
    core.apply_scope.calvin_txn_ordinal = None;
    assert_eq!(response.status, Status::Ok);
    assert_eq!(
        stored_frames(&core),
        vec![(5, 1_000, 1), (5, 1_001, 1)],
        "the live frame stores above the restored head"
    );
    assert_eq!(hits(&core, "notes".into(), "live"), vec![5]);

    core.rollback_undo_log(nodedb_types::DatabaseId::DEFAULT.as_u64(), TID, undo)
        .expect("rollback");
    assert_eq!(
        stored_frames(&core),
        vec![(5, 1_000, 1)],
        "the rollback removes the live frame"
    );
    assert!(hits(&core, "notes".into(), "live").is_empty());
    assert_eq!(hits(&core, "notes".into(), "restored"), vec![5]);
}
