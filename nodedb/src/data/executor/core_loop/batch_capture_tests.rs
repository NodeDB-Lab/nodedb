// SPDX-License-Identifier: BUSL-1.1

//! Each committed batched put captures its own image and actual version key.

use super::tests::{captures, ordinary_group, ordinary_task};
use super::*;
use crate::bridge::envelope::RowEffect;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::doc_format;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::{StorageKey, Surrogate};

#[test]
fn shared_batches_capture_each_distinct_and_repeated_rows_own_version() {
    for repeated in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
        for id in [1, 2] {
            let mut task = ordinary_task(id);
            if repeated && id == 2 {
                let PhysicalPlan::Document(DocumentOp::PointPut {
                    document_id,
                    surrogate,
                    ..
                }) = &mut task.request.plan
                else {
                    panic!("ordinary fixture must be a PointPut");
                };
                *document_id = "row-1".into();
                *surrogate = Surrogate::new(1);
            }
            core.keep_write_set_journal(
                id,
                WriteSetJournal {
                    group: Some(ordinary_group(id)),
                    ..WriteSetJournal::default()
                },
            );
            core.task_queue.push(task);
        }
        assert_eq!(core.poll_write_batch(), 2);
        let stored = captures(&core);
        assert_eq!(stored.len(), 2);
        let mut actual_versions = std::collections::BTreeSet::new();
        core.sparse
            .versioned_for_each_version(0, 1, "ordinary_rows", |key, ordinal, body| {
                assert!(body.is_some());
                actual_versions.insert((key.surrogate().as_u32(), ordinal));
                Ok(())
            })
            .expect("read actual versions");
        assert_eq!(actual_versions.len(), 2);
        let mut previous_ordinal = None;
        for (id, capture) in [1, 2].into_iter().zip(stored) {
            let response = responses.try_pop().expect("response").inner;
            assert_eq!(response.status, Status::Ok);
            assert_eq!(response.request_id.as_u64(), id);
            assert_eq!(capture.origin, id);
            assert_eq!(response.write_set.len(), 2);
            assert_eq!(capture.entries.len(), 2);
            let entries = capture.write_set().expect("capture entries");
            for (reported, captured) in response.write_set.iter().zip(&entries) {
                assert_eq!(reported.effect, captured.effect);
                assert_eq!(reported.identity, captured.identity);
                assert_eq!(reported.surrogate, captured.surrogate);
            }
            let image = &entries[0];
            let RowEffect::InstalledPut {
                image: installed,
                event,
            } = &image.effect
            else {
                panic!("first entry must be the stamped put image");
            };
            let version = installed.version;
            assert!(matches!(entries[1].effect, RowEffect::CancelForward));
            let surrogate = if repeated { 1 } else { id as u32 };
            assert_eq!(image.surrogate, surrogate);
            assert_eq!(image.identity.as_str(), format!("row-{surrogate}"));
            assert_eq!(entries[1].identity, image.identity);
            assert_eq!(entries[1].surrogate, image.surrogate);
            assert!(actual_versions.contains(&(surrogate, version.sys_from)));
            if let Some(previous) = previous_ordinal {
                assert!(version.sys_from > previous);
            }
            previous_ordinal = Some(version.sys_from);
            let canonical =
                doc_format::decode_document(&installed.canonical).expect("decode canonical image");
            assert_eq!(canonical["value"].as_u64(), Some(id));
            let body = core
                .sparse
                .versioned_get_as_of(
                    0,
                    1,
                    "ordinary_rows",
                    &StorageKey::for_surrogate(Surrogate::new(surrogate)),
                    Some(version.sys_from),
                    None,
                )
                .expect("read version")
                .expect("version exists");
            let actual = doc_format::decode_document(&body).expect("decode stored version");
            assert_eq!(body, installed.stored);
            assert_eq!(actual, canonical);
            // The public event image is the stored row plus the identity a
            // schemaless read injects under the identity column.
            let event_value =
                doc_format::decode_document(&event.new_value).expect("decode event image");
            let mut expected_event = actual.clone();
            expected_event
                .as_object_mut()
                .expect("stored row is an object")
                .insert(
                    nodedb_types::DEFAULT_IDENTITY_COLUMN.to_string(),
                    serde_json::Value::String(image.identity.as_str().to_string()),
                );
            assert_eq!(event_value, expected_event);
            if repeated && id == 2 {
                let prior = doc_format::decode_document(
                    event.old_value.as_deref().expect("prior row image"),
                )
                .expect("decode prior image");
                assert_eq!(prior["value"].as_u64(), Some(1));
            } else {
                assert!(event.old_value.is_none());
            }
        }
    }
}
