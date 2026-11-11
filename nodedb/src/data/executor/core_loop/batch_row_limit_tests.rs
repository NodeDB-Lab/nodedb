// SPDX-License-Identifier: BUSL-1.1

//! A coalesced put whose event image is over the row image limit fails that
//! put alone. Its siblings commit.

use super::tests::{captures, ordinary_group, ordinary_task};
use super::*;
use crate::bridge::envelope::{ErrorCode, RowEffect};
use crate::data::executor::core_loop::tests::make_core_with_dir;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::{MAX_ROW_IMAGE_BYTES, StorageKey, Surrogate};

/// A plain MessagePack body `{"value": ...}` of `len` bytes.
fn body_of_len(len: usize) -> Vec<u8> {
    // A one-entry map header, the `fixstr` key `value` and a `str 32` header.
    let text = len - (1 + 6 + 5);
    let mut body = Vec::with_capacity(len);
    body.push(0x81);
    body.push(0xa5);
    body.extend_from_slice(b"value");
    body.push(0xdb);
    body.extend_from_slice(&u32::try_from(text).expect("text length").to_be_bytes());
    body.resize(len, b'v');
    body
}

fn stored(core: &CoreLoop, surrogate: u32) -> Option<Vec<u8>> {
    core.sparse
        .get(
            0,
            1,
            "ordinary_rows",
            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
        )
        .expect("read row")
}

#[test]
fn a_put_whose_event_image_is_over_the_limit_fails_alone() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
    for id in [1, 2, 3] {
        let mut task = ordinary_task(id);
        if id == 2 {
            // Within the limit as sent and as stored. The event image gains
            // the row's identity and passes the limit.
            let PhysicalPlan::Document(DocumentOp::PointPut { value, .. }) = &mut task.request.plan
            else {
                panic!("ordinary fixture must be a PointPut");
            };
            *value = body_of_len(MAX_ROW_IMAGE_BYTES - 4);
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

    assert_eq!(core.poll_write_batch(), 1, "only the refused put answers");
    let refused = responses.try_pop().expect("refusal").inner;
    assert_eq!(refused.request_id.as_u64(), 2);
    assert_eq!(refused.status, Status::Error);
    assert!(
        matches!(
            refused.error_code.as_deref(),
            Some(ErrorCode::ProgramLimitExceeded { .. })
        ),
        "{:?}",
        refused.error_code
    );
    assert!(responses.try_pop().is_err(), "the siblings run again");

    assert_eq!(core.poll_write_batch(), 2, "the siblings commit together");
    for id in [1, 3] {
        let response = responses.try_pop().expect("response").inner;
        assert_eq!(response.request_id.as_u64(), id);
        assert_eq!(response.status, Status::Ok, "{response:?}");
    }
    assert!(stored(&core, 1).is_some());
    assert!(stored(&core, 2).is_none(), "the refused put stores no row");
    assert!(stored(&core, 3).is_some());
    let refused_rows = captures(&core)
        .into_iter()
        .filter(|capture| capture.origin == 2)
        .flat_map(|capture| capture.write_set().expect("capture entries"))
        .filter(|entry| matches!(entry.effect, RowEffect::InstalledPut { .. }))
        .count();
    assert_eq!(refused_rows, 0, "the refused put journals no row");
}
