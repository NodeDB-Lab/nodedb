// SPDX-License-Identifier: BUSL-1.1

//! A document row whose largest image is exactly the row image limit.
//!
//! The write is accepted, journals its parts, settles at boot and replays
//! after a restart with the row intact.

use nodedb_types::MAX_ROW_IMAGE_BYTES;

use super::*;
use crate::bridge::envelope::RowEffect;
use crate::types::Lsn;

/// The bytes a put of a note spends beyond the note, in its largest image.
/// Past 64 KiB of note every image grows byte for byte.
fn largest_image_overhead() -> usize {
    const FILL: usize = 100_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    let origin = GroupOrigin { lsn: Lsn::new(1) };
    let response = core.execute(&ExecutionTask::new(request(put(&"x".repeat(FILL)), origin)));
    assert_eq!(response.status, Status::Ok, "{response:?}");
    let largest = response
        .write_set
        .iter()
        .find_map(|entry| match &entry.effect {
            RowEffect::InstalledPut { image, event } => Some(
                image
                    .canonical
                    .len()
                    .max(image.stored.len())
                    .max(event.new_value.len()),
            ),
            _ => None,
        })
        .expect("installed row");
    largest - FILL
}

/// A put whose largest image is exactly the limit is accepted, and a
/// restart on the WAL and the stores replays it.
#[test]
fn a_row_at_the_limit_journals_and_survives_a_restart() {
    let note = "r".repeat(MAX_ROW_IMAGE_BYTES - largest_image_overhead());
    let mut live = Live::open();
    live.committed(put(&note));

    let wal = live.wal_image();
    let stores = live.crash_image();
    drop(live);
    let reopened = WalManager::open_for_testing(&wal.path().join("wal")).expect("reopen wal");
    let records = settle(&reopened, stores.path());
    assert!(
        membership(&records).broken_through(u64::MAX).is_empty(),
        "every group is whole"
    );
    assert_views_agree(stores.path(), &records, &note);
}

/// A crash after the put at the limit stores its write set and before its
/// parts are journalled. Boot journals the parts, and a restart replays the
/// row.
#[test]
fn a_row_at_the_limit_settles_from_its_stored_write_set() {
    let note = "s".repeat(MAX_ROW_IMAGE_BYTES - largest_image_overhead());
    let mut live = Live::open();
    let origin = live.journalled_until_crash(put(&note), true);
    let stores = live.crash_image();

    let records = settle(&live.wal, stores.path());
    assert!(
        !membership(&records)
            .broken_through(u64::MAX)
            .contains(&origin.lsn.as_u64()),
        "the put's group is whole"
    );
    let wal = live.wal_image();
    drop(live);
    let reopened = WalManager::open_for_testing(&wal.path().join("wal")).expect("reopen wal");
    let records = reopened.replay().expect("replay");
    assert_views_agree(stores.path(), &records, &note);
}
