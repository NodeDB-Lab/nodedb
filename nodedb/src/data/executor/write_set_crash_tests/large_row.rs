// SPDX-License-Identifier: BUSL-1.1

//! A document update whose write set outgrows the WAL write buffer.
//!
//! A write-set part carries several copies of the row, so a large row makes
//! one WAL record larger than the default 2 MiB write buffer. The record
//! journals, settles at boot, and replays after a restart with the row intact.

use super::*;

/// A note of 1 MiB made of `fill`.
fn large_note(fill: char) -> String {
    std::iter::repeat_n(fill, 1 << 20).collect()
}

/// Whether `records` hold a record larger than the default write buffer.
fn holds_oversize_record(records: &[WalRecord]) -> bool {
    records
        .iter()
        .any(|record| record.payload.len() > nodedb_wal::writer::DEFAULT_WRITE_BUFFER_SIZE)
}

/// A live 1 MiB update journals its parts, and a restart on the WAL and the
/// stores replays the updated row.
#[test]
fn a_large_document_update_journals_and_survives_a_restart() {
    let mut live = Live::open();
    let new = large_note('b');
    live.committed(put(&large_note('a')));
    live.committed(update(&new));

    let wal = live.wal_image();
    let stores = live.crash_image();
    drop(live);
    let reopened = WalManager::open_for_testing(&wal.path().join("wal")).expect("reopen wal");
    let records = settle(&reopened, stores.path());
    assert!(
        holds_oversize_record(&records),
        "the update journals a record larger than the write buffer"
    );
    assert!(
        membership(&records).broken_through(u64::MAX).is_empty(),
        "every group is whole"
    );
    assert_views_agree(stores.path(), &records, &new);
}

/// A crash after the 1 MiB update stores its write set and before its parts
/// are journalled. Boot journals the parts from the stored write set, and a
/// later restart replays the updated row.
#[test]
fn a_large_document_update_settles_from_its_stored_write_set() {
    let mut live = Live::open();
    let new = large_note('d');
    live.committed(put(&large_note('c')));
    let origin = live.journalled_until_crash(update(&new), true);
    let stores = live.crash_image();

    let records = settle(&live.wal, stores.path());
    assert!(
        !membership(&records)
            .broken_through(u64::MAX)
            .contains(&origin.lsn.as_u64()),
        "the update's group is whole"
    );
    assert!(
        holds_oversize_record(&records),
        "boot journals a record larger than the write buffer"
    );

    let wal = live.wal_image();
    drop(live);
    let reopened = WalManager::open_for_testing(&wal.path().join("wal")).expect("reopen wal");
    let records = reopened.replay().expect("replay");
    assert_views_agree(stores.path(), &records, &new);
}
