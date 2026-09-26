// SPDX-License-Identifier: BUSL-1.1

//! Events rebuilt from the WAL carry the source their write ran with.
//!
//! The Event Plane rebuilds events from the WAL after its ring overflows or
//! after a crash. Each row-write record stores its event source, so a
//! restored, trigger, CRDT-sync or deferred row replays with that source. A
//! restored row therefore fires no AFTER trigger on replay, as on its live
//! write.

use nodedb::event::EventSource;
use nodedb::event::wal_replay::replay_wal_to_events;
use nodedb::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb::wal::manager::{NO_APPLY_KEY, WalManager};

fn document_put(id: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&("orders", id, b"row".to_vec())).expect("encode put")
}

#[test]
fn a_replayed_row_carries_the_source_of_its_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    let sources = [
        EventSource::Restore,
        EventSource::Trigger,
        EventSource::CrdtSync,
        EventSource::Deferred,
    ];
    for (i, source) in sources.iter().enumerate() {
        wal.appender(NO_APPLY_KEY)
            .with_event_source(*source)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &document_put(&format!("row-{i}")),
            )
            .expect("append");
    }
    wal.sync().expect("sync");

    let events = replay_wal_to_events(&wal, Lsn::new(0), 0, 1, 0).expect("replay");
    let replayed: Vec<EventSource> = events.iter().map(|event| event.source).collect();
    assert_eq!(replayed, sources.to_vec());
}

#[test]
fn a_row_write_without_a_source_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    let appended = wal.appender(NO_APPLY_KEY).append_put(
        TenantId::new(1),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        &document_put("row"),
    );
    assert!(
        appended.is_err(),
        "a row-write record must name the source of its write"
    );
}
