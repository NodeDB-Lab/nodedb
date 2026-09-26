// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for Event Plane CDC / change streams.
//!
//! Tests: stream buffer, consumer group offsets, partitioned consumption,
//! log compaction, exactly-once transactional offset commit.

use nodedb::event::cdc::CdcOffset;
use nodedb::event::cdc::buffer::StreamBuffer;
use nodedb::event::cdc::consumer_group::state::OffsetStore;
use nodedb::event::cdc::event::CdcEvent;
use nodedb::event::cdc::stream_def::{CompactionConfig, RetentionConfig};
use nodedb::types::DatabaseId;
use nodedb_test_support::{make_cdc_event, now_ms};

#[test]
fn stream_buffer_push_and_read() {
    let buf = StreamBuffer::new("orders_stream".to_string(), RetentionConfig::default());
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        1,
        0,
        "orders",
        "INSERT",
    ));
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        2,
        0,
        "orders",
        "UPDATE",
    ));
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        3,
        1,
        "orders",
        "INSERT",
    ));

    let all = buf.read_from(CdcOffset::ZERO, 100);
    assert_eq!(all.len(), 3);

    let p0 = buf.read_partition_from(0, CdcOffset::ZERO, 100);
    assert_eq!(p0.len(), 2);

    let p1 = buf.read_partition_from(1, CdcOffset::ZERO, 100);
    assert_eq!(p1.len(), 1);
}

#[test]
fn stream_buffer_retention_evicts_oldest() {
    let retention = RetentionConfig {
        max_events: 3,
        max_age_secs: 86_400,
    };
    let buf = StreamBuffer::new("orders_stream".to_string(), retention);

    for i in 1..=5 {
        buf.push(make_cdc_event(
            DatabaseId::DEFAULT,
            i,
            0,
            "orders",
            "INSERT",
        ));
    }

    let events = buf.read_from(CdcOffset::ZERO, 100);
    assert_eq!(events.len(), 3);
    // Oldest (seq 1, 2) evicted; remaining are 3, 4, 5.
    assert_eq!(events[0].sequence, 3);
}

#[test]
fn consumer_group_offset_tracking() {
    let dir = tempfile::tempdir().unwrap();
    let store = OffsetStore::open(dir.path()).unwrap();

    let database_id = DatabaseId::new(7);
    // Commit offsets for two partitions.
    store
        .commit_offset(database_id, 1, "orders_stream", "analytics", 0, 100)
        .unwrap();
    store
        .commit_offset(database_id, 1, "orders_stream", "analytics", 1, 200)
        .unwrap();

    // Read back.
    assert_eq!(
        store.get_offset(database_id, 1, "orders_stream", "analytics", 0),
        100
    );
    assert_eq!(
        store.get_offset(database_id, 1, "orders_stream", "analytics", 1),
        200
    );
    assert_eq!(
        store.get_offset(database_id, 1, "orders_stream", "analytics", 99),
        0
    ); // Unknown partition.

    // All offsets.
    let all = store.get_all_offsets(database_id, 1, "orders_stream", "analytics");
    assert_eq!(all.len(), 2);
}

#[test]
fn consumer_group_offset_advances_monotonically() {
    let dir = tempfile::tempdir().unwrap();
    let store = OffsetStore::open(dir.path()).unwrap();

    let database_id = DatabaseId::new(7);
    store
        .commit_offset(database_id, 1, "s", "g", 0, 100)
        .unwrap();
    store
        .commit_offset(database_id, 1, "s", "g", 0, 200)
        .unwrap();
    assert_eq!(store.get_offset(database_id, 1, "s", "g", 0), 200);

    // Offset persists across reopen.
    drop(store);
    let store2 = OffsetStore::open(dir.path()).unwrap();
    assert_eq!(store2.get_offset(database_id, 1, "s", "g", 0), 200);
}

#[test]
fn log_compaction_keeps_latest_per_key() {
    let config = CompactionConfig::key("id");
    let buf = StreamBuffer::new(
        "users_stream".to_string(),
        RetentionConfig {
            max_events: 1_000_000,
            max_age_secs: 86_400,
        },
    );

    // Two events for same row_id but different sequences.
    buf.push(CdcEvent {
        sequence: 1,
        partition: 0,
        collection: "users".into(),
        op: "INSERT".into(),
        row_id: "u-1".into(),
        event_time: now_ms(),
        lsn: 10,
        database_id: DatabaseId::new(7),
        tenant_id: 1,
        new_value: Some(serde_json::json!({"id": "u-1", "name": "Alice"})),
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: None,
        valid_time_ms: None,
        source: nodedb::event::EventSource::User,
    });
    buf.push(CdcEvent {
        sequence: 2,
        partition: 0,
        collection: "users".into(),
        op: "UPDATE".into(),
        row_id: "u-1".into(),
        event_time: now_ms(),
        lsn: 20,
        database_id: DatabaseId::new(7),
        tenant_id: 1,
        new_value: Some(serde_json::json!({"id": "u-1", "name": "Bob"})),
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: None,
        valid_time_ms: None,
        source: nodedb::event::EventSource::User,
    });

    // Before compaction: both events present.
    assert_eq!(buf.read_from(CdcOffset::ZERO, 100).len(), 2);

    // Compact.
    buf.compact(&config.key_field, config.tombstone_grace_secs);

    // After compaction: only latest event per row_id.
    let events = buf.read_from(CdcOffset::ZERO, 100);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 2);
    assert_eq!(events[0].op, "UPDATE");
}

#[test]
fn partitioned_read_isolates_vshards() {
    let buf = StreamBuffer::new("orders_stream".to_string(), RetentionConfig::default());

    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        1,
        0,
        "orders",
        "INSERT",
    ));
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        2,
        1,
        "orders",
        "INSERT",
    ));
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        3,
        2,
        "orders",
        "INSERT",
    ));
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        4,
        0,
        "orders",
        "UPDATE",
    ));

    // Each partition only sees its own events.
    assert_eq!(buf.read_partition_from(0, CdcOffset::ZERO, 100).len(), 2);
    assert_eq!(buf.read_partition_from(1, CdcOffset::ZERO, 100).len(), 1);
    assert_eq!(buf.read_partition_from(2, CdcOffset::ZERO, 100).len(), 1);
    assert_eq!(buf.read_partition_from(99, CdcOffset::ZERO, 100).len(), 0);
}
